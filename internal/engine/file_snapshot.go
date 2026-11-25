package engine

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
)

// summaryRelations are the rollup summary tables a snapshot read can name.
var summaryRelations = []string{"receiver_rollup", "receiver_points", "resource_rollup", "receiver_entity_rollup"}

type snapshotSQL interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func batchTime(b telemetry.BatchMetadata, signal string) (telemetry.TimeRange, int) {
	s, _ := telemetry.LookupSignal(signal)
	return b.Signal(s)
}
func overlapping(b telemetry.BatchMetadata, signal string, w queryrows.Window) bool {
	bounds, count := batchTime(b, signal)
	return count > 0 && (!bounds.Known || bounds.MaxNanos >= w.Start.UnixNano() && bounds.MinNanos < w.End.UnixNano())
}

// signalWindowBounded reports whether a windowed column from a trusted SQL
// panel bounds this signal's own event-time clock, so that pruning files to the
// read window is sound. A bare column must equal the clock; a qualified column
// must name the signal's own relation (alias qualifiers we cannot resolve stay
// unbounded, which binds every file — correct, if slower). Columns come only
// from the validated macro expansion, never from unvalidated statement text.
func signalWindowBounded(columns []string, s *telemetry.Signal) bool {
	clock := s.Clock()
	for _, column := range columns {
		if dot := strings.LastIndexByte(column, '.'); dot >= 0 {
			if column[dot+1:] == clock && strings.EqualFold(column[:dot], s.Name) {
				return true
			}
			continue
		}
		if column == clock {
			return true
		}
	}
	return false
}

// physicalSource enumerates the captured files even for broad selections.
// A glob plus filename IN becomes expensive at larger file counts. The caller's
// pin keeps these exact immutable paths available through row consumption.
func (d *Duck) physicalSource(signal string, batches []telemetry.BatchMetadata) string {
	s, _ := telemetry.LookupSignal(signal)
	sort.SliceStable(batches, func(i, j int) bool {
		a, _ := batches[i].Signal(s)
		b, _ := batches[j].Signal(s)
		if a.MaxNanos != b.MaxNanos {
			return a.MaxNanos > b.MaxNanos
		}
		return batches[i].ID < batches[j].ID
	})
	store := d.repository.Parquet
	files := make([]string, 0, len(batches))
	for _, b := range batches {
		files = append(files, sqlLiteral(filepath.ToSlash(filepath.Join(store.BatchPath(b.ID), signal+".parquet"))))
	}
	if len(files) == 0 {
		files = append(files, sqlLiteral(filepath.ToSlash(filepath.Join(store.BatchesDir(), telemetry.SchemaBatch, signal+".parquet"))))
	}
	return "SELECT " + rawProjection(s) + " FROM read_parquet([" + strings.Join(files, ",") + "], union_by_name=false, hive_partitioning=false)"
}

func (d *Duck) batchIDColumn(signal, filename string) string {
	root := filepath.ToSlash(d.repository.Parquet.BatchesDir())
	suffix := ".batch/" + signal + ".parquet"
	return fmt.Sprintf("substr(%s,%d,length(%s)-%d)", filename, utf8.RuneCountInString(root)+2, filename, utf8.RuneCountInString(root)+1+len(suffix))
}

func cleanSource(signal, physical string) string {
	return cleanSelect(signal, "("+physical+")")
}

// parquetGlob lists the schema batch first for stable view binding.
func parquetGlob(parquetDir, signal string) string {
	batches := filepath.Join(parquetDir, "batches")
	return "[" + sqlLiteral(filepath.ToSlash(filepath.Join(batches, telemetry.SchemaBatch, signal+".parquet"))) + "," +
		sqlLiteral(filepath.ToSlash(filepath.Join(batches, "*"+telemetry.BatchSuffix, signal+".parquet"))) + "]"
}

// bindRollupSnapshot shadows the clean signal views on tx's connection with
// the catalog captured now, under the caller's pin. Rollup statements name the
// bare relations, and the views' glob would also match inputs a publication
// retired but pinned readers still use. The returned func drops the views
// before commit; a rollback drops them with the transaction.
func (d *Duck) bindRollupSnapshot(ctx context.Context, tx *sql.Tx) (func() error, error) {
	if d.repository == nil || d.repository.Parquet == nil {
		return func() error { return nil }, nil
	}
	return d.bindRollupSnapshotFiles(ctx, tx, d.repository.Parquet.BatchMetadata(), nil)
}

// Discovery binds the full captured catalog. Reaggregation may narrow that
// same catalog to its known event-time interval; the caller retains the pin
// and transaction throughout both bindings. Each signal uses its own footer.
func (d *Duck) bindRollupSnapshotFiles(ctx context.Context, tx *sql.Tx, batches []telemetry.BatchMetadata, window *queryrows.Window) (func() error, error) {
	if d.repository == nil || d.repository.Parquet == nil {
		return func() error { return nil }, nil
	}
	for _, s := range telemetry.Signals {
		selected := make([]telemetry.BatchMetadata, 0, len(batches))
		for _, b := range batches {
			if _, n := b.Signal(s); n > 0 && (window == nil || overlapping(b, s.Name, *window)) {
				selected = append(selected, b)
			}
		}
		physical := d.physicalSource(s.Name, selected)
		if _, err := tx.ExecContext(ctx, "CREATE OR REPLACE TEMP VIEW "+s.Name+" AS "+cleanSource(s.Name, physical)); err != nil {
			return nil, err
		}
	}
	return func() error {
		for _, s := range telemetry.Signals {
			if _, err := tx.ExecContext(ctx, "DROP VIEW IF EXISTS temp.main."+s.Name); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

// bindSnapshot rewrites table references through DuckDB's own AST. The caller
// holds the publication lease and a read transaction: batch markers, aggregate
// rows and immutable files all belong to the same snapshot. No session views
// survive a pooled connection, and parameter indices are preserved by the AST.
func (d *Duck) bindSnapshot(ctx context.Context, db snapshotSQL, query string, w queryrows.Window) (string, error) {
	compiled, err := d.compileSnapshot(ctx, db, query)
	if err != nil {
		return "", err
	}
	batches := d.repository.Parquet.BatchMetadata()
	sources := map[string]string{}
	summaries := false
	for _, name := range summaryRelations {
		if _, used := compiled.names[name]; used {
			sources[name] = "SELECT * FROM " + name
			summaries = true
		}
	}
	if summaries {
		var horizon sql.NullInt64
		if err := db.QueryRowContext(ctx, "SELECT last_ingested_unix_nano FROM rollup_state WHERE cache_key=?", serviceRollupStateKey).Scan(&horizon); err != nil && err != sql.ErrNoRows {
			return "", err
		}
		fresh := time.Now()
		if horizon.Valid {
			fresh = time.Unix(0, horizon.Int64)
		} else {
			// A cold cache is complete only before the oldest captured ingestion.
			for _, batch := range batches {
				fresh = minTime(fresh, time.Unix(0, batch.MinIngestedNanos).Add(-time.Nanosecond))
			}
		}
		queryrows.RecordCoverage(ctx, fresh)
	}
	windowColumns, scopedColumns := queryrows.ReadWindowColumns(ctx)
	for _, s := range telemetry.Signals {
		signal := s.Name
		if _, raw := compiled.names["telemetry."+signal]; !raw {
			if _, clean := compiled.names[signal]; !clean {
				continue
			}
		}
		// A raw signal is event-time pruned only when the statement actually
		// bounds its own clock with the window. When a trusted SQL panel windows
		// on another column (or joins this signal unwindowed), pruning by the
		// clock would drop rows the statement keeps, so bind every captured file.
		selectWindow := w
		if scopedColumns && !signalWindowBounded(windowColumns, s) {
			selectWindow = allTime
		}
		selected := []telemetry.BatchMetadata{}
		for _, b := range batches {
			if overlapping(b, signal, selectWindow) {
				selected = append(selected, b)
			}
		}
		// Only an explicit internal window may prune. Public arbitrary SQL
		// uses allTime and always binds every captured file. Metric-name
		// pruning likewise requires the metrics signal to be genuinely
		// window-bounded.
		if signal == "metrics" && len(compiled.metricNames) > 0 && selectWindow != allTime {
			var err error
			selected, err = pruneMetricNames(ctx, db, selected, compiled.metricNames)
			if err != nil {
				return "", err
			}
		}
		physical := d.physicalSource(signal, selected)
		sources["telemetry."+signal] = physical
		sources[signal] = cleanSource(signal, physical)
	}
	_, inventory := compiled.names["resource_inventory"]
	_, traces := compiled.names["trace_exemplars"]
	traces = traces && w.Kind != queryrows.RawRead
	if traces || inventory {
		if err := d.aggregateSources(ctx, db, batches, w, inventory, traces, sources); err != nil {
			return "", err
		}
	}
	return bindCompiledSnapshot(compiled, sources)
}
func parseSQLTree(ctx context.Context, db snapshotSQL, query string) (map[string]any, error) {
	var serialized string
	if err := db.QueryRowContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", query).Scan(&serialized); err != nil {
		return nil, err
	}
	var root map[string]any
	decoder := json.NewDecoder(strings.NewReader(serialized))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, err
	}
	if root["error"] == true {
		return nil, fmt.Errorf("parse trusted SQL: %v", root["error_message"])
	}
	return root, nil
}

// compileRelations replaces the relations in sources and rewrites standard
// key subscripts (rewriteKeys).
func compileRelations(ctx context.Context, db snapshotSQL, query string, sources map[string]string, used map[string]string) (string, error) {
	root, err := parseSQLTree(ctx, db, query)
	if err != nil {
		return "", err
	}
	// Conservatively retain unqualified references shadowed anywhere by a CTE.
	// Qualified telemetry references always refer to the physical relation.
	shadowed := map[string]bool{}
	var ctes func(any)
	ctes = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			if entries, ok := v["cte_map"].(map[string]any); ok {
				if list, ok := entries["map"].([]any); ok {
					for _, entry := range list {
						if item, ok := entry.(map[string]any); ok {
							if name, ok := item["key"].(string); ok {
								shadowed[strings.ToLower(name)] = true
							}
						}
					}
				}
			}
			for _, child := range v {
				ctes(child)
			}
		case []any:
			for _, child := range v {
				ctes(child)
			}
		}
	}
	ctes(root)
	for name := range shadowed {
		if strings.HasPrefix(name, "__pitot_snapshot_") {
			return "", fmt.Errorf("reserved snapshot CTE name %s", name)
		}
	}
	// Resolve qualifiers in their original lexical scopes before table replacement.
	// Struct fields and aliases must retain the bindings used by DESCRIBE, and a
	// name a CTE shadows is never a physical relation.
	physical := make(map[string]string, len(sources))
	for key, body := range sources {
		if !shadowed[key] {
			physical[key] = body
		}
	}
	normalizeRelationQualifiers(root, physical)
	statements, _ := root["statements"].([]any)
	for _, statement := range statements {
		statement, _ := statement.(map[string]any)
		node, _ := statement["node"].(map[string]any)
		if err := rewriteKeys(ctx, db, node); err != nil {
			return "", err
		}
	}
	templates := map[string]map[string]any{}
	var walk func(any) error
	walk = func(value any) error {
		switch v := value.(type) {
		case map[string]any:
			if v["type"] == "BASE_TABLE" {
				catalog, _ := v["catalog_name"].(string)
				if catalog != "" {
					return nil
				}
				name, _ := v["table_name"].(string)
				schema, _ := v["schema_name"].(string)
				key := strings.ToLower(name)
				if schema != "" && schema != "main" {
					key = strings.ToLower(schema) + "." + key
				} else if shadowed[key] {
					return nil
				}
				if body, ok := sources[key]; ok {
					used[key] = strings.TrimPrefix(body, "SELECT * FROM ")
					template := templates[key]
					if template == nil {
						parsed, e := parseSQLTree(ctx, db, "SELECT * FROM ("+body+") AS snapshot_relation")
						if e != nil {
							return e
						}
						template = parsed["statements"].([]any)[0].(map[string]any)["node"].(map[string]any)["from_table"].(map[string]any)
						templates[key] = template
					}
					replacement := clone(template)
					alias, _ := v["alias"].(string)
					if alias == "" {
						alias = name
					}
					replacement["alias"] = alias
					for _, field := range []string{"sample", "column_name_alias", "query_location"} {
						if x, ok := v[field]; ok {
							replacement[field] = x
						}
					}
					for k := range v {
						delete(v, k)
					}
					for k, x := range replacement {
						v[k] = x
					}
					return nil
				}
			}
			for _, child := range v {
				if e := walk(child); e != nil {
					return e
				}
			}
		case []any:
			for _, child := range v {
				if e := walk(child); e != nil {
					return e
				}
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(root)
	if err != nil {
		return "", err
	}
	var rewritten string
	err = db.QueryRowContext(ctx, "SELECT json_deserialize_sql(?::JSON)::VARCHAR", string(encoded)).Scan(&rewritten)
	return rewritten, err
}

type compiledSnapshot struct {
	query       string
	names       map[string]string
	metricNames []string
}

// snapshotCacheCapacity bounds the compile cache. Reaching it evicts the single
// least-recently-used entry, so a burst of one-off statements cannot discard
// the dashboard's stable, repeatedly-run ones.
const snapshotCacheCapacity = 128

// snapshotCacheEntry is one cached compilation, held in the LRU list under its
// statement text.
type snapshotCacheEntry struct {
	key      string
	compiled compiledSnapshot
}

// cachedSnapshot returns a compilation for query and moves it to the front of
// the LRU when present.
func (d *Duck) cachedSnapshot(query string) (compiledSnapshot, bool) {
	d.snapshotMu.Lock()
	defer d.snapshotMu.Unlock()
	element, ok := d.snapshotQueries[query]
	if !ok {
		return compiledSnapshot{}, false
	}
	d.snapshotLRU.MoveToFront(element)
	return element.Value.(*snapshotCacheEntry).compiled, true
}

// storeSnapshot records compiled under query at the front of the LRU, evicting
// the least-recently-used entry once over capacity.
func (d *Duck) storeSnapshot(query string, compiled compiledSnapshot) {
	d.snapshotMu.Lock()
	defer d.snapshotMu.Unlock()
	if d.snapshotQueries == nil {
		d.snapshotQueries = map[string]*list.Element{}
		d.snapshotLRU = list.New()
	}
	if element, ok := d.snapshotQueries[query]; ok {
		element.Value.(*snapshotCacheEntry).compiled = compiled
		d.snapshotLRU.MoveToFront(element)
		return
	}
	d.snapshotQueries[query] = d.snapshotLRU.PushFront(&snapshotCacheEntry{key: query, compiled: compiled})
	for d.snapshotLRU.Len() > snapshotCacheCapacity {
		oldest := d.snapshotLRU.Back()
		d.snapshotLRU.Remove(oldest)
		delete(d.snapshotQueries, oldest.Value.(*snapshotCacheEntry).key)
	}
}

// compileSnapshot compiles relation aliases once with the native parser. Per-read
// scopes become ordinary CTEs, avoiding repeated JSON SQL parse/deserialize work.
// The window bounds ride as bind parameters, so a panel's statement text is
// identical across time windows and this cache hits instead of recompiling.
func (d *Duck) compileSnapshot(ctx context.Context, db snapshotSQL, query string) (compiledSnapshot, error) {
	if compiled, ok := d.cachedSnapshot(query); ok {
		d.snapshotHits.Add(1)
		return compiled, nil
	}
	d.snapshotMisses.Add(1)
	aliases := map[string]string{}
	names := append([]string{"trace_exemplars", "resource_inventory"}, summaryRelations...)
	for _, s := range telemetry.Signals {
		names = append(names, "telemetry."+s.Name, s.Name)
	}
	for _, name := range names {
		alias := "__pitot_snapshot_" + strings.ReplaceAll(name, ".", "_")
		aliases[name] = "SELECT * FROM " + alias
	}
	used := map[string]string{}
	rewritten, err := compileRelations(ctx, db, query, aliases, used)
	if err != nil {
		return compiledSnapshot{}, err
	}
	// The AST records only rewritten table references, never SQL literals.
	compiled := compiledSnapshot{query: rewritten, names: used}
	if _, ok := used["metrics"]; ok {
		root, err := parseSQLTree(ctx, db, query)
		if err != nil {
			return compiledSnapshot{}, err
		}
		compiled.metricNames = singleMetricNames(root)
	}
	d.storeSnapshot(query, compiled)
	return compiled, nil
}

func bindCompiledSnapshot(compiled compiledSnapshot, sources map[string]string) (string, error) {
	definitions := []string{}
	for name, alias := range compiled.names {
		body, ok := sources[name]
		if !ok {
			return "", fmt.Errorf("missing snapshot relation %s", name)
		}
		materialization := " AS ("
		if name == "trace_exemplars" || slices.Contains(summaryRelations, name) {
			// These sources can have several consumers. Inline them so their
			// time/key predicates reach scans instead of copying every retained
			// exemplar or receiver payload into a temporary relation.
			materialization = " AS NOT MATERIALIZED ("
		}
		definitions = append(definitions, alias+materialization+body+")")
	}
	if len(definitions) == 0 {
		return compiled.query, nil
	}
	sort.Strings(definitions)
	statement := strings.TrimSpace(compiled.query)
	upper := strings.ToUpper(statement)
	switch {
	case strings.HasPrefix(upper, "WITH RECURSIVE "):
		return "WITH RECURSIVE " + strings.Join(definitions, ",") + ", " + statement[len("WITH RECURSIVE "):], nil
	case strings.HasPrefix(upper, "WITH "):
		return "WITH " + strings.Join(definitions, ",") + ", " + statement[len("WITH "):], nil
	default:
		return "WITH " + strings.Join(definitions, ",") + " " + statement, nil
	}
}
