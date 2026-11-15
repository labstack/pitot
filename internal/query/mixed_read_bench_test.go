//go:build readbench

package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/metrics"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
	dto "github.com/prometheus/client_model/go"
)

// This deliberately small synthetic workload is an experiment, not production
// capacity evidence. One arrival is one executor request, not one panel or SQL.

const mixedReadClientWorkers = 32
const mixedReadClientQueue = 64

var mixedReadKinds = [...]string{"single_p95", "batch_four", "batch_twelve"}

type mixedReadJob struct {
	slot      int
	scheduled time.Time
}
type mixedReadKind struct {
	Offered, Missed, ClientQueueFull, Dispatched, Success, BackendBusy, Errors, DuringLoad, DuringDrain, CachedPanels int
	E2EMS                                                                                                             []float64 `json:"-"`
	QueueMS                                                                                                           []float64 `json:"-"`
	P50MS, P95MS, P99MS, QueueP95MS                                                                                   float64
	// P99Qualified reports sample sufficiency only, not a passed latency or loss gate.
	P99Qualified bool
}
type mixedReadReport struct {
	RSSMethod                                                 string
	ParquetMetadataCache                                      bool
	ObjectCacheBytes                                          int64
	ExpectedShutdownInterrupts, ExpectedShutdownCancellations int
	ResultCacheOutcomes                                       map[string]float64
	NativeQueryStatementCountDelta                            uint64
	DiagnosticCountersReset                                   bool

	SeedState, WindowStart, WindowEnd string
	InitialGenerationCounts           map[uint32]int

	IngestRowsPerSignalPerBatch, IngestCommitTargetPerSecond, IngestTargetItemsPerSecond int
	PeakActiveWorkers                                                                    int64
	GeneratorLimited                                                                     bool
	BackgroundErrors                                                                     []string
	MaintenanceInterval, MaintenanceMode, CachePreparation                               string

	OperationUnit                               string
	ArrivalIntervalNS                           float64
	EngineVersion, Platform, EffectiveMemory    string
	InitialActiveBatches, InitialCompactionDebt int
	InitialReadCacheMarkers                     int64
	InitialRollupLagMS                          float64
	SingleP95BudgetMS, BatchP95BudgetMS         float64

	Methodology                                                              string
	Rate                                                                     int
	DurationSeconds                                                          float64
	Scale, SeedSpans, SeedLogs, Threads, Connections, Workers, QueueCapacity int
	Memory, CacheTTL                                                         string
	CacheBypass                                                              bool
	Kinds                                                                    map[string]*mixedReadKind
	IngestAcceptedBatches, IngestAcceptedRows, IngestErrors                  int64
	IngestRowsPerSecond, PeakRSSMiB                                          float64
	RSSError                                                                 string
	ActiveBatches, CompactionDebt                                            int
	ReadCacheMarkers                                                         int64
	RollupLagMS                                                              float64
	Errors                                                                   []string
}

type mixedReadLogState struct {
	sync.Mutex
	failures                                  []string
	background                                context.Context
	stopping                                  atomic.Bool
	shutdownInterrupts, shutdownCancellations int
}
type mixedReadLogHandler struct {
	slog.Handler
	state *mixedReadLogState
	attrs []slog.Attr
}

// Classify the entire error tree so an interrupt joined with an unrelated
// failure is never excused by shutdown. Ordinary wrappers retain their cause.
func mixedReadShutdownError(err error) (expected, interrupt bool) {
	if native, ok := err.(*duckdb.Error); ok {
		return native.Type == duckdb.ErrorTypeInterrupt, native.Type == duckdb.ErrorTypeInterrupt
	}
	if err == context.Canceled {
		return true, false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false, false
		}
		for _, cause := range causes {
			allowed, native := mixedReadShutdownError(cause)
			if !allowed {
				return false, false
			}
			interrupt = interrupt || native
		}
		return true, interrupt
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return mixedReadShutdownError(wrapped.Unwrap())
	}
	return false, false
}

func (h mixedReadLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn || h.Handler.Enabled(ctx, level)
}
func (h mixedReadLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		stopping := h.state.stopping.Load() && h.state.background != nil && h.state.background.Err() == context.Canceled
		seenError, unexpected, interrupted := false, false, false
		details := ""
		inspect := func(a slog.Attr) bool {
			if e, ok := a.Value.Any().(error); ok {
				seenError = true
				expected, native := mixedReadShutdownError(e)
				if !stopping || !expected {
					unexpected = true
				}
				interrupted = interrupted || native
			}
			details += " " + a.Key + "=" + a.Value.String()
			return true
		}
		for _, a := range h.attrs {
			inspect(a)
		}
		r.Attrs(inspect)
		h.state.Lock()
		if seenError && !unexpected {
			if interrupted {
				h.state.shutdownInterrupts++
			} else {
				h.state.shutdownCancellations++
			}
		} else if len(h.state.failures) < 10 {
			h.state.failures = append(h.state.failures, r.Message+details)
		}
		h.state.Unlock()
	}
	if h.Handler.Enabled(ctx, r.Level) {
		return h.Handler.Handle(ctx, r)
	}
	return nil
}
func (h mixedReadLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return mixedReadLogHandler{Handler: h.Handler.WithAttrs(attrs), state: h.state, attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...)}
}
func (h mixedReadLogHandler) WithGroup(name string) slog.Handler {
	return mixedReadLogHandler{Handler: h.Handler.WithGroup(name), state: h.state, attrs: h.attrs}
}

// Never wrap slog.Default().Handler(): its built-in bridge calls log.Output,
// and SetDefault redirects that standard logger back into the new handler.
func mixedReadCaptureLogs(background context.Context) (*mixedReadLogState, func()) {
	oldLogger := slog.Default()
	oldWriter, oldFlags := log.Writer(), log.Flags()
	state := &mixedReadLogState{background: background}
	slog.SetDefault(slog.New(mixedReadLogHandler{Handler: slog.NewTextHandler(os.Stderr, nil), state: state}))
	return state, func() { slog.SetDefault(oldLogger); log.SetOutput(oldWriter); log.SetFlags(oldFlags) }
}

func mixedReadWarmCache(t *testing.T, d *engine.Duck, repo *telemetrystore.Repository) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	target := int64(len(repo.Parquet.BatchMetadata()))
	var before int64
	for {
		if _, err := d.RefreshReadCaches(ctx); err != nil {
			t.Fatal("completed read cache preparation:", err)
		}
		var marked int64
		if err := d.DB.QueryRowContext(ctx, "SELECT count(*) FROM read_batches").Scan(&marked); err != nil {
			t.Fatal(err)
		}
		if marked == target {
			return
		}
		if marked <= before || marked > target {
			t.Fatalf("completed read cache preparation made no progress: marked=%d target=%d", marked, target)
		}
		before = marked
	}
}

type mixedReadWorkerActivity struct{ active, peak atomic.Int64 }

func (a *mixedReadWorkerActivity) start() {
	current := a.active.Add(1)
	for previous := a.peak.Load(); current > previous; previous = a.peak.Load() {
		if a.peak.CompareAndSwap(previous, current) {
			break
		}
	}
}
func (a *mixedReadWorkerActivity) stop() { a.active.Add(-1) }

type mixedReadDiagnosticSnapshot struct {
	cache      map[string]float64
	statements uint64
}

func mixedReadDiagnostics() (mixedReadDiagnosticSnapshot, error) {
	snapshot := mixedReadDiagnosticSnapshot{cache: map[string]float64{}}
	for _, outcome := range []string{"hit", "stale", "miss", "coalesced"} {
		metric := &dto.Metric{}
		if err := metrics.QueryCacheRequests.WithLabelValues(outcome).Write(metric); err != nil {
			return snapshot, err
		}
		snapshot.cache[outcome] = metric.GetCounter().GetValue()
	}
	metric := &dto.Metric{}
	if err := metrics.DuckDBStatement.Write(metric); err != nil {
		return snapshot, err
	}
	snapshot.statements = metric.GetHistogram().GetSampleCount()
	return snapshot, nil
}
func mixedReadDiagnosticDelta(before, after mixedReadDiagnosticSnapshot) (map[string]float64, uint64, bool) {
	outcomes := map[string]float64{}
	reset := false
	for _, outcome := range []string{"hit", "stale", "miss", "coalesced"} {
		delta := after.cache[outcome] - before.cache[outcome]
		if delta < 0 {
			reset = true
			delta = 0
		}
		outcomes[outcome] = delta
	}
	var statements uint64
	if after.statements >= before.statements {
		statements = after.statements - before.statements
	} else {
		reset = true
	}
	return outcomes, statements, reset
}

func mixedReadRate(raw string) (int, error) {
	if raw == "" {
		return 20, nil
	}
	rate, err := strconv.Atoi(raw)
	if err != nil || rate < 1 || rate > 320 {
		return 0, errors.New("PITOT_MIXED_RATE must be an integer from 1 to 320 read operations per second")
	}
	return rate, nil
}

func mixedReadIngestRows(raw string) (int, error) {
	if raw == "" {
		return 2048, nil
	}
	rows, err := strconv.Atoi(raw)
	if err != nil || rows < 128 || rows > 8192 {
		return 0, errors.New("PITOT_MIXED_INGEST_ROWS must be an integer from 128 to 8192 per signal per batch")
	}
	return rows, nil
}

func mixedReadSeedConfig(rawState, rawEnd string, now time.Time) (string, time.Time, error) {
	state := rawState
	if state == "" {
		state = "backlog"
	}
	if state != "backlog" && state != "settled" {
		return "", time.Time{}, errors.New("PITOT_MIXED_SEED_STATE must be backlog or settled")
	}
	end := now.UTC().Truncate(time.Minute).Add(-time.Minute)
	if rawEnd != "" {
		var err error
		end, err = time.Parse(time.RFC3339Nano, rawEnd)
		if err != nil {
			return "", time.Time{}, errors.New("PITOT_MIXED_END must be RFC3339Nano")
		}
		end = end.UTC()
	}
	if end.After(now) || end.Add(-24*time.Hour).Before(now.Add(-30*24*time.Hour)) {
		return "", time.Time{}, errors.New("PITOT_MIXED_END must place the complete 24h window inside retention and not in the future")
	}
	return state, end, nil
}
func mixedReadCompactionProgress(before, after, merged, debt int) (bool, error) {
	if merged == 0 {
		if debt != 0 {
			return false, fmt.Errorf("seed compaction stopped with debt=%d", debt)
		}
		return true, nil
	}
	if merged < 0 || after >= before {
		return false, fmt.Errorf("seed compaction made no progress: before=%d after=%d merged=%d", before, after, merged)
	}
	return false, nil
}
func mixedReadSettleSeed(t *testing.T, repo *telemetrystore.Repository, d *engine.Duck) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	passes := len(repo.Parquet.BatchMetadata()) + 1
	for range passes {
		before := len(repo.Parquet.BatchMetadata())
		n, err := repo.CompactParquet(ctx, d, 128)
		if err != nil {
			t.Fatal("settle seed compaction:", err)
		}
		done, err := mixedReadCompactionProgress(before, len(repo.Parquet.BatchMetadata()), n, repo.CompactionDebt())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
	}
	t.Fatal("seed compaction exceeded bounded pass count")
}

func mixedReadOffset(slot, rate int) time.Duration {
	return time.Duration(int64(slot) * int64(time.Second) / int64(rate))
}
func mixedReadTotal(duration time.Duration, rate int) int {
	return int((int64(duration)*int64(rate) + int64(time.Second) - 1) / int64(time.Second))
}
func mixedReadDue(elapsed time.Duration, rate, total int) int {
	if elapsed < 0 {
		return 0
	}
	// Offsets are floor(slot*1s/rate). Use the same rational schedule here;
	// rounding an interval first would offer an extra slot for rates such as 3.
	return min(total, int(((int64(elapsed)+1)*int64(rate)-1)/int64(time.Second))+1)
}

func mixedReadPercentile(values []float64, percentile int) float64 {
	if len(values) == 0 {
		return 0
	}
	copyOf := append([]float64(nil), values...)
	sort.Float64s(copyOf)
	return copyOf[(percentile*len(copyOf)+99)/100-1]
}

// Getrusage measures this process without starting ps or inspecting other
// processes. Its high-water mark includes fixture preparation, not just load.
func mixedReadPeakRSS() (float64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	divisor := float64(1024)
	if runtime.GOOS == "darwin" {
		divisor = 1 << 20 // Darwin reports bytes; Linux reports KiB.
	}
	return float64(usage.Maxrss) / divisor, nil
}
func mixedReadBusy(err error, results []Result) bool {
	if errors.Is(err, ErrBusy) {
		return true
	}
	for _, r := range results {
		if r.Status == StatusError && r.Error == BusyMessage {
			return true
		}
	}
	return false
}

func mixedReadKindGate(k *mixedReadKind, budget float64) error {
	if k.Offered != k.Missed+k.ClientQueueFull+k.Dispatched || k.Dispatched != k.Success+k.BackendBusy+k.Errors || k.Success != k.DuringLoad+k.DuringDrain {
		return errors.New("operation accounting does not reconcile")
	}
	if k.Success == 0 || k.Missed+k.ClientQueueFull+k.BackendBusy+k.Errors > 0 {
		return errors.New("offered work did not all complete successfully")
	}
	if k.P95MS > budget {
		return fmt.Errorf("E2E p95 %.3f ms exceeds %.3f ms", k.P95MS, budget)
	}
	return nil
}

// Every successful operation checks exact counts and rank, including during
// ingestion/compaction. Live writes fall outside the immutable oracle window.
func mixedReadOracle(results []Result, panels int, expected int) error {
	if len(results) != panels {
		return fmt.Errorf("results=%d want=%d", len(results), panels)
	}
	for _, r := range results {
		if r.Cached {
			return fmt.Errorf("panel=%s served cached result under explicit bypass", r.ID)
		}
		if r.Status != StatusOK || r.Frame == nil || r.Frame.Rows == 0 || r.AnnotationError != "" {
			return fmt.Errorf("panel=%s status=%s error=%s annotation=%s", r.ID, r.Status, r.Error, r.AnnotationError)
		}
		switch {
		case r.ID == "traces_all" || r.ID == "traces_selected" || r.ID == "traces_checkout":
			want := expected / 4
			if r.ID == "traces_checkout" {
				want = expected / 2048 * 342
			}
			found := false
			for i, c := range r.Frame.Columns {
				if c.Role == "measure" {
					if i >= len(r.Frame.Values) || len(r.Frame.Values[i]) != 1 {
						return fmt.Errorf("panel=%s missing scalar distinct trace count", r.ID)
					}
					value, err := strconv.ParseFloat(fmt.Sprint(r.Frame.Values[i][0]), 64)
					if err != nil || value != float64(want) {
						return fmt.Errorf("panel=%s traces=%v want=%d", r.ID, r.Frame.Values[i][0], want)
					}
					found = true
				}
			}
			if !found {
				return fmt.Errorf("panel=%s missing distinct trace measure", r.ID)
			}
		case strings.HasPrefix(r.ID, "count"), strings.HasPrefix(r.ID, "logs"):
			found := false
			for i, c := range r.Frame.Columns {
				if c.Role == "measure" && c.Name == "count" && i < len(r.Frame.Totals) {
					v, err := strconv.ParseFloat(fmt.Sprint(r.Frame.Totals[i]), 64)
					if err != nil || v != float64(expected) {
						return fmt.Errorf("panel=%s total=%v want=%d", r.ID, r.Frame.Totals[i], expected)
					}
					found = true
				}
			}
			if !found {
				return fmt.Errorf("panel=%s missing count total", r.ID)
			}
		case strings.HasPrefix(r.ID, "p95"):
			found := false
			for i, c := range r.Frame.Columns {
				if c.Name == "service" && len(r.Frame.Values[i]) > 0 {
					if r.Frame.Values[i][0] != "svc5" {
						return fmt.Errorf("panel=%s first=%v want svc5", r.ID, r.Frame.Values[i][0])
					}
					found = true
				}
			}
			if !found {
				return fmt.Errorf("panel=%s missing service ranking", r.ID)
			}
		}
	}
	return nil
}

func mixedReadSpecs(start, end time.Time) []Dashboard {
	four := pressureSpec(start, end)
	twelve := panelBenchmarkDashboard(panelBenchData{Start: start, End: end, Services: []string{"svc0", "svc1", "svc2", "svc3", "svc4", "svc5"}, Namespaces: []string{"mixed_synthetic"}})
	twelve.Name = "Synthetic varied twelve panel mixed load"
	single := four
	single.Name = "Synthetic single p95 mixed load"
	single.Panels = []Panel{four.Panels[2]}
	return []Dashboard{single, four, twelve}
}

func mixedReadBatch(id string, start time.Time, rows int) telemetrystore.Batch {
	batch := telemetrystore.Batch{ID: id}
	for i := range rows {
		service := i % 6
		n := start.Add(time.Duration(i) * time.Nanosecond).UnixNano()
		duration := int64(service+1) * 10_000_000
		batch.Spans = append(batch.Spans, telemetry.Span{Namespace: "mixed_synthetic", ServiceName: fmt.Sprintf("svc%d", service), TraceID: fmt.Sprintf("%s-trace-%d", id, i/4), SpanID: fmt.Sprint(i), StartUnixNanos: n, EndUnixNanos: n + duration, DurationMS: float64(service+1) * 10, Kind: "SPAN_KIND_SERVER", StatusCode: "STATUS_CODE_OK", Resource: map[string]any{"service.version": "synthetic"}})
		batch.Logs = append(batch.Logs, telemetry.Log{Namespace: "mixed_synthetic", ServiceName: fmt.Sprintf("svc%d", service), TraceID: fmt.Sprintf("%s-trace-%d", id, i/4), EventUnixNanos: n, TimeUnixNanos: n, Body: "synthetic mixed-load event", Severity: "INFO"})
	}
	return batch
}

func TestMixedReadLoad(t *testing.T) {
	if os.Getenv("PITOT_MIXED_READ") != "1" {
		t.Skip("opt-in synthetic mixed read/load experiment")
	}
	rate, err := mixedReadRate(os.Getenv("PITOT_MIXED_RATE"))
	if err != nil {
		t.Fatal(err)
	}
	ingestRows, err := mixedReadIngestRows(os.Getenv("PITOT_MIXED_INGEST_ROWS"))
	if err != nil {
		t.Fatal(err)
	}
	seedState, windowEnd, err := mixedReadSeedConfig(os.Getenv("PITOT_MIXED_SEED_STATE"), os.Getenv("PITOT_MIXED_END"), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	duration := 15 * time.Second
	if raw := os.Getenv("PITOT_MIXED_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		if err != nil || duration < time.Second || duration > 5*time.Minute {
			t.Fatal("PITOT_MIXED_DURATION must be between 1s and 5m")
		}
	}
	scale := 1
	if raw := os.Getenv("PITOT_MIXED_SCALE"); raw != "" {
		var err error
		scale, err = strconv.Atoi(raw)
		if err != nil || scale < 1 || scale > 16 {
			t.Fatal("PITOT_MIXED_SCALE must be 1..16")
		}
	}
	singleBudget, batchBudget := 500.0, 1500.0
	for name, target := range map[string]*float64{"PITOT_MIXED_SINGLE_P95_MS": &singleBudget, "PITOT_MIXED_BATCH_P95_MS": &batchBudget} {
		if raw := os.Getenv(name); raw != "" {
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
				t.Fatalf("invalid %s", name)
			}
			*target = v
		}
	}
	root := t.TempDir()
	cfg := config.Config{DataDir: filepath.Join(root, "state"), RetentionDays: 30, DuckDBMemory: "4GB", DuckDBThreads: 4, DuckDBMaxConns: 8, RollupInterval: time.Second, MaintenanceInterval: 5 * time.Second}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	end := windowEnd
	start := end.Add(-24 * time.Hour)
	// 96 evenly spaced batches populate the complete day. Keep a minute margin
	// on each edge; per-service duration makes svc5 the unambiguous p95 winner.
	for batchIndex := range 96 * scale {
		at := start.Add(time.Minute + time.Duration(batchIndex)*(24*time.Hour-4*time.Minute)/time.Duration(96*scale))
		batch := mixedReadBatch(fmt.Sprintf("mixed-seed-%06d", batchIndex), at, 2048)
		// Spread the 2048 rows over this batch's portion of the day.
		for i := range batch.Spans {
			n := at.Add(time.Duration(i) * (24*time.Hour - 4*time.Minute) / time.Duration(96*scale*2048)).UnixNano()
			batch.Spans[i].StartUnixNanos = n
			batch.Spans[i].EndUnixNanos = n + int64(batch.Spans[i].DurationMS*1e6)
			batch.Logs[i].EventUnixNanos = n
			batch.Logs[i].TimeUnixNanos = n
		}
		if err := repo.Commit(t.Context(), batch); err != nil {
			t.Fatal(err)
		}
	}
	d, err := engine.NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if seedState == "settled" {
		mixedReadSettleSeed(t, repo, d)
	}
	horizon, _, _ := repo.PublicationBounds()
	panelBenchWarmDashboard(t, d, horizon)
	mixedReadWarmCache(t, d, repo)
	var probe *mixedReadProbe
	var queryEngine Engine = d
	if os.Getenv("PITOT_MIXED_DIAGNOSTICS") == "1" {
		probe = &mixedReadProbe{duck: d, walPath: cfg.QueryDuckDBPath() + ".wal", stackPath: os.Getenv("PITOT_MIXED_STACK_FILE")}
		queryEngine = &mixedReadObservedEngine{Duck: d, probe: probe}
	}
	executor := NewExecutor(queryEngine, 30)
	if probe != nil {
		probe.executor = executor
	}
	d.SetReadPressure(executor.Pressure)
	executor.SetCacheTTL(10 * time.Second)
	defer executor.Close()
	specs := mixedReadSpecs(start, end)
	expected := 96 * scale * 2048
	for _, spec := range specs {
		results, err := executor.Run(WithoutCachedResults(t.Context()), RunRequest{Dashboard: spec})
		if err != nil {
			t.Fatal(err)
		}
		if err := mixedReadOracle(results, len(spec.Panels), expected); err != nil {
			t.Fatal("preflight oracle:", err)
		}
	}
	report := mixedReadReport{Methodology: "synthetic_fixed_rate_fresh_executor_frozen_exact_shapes_seed_control_v8_native_rss_tempdir_not_production_capacity", MaintenanceInterval: "5s", MaintenanceMode: "accelerated_probe_product_default_1h", CachePreparation: "analytical_and_completed_read_caches_warm", Rate: rate, OperationUnit: "executor_request_not_individual_panels", ArrivalIntervalNS: float64(time.Second) / float64(rate), DurationSeconds: duration.Seconds(), Scale: scale, SeedSpans: expected, SeedLogs: expected, Threads: 4, Connections: 8, Workers: mixedReadClientWorkers, QueueCapacity: mixedReadClientQueue, Memory: "4GB", CacheTTL: "10s", CacheBypass: true, Kinds: map[string]*mixedReadKind{}}
	for _, kind := range mixedReadKinds {
		report.Kinds[kind] = &mixedReadKind{}
	}
	report.SeedState = seedState
	report.WindowStart = start.Format(time.RFC3339Nano)
	report.WindowEnd = end.Format(time.RFC3339Nano)
	report.InitialGenerationCounts = map[uint32]int{}
	for _, m := range repo.Parquet.BatchMetadata() {
		report.InitialGenerationCounts[m.Generation]++
	}
	report.IngestRowsPerSignalPerBatch = ingestRows
	report.IngestCommitTargetPerSecond = 5
	report.IngestTargetItemsPerSecond = ingestRows * 2 * 5
	report.SingleP95BudgetMS = singleBudget
	report.BatchP95BudgetMS = batchBudget
	report.Platform = runtime.GOOS + "/" + runtime.GOARCH
	report.RSSMethod = "process_lifetime_peak_getrusage_including_setup"
	if err := d.DB.QueryRowContext(t.Context(), "SELECT version(),current_setting('memory_limit')").Scan(&report.EngineVersion, &report.EffectiveMemory); err != nil {
		t.Fatal(err)
	}
	report.InitialActiveBatches = len(repo.Parquet.BatchMetadata())
	if err := d.DB.QueryRowContext(t.Context(), "SELECT current_setting('parquet_metadata_cache')::BOOLEAN").Scan(&report.ParquetMetadataCache); err != nil {
		t.Fatal(err)
	}
	report.InitialCompactionDebt = repo.CompactionDebt()
	if err := d.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM read_batches").Scan(&report.InitialReadCacheMarkers); err != nil {
		t.Fatal(err)
	}
	initialStamp, err := d.RollupWatermark(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !initialStamp.IsZero() {
		report.InitialRollupLagMS = max(0, float64(horizon-initialStamp.UnixNano())/1e6)
	}

	phaseDiagnostics, err := mixedReadDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	phaseCtx, stopPhase := context.WithTimeout(t.Context(), duration)
	defer stopPhase()
	background, stopBackground := context.WithCancel(t.Context())
	defer stopBackground()
	logState, restoreLogs := mixedReadCaptureLogs(background)
	defer restoreLogs()
	backgroundDone := make(chan struct{})
	go func() { defer close(backgroundDone); d.RunRollups(background) }()
	var accepted, acceptedRows, ingestErrors atomic.Int64
	ingestDone := make(chan struct{})
	go func() {
		defer close(ingestDone)
		ticker := time.NewTicker(time.Second / 5)
		defer ticker.Stop()
		for index := 0; ; index++ {
			select {
			case <-phaseCtx.Done():
				return
			case <-ticker.C:
			}
			batch := mixedReadBatch(fmt.Sprintf("mixed-live-%06d", index), time.Now().UTC(), ingestRows)
			if err := repo.Commit(phaseCtx, batch); err != nil {
				if phaseCtx.Err() == nil {
					ingestErrors.Add(1)
				}
				return
			}
			accepted.Add(1)
			acceptedRows.Add(int64(len(batch.Spans) + len(batch.Logs)))
		}
	}()
	rssDone := make(chan struct{})
	go func() {
		defer close(rssDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-background.Done():
				return
			case <-ticker.C:
				probe.record("sample", time.Now(), false)
				rss, err := mixedReadPeakRSS()
				if err != nil {
					report.RSSError = err.Error()
				} else {
					report.PeakRSSMiB = max(report.PeakRSSMiB, rss)
				}
			}
		}
	}()
	jobs := make(chan mixedReadJob, mixedReadClientQueue)
	var workers sync.WaitGroup
	var activity mixedReadWorkerActivity
	var lock sync.Mutex
	arrivalStart := time.Now()
	if probe != nil {
		probe.setStart(arrivalStart)
	}
	cutoff := arrivalStart.Add(duration)
	readCtx, stopReads := context.WithDeadline(t.Context(), cutoff.Add(15*time.Second))
	defer stopReads()
	for range mixedReadClientWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				activity.start()
				kind := mixedReadKinds[job.slot%len(mixedReadKinds)]
				spec := specs[job.slot%len(specs)]
				began := time.Now()
				request, cancel := context.WithTimeout(WithoutCachedResults(readCtx), 15*time.Second)
				results, err := executor.Run(request, RunRequest{Dashboard: spec})
				cancel()
				finished := time.Now()
				probe.record(kind, began, mixedReadBusy(err, results))
				semanticErr := error(nil)
				if err == nil {
					semanticErr = mixedReadOracle(results, len(spec.Panels), expected)
				}
				lock.Lock()
				k := report.Kinds[kind]
				switch {
				case mixedReadBusy(err, results):
					k.BackendBusy++
				case err != nil || semanticErr != nil:
					k.Errors++
					if len(report.Errors) < 5 {
						report.Errors = append(report.Errors, fmt.Sprintf("kind=%s error=%v oracle=%v", kind, err, semanticErr))
					}
				default:
					k.Success++
					k.E2EMS = append(k.E2EMS, float64(finished.Sub(job.scheduled))/float64(time.Millisecond))
					k.QueueMS = append(k.QueueMS, float64(began.Sub(job.scheduled))/float64(time.Millisecond))
					if finished.Before(cutoff) {
						k.DuringLoad++
					} else {
						k.DuringDrain++
					}
					for _, r := range results {
						if r.Cached {
							k.CachedPanels++
						}
					}
				}
				lock.Unlock()
				activity.stop()
			}
		}()
	}
	total := mixedReadTotal(duration, rate)
	for next := 0; next < total; {
		now := time.Now()
		if !now.Before(cutoff) {
			for ; next < total; next++ {
				k := report.Kinds[mixedReadKinds[next%3]]
				lock.Lock()
				k.Offered++
				k.Missed++
				lock.Unlock()
			}
			break
		}
		due := mixedReadDue(now.Sub(arrivalStart), rate, total)
		if due <= next {
			timer := time.NewTimer(time.Until(arrivalStart.Add(mixedReadOffset(next, rate))))
			select {
			case <-phaseCtx.Done():
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		for ; next < due-1; next++ {
			lock.Lock()
			k := report.Kinds[mixedReadKinds[next%3]]
			k.Offered++
			k.Missed++
			lock.Unlock()
		}
		job := mixedReadJob{slot: next, scheduled: arrivalStart.Add(mixedReadOffset(next, rate))}
		lock.Lock()
		k := report.Kinds[mixedReadKinds[next%3]]
		k.Offered++
		select {
		case jobs <- job:
			k.Dispatched++
		default:
			k.ClientQueueFull++
		}
		lock.Unlock()
		next++
	}
	close(jobs)
	<-phaseCtx.Done() // Keep background work running for the full offered interval.
	workers.Wait()
	stopPhase()
	<-ingestDone
	logState.stopping.Store(true)
	stopBackground()
	<-backgroundDone
	<-rssDone
	probe.report(t)
	afterDiagnostics, err := mixedReadDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	report.ResultCacheOutcomes, report.NativeQueryStatementCountDelta, report.DiagnosticCountersReset = mixedReadDiagnosticDelta(phaseDiagnostics, afterDiagnostics)
	logState.Lock()
	report.BackgroundErrors = append([]string(nil), logState.failures...)
	report.ExpectedShutdownInterrupts = logState.shutdownInterrupts
	report.ExpectedShutdownCancellations = logState.shutdownCancellations
	logState.Unlock()
	report.PeakActiveWorkers = activity.peak.Load()
	report.IngestAcceptedBatches = accepted.Load()
	report.IngestAcceptedRows = acceptedRows.Load()
	report.IngestErrors = ingestErrors.Load()
	report.IngestRowsPerSecond = float64(acceptedRows.Load()) / duration.Seconds()
	report.ActiveBatches = len(repo.Parquet.BatchMetadata())
	report.CompactionDebt = repo.CompactionDebt()
	if err := d.DB.QueryRowContext(t.Context(), "SELECT coalesce(sum(memory_usage_bytes),0)::BIGINT FROM duckdb_memory() WHERE tag='OBJECT_CACHE'").Scan(&report.ObjectCacheBytes); err != nil {
		t.Fatal(err)
	}
	stamp, err := d.RollupWatermark(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	horizon, _, _ = repo.PublicationBounds()
	if !stamp.IsZero() {
		report.RollupLagMS = max(0, float64(horizon-stamp.UnixNano())/1e6)
	}
	if err := d.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM read_batches").Scan(&report.ReadCacheMarkers); err != nil {
		t.Fatal(err)
	}
	for _, kind := range mixedReadKinds {
		k := report.Kinds[kind]
		k.P50MS = mixedReadPercentile(k.E2EMS, 50)
		k.P95MS = mixedReadPercentile(k.E2EMS, 95)
		k.P99MS = mixedReadPercentile(k.E2EMS, 99)
		k.QueueP95MS = mixedReadPercentile(k.QueueMS, 95)
		k.P99Qualified = len(k.E2EMS) >= 1000
	}
	for _, k := range report.Kinds {
		report.GeneratorLimited = report.GeneratorLimited || k.ClientQueueFull > 0 || k.Missed > 0
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MIXED_READ_REPORT %s", encoded)
	offered := 0
	for i, kind := range mixedReadKinds {
		k := report.Kinds[kind]
		offered += k.Offered
		report.GeneratorLimited = report.GeneratorLimited || k.ClientQueueFull > 0 || k.Missed > 0
		budget := batchBudget
		if i == 0 {
			budget = singleBudget
		}
		if err := mixedReadKindGate(k, budget); err != nil {
			t.Errorf("%s: %v", kind, err)
		}
	}
	if len(report.BackgroundErrors) > 0 {
		t.Errorf("background operational errors invalidate run: %v", report.BackgroundErrors)
	}
	if offered != total {
		t.Errorf("offered=%d want=%d", offered, total)
	}
	if report.IngestAcceptedBatches == 0 || report.IngestErrors != 0 {
		t.Errorf("background ingest accepted=%d errors=%d", report.IngestAcceptedBatches, report.IngestErrors)
	}
}

func TestMixedReadScheduleAndGates(t *testing.T) {
	if !mixedReadBusy(nil, []Result{{Status: StatusError, Error: BusyMessage}}) {
		t.Fatal("panel busy result misclassified")
	}
	if got := mixedReadDue(175*time.Millisecond, 20, 20); got != 4 {
		t.Fatalf("delayed wake due=%d", got)
	}
	if got := mixedReadDue(time.Second, 20, 20); got != 20 {
		t.Fatalf("cutoff due=%d", got)
	}
	k := &mixedReadKind{Offered: 3, Missed: 1, Dispatched: 2, Success: 2, DuringLoad: 1, DuringDrain: 1, P95MS: 1}
	if err := mixedReadKindGate(k, 500); err == nil {
		t.Fatal("scheduler misses passed")
	}
	k.Offered = 2
	k.Missed = 0
	k.P95MS = 501
	if err := mixedReadKindGate(k, 500); err == nil {
		t.Fatal("slow operation kind passed")
	}
	k.P95MS = 1
	k.Dispatched = 3
	if err := mixedReadKindGate(k, 500); err == nil {
		t.Fatal("broken accounting passed")
	}
	k.Dispatched = 2
	if err := mixedReadKindGate(k, 500); err != nil {
		t.Fatal(err)
	}
	if p := mixedReadPercentile([]float64{1, 2, 3, 100}, 99); p != 100 {
		t.Fatalf("tail=%v", p)
	}
	frame := &Frame{Rows: 1, Columns: []Column{{Name: "service", Role: "dimension"}}, Values: [][]any{{"svc0"}}}
	if err := mixedReadOracle([]Result{{ID: "p95", Status: StatusOK, Frame: frame}}, 1, 100); err == nil {
		t.Fatal("wrong rank passed")
	}
	countFrame := &Frame{Rows: 1, Columns: []Column{{Name: "time", Role: "time"}, {Name: "count", Role: "measure"}}, Values: [][]any{{int64(0)}, {99.0}}, Totals: []any{nil, 99.0}}
	if err := mixedReadOracle([]Result{{ID: "count", Status: StatusOK, Frame: countFrame}}, 1, 100); err == nil {
		t.Fatal("wrong exact total passed")
	}
	countFrame.Totals[1] = 100.0
	if err := mixedReadOracle([]Result{{ID: "count", Status: StatusOK, Frame: countFrame}}, 1, 100); err != nil {
		t.Fatal(err)
	}
	frame.Values[0][0] = "svc5"
	if err := mixedReadOracle([]Result{{ID: "p95", Status: StatusOK, Frame: frame}}, 1, 100); err != nil {
		t.Fatal(err)
	}
}

func TestMixedReadConfigurableRateSchedule(t *testing.T) {
	for _, rate := range []int{3, 20, 40, 80, 160, 320} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			if parsed, err := mixedReadRate(fmt.Sprint(rate)); err != nil || parsed != rate {
				t.Fatalf("rate=%d error=%v", parsed, err)
			}
			duration := 15 * time.Second
			total := mixedReadTotal(duration, rate)
			if total != 15*rate {
				t.Fatalf("offered=%d want=%d", total, 15*rate)
			}
			// Every scheduled slot is due exactly at its offset, and not one ns before.
			for slot := range total {
				at := mixedReadOffset(slot, rate)
				if got := mixedReadDue(at, rate, total); got != slot+1 {
					t.Fatalf("slot=%d due=%d", slot, got)
				}
				if got := mixedReadDue(at-time.Nanosecond, rate, total); got != slot {
					t.Fatalf("slot=%d early due=%d", slot, got)
				}
			}
			offered := 0
			missed := 0
			dispatched := 0
			for next := 0; next < total; {
				due := mixedReadDue(mixedReadOffset(min(total-1, next+3), rate), rate, total)
				offered += due - next
				missed += due - next - 1
				dispatched++
				next = due
			}
			if offered != total || offered != missed+dispatched {
				t.Fatalf("offered=%d missed=%d dispatched=%d", offered, missed, dispatched)
			}
		})
	}
	for _, raw := range []string{"0", "-1", "321", "2.5", "invalid"} {
		if _, err := mixedReadRate(raw); err == nil {
			t.Fatalf("invalid rate %q accepted", raw)
		}
	}
	if rate, err := mixedReadRate(""); err != nil || rate != 20 {
		t.Fatalf("default=%d error=%v", rate, err)
	}
}

func TestMixedReadCapturesOperationalBackgroundErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state := &mixedReadLogState{background: ctx}
	state.stopping.Store(true)
	h := mixedReadLogHandler{Handler: slog.NewTextHandler(io.Discard, nil), state: state}
	canceled := slog.NewRecord(time.Now(), slog.LevelWarn, "stopping", 0)
	canceled.AddAttrs(slog.Any("err", context.Canceled))
	if err := h.Handle(context.Background(), canceled); err != nil {
		t.Fatal(err)
	}
	failed := slog.NewRecord(time.Now(), slog.LevelWarn, "telemetry maintenance failed", 0)
	failed.AddAttrs(slog.Any("err", errors.New("cannot CHECKPOINT while another write is active")))
	if err := h.Handle(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	if len(state.failures) != 1 || !strings.Contains(state.failures[0], "CHECKPOINT") {
		t.Fatalf("background failures=%v", state.failures)
	}
}

func TestMixedReadArrivalWorkerActivity(t *testing.T) {
	var activity mixedReadWorkerActivity
	for range 3 {
		activity.start()
	}
	activity.stop()
	activity.stop()
	activity.start()
	if got := activity.peak.Load(); got != 3 {
		t.Fatalf("peak active=%d want3", got)
	}
	if got := activity.active.Load(); got != 2 {
		t.Fatalf("active=%d want2", got)
	}
	if mixedReadClientWorkers != 32 || mixedReadClientQueue != 64 {
		t.Fatal("capacity sweep generator envelope changed")
	}
}

func TestMixedReadDefaultLogBridge(t *testing.T) {
	if os.Getenv("PITOT_MIXED_LOG_CHILD") == "1" {
		originalWriter, originalFlags := log.Writer(), log.Flags()
		state, restore := mixedReadCaptureLogs(context.Background())
		slog.Info("synthetic bridge info")
		log.Print("synthetic standard log bridge")
		slog.Warn("synthetic maintenance failed", "err", errors.New("synthetic CHECKPOINT conflict"))
		restore()
		if log.Writer() != originalWriter || log.Flags() != originalFlags {
			t.Fatal("standard log configuration not restored")
		}
		slog.Info("synthetic restored slog bridge")
		log.Print("synthetic restored standard log")
		if len(state.failures) != 1 {
			t.Fatalf("captured=%v", state.failures)
		}
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMixedReadDefaultLogBridge$", "-test.count=1")
	cmd.Env = append(os.Environ(), "PITOT_MIXED_LOG_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("default log bridge subprocess error=%v timeout=%v output=%s", err, ctx.Err(), out)
	}
}

func TestMixedReadVariedShapesAndTraceOracle(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	specs := mixedReadSpecs(start, start.Add(24*time.Hour))
	if len(specs) != 3 || len(specs[0].Panels) != 1 || len(specs[1].Panels) != 4 || len(specs[2].Panels) != 12 {
		t.Fatal("unexpected operation/panel shape counts")
	}
	tracePanels := 0
	for _, p := range specs[2].Panels {
		if strings.HasPrefix(p.ID, "traces_") {
			tracePanels++
		}
	}
	if tracePanels != 3 || len(specs[2].Variables) != 1 {
		t.Fatal("varied dashboard lost raw distinct-trace shapes or selected variable")
	}
	for _, test := range []struct {
		id   string
		want float64
	}{{"traces_all", 512}, {"traces_checkout", 342}, {"traces_selected", 512}} {
		frame := &Frame{Rows: 1, Columns: []Column{{Name: "trace_count", Role: "measure"}}, Values: [][]any{{test.want}}}
		result := []Result{{ID: test.id, Status: StatusOK, Frame: frame}}
		if err := mixedReadOracle(result, 1, 2048); err != nil {
			t.Fatal(err)
		}
		frame.Values[0][0] = test.want + 1
		if err := mixedReadOracle(result, 1, 2048); err == nil {
			t.Fatalf("incorrect %s count passed", test.id)
		}
	}
}
func TestMixedReadIngestScale(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want int
	}{{"", 2048}, {"128", 128}, {"2048", 2048}, {"8192", 8192}} {
		if got, err := mixedReadIngestRows(test.raw); err != nil || got != test.want {
			t.Fatalf("input=%q rows=%d error=%v", test.raw, got, err)
		}
	}
	for _, raw := range []string{"0", "127", "8193", "invalid"} {
		if _, err := mixedReadIngestRows(raw); err == nil {
			t.Fatalf("invalid %q accepted", raw)
		}
	}
	at := time.Now().UTC()
	batch := mixedReadBatch("live-test", at, 128)
	for i, s := range batch.Spans {
		if s.StartUnixNanos != at.UnixNano()+int64(i) || batch.Logs[i].EventUnixNanos != s.StartUnixNanos {
			t.Fatal("live timestamps extend beyond nanosecond offsets")
		}
	}
}

func TestMixedReadSeedControls(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	state, end, err := mixedReadSeedConfig("", "", now)
	if err != nil || state != "backlog" || !end.Equal(now.Add(-time.Minute)) {
		t.Fatalf("default state=%s end=%s error=%v", state, end, err)
	}
	fixed := now.Add(-time.Hour).Add(17 * time.Nanosecond)
	state, end, err = mixedReadSeedConfig("settled", fixed.Format(time.RFC3339Nano), now)
	if err != nil || state != "settled" || !end.Equal(fixed) {
		t.Fatalf("fixed state=%s end=%s error=%v", state, end, err)
	}
	for _, test := range []struct{ state, end string }{{"invalid", ""}, {"backlog", "invalid"}, {"settled", now.Add(time.Second).Format(time.RFC3339Nano)}, {"settled", now.Add(-30 * 24 * time.Hour).Format(time.RFC3339Nano)}} {
		if _, _, err := mixedReadSeedConfig(test.state, test.end, now); err == nil {
			t.Fatalf("invalid seed control passed: %+v", test)
		}
	}
	if done, err := mixedReadCompactionProgress(4, 2, 3, 0); err != nil || done {
		t.Fatalf("positive merge done=%t error=%v", done, err)
	}
	if done, err := mixedReadCompactionProgress(2, 2, 0, 0); err != nil || !done {
		t.Fatalf("settled done=%t error=%v", done, err)
	}
	if _, err := mixedReadCompactionProgress(2, 2, 0, 1); err == nil {
		t.Fatal("residual debt passed")
	}
	if _, err := mixedReadCompactionProgress(2, 2, 1, 0); err == nil {
		t.Fatal("nonprogressing merge passed")
	}
}

func TestMixedReadSettledSeedTinyFixture(t *testing.T) {
	if os.Getenv("PITOT_MIXED_SETTLE_SMOKE") != "1" {
		t.Skip("opt-in native seed-settlement smoke; run only with timing slot")
	}
	cfg := config.Config{DataDir: panelBenchScratch(t), RetentionDays: 30, DuckDBMemory: "4GB", DuckDBThreads: 4, DuckDBMaxConns: 8, RollupInterval: time.Second}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	at := time.Now().UTC().Add(-time.Minute)
	for i := range 128 {
		if err := repo.Commit(t.Context(), mixedReadBatch(fmt.Sprintf("settle-smoke-%03d", i), at, 4)); err != nil {
			t.Fatal(err)
		}
	}
	d, err := engine.NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	mixedReadSettleSeed(t, repo, d)
	metadata := repo.Parquet.BatchMetadata()
	if len(metadata) >= 128 || repo.CompactionDebt() != 0 {
		t.Fatalf("settlement failed: batches=%d debt=%d", len(metadata), repo.CompactionDebt())
	}
	var spans, logs int64
	if err := d.QueryRowScan(t.Context(), []any{&spans, &logs}, "SELECT (SELECT count(*) FROM spans),(SELECT count(*) FROM logs)"); err != nil {
		t.Fatal(err)
	}
	if spans != 512 || logs != 512 {
		t.Fatalf("settlement changed rows: spans=%d logs=%d", spans, logs)
	}
}

func TestMixedReadDiagnosticCounterDeltas(t *testing.T) {
	before := mixedReadDiagnosticSnapshot{cache: map[string]float64{"hit": 10, "miss": 20, "coalesced": 30, "stale": 1}, statements: 100}
	after := mixedReadDiagnosticSnapshot{cache: map[string]float64{"hit": 12, "miss": 25, "coalesced": 33, "stale": 1}, statements: 109}
	outcomes, statements, reset := mixedReadDiagnosticDelta(before, after)
	if reset || statements != 9 || outcomes["hit"] != 2 || outcomes["miss"] != 5 || outcomes["coalesced"] != 3 || outcomes["stale"] != 0 {
		t.Fatalf("outcomes=%v statements=%d reset=%t", outcomes, statements, reset)
	}
	outcomes, statements, reset = mixedReadDiagnosticDelta(after, before)
	if !reset || statements != 0 {
		t.Fatalf("reset statements=%d reset=%t", statements, reset)
	}
	for _, delta := range outcomes {
		if delta < 0 {
			t.Fatal("negative diagnostic counter delta")
		}
	}
}

func TestMixedReadShutdownInterruptClassification(t *testing.T) {
	interrupt := &duckdb.Error{Type: duckdb.ErrorTypeInterrupt, Msg: "interrupted"}
	unrelated := errors.New("cannot CHECKPOINT while another write is active")
	for _, test := range []struct {
		name               string
		stopping, canceled bool
		err                error
		expected           bool
		interruptCount     int
	}{
		{"running_interrupt", false, false, interrupt, false, 0},
		{"canceled_without_intent", false, true, interrupt, false, 0},
		{"intent_before_cancel", true, false, interrupt, false, 0},
		{"intentional_interrupt", true, true, fmt.Errorf("rollup: %w", interrupt), true, 1},
		{"poststop_unrelated", true, true, unrelated, false, 0},
		{"joined_unrelated", true, true, errors.Join(interrupt, unrelated), false, 0},
		{"poststop_other_native", true, true, &duckdb.Error{Type: duckdb.ErrorTypeInvalidInput, Msg: "bad input"}, false, 0},
		{"intentional_go_cancel", true, true, context.Canceled, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.canceled {
				cancel()
			}
			state := &mixedReadLogState{background: ctx}
			state.stopping.Store(test.stopping)
			var raw strings.Builder
			h := mixedReadLogHandler{Handler: slog.NewTextHandler(&raw, nil), state: state}
			record := slog.NewRecord(time.Now(), slog.LevelError, "background rollup failed", 0)
			record.AddAttrs(slog.Any("err", test.err))
			if err := h.Handle(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			if (len(state.failures) == 0) != test.expected || state.shutdownInterrupts != test.interruptCount {
				t.Fatalf("failures=%v shutdown_interrupts=%d", state.failures, state.shutdownInterrupts)
			}
			if !strings.Contains(raw.String(), "background rollup failed") {
				t.Fatal("raw error log was suppressed")
			}
		})
	}
}
