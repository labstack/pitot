//go:build readbench

package query

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/queryrows"
)

// Recording includes result consumption and preserves the concrete engine's
// optional interfaces. It is diagnostic instrumentation, not a latency gate.
type panelWorkEngine struct {
	*engine.Duck
	mu   sync.Mutex
	work map[string]*panelWorkStatement
}

type panelWorkStatement struct {
	sql     string
	count   int
	elapsed time.Duration
}

func (e *panelWorkEngine) QueryContext(ctx context.Context, sql string, args ...any) (queryrows.Rows, error) {
	started := time.Now()
	rows, err := e.Duck.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &panelWorkRows{Rows: rows, done: func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		key := fmt.Sprintf("%x", sha256.Sum256([]byte(sql)))[:12]
		statement := e.work[key]
		if statement == nil {
			statement = &panelWorkStatement{sql: sql}
			e.work[key] = statement
		}
		statement.count++
		statement.elapsed += time.Since(started)
	}}, nil
}

type panelWorkRows struct {
	queryrows.Rows
	once sync.Once
	done func()
}

func (r *panelWorkRows) Close() error {
	err := r.Rows.Close()
	r.once.Do(r.done)
	return err
}

// Run with a small fixed count to locate repeated statements before choosing
// an optimization. Use the ordinary panel benchmark for acceptance timings.
func BenchmarkPanelReadWork(b *testing.B) {
	data := preparePanelRegression(b)
	d, cfg := panelBenchEngine(b, data)
	metadata, err := panelReplayMetadata(cfg.TelemetryDir())
	if err != nil {
		b.Fatal(err)
	}
	horizon := metadata[0].MaxIngestedNanos
	for _, batch := range metadata {
		horizon = max(horizon, batch.MaxIngestedNanos)
	}
	panelBenchWarmDashboard(b, d, horizon)
	dashboard := panelBenchmarkDashboard(data)
	for _, panel := range dashboard.Panels {
		b.Run(panel.ID, func(b *testing.B) {
			recorder := &panelWorkEngine{Duck: d, work: map[string]*panelWorkStatement{}}
			spec := dashboard
			spec.Panels = []Panel{panel}
			if panel.ID != "traces_selected" {
				spec.Variables = nil
			}
			measurePanelBenchmark(b, recorder, spec, data.End)
			keys := make([]string, 0, len(recorder.work))
			for key := range recorder.work {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				statement := recorder.work[key]
				b.Logf("statement=%s count=%d total_ms=%.3f sql=%s", key, statement.count, float64(statement.elapsed)/float64(time.Millisecond), statement.sql)
			}
		})
	}
}
