//go:build readbench

package query

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/queryrows"
)

// Optional bounded observations distinguish engine work from admission wait.
// Keep SQL and telemetry values out of the diagnostic output.
type mixedReadObservation struct {
	Kind                string
	StartMS, DurationMS float64
	Busy                bool
	WALBytes            int64
	CPUSeconds          float64
	// These counts describe completion time, not peak occupancy.
	Batches, ActivePanels, WaitingPanels, PoolInUse int
}

type mixedReadProbe struct {
	sync.Mutex
	start     time.Time
	events    []mixedReadObservation
	executor  *Executor
	duck      *engine.Duck
	walPath   string
	dropped   int
	stackPath string
	stackOnce sync.Once
}

func (p *mixedReadProbe) setStart(at time.Time) { p.Lock(); p.start = at; p.Unlock() }

func (p *mixedReadProbe) record(kind string, start time.Time, busy bool) {
	if p == nil {
		return
	}
	p.Lock()
	origin := p.start
	p.Unlock()
	if origin.IsZero() {
		return
	}
	duration := time.Since(start)
	if kind != "sample" && !busy && duration < 25*time.Millisecond {
		return
	}
	event := mixedReadObservation{Kind: kind, StartMS: float64(start.Sub(origin)) / float64(time.Millisecond), DurationMS: float64(duration) / float64(time.Millisecond), Busy: busy}
	if info, err := os.Stat(p.walPath); err == nil {
		event.WALBytes = info.Size()
	}
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		event.CPUSeconds = float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	}
	a := p.executor.admission
	a.mu.Lock()
	event.Batches, event.ActivePanels, event.WaitingPanels = a.batches, a.active[panelClass], len(a.queues[panelClass])
	a.mu.Unlock()
	event.PoolInUse = p.duck.DB.Stats().InUse
	p.Lock()
	if len(p.events) < 4096 {
		p.events = append(p.events, event)
	} else {
		p.dropped++
	}
	p.Unlock()
	if busy && p.stackPath != "" {
		p.stackOnce.Do(func() {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			_ = os.WriteFile(p.stackPath, buf[:n], 0600)
		})
	}
}

func (p *mixedReadProbe) report(t *testing.T) {
	if p == nil {
		return
	}
	p.Lock()
	defer p.Unlock()
	encoded, err := json.Marshal(p.events)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MIXED_READ_DIAGNOSTICS origin_utc=%s dropped=%d events=%s", p.start.UTC().Format(time.RFC3339Nano), p.dropped, encoded)
}

type mixedReadObservedEngine struct {
	*engine.Duck
	probe *mixedReadProbe
}

func (e *mixedReadObservedEngine) QueryContext(ctx context.Context, query string, args ...any) (queryrows.Rows, error) {
	start := time.Now()
	kind := "engine_other"
	if strings.Contains(query, "__hist_") {
		kind = "engine_histogram"
	} else if strings.Contains(strings.ToLower(query), "distinct") {
		kind = "engine_distinct"
	}
	rows, err := e.Duck.QueryContext(ctx, query, args...)
	if err != nil {
		e.probe.record(kind, start, false)
		return nil, err
	}
	return &mixedReadObservedRows{Rows: rows, done: func() { e.probe.record(kind, start, false) }}, nil
}

type mixedReadObservedRows struct {
	queryrows.Rows
	once sync.Once
	done func()
}

func (r *mixedReadObservedRows) Close() error {
	err := r.Rows.Close()
	r.once.Do(r.done)
	return err
}
