package query

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Delay the third caller after Executor.run captures its arrival clock, at the
// next context.WithTimeout's Deadline lookup. This ensures the second caller
// registers the replacement before the third reaches the cache. Context.Err on
// the parent is not a suitable barrier because the derived timer context owns
// its own Err method.
type freshWindowArrivalBarrier struct {
	context.Context
	once    sync.Once
	reached chan struct{}
	release chan struct{}
}

func (c *freshWindowArrivalBarrier) Deadline() (time.Time, bool) {
	c.once.Do(func() {
		close(c.reached)
		select {
		case <-c.release:
		case <-c.Context.Done():
		}
	})
	return c.Context.Deadline()
}

type freshWindowEngine struct {
	*engine.Duck
	calls        atomic.Int64
	oldCaptured  chan struct{}
	oldRelease   chan struct{}
	freshEntered chan struct{}
	freshRelease chan struct{}
}

func (e *freshWindowEngine) QueryContext(ctx context.Context, sql string, args ...any) (queryrows.Rows, error) {
	switch e.calls.Add(1) {
	case 1:
		// Hold actual native rows, so the old flight has a real pinned snapshot.
		rows, err := e.Duck.QueryContext(ctx, sql, args...)
		close(e.oldCaptured)
		if err != nil {
			return nil, err
		}
		select {
		case <-e.oldRelease:
			return rows, nil
		case <-ctx.Done():
			rows.Close()
			return nil, ctx.Err()
		}
	case 2:
		// Registration has happened, but the fresh snapshot is not acquired yet.
		close(e.freshEntered)
		select {
		case <-e.freshRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return e.Duck.QueryContext(ctx, sql, args...)
}

func TestFreshFixedWindowReplacementSharesLaterRegisteredSnapshot(t *testing.T) {
	for _, shape := range []struct {
		name            string
		dashboardFixed  bool
		requestOverride bool
		requestFixed    bool
		wantCalls       int64
		lateCaller      bool
		cancelThird     bool
		fullQueue       bool
		panelRange      bool
	}{
		{name: "absolute", dashboardFixed: true, wantCalls: 2},
		{name: "relative", wantCalls: 3},
		{name: "absolute_request_override", requestOverride: true, requestFixed: true, wantCalls: 2},
		{name: "relative_request_override", dashboardFixed: true, requestOverride: true, wantCalls: 3},
		{name: "arrival_after_registration", dashboardFixed: true, wantCalls: 3, lateCaller: true},
		{name: "cancel_one_shared_waiter", dashboardFixed: true, wantCalls: 2, cancelThird: true},
		{name: "full_queue_joins_fresh_replacement", dashboardFixed: true, wantCalls: 2, fullQueue: true},
		{name: "absolute_panel_range", dashboardFixed: true, wantCalls: 2, panelRange: true},
		{name: "relative_panel_range", wantCalls: 3, panelRange: true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			d, repo := newTestEngine(t)
			base := fixtureStart.Add(time.Hour)
			start, end := base.Add(-time.Hour), base
			window := func(fixed bool) Time {
				if fixed {
					return Time{From: &start, To: &end}
				}
				return Time{Range: "1h"}
			}
			publish := func(id string) {
				t.Helper()
				n := base.Add(-time.Minute).UnixNano()
				if err := repo.Commit(t.Context(), telemetrystore.Batch{ID: id, Logs: []telemetry.Log{{Namespace: "freshness", TimeUnixNanos: n, EventUnixNanos: n, Body: id, Severity: "INFO"}}}); err != nil {
					t.Fatal(err)
				}
			}
			publish("seed")
			observed := &freshWindowEngine{Duck: d, oldCaptured: make(chan struct{}), oldRelease: make(chan struct{}), freshEntered: make(chan struct{}), freshRelease: make(chan struct{})}
			e := NewExecutor(observed, 30)
			e.SetCacheTTL(10 * time.Second)
			var offset atomic.Int64
			e.now = func() time.Time { return base.Add(time.Duration(offset.Load())) }
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			barrier := &freshWindowArrivalBarrier{Context: ctx, reached: make(chan struct{}), release: make(chan struct{})}
			thirdContext, stopThird := context.WithCancel(barrier)
			releaseOld := sync.OnceFunc(func() { close(observed.oldRelease) })
			releaseFresh := sync.OnceFunc(func() { close(observed.freshRelease) })
			releaseArrival := sync.OnceFunc(func() { close(barrier.release) })
			t.Cleanup(func() {
				releaseOld()
				releaseFresh()
				releaseArrival()
				cancel()
				stopThird()
				e.Close()
			})
			req := RunRequest{Dashboard: Dashboard{Name: "Fresh fixed window", Time: window(shape.dashboardFixed), Panels: []Panel{{ID: "records", Title: "Records", Viz: "table", SQL: "SELECT body FROM logs WHERE $__window(time) ORDER BY body"}}}}
			if shape.panelRange {
				req.Dashboard.Panels[0].Time = &TimeOverride{Range: "15m"}
			}
			if shape.requestOverride {
				override := window(shape.requestFixed)
				req.Time = &override
			}
			type answer struct {
				results []Result
				err     error
			}
			run := func(c context.Context) <-chan answer {
				done := make(chan answer, 1)
				go func() {
					results, err := e.Run(WithoutCachedResults(c), req)
					done <- answer{results, err}
				}()
				return done
			}
			await := func(signal <-chan struct{}, label string, done <-chan answer) {
				t.Helper()
				select {
				case <-signal:
				case got := <-done:
					t.Fatalf("request returned before %s: results=%+v error=%v", label, got.results, got.err)
				case <-ctx.Done():
					t.Fatalf("waiting for %s: %v", label, ctx.Err())
				}
			}
			old := run(ctx)
			await(observed.oldCaptured, "old native snapshot", old)
			offset.Store(int64(time.Second))
			second := run(ctx)
			freshWindowWaiters(t, ctx, e, 2)
			offset.Store(int64(2 * time.Second))
			third := run(thirdContext)
			await(barrier.reached, "third caller's captured arrival", third)
			offset.Store(int64(3 * time.Second))
			releaseOld()
			await(observed.freshEntered, "replacement registered by second caller", second)
			// Both fresh callers arrived before this replacement. Publish real
			// telemetry before it acquires its snapshot, then let the third join.
			publish("late")
			if shape.fullQueue {
				// The old request/flight must have released its lifetime ticket
				// before filling the pool; the third caller is still at its barrier.
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					e.admission.mu.Lock()
					batches := e.admission.batches
					e.admission.mu.Unlock()
					if batches == 1 {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("old request retained its admission ticket")
					case <-ticker.C:
					}
				}
				reserved := 0
				for {
					err := e.admission.reserveBatch()
					if errors.Is(err, errQueueFull) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					reserved++
				}
				t.Cleanup(func() {
					for range reserved {
						e.admission.releaseBatch()
					}
				})
			}
			releaseArrival()
			freshWindowWaiters(t, ctx, e, 2)
			answers := []<-chan answer{old, second, third}
			if shape.cancelThird {
				stopThird()
				// The remaining owner must keep the shared native run alive.
				freshWindowWaiters(t, ctx, e, 1)
			}
			if shape.lateCaller {
				offset.Store(int64(4 * time.Second))
				answers = append(answers, run(ctx))
				freshWindowWaiters(t, ctx, e, 3)
			}
			releaseFresh()
			for i, result := range answers {
				var got answer
				select {
				case got = <-result:
				case <-ctx.Done():
					t.Fatalf("caller%d did not finish: %v", i, ctx.Err())
				}
				if shape.cancelThird && i == 2 {
					if !errors.Is(got.err, context.Canceled) {
						t.Fatalf("canceled waiter error=%v want context.Canceled", got.err)
					}
					continue
				}
				if got.err != nil || len(got.results) != 1 {
					t.Fatalf("caller%d results=%v error=%v", i, got.results, got.err)
				}
				r := got.results[0]
				if r.Status != StatusOK || r.Cached || r.Frame == nil || len(r.Frame.Values) != 1 {
					t.Fatalf("caller%d invalid result: %+v", i, r)
				}
				want := []any{"late", "seed"}
				if i == 0 {
					want = []any{"seed"}
				}
				if !reflect.DeepEqual(r.Frame.Values[0], want) {
					t.Fatalf("caller%d snapshot records=%v want=%v", i, r.Frame.Values[0], want)
				}
				wantEnd := base
				fixed := shape.dashboardFixed
				if shape.requestOverride {
					fixed = shape.requestFixed
				}
				if !fixed {
					wantEnd = base.Add(time.Duration(i) * time.Second)
				}
				if r.ToMS != wantEnd.UnixMilli() {
					t.Fatalf("caller%d window end=%d want=%d", i, r.ToMS, wantEnd.UnixMilli())
				}
			}
			if calls := observed.calls.Load(); calls != shape.wantCalls {
				t.Fatalf("engine queries=%d want=%d: a replacement registered after both fresh callers arrived must serve both fixed-window requests", calls, shape.wantCalls)
			}
			// Registration must not renew the stored answer's original TTL age.
			stampOffset := time.Second
			if shape.wantCalls == 3 {
				stampOffset = 2 * time.Second
			}
			if shape.lateCaller {
				stampOffset = 4 * time.Second
			}
			e.cache.mu.Lock()
			var stored []time.Time
			for _, entry := range e.cache.entries {
				stored = append(stored, entry.Value.(*cacheEntry).stamp)
			}
			e.cache.mu.Unlock()
			if len(stored) != 1 || !stored[0].Equal(base.Add(stampOffset)) {
				t.Fatalf("cached stamp=%v want original arrival %s", stored, base.Add(stampOffset))
			}
		})
	}
}

func freshWindowWaiters(t *testing.T, ctx context.Context, e *Executor, want int) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		e.cache.mu.Lock()
		waiters, flights := 0, len(e.cache.flights)
		for _, flight := range e.cache.flights {
			waiters += flight.waiters
		}
		e.cache.mu.Unlock()
		if flights == 1 && waiters == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %d callers: flights=%d waiters=%d: %v", want, flights, waiters, ctx.Err())
		case <-ticker.C:
		}
	}
}
