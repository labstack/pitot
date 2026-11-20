package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
)

type fakeRows struct{ closed *bool }

func (fakeRows) Columns() ([]string, error) { return nil, nil }
func (fakeRows) Err() error                 { return nil }
func (fakeRows) Next() bool                 { return false }
func (fakeRows) Scan(...any) error          { return nil }
func (r fakeRows) Close() error             { *r.closed = true; return nil }

type fakeReader struct {
	namespace string
	rows      queryrows.Rows
	queried   *bool
	txRan     *bool
}

func (f fakeReader) QueryContext(context.Context, string, ...any) (queryrows.Rows, error) {
	if f.queried != nil {
		*f.queried = true
	}
	return f.rows, nil
}

func (f fakeReader) DefaultNamespace() string { return f.namespace }

func (f fakeReader) WithReadTransaction(ctx context.Context, read func(queryrows.Queryer) error) error {
	if f.txRan != nil {
		*f.txRan = true
	}
	return read(f)
}

func newAdmissionFixture(t *testing.T, slots, reserved, queue int) *Executor {
	t.Helper()
	e := NewExecutor(pooledEngine{pool: 8}, 30)
	t.Cleanup(e.Close)
	e.admission = newReadAdmission(slots, reserved, queue)
	return e
}

// A routed read holds its admission slot until the rows close, then frees it, so
// a bypass reader can no longer occupy the pool after it has finished reading.
func TestAdmittedReaderHoldsItsSlotUntilRowsClose(t *testing.T) {
	e := newAdmissionFixture(t, 1, 0, 4)
	closed := false
	reader := e.AdmitReader(fakeReader{rows: fakeRows{closed: &closed}})
	rows, err := reader.QueryContext(t.Context(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if e.admission.active[interactiveClass] != 1 {
		t.Fatalf("slot not held during read: active=%d", e.admission.active[interactiveClass])
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !closed {
		t.Fatal("underlying rows were not closed")
	}
	if e.admission.active[interactiveClass] != 0 {
		t.Fatalf("slot not released after close: active=%d", e.admission.active[interactiveClass])
	}
}

// Closing twice releases the slot once, so a defensive double close cannot
// drive the admission count negative.
func TestAdmittedReaderReleasesItsSlotOnceOnDoubleClose(t *testing.T) {
	e := newAdmissionFixture(t, 2, 0, 4)
	closed := false
	reader := e.AdmitReader(fakeReader{rows: fakeRows{closed: &closed}})
	rows, err := reader.QueryContext(t.Context(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	_ = rows.Close()
	if e.admission.active[interactiveClass] != 0 {
		t.Fatalf("double close changed the count: active=%d", e.admission.active[interactiveClass])
	}
}

// When the interactive lane is full the routed read is shed as ErrBusy without
// ever reaching the engine, instead of oversubscribing the pool.
func TestAdmittedReaderShedsWhenTheLaneIsFull(t *testing.T) {
	e := newAdmissionFixture(t, 1, 0, 1)
	e.admission.bounds[interactiveClass] = 0
	if err := e.admission.acquire(t.Context(), interactiveClass); err != nil {
		t.Fatal(err)
	}
	queried := false
	reader := e.AdmitReader(fakeReader{rows: fakeRows{closed: new(bool)}, queried: &queried})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	rows, err := reader.QueryContext(ctx, "SELECT 1")
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("err=%v, want ErrBusy", err)
	}
	if rows != nil {
		t.Fatal("rows returned on a shed read")
	}
	if queried {
		t.Fatal("engine was queried on a shed read")
	}
	if e.InteractiveRefusals() == 0 {
		t.Fatal("a shed read did not count as an interactive refusal")
	}
}

// A routed read transaction takes one slot for the whole callback and runs on
// the wrapped surface's own transaction, so a reader needing one consistent
// snapshot keeps it and is admitted as a unit rather than per sub-read.
func TestAdmittedReadTransactionTakesOneSlotForTheWholeCallback(t *testing.T) {
	e := newAdmissionFixture(t, 1, 0, 4)
	ran := false
	reader := e.AdmitReader(fakeReader{txRan: &ran}).(queryrows.ReadTransactioner)
	err := reader.WithReadTransaction(t.Context(), func(queryrows.Queryer) error {
		if e.admission.active[interactiveClass] != 1 {
			t.Fatalf("slot not held during the callback: active=%d", e.admission.active[interactiveClass])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("the wrapped read transaction did not run")
	}
	if e.admission.active[interactiveClass] != 0 {
		t.Fatalf("slot not released after the callback: active=%d", e.admission.active[interactiveClass])
	}
}

// The wrapper forwards the engine's default namespace, which annotation reads
// probe through an optional interface.
func TestAdmittedReaderForwardsTheDefaultNamespace(t *testing.T) {
	e := newAdmissionFixture(t, 1, 0, 4)
	reader := e.AdmitReader(fakeReader{namespace: "edge"})
	ns, ok := reader.(interface{ DefaultNamespace() string })
	if !ok {
		t.Fatal("wrapper does not expose DefaultNamespace")
	}
	if got := ns.DefaultNamespace(); got != "edge" {
		t.Fatalf("DefaultNamespace=%q, want edge", got)
	}
}

type fakeTraceDetail struct{ read func() }

func (f fakeTraceDetail) TraceDetail(context.Context, telemetry.TraceQuery) (telemetry.TraceDetail, error) {
	f.read()
	return telemetry.TraceDetail{}, nil
}

// An indexed trace read holds one interactive slot while it reads and is shed
// as ErrBusy when the lane is full, as the DuckDB log query it replaced was.
func TestAdmittedTraceDetailHoldsOneSlotAndSheds(t *testing.T) {
	e := newAdmissionFixture(t, 1, 0, 1)
	reader := e.AdmitTraceDetail(fakeTraceDetail{read: func() {
		if e.admission.active[interactiveClass] != 1 {
			t.Fatalf("slot not held during the read: active=%d", e.admission.active[interactiveClass])
		}
	}})
	if _, err := reader.TraceDetail(t.Context(), telemetry.TraceQuery{}); err != nil {
		t.Fatal(err)
	}
	if e.admission.active[interactiveClass] != 0 {
		t.Fatalf("slot not released: active=%d", e.admission.active[interactiveClass])
	}
	e.admission.bounds[interactiveClass] = 0
	if err := e.admission.acquire(t.Context(), interactiveClass); err != nil {
		t.Fatal(err)
	}
	shed := e.AdmitTraceDetail(fakeTraceDetail{read: func() { t.Fatal("read ran on a shed trace") }})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := shed.TraceDetail(ctx, telemetry.TraceQuery{}); !errors.Is(err, ErrBusy) {
		t.Fatalf("err=%v, want ErrBusy", err)
	}
}
