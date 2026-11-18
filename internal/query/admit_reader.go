package query

import (
	"context"
	"errors"
	"sync"

	"github.com/labstack/pitot/internal/queryrows"
)

// AdmitReader wraps a read surface so every read it issues passes through the
// interactive admission lane. The observability readers, annotations, and
// variable-option queries otherwise call the engine directly and compete for
// the read pool without being admitted, so a saturating dashboard oversubscribes
// the pool, starves admitted panels, and defeats the deadline shedding the pool
// size is meant to enforce. Routing those reads through admission restores the
// invariant that no more reads run concurrently than the pool can serve, and
// turns excess load into a bounded ErrBusy shed rather than unbounded queueing
// inside the engine.
//
// The admission slot is held for the lifetime of the returned rows and released
// when they close, mirroring how the engine holds a pool connection until its
// rows close. Callers already close every result set before issuing the next
// read, so one interactive slot is never held across acquiring another.
func (e *Executor) AdmitReader(inner queryrows.Queryer) queryrows.Queryer {
	return &admittingQueryer{inner: inner, exec: e}
}

type admittingQueryer struct {
	inner queryrows.Queryer
	exec  *Executor
}

func (a *admittingQueryer) QueryContext(ctx context.Context, query string, args ...any) (queryrows.Rows, error) {
	release, err := a.exec.admitInteractive(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := a.inner.QueryContext(ctx, query, args...)
	if err != nil {
		release()
		return nil, err
	}
	return &admittedRows{Rows: rows, release: release}, nil
}

// WithReadTransaction admits the whole callback as one unit and runs it on the
// wrapped surface's own read transaction, so a reader that needs a single
// consistent snapshot (annotations reads deploys, anomalies, and the watermark
// together) keeps it and takes one slot for the run rather than one per
// sub-read. A surface without transaction support runs the callback against
// this admitting queryer instead, so its reads are still admitted.
func (a *admittingQueryer) WithReadTransaction(ctx context.Context, read func(queryrows.Queryer) error) error {
	rt, ok := a.inner.(queryrows.ReadTransactioner)
	if !ok {
		return read(a)
	}
	release, err := a.exec.admitInteractive(ctx)
	if err != nil {
		return err
	}
	defer release()
	return rt.WithReadTransaction(ctx, read)
}

// DefaultNamespace forwards to the wrapped surface when it exposes one, matching
// the optional-interface probe callers use, and is empty otherwise.
func (a *admittingQueryer) DefaultNamespace() string {
	if d, ok := a.inner.(interface{ DefaultNamespace() string }); ok {
		return d.DefaultNamespace()
	}
	return ""
}

// admitInteractive takes one interactive slot, bounding the wait by the batch
// timeout as the exemplar and trace-detail path does, so a deadline-less caller
// (a background or MCP read) cannot sit in the bounded queue indefinitely. A
// full queue or an expired wait is counted as an interactive refusal and
// returned as ErrBusy, except when the caller's own context ended first. These
// refusals share the executor's interactive-pressure counter, so a flood of
// routed observability or annotation reads also feeds ingest backpressure —
// intended, since interactive read starvation is exactly when ingest should
// yield.
func (e *Executor) admitInteractive(ctx context.Context) (func(), error) {
	wait, cancel := context.WithTimeout(ctx, e.batchTimeout)
	defer cancel()
	err := e.acquireClass(wait, interactiveClass)
	if errors.Is(err, errQueueFull) || errors.Is(err, context.DeadlineExceeded) {
		e.interactiveRefusals.Add(1)
	}
	switch {
	case err == nil:
		return func() { e.admission.release(interactiveClass) }, nil
	case ctx.Err() != nil:
		return nil, ctx.Err()
	default:
		return nil, ErrBusy
	}
}

// admittedRows releases the admission slot when the wrapped rows close. Close is
// idempotent, so a double close or a close after exhaustion releases the slot
// exactly once.
type admittedRows struct {
	queryrows.Rows
	release func()
	once    sync.Once
}

func (r *admittedRows) Close() error {
	err := r.Rows.Close()
	r.once.Do(r.release)
	return err
}
