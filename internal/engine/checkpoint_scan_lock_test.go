package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// This is a physical-design control, independent of production cache schemas.
// In the pinned engine an indexed scan considers ART selection under the shared
// vacuum lock even when its compound index cannot answer the range predicate.
// A FULL checkpoint owns the exclusive lock during its debug sleep. The heap
// has the same rows and NOT NULL columns, but no index-selection lock to acquire.
func TestCheckpointCompoundIndexScanWaitsWhileHeapScanProgresses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scan-lock.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	// Cancel active statements before closing the shared native database.
	defer func() { cancel(); _ = db.Close() }()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != "v2.0.0" {
		t.Fatalf("proof requires pinned engine v2.0.0, got %q", version)
	}
	for _, statement := range []string{
		"SET threads=2",
		"SET memory_limit='256MB'",
		"CREATE TABLE scan_heap(k BIGINT NOT NULL, part BIGINT NOT NULL, value BIGINT NOT NULL)",
		"CREATE TABLE scan_compound(k BIGINT NOT NULL, part BIGINT NOT NULL, value BIGINT NOT NULL, PRIMARY KEY(k,part))",
		"INSERT INTO scan_heap SELECT i,0,i FROM range(256) AS r(i)",
		"INSERT INTO scan_compound SELECT * FROM scan_heap",
		"CHECKPOINT",
		"UPDATE scan_heap SET value=value+1",
		"UPDATE scan_compound SET value=value+1",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	checkpointConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer checkpointConn.Close()
	if _, err := checkpointConn.ExecContext(ctx, "SET debug_checkpoint_sleep_ms=1000"); err != nil {
		t.Fatal(err)
	}
	heapConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer heapConn.Close()
	compoundConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer compoundConn.Close()
	const query = "SELECT count(*) FROM %s WHERE k>=? AND k<?"
	for name, conn := range map[string]*sql.Conn{"scan_heap": heapConn, "scan_compound": compoundConn} {
		var count int
		if err := conn.QueryRowContext(ctx, fmt.Sprintf(query, name), 32, 128).Scan(&count); err != nil || count != 96 {
			t.Fatalf("preflight %s count=%d want96: %v", name, count, err)
		}
	}
	wal := path + ".wal"
	baseline, err := os.Stat(wal)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		count int
		at    time.Time
		err   error
	}
	checkpointDone := make(chan outcome, 1)
	heapDone, compoundDone := make(chan outcome, 1), make(chan outcome, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := checkpointConn.ExecContext(ctx, "CHECKPOINT")
		checkpointDone <- outcome{at: time.Now(), err: err}
	}()
	// Every launched native statement is joined on all failure paths. The debug
	// sleep is finite; caller cancellation remains available for actual I/O.
	defer func() { cancel(); <-finished }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stat, err := os.Stat(wal)
		if err == nil && stat.Size() > baseline.Size() {
			break
		}
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case result := <-checkpointDone:
			t.Fatalf("checkpoint returned before flushed WAL checkpoint-record barrier: %v", result.err)
		case <-ctx.Done():
			t.Fatalf("checkpoint-record barrier: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	// The WAL record is appended/flushed by WALStartCheckpoint before the
	// configured debug sleep. No guessed delay is used to start either read.
	readsFinished := make(chan struct{})
	go func() {
		defer close(readsFinished)
		start := make(chan struct{})
		inner := make(chan struct{})
		go func() {
			defer close(inner)
			close(start)
			var count int
			err := checkpointCompoundRangeScan(ctx, compoundConn, &count)
			compoundDone <- outcome{count: count, at: time.Now(), err: err}
		}()
		<-start
		// Observe the control in a native call before issuing the heap query,
		// so an unscheduled control goroutine cannot pass this proof vacuously.
		buffer := make([]byte, 1<<20)
		for {
			entered := false
			for stack := range strings.SplitSeq(string(buffer[:runtime.Stack(buffer, true)]), "\n\n") {
				if strings.Contains(stack, "checkpointCompoundRangeScan") && strings.Contains(stack, "_Cfunc_duckdb_execute_pending") {
					entered = true
					break
				}
			}
			if entered {
				break
			}
			select {
			case <-ctx.Done():
				heapDone <- outcome{err: ctx.Err(), at: time.Now()}
				<-inner
				return
			case <-ticker.C:
			}
		}
		var count int
		err := heapConn.QueryRowContext(ctx, fmt.Sprintf(query, "scan_heap"), 32, 128).Scan(&count)
		heapDone <- outcome{count: count, at: time.Now(), err: err}
		<-inner
	}()
	defer func() { cancel(); <-readsFinished }()
	heap := <-heapDone
	if heap.err != nil {
		t.Fatal(heap.err)
	}
	select {
	case checkpoint := <-checkpointDone:
		t.Fatalf("checkpoint returned before heap-progress barrier: %v", checkpoint.err)
	default:
	}
	select {
	case compound := <-compoundDone:
		t.Fatalf("compound-index control completed during checkpoint-active heap progress: count=%d error=%v", compound.count, compound.err)
	default:
	}
	checkpoint := <-checkpointDone
	compound := <-compoundDone
	for name, result := range map[string]outcome{"heap": heap, "compound": compound} {
		if result.err != nil || result.count != 96 {
			t.Fatalf("%s count=%d want96: %v", name, result.count, result.err)
		}
	}
	if checkpoint.err != nil {
		t.Fatal(checkpoint.err)
	}
	if !heap.at.Before(checkpoint.at) {
		t.Fatal("heap range scan failed to complete during the checkpoint-active interval")
	}
	// Native lock release precedes ExecContext returning to Go, so relative
	// Go return timestamps cannot safely order the final release and reader.
	t.Logf("heap completed while checkpoint/control pending; both exact counts=%d/%d; compound_minus_heap=%s", heap.count, compound.count, compound.at.Sub(heap.at))
}

// A distinct frame lets the barrier observe this control inside native code.
func checkpointCompoundRangeScan(ctx context.Context, conn *sql.Conn, count *int) error {
	return conn.QueryRowContext(ctx, "SELECT count(*) FROM scan_compound WHERE k>=? AND k<?", 32, 128).Scan(count)
}
