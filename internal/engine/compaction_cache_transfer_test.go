package engine

import (
	"testing"
	"time"

	"github.com/labstack/pitot/internal/engine/writegate"
)

// A busy background lease must defer reuse of complete input contributions,
// rather than make the refresh worker scan and aggregate the replacement again.
func TestCompactionDefersWarmCacheTransfer(t *testing.T) {
	for _, cacheReader := range []bool{false, true} {
		t.Run(map[bool]string{false: "analytical", true: "cache"}[cacheReader], func(t *testing.T) {
			d, repo := newBatchCacheTest(t)
			commitCurrentBatches(t, repo, "input", 24, time.Now().UTC().Add(-2*time.Minute))
			if _, err := d.RefreshReadCaches(t.Context()); err != nil {
				t.Fatal(err)
			}
			func() {
				unlock := d.writeGate.Lock(writegate.RollupEdge)
				if cacheReader {
					unlock()
					unlock = d.cacheGate.Lock(writegate.ReadCache)
				}
				defer unlock()
				background, err := d.lockBackground(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer background()
				for range 3 {
					if n, err := repo.CompactParquet(t.Context(), d, 8); err != nil || n != 8 {
						t.Fatalf("compacted %d: %v", n, err)
					}
				}
			}()
			n, err := d.RefreshReadCaches(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("rebuilt %d contributions from already cached inputs", n)
			}
			var exemplars, owners, markers int
			if err := d.DB.QueryRow(exemplarOwnersQuery).Scan(&exemplars, &owners, &markers); err != nil {
				t.Fatal(err)
			}
			if exemplars != 24 || owners != 3 || markers != 3 {
				t.Fatalf("exemplars=%d owners=%d markers=%d", exemplars, owners, markers)
			}
		})
	}
}

func TestDeferredCacheTransferFollowsCompactionGenerations(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	commitCurrentBatches(t, repo, "input", 64, time.Now().UTC().Add(-2*time.Minute))
	if _, err := d.RefreshReadCaches(t.Context()); err != nil {
		t.Fatal(err)
	}
	func() {
		unlock := d.cacheGate.Lock(writegate.ReadCache)
		defer unlock()
		background, err := d.lockBackground(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer background()
		for range 9 {
			if n, err := repo.CompactParquet(t.Context(), d, 8); err != nil || n != 8 {
				t.Fatalf("compacted %d: %v", n, err)
			}
		}
	}()
	n, err := d.RefreshReadCaches(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rebuilt %d contributions across warm generations", n)
	}
	var exemplars, owners, markers int
	if err := d.DB.QueryRow(exemplarOwnersQuery).Scan(&exemplars, &owners, &markers); err != nil {
		t.Fatal(err)
	}
	if exemplars == 0 || owners != 1 || markers != 1 {
		t.Fatalf("exemplars=%d owners=%d markers=%d", exemplars, owners, markers)
	}
}

// Exemplar rows, the batches owning them, and the cached batches: transferred
// rows belong only to the active output.
const exemplarOwnersQuery = `SELECT (SELECT count(*) FROM read_trace_exemplars),(SELECT count(DISTINCT batch_id) FROM read_trace_exemplars),(SELECT count(*) FROM read_batches)`
