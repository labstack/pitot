package query

import (
	"math"
	"strings"
	"testing"
)

// Expanding an empty map must not remove the aggregate group's count or turn
// an absent quantile into zero. Zero-duration bins remain real measurements.
func TestHistogramExpansionPreservesEmptyGroupsAndZeroBins(t *testing.T) {
	d, _ := newTestEngine(t)
	var query strings.Builder
	query.WriteString(`WITH input(service,spans,hist) AS (VALUES
 ('empty',2::BIGINT,map([]::INTEGER[],[]::UBIGINT[])),
 ('null',3::BIGINT,NULL::MAP(INTEGER,UBIGINT)),
 ('zero',4::BIGINT,map([-2147483648],[4]::UBIGINT[])),
 ('positive',5::BIGINT,map([8,16],[2,3]::UBIGINT[])))`)
	from := histogramAggregate(&query, "checked", "input",
		[]string{"service", "sum(spans)::DOUBLE AS n", "NULL::DOUBLE AS p50", "NULL::DOUBLE AS p95"},
		[]string{"service", "n", "p50", "p95"}, []string{"service"}, false,
		map[int]Measure{2: {Q: .5}, 3: {Q: .95}})
	query.WriteString(" SELECT service,n,p50,p95 FROM " + from + " ORDER BY service")
	rows, err := d.DB.QueryContext(t.Context(), query.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	lower := math.Exp2(15.0 / 8)
	want := []struct {
		service     string
		count       float64
		median, p95 *float64
	}{
		{service: "empty", count: 2}, {service: "null", count: 3},
		{service: "positive", count: 5, median: histogramTestFloat(lower + (4-lower)/6), p95: histogramTestFloat(lower + (4-lower)*11/12)},
		{service: "zero", count: 4, median: histogramTestFloat(0), p95: histogramTestFloat(0)},
	}
	for _, expected := range want {
		if !rows.Next() {
			t.Fatalf("missing group %s: %v", expected.service, rows.Err())
		}
		var service string
		var count float64
		var median, p95 *float64
		if err := rows.Scan(&service, &count, &median, &p95); err != nil {
			t.Fatal(err)
		}
		if service != expected.service || count != expected.count {
			t.Fatalf("group=%s count=%g want=%s/%g", service, count, expected.service, expected.count)
		}
		for i, pair := range [][2]*float64{{median, expected.median}, {p95, expected.p95}} {
			if pair[0] == nil || pair[1] == nil {
				if pair[0] != pair[1] {
					t.Fatalf("%s quantile%d null changed", service, i)
				}
				continue
			}
			if math.Abs(*pair[0]-*pair[1]) > 1e-12 {
				t.Fatalf("%s quantile%d=%g want=%g", service, i, *pair[0], *pair[1])
			}
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected groups/error: %v", rows.Err())
	}
}

func histogramTestFloat(value float64) *float64 { return &value }
