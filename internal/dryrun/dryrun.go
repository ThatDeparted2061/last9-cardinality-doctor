// Package dryrun proves a plan before anyone deploys it: it applies the plan
// to a copy of the data (dropping, normalising and aggregating exactly as the
// generated collector config would), then runs every dashboard query with the
// real PromQL engine against the original and the reduced data and compares
// the results.
package dryrun

import (
	"context"
	"fmt"
	"hash/maphash"
	"log/slog"
	"math"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/analyze"
	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/fix"
)

// QueryResult compares one dashboard query.
type QueryResult struct {
	Name       string  `json:"name"`
	Expr       string  `json:"expr"`
	Instants   int     `json:"instants_compared"`
	Series     int     `json:"result_series"` // per instant, original data
	Equal      bool    `json:"equal"`
	MaxRelDiff float64 `json:"max_relative_difference"`
	Note       string  `json:"note,omitempty"`
}

// Result is the dry run's verdict.
type Result struct {
	SeriesBefore int           `json:"series_before"`
	SeriesAfter  int           `json:"series_after"`
	Reduction    float64       `json:"reduction"`
	Instants     []time.Time   `json:"instants"`
	Queries      []QueryResult `json:"queries"`
	AllEqual     bool          `json:"all_queries_equal"`
}

// Tolerance is the relative float difference treated as equal (summation
// order changes the last bits).
const Tolerance = 1e-9

// Run applies plan to src into workDir and compares the dashboards.
func Run(ctx context.Context, src *analyze.TSDB, plan fix.Plan, ds []fix.Dashboard, workDir string, lookback time.Duration, log func(string, ...any)) (Result, error) {
	var r Result
	os.RemoveAll(workDir)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return r, err
	}
	churn, before, after, err := apply(ctx, src, plan, workDir, log)
	if err != nil {
		return r, err
	}
	r.SeriesBefore, r.SeriesAfter = before, after
	r.Reduction = 1 - float64(after)/float64(before)

	reduced, err := analyze.OpenTSDB(workDir, 0)
	if err != nil {
		return r, err
	}
	defer reduced.Close()

	// Compare at every minute where no series started or stopped inside the
	// query's lookback: there, aggregation at ingest must give identical
	// answers. (Across a churn boundary, Prometheus extrapolates rate() per
	// series, which is exactly why the collector aggregates deltas.)
	window := lookback + time.Minute
	for t := src.MinT + window.Milliseconds(); t <= src.MaxT; t += time.Minute.Milliseconds() {
		clean := true
		for c := range churn {
			if c > t-window.Milliseconds() && c <= t {
				clean = false
				break
			}
		}
		if clean {
			r.Instants = append(r.Instants, time.UnixMilli(t))
		}
	}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100_000_000, Timeout: 5 * time.Minute, LookbackDelta: lookback})
	orig := storage.QueryableFunc(src.Querier)
	red := storage.QueryableFunc(reduced.Querier)
	r.AllEqual = len(r.Instants) > 0
	for _, d := range ds {
		qr := QueryResult{Name: d.Name, Expr: d.Expr, Equal: true}
		for _, at := range r.Instants {
			a, err := eval(ctx, engine, orig, d.Expr, at)
			if err != nil {
				return r, fmt.Errorf("%s on original data: %w", d.Name, err)
			}
			b, err := eval(ctx, engine, red, d.Expr, at)
			if err != nil {
				return r, fmt.Errorf("%s on reduced data: %w", d.Name, err)
			}
			eq, diff, note := compare(a, b)
			qr.Instants++
			qr.Series = max(qr.Series, len(a))
			qr.MaxRelDiff = max(qr.MaxRelDiff, diff)
			if !eq {
				qr.Equal, qr.Note = false, note
			}
		}
		r.AllEqual = r.AllEqual && qr.Equal
		r.Queries = append(r.Queries, qr)
		log("query %-40s %d instants, %d series each, equal=%v", d.Name, qr.Instants, qr.Series, qr.Equal)
	}
	return r, nil
}

func eval(ctx context.Context, e *promql.Engine, q storage.Queryable, expr string, at time.Time) (promql.Vector, error) {
	qry, err := e.NewInstantQuery(ctx, q, nil, expr, at)
	if err != nil {
		return nil, err
	}
	defer qry.Close()
	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	v, err := res.Vector()
	if err != nil {
		return nil, err
	}
	return slices.Clone(v), nil
}

func compare(a, b promql.Vector) (bool, float64, string) {
	key := func(s promql.Sample) string { return s.Metric.String() }
	sort.Slice(a, func(i, j int) bool { return key(a[i]) < key(a[j]) })
	sort.Slice(b, func(i, j int) bool { return key(b[i]) < key(b[j]) })
	if len(a) != len(b) {
		return false, math.Inf(1), fmt.Sprintf("%d result series on the original data, %d after reduction", len(a), len(b))
	}
	worst := 0.0
	for i := range a {
		if key(a[i]) != key(b[i]) {
			return false, math.Inf(1), fmt.Sprintf("series %s vs %s", key(a[i]), key(b[i]))
		}
		x, y := a[i].F, b[i].F
		if math.IsNaN(x) && math.IsNaN(y) {
			continue
		}
		d := math.Abs(x-y) / math.Max(1e-12, math.Max(math.Abs(x), math.Abs(y)))
		worst = max(worst, d)
	}
	if worst > Tolerance {
		return false, worst, fmt.Sprintf("values differ by up to %.3g (relative)", worst)
	}
	return true, worst, ""
}

type aggSeries struct {
	lbls labels.Labels
	vals map[int64]float64
}

// apply writes the reduced data and returns the times at which any series
// started or stopped, and the series counts before and after.
func apply(ctx context.Context, src *analyze.TSDB, plan fix.Plan, dir string, log func(string, ...any)) (map[int64]bool, int, int, error) {
	plans := map[string]fix.MetricPlan{}
	for _, mp := range plan.Metrics {
		plans[mp.Metric] = mp
	}
	names, err := src.Metrics()
	if err != nil {
		return nil, 0, 0, err
	}
	w, err := tsdb.NewBlockWriter(slog.New(slog.DiscardHandler), dir, int64(24*time.Hour/time.Millisecond))
	if err != nil {
		return nil, 0, 0, err
	}
	defer w.Close()
	churn := map[int64]bool{}
	seed := maphash.MakeSeed()
	before, after := 0, 0
	for _, name := range names {
		mp, planned := plans[name]
		agg := map[uint64]*aggSeries{}
		start := before
		b := labels.NewBuilder(labels.EmptyLabels())
		err := src.EachSamples(name, func(l labels.Labels, ts []int64, vs []float64) {
			before++
			if len(ts) > 0 {
				churn[ts[0]], churn[ts[len(ts)-1]+1] = true, true // first sample, and just after the last
			}
			b.Reset(l)
			if planned {
				for _, d := range mp.Drop {
					b.Del(d)
				}
				for _, d := range mp.DropSafe {
					b.Del(d)
				}
				for _, n := range mp.Normalize {
					b.Set(n, analyze.NormalizePath(l.Get(n)))
				}
			}
			nl := b.Labels()
			h := maphash.Bytes(seed, []byte(nl.String()))
			s := agg[h]
			if s == nil {
				s = &aggSeries{lbls: nl, vals: map[int64]float64{}}
				agg[h] = s
			}
			for i, t := range ts {
				old, seen := s.vals[t]
				switch {
				case !seen:
					s.vals[t] = vs[i]
				case mp.Aggregation == "max":
					s.vals[t] = math.Max(old, vs[i])
				default:
					s.vals[t] = old + vs[i]
				}
			}
		})
		if err != nil {
			return nil, 0, 0, err
		}
		app := w.Appender(ctx)
		for _, s := range agg {
			ts := make([]int64, 0, len(s.vals))
			for t := range s.vals {
				ts = append(ts, t)
			}
			slices.Sort(ts)
			var ref storage.SeriesRef
			for _, t := range ts {
				if ref, err = app.Append(ref, s.lbls, t, s.vals[t]); err != nil {
					return nil, 0, 0, err
				}
			}
		}
		if err := app.Commit(); err != nil {
			return nil, 0, 0, err
		}
		after += len(agg)
		log("%-45s %9d -> %7d series", name, before-start, len(agg))
	}
	if _, err := w.Flush(ctx); err != nil {
		return nil, 0, 0, err
	}
	// The windows' outer edges are not churn.
	delete(churn, src.MinT)
	delete(churn, src.MaxT)
	delete(churn, src.MaxT+1)
	return churn, before, after, nil
}
