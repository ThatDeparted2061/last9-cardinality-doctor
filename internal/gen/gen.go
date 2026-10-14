// Package gen writes a synthetic Prometheus TSDB with planted cardinality
// offenders, sized like a mid-size production fleet.
//
// Two 10-minute windows ("earlier" and "now") are written, 60 s apart per
// sample. Well-behaved series exist in both; offenders churn, so most of
// their series in the "now" window are new — the growth the doctor ranks by.
//
// Planted offenders:
//   - http_requests_total: user_id per series (unbounded IDs)
//   - http_request_duration_seconds_bucket: route_path with numeric IDs in it
//   - session_events_total: session_id UUIDs
//   - batch_job_last_success_timestamp_seconds: run_id holding a Unix timestamp
//   - container_cpu_usage_seconds_total: pod_ip, one-to-one with pod (redundant)
package gen

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

// Config sizes the dataset. Scale multiplies every offender's cardinality;
// 1.0 is ~5M distinct series.
type Config struct {
	Dir   string
	Scale float64
	Start time.Time // start of the "earlier" window
	Seed  uint64
	Log   func(format string, a ...any)
}

const (
	Step    = time.Minute
	Samples = 10 // per series per window
	Window  = Step * Samples
)

var (
	services = []string{"checkout", "cart", "catalog", "search", "payments", "orders", "users", "auth", "inventory", "shipping",
		"pricing", "reviews", "recommendations", "notifications", "gateway", "media", "billing", "fraud", "loyalty", "support"}
	methods  = []string{"GET", "POST", "PUT", "DELETE"}
	routes   = []string{"/api/cart", "/api/items", "/api/orders", "/api/users", "/api/search", "/api/pay", "/api/login", "/api/stock", "/api/ship", "/api/price"}
	statuses = []string{"200", "201", "400", "404", "500"}
	les      = []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	events   = []string{"view", "click", "add_to_cart", "purchase", "scroll"}
	modes    = []string{"user", "system", "idle", "iowait", "irq", "softirq", "steal", "nice"}
)

// series is one series' identity plus how its value evolves.
type series struct {
	lbls    labels.Labels
	base    float64
	slope   float64 // counters grow by slope per step; gauges stay at base
	windows int     // bit 0: earlier, bit 1: now
}

// Generate writes the dataset and returns how many distinct series it wrote.
func Generate(ctx context.Context, c Config) (int, error) {
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return 0, err
	}
	rng := rand.New(rand.NewPCG(c.Seed, 1))
	n := func(x int) int { return max(1, int(float64(x)*c.Scale)) }
	var shard []series
	total := 0
	flush := func() error {
		if len(shard) == 0 {
			return nil
		}
		if err := writeBlock(ctx, c, shard); err != nil {
			return err
		}
		total += len(shard)
		c.Log("wrote %d series (total %d)", len(shard), total)
		shard = shard[:0]
		return nil
	}
	add := func(s series) error {
		shard = append(shard, s)
		if len(shard) >= 400_000 { // bounds the in-memory head per block
			return flush()
		}
		return nil
	}
	both, earlier, now := 3, 1, 2
	churn := func() int { // offenders: 45% only earlier, 45% only now, 10% both
		switch x := rng.Float64(); {
		case x < 0.45:
			return earlier
		case x < 0.90:
			return now
		}
		return both
	}

	// http_requests_total{service,method,route,status,user_id}: 4,000 base combos x users.
	users := n(500)
	combo := 0
	for _, svc := range services {
		for _, m := range methods {
			for _, r := range routes {
				for _, st := range statuses {
					combo++
					for u := range users { // distinct per combination, overlapping across them
						if err := add(series{lbls: labels.FromStrings("__name__", "http_requests_total", "service", svc, "method", m, "route", r, "status", st,
							"user_id", fmt.Sprintf("u-%d", 100000+(u*7919+combo*31)%900000)), base: float64(rng.IntN(1000)), slope: 1 + rng.Float64()*5, windows: churn()}); err != nil {
							return total, err
						}
					}
				}
			}
		}
	}
	// http_request_duration_seconds_bucket{service,route_path,le}: paths with IDs.
	paths := n(6000)
	for _, svc := range services {
		for p := range paths {
			path := fmt.Sprintf("/api/orders/%d/items/%d", 10000+p*7+rng.IntN(7), rng.IntN(40))
			cum := 0.0
			for _, le := range les {
				cum += float64(rng.IntN(20))
				if err := add(series{lbls: labels.FromStrings("__name__", "http_request_duration_seconds_bucket", "service", svc, "route_path", path, "le", le),
					base: cum, slope: cum / 10, windows: churn()}); err != nil {
					return total, err
				}
			}
		}
	}
	// session_events_total{tenant,event,session_id}: UUIDs.
	sessions := n(40_000)
	for t := range 5 {
		for _, ev := range events {
			for range sessions {
				if err := add(series{lbls: labels.FromStrings("__name__", "session_events_total", "tenant", fmt.Sprintf("tenant-%d", t), "event", ev,
					"session_id", uuid(rng)), base: float64(rng.IntN(5)), slope: rng.Float64(), windows: churn()}); err != nil {
					return total, err
				}
			}
		}
	}
	// batch_job_last_success_timestamp_seconds{job,run_id}: run_id is an epoch.
	runs := n(1500)
	for j := range 200 {
		for r := range runs {
			ts := c.Start.Add(-time.Duration(r) * time.Minute).Unix()
			if err := add(series{lbls: labels.FromStrings("__name__", "batch_job_last_success_timestamp_seconds", "job", fmt.Sprintf("etl-%03d", j),
				"run_id", fmt.Sprint(ts)), base: float64(ts), windows: churn()}); err != nil {
				return total, err
			}
		}
	}
	// container_cpu_usage_seconds_total{namespace,pod,container,pod_ip}: pod_ip is redundant.
	for ns := range 50 {
		for p := range 200 {
			ip := fmt.Sprintf("10.%d.%d.%d", ns, p/250, p%250+1)
			for _, ctr := range []string{"app", "sidecar", "init"} {
				if err := add(series{lbls: labels.FromStrings("__name__", "container_cpu_usage_seconds_total", "namespace", fmt.Sprintf("ns-%02d", ns),
					"pod", fmt.Sprintf("pod-%02d-%03d", ns, p), "container", ctr, "pod_ip", ip), base: rng.Float64() * 1000, slope: rng.Float64(), windows: both}); err != nil {
					return total, err
				}
			}
		}
	}
	// Well-behaved metrics.
	for i := range 500 {
		inst := fmt.Sprintf("node-%03d:9100", i)
		for cpu := range 8 {
			for _, m := range modes {
				if err := add(series{lbls: labels.FromStrings("__name__", "node_cpu_seconds_total", "instance", inst, "cpu", fmt.Sprint(cpu), "mode", m),
					base: rng.Float64() * 1e5, slope: rng.Float64() * 60, windows: both}); err != nil {
					return total, err
				}
			}
		}
	}
	for t := range 100 {
		for p := range 64 {
			for g := range 16 {
				if err := add(series{lbls: labels.FromStrings("__name__", "kafka_consumergroup_lag", "topic", fmt.Sprintf("topic-%02d", t), "partition", fmt.Sprint(p),
					"group", fmt.Sprintf("cg-%02d", g)), base: float64(rng.IntN(5000)), windows: both}); err != nil {
					return total, err
				}
			}
		}
	}
	return total, flush()
}

func uuid(rng *rand.Rand) string {
	return fmt.Sprintf("%08x-%04x-4%03x-%04x-%012x", rng.Uint32(), rng.Uint32()&0xffff, rng.Uint32()&0xfff, rng.Uint32()&0x3fff|0x8000, rng.Uint64()&0xffffffffffff)
}

// writeBlock writes one block holding shard's samples in both windows.
func writeBlock(ctx context.Context, c Config, shard []series) error {
	w, err := tsdb.NewBlockWriter(slog.New(slog.DiscardHandler), c.Dir, int64(4*time.Hour/time.Millisecond))
	if err != nil {
		return err
	}
	defer w.Close()
	app := w.Appender(ctx)
	refs := make([]storage.SeriesRef, len(shard))
	n := 0
	for win := range 2 {
		for i := range Samples {
			t := c.Start.Add(time.Duration(win)*Window + time.Duration(i)*Step)
			step := float64(win*Samples + i)
			for k, s := range shard {
				if s.windows&(1<<win) == 0 {
					continue
				}
				v := s.base + s.slope*step
				ref, err := app.Append(refs[k], s.lbls, t.UnixMilli(), v)
				if err != nil {
					return err
				}
				refs[k] = ref
				if n++; n%1_000_000 == 0 { // keep each commit's buffer small
					if err := app.Commit(); err != nil {
						return err
					}
					app = w.Appender(ctx)
				}
			}
		}
	}
	if err := app.Commit(); err != nil {
		return err
	}
	_, err = w.Flush(ctx)
	return err
}
