// Command cardinality-doctor finds what is blowing up a Prometheus/OTel
// metrics bill and writes the config that fixes it.
//
//	cardinality-doctor generate -out data/tsdb            # synthetic 5M-series dataset with planted offenders
//	cardinality-doctor analyze  -tsdb data/tsdb           # rank metrics and labels, flag unbounded ones
//	cardinality-doctor fix      -tsdb data/tsdb -dashboards examples/dashboards.yaml -out fixes
//	cardinality-doctor dry-run  -tsdb data/tsdb -dashboards examples/dashboards.yaml -plan fixes/plan.json
//	cardinality-doctor mcp      -tsdb data/tsdb           # serve the analysis as an MCP tool over stdio
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/analyze"
	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/dryrun"
	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/fix"
	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/gen"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cardinality-doctor generate|analyze|fix|dry-run|mcp [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "generate":
		err = generate(args)
	case "analyze":
		err = analyzeCmd(args)
	case "fix":
		err = fixCmd(args)
	case "dry-run":
		err = dryRun(args)
	case "mcp":
		err = mcp(args)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func logf(f string, a ...any) { log.Printf(f, a...) }

func generate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	out := fs.String("out", "data/tsdb", "TSDB directory to write")
	scale := fs.Float64("scale", 1, "1.0 = ~5M series")
	fs.Parse(args)
	start := time.Now().Add(-30 * time.Minute).Truncate(time.Minute)
	t0 := time.Now()
	n, err := gen.Generate(context.Background(), gen.Config{Dir: *out, Scale: *scale, Start: start, Seed: 42, Log: logf})
	if err == nil {
		logf("generated %d series in %s into %s", n, time.Since(t0).Round(time.Second), *out)
	}
	return err
}

type costFlags struct {
	interval time.Duration
	price    float64
}

func (c *costFlags) register(fs *flag.FlagSet) {
	fs.DurationVar(&c.interval, "scrape-interval", 30*time.Second, "scrape interval, for samples per month")
	fs.Float64Var(&c.price, "price", 0, "USD per million samples ingested (0: show volumes only)")
}

// samplesPerMonth is what one series costs in ingested samples.
func (c costFlags) samplesPerMonth(series int) float64 {
	return float64(series) * (30 * 24 * time.Hour).Seconds() / c.interval.Seconds()
}

func (c costFlags) describe(series int) string {
	s := c.samplesPerMonth(series)
	gb := s * 1.3 / 1e9 // ~1.3 bytes per compressed sample
	out := fmt.Sprintf("%s samples/mo, ~%.1f GB/mo", human(s), gb)
	if c.price > 0 {
		out += fmt.Sprintf(", ~$%s/mo", human(s/1e6*c.price))
	}
	return out
}

func human(v float64) string {
	switch {
	case v >= 1e12:
		return fmt.Sprintf("%.1fT", v/1e12)
	case v >= 1e9:
		return fmt.Sprintf("%.1fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	}
	return fmt.Sprintf("%.0f", v)
}

// source opens the input: a TSDB directory or an OTLP JSON export.
func source(tsdbDir, otlp string) (analyze.Source, func(), error) {
	if otlp != "" {
		s, err := analyze.OpenOTLP(otlp)
		return s, func() {}, err
	}
	t, err := analyze.OpenTSDB(tsdbDir, 0)
	if err != nil {
		return nil, nil, err
	}
	return t, func() { t.Close() }, nil
}

func analyzeCmd(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ExitOnError)
	dir := fs.String("tsdb", "data/tsdb", "Prometheus TSDB directory (snapshot or data dir)")
	otlp := fs.String("otlp", "", "OTLP/JSON metrics export to read instead of a TSDB")
	top := fs.Int("top", 15, "metrics to show")
	jsonOut := fs.String("json", "", "also write the full analysis as JSON here")
	minDistinct := fs.Int("min-distinct", 1000, "values a label needs before it can be called unbounded")
	var cost costFlags
	cost.register(fs)
	fs.Parse(args)
	src, closer, err := source(*dir, *otlp)
	if err != nil {
		return err
	}
	defer closer()
	t0 := time.Now()
	ms, err := analyze.Analyze(src, analyze.Options{MinDistinct: *minDistinct})
	if err != nil {
		return err
	}
	total := 0
	for _, m := range ms {
		total += m.Series
	}
	fmt.Printf("%s series across %d metrics (%s), analysed in %s\n\n", human(float64(total)), len(ms), cost.describe(total), time.Since(t0).Round(time.Millisecond))
	render(os.Stdout, ms[:min(*top, len(ms))], cost)
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(ms, "", "  ")
		return os.WriteFile(*jsonOut, b, 0o644)
	}
	return nil
}

func render(w io.Writer, ms []analyze.Metric, cost costFlags) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "METRIC\tSERIES\tGROWTH\tCOST\tFINDINGS")
	for _, m := range ms {
		var f []string
		for _, l := range m.Labels {
			switch {
			case l.Class == "redundant":
				f = append(f, fmt.Sprintf("%s is 1:1 with %s (safe to drop)", l.Name, l.SameAs))
			case l.Class != "" && l.SeriesWithout > 0:
				f = append(f, fmt.Sprintf("%s: %s values, %s (e.g. %s) -> %s series without it", l.Name, human(float64(l.Distinct)), l.Class, first(l.Examples), human(float64(l.SeriesWithout))))
			}
		}
		if len(f) == 0 {
			f = []string{"ok"}
		}
		fmt.Fprintf(tw, "%s\t%s\t%+.0f%%\t%s\t%s\n", m.Name, human(float64(m.Series)), m.Growth*100, cost.describe(m.Series), strings.Join(f, "; "))
	}
	tw.Flush()
}

func first(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

func fixCmd(args []string) error {
	fs := flag.NewFlagSet("fix", flag.ExitOnError)
	dir := fs.String("tsdb", "data/tsdb", "Prometheus TSDB directory")
	otlp := fs.String("otlp", "", "OTLP/JSON metrics export to read instead")
	dash := fs.String("dashboards", "", "YAML list of dashboard queries whose labels must be kept")
	out := fs.String("out", "fixes", "directory for plan.json, otel-collector.yaml, prometheus-relabel.yaml")
	fs.Parse(args)
	src, closer, err := source(*dir, *otlp)
	if err != nil {
		return err
	}
	defer closer()
	ms, err := analyze.Analyze(src, analyze.Options{MinDistinct: 1000})
	if err != nil {
		return err
	}
	var ds []fix.Dashboard
	if *dash != "" {
		if ds, err = fix.LoadDashboards(*dash); err != nil {
			return err
		}
	}
	used, err := fix.UsedLabels(ds)
	if err != nil {
		return err
	}
	plan := fix.Build(ms, used)
	os.MkdirAll(*out, 0o755)
	pb, _ := json.MarshalIndent(plan, "", "  ")
	otel, err := fix.OTelCollector(plan, ms)
	if err != nil {
		return err
	}
	prom, err := fix.PrometheusRelabel(plan)
	if err != nil {
		return err
	}
	for name, b := range map[string][]byte{"plan.json": pb, "otel-collector.yaml": otel, "prometheus-relabel.yaml": prom} {
		if err := os.WriteFile(filepath.Join(*out, name), b, 0o644); err != nil {
			return err
		}
	}
	for _, mp := range plan.Metrics {
		fmt.Printf("%-42s drop %v normalise %v drop-redundant %v keep %v (%s)\n", mp.Metric, mp.Drop, mp.Normalize, mp.DropSafe, mp.Kept, mp.Aggregation)
	}
	fmt.Printf("\nwrote %s/{plan.json,otel-collector.yaml,prometheus-relabel.yaml}\n", *out)
	return nil
}

func dryRun(args []string) error {
	fs := flag.NewFlagSet("dry-run", flag.ExitOnError)
	dir := fs.String("tsdb", "data/tsdb", "Prometheus TSDB directory")
	planPath := fs.String("plan", "fixes/plan.json", "plan from `fix`")
	dash := fs.String("dashboards", "examples/dashboards.yaml", "dashboard queries to compare")
	work := fs.String("work", "data/reduced", "where to write the reduced copy")
	out := fs.String("out", "", "write the JSON result here")
	fs.Parse(args)
	var plan fix.Plan
	b, err := os.ReadFile(*planPath)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &plan); err != nil {
		return err
	}
	ds, err := fix.LoadDashboards(*dash)
	if err != nil {
		return err
	}
	src, err := analyze.OpenTSDB(*dir, 0)
	if err != nil {
		return err
	}
	defer src.Close()
	r, err := dryrun.Run(context.Background(), src, plan, ds, *work, 5*time.Minute, logf)
	if err != nil {
		return err
	}
	fmt.Printf("\nseries: %d -> %d (%.1f%% fewer); %d dashboard queries x %d instants: all equal = %v\n",
		r.SeriesBefore, r.SeriesAfter, r.Reduction*100, len(r.Queries), len(r.Instants), r.AllEqual)
	if *out != "" {
		b, _ := json.MarshalIndent(r, "", "  ")
		os.WriteFile(*out, b, 0o644)
	}
	if !r.AllEqual {
		return fmt.Errorf("some dashboard queries changed")
	}
	return nil
}
