// Package fix turns an analysis into a reduction plan and the configs that
// apply it: OpenTelemetry Collector processors (aggregation at ingest) and
// Prometheus metric_relabel_configs (only for drops that merge no series).
//
// A label is never touched if a dashboard query uses it: the queries are
// parsed with the PromQL parser and every label they group by, match on or
// join on is protected.
package fix

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"

	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/analyze"
)

// Dashboard is one saved query.
type Dashboard struct {
	Name string `yaml:"name" json:"name"`
	Expr string `yaml:"expr" json:"expr"`
}

// LoadDashboards reads a YAML list of {name, expr}.
func LoadDashboards(path string) ([]Dashboard, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ds []Dashboard
	return ds, yaml.Unmarshal(b, &ds)
}

// Used maps metric -> labels a query depends on ("*" = all of them).
type Used map[string]map[string]bool

// UsedLabels walks every query's syntax tree. A label counts as used if a
// selector matches on it, or an aggregation or vector match directly above
// the raw series keeps it.
func UsedLabels(ds []Dashboard) (Used, error) {
	used := Used{}
	mark := func(metric, label string) {
		if used[metric] == nil {
			used[metric] = map[string]bool{}
		}
		used[metric][label] = true
	}
	for _, d := range ds {
		expr, err := parser.NewParser(parser.Options{}).ParseExpr(d.Expr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", d.Name, err)
		}
		// Labels named in grouping clauses apply to every metric below them.
		var walk func(n parser.Node, grouping []string, all bool)
		walk = func(n parser.Node, grouping []string, all bool) {
			switch e := n.(type) {
			case *parser.AggregateExpr:
				// Below an aggregation only its own grouping matters: whatever
				// the outer query does, it sees the aggregated labels.
				grouping, all = slices.Clone(e.Grouping), e.Without // without(...) keeps every other label
			case *parser.BinaryExpr:
				if e.VectorMatching != nil {
					grouping = append(slices.Clone(grouping), e.VectorMatching.MatchingLabels...)
					grouping = append(grouping, e.VectorMatching.Include...)
					if !e.VectorMatching.On {
						all = true // ignoring(...) matches on everything else
					}
				}
			case *parser.VectorSelector:
				for _, m := range e.LabelMatchers {
					if m.Name != "__name__" {
						mark(e.Name, m.Name)
					}
				}
				for _, g := range grouping {
					mark(e.Name, g)
				}
				if all {
					mark(e.Name, "*")
				}
			}
			for _, c := range parser.Children(n) {
				walk(c, grouping, all)
			}
		}
		walk(expr, nil, false)
	}
	return used, nil
}

// MetricPlan is what to do with one metric.
type MetricPlan struct {
	Metric       string   `json:"metric"`
	Drop         []string `json:"drop,omitempty"`           // aggregate these labels away
	Normalize    []string `json:"normalize,omitempty"`      // replace ID path segments with {id}, then aggregate
	DropSafe     []string `json:"drop_redundant,omitempty"` // one-to-one with another label: plain drop
	Aggregation  string   `json:"aggregation,omitempty"`    // sum | max
	Kept         []string `json:"kept_because_dashboards_use,omitempty"`
	SeriesBefore int      `json:"series_before"`
	SeriesAfter  int      `json:"series_after_estimate"`
}

// Plan is the whole reduction.
type Plan struct {
	Metrics      []MetricPlan `json:"metrics"`
	SeriesBefore int          `json:"series_before"`
	SeriesAfter  int          `json:"series_after_estimate"`
}

func aggregationFor(metric string) string {
	if strings.HasSuffix(metric, "_timestamp_seconds") || strings.HasSuffix(metric, "_timestamp") || strings.Contains(metric, "last_") {
		return "max" // "when did it last happen" survives as the latest time
	}
	return "sum" // counters, histogram buckets, sizes
}

// Build makes a plan from the analysis, protecting labels dashboards use.
func Build(ms []analyze.Metric, used Used) Plan {
	var p Plan
	for _, m := range ms {
		p.SeriesBefore += m.Series
		mp := MetricPlan{Metric: m.Name, SeriesBefore: m.Series, SeriesAfter: m.Series, Aggregation: aggregationFor(m.Name)}
		u := used[m.Name]
		for _, l := range m.Labels {
			if l.Class == "" {
				continue
			}
			if u["*"] || u[l.Name] {
				mp.Kept = append(mp.Kept, l.Name)
				continue
			}
			switch l.Class {
			case "redundant":
				mp.DropSafe = append(mp.DropSafe, l.Name)
			case "url_with_ids":
				mp.Normalize = append(mp.Normalize, l.Name)
				mp.SeriesAfter = min(mp.SeriesAfter, l.SeriesWithout)
			case "ip":
			default:
				mp.Drop = append(mp.Drop, l.Name)
				mp.SeriesAfter = min(mp.SeriesAfter, l.SeriesWithout)
			}
		}
		if len(mp.Drop)+len(mp.Normalize) > 1 {
			mp.SeriesAfter = -1 // several labels at once: the dry run measures it
		}
		if len(mp.Drop)+len(mp.Normalize)+len(mp.DropSafe) > 0 || len(mp.Kept) > 0 {
			p.Metrics = append(p.Metrics, mp)
		}
		if mp.SeriesAfter >= 0 {
			p.SeriesAfter += mp.SeriesAfter
		} else {
			p.SeriesAfter += m.Series
		}
	}
	sort.Slice(p.Metrics, func(i, j int) bool { return p.Metrics[i].SeriesBefore > p.Metrics[j].SeriesBefore })
	return p
}

// labelsAfter lists a metric's labels once the plan's drops are applied.
func labelsAfter(m analyze.Metric, mp MetricPlan) []string {
	var out []string
	for _, l := range m.Labels {
		if !slices.Contains(mp.Drop, l.Name) && !slices.Contains(mp.DropSafe, l.Name) {
			out = append(out, l.Name)
		}
	}
	slices.Sort(out)
	return out
}

// OTelCollector renders the processors that apply the plan at ingest, in an
// OpenTelemetry Collector pipeline.
func OTelCollector(p Plan, ms []analyze.Metric) ([]byte, error) {
	byName := map[string]analyze.Metric{}
	for _, m := range ms {
		byName[m.Name] = m
	}
	var stmts []string
	var transforms []map[string]any
	for _, mp := range p.Metrics {
		if len(mp.Drop)+len(mp.Normalize)+len(mp.DropSafe) == 0 {
			continue
		}
		otelName := strings.TrimSuffix(mp.Metric, "_bucket") // Prometheus histograms are one OTel metric
		for _, l := range mp.Normalize {
			stmts = append(stmts, fmt.Sprintf(`replace_pattern(datapoint.attributes["%s"], "/([0-9]+|[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}|[0-9a-fA-F]{12,})(/|$)", "/{id}$$2") where metric.name == "%s"`, l, otelName))
		}
		keep := labelsAfter(byName[mp.Metric], mp)
		keep = slices.DeleteFunc(keep, func(s string) bool { return s == "le" })
		transforms = append(transforms, map[string]any{
			"include": otelName,
			"action":  "update",
			"operations": []map[string]any{{
				"action":           "aggregate_labels",
				"label_set":        keep,
				"aggregation_type": mp.Aggregation,
			}},
		})
	}
	cfg := map[string]any{
		"processors": map[string]any{
			// Counters must be aggregated as deltas, or a series appearing
			// mid-window would look like a jump in the aggregate.
			"cumulativetodelta": map[string]any{},
			"transform/cardinality": map[string]any{
				"error_mode":        "ignore",
				"metric_statements": []map[string]any{{"context": "datapoint", "statements": stmts}},
			},
			"metricstransform/cardinality": map[string]any{"transforms": transforms},
			"deltatocumulative":            map[string]any{},
		},
		"service": map[string]any{"pipelines": map[string]any{"metrics": map[string]any{
			"processors": []string{"cumulativetodelta", "transform/cardinality", "metricstransform/cardinality", "deltatocumulative", "batch"},
		}}},
	}
	b, err := yaml.Marshal(cfg)
	return append([]byte("# Generated by cardinality-doctor: merge into your OpenTelemetry Collector config.\n"), b...), err
}

// PrometheusRelabel renders metric_relabel_configs for the drops Prometheus
// can do safely at scrape time: labels one-to-one with another label (no
// series merge). Aggregating drops are listed as comments: Prometheus cannot
// sum at ingest, so they belong in the collector (or stream aggregation).
func PrometheusRelabel(p Plan) ([]byte, error) {
	var rules []map[string]any
	var notes []string
	for _, mp := range p.Metrics {
		for _, l := range mp.DropSafe {
			rules = append(rules, map[string]any{
				"source_labels": []string{"__name__"},
				"regex":         mp.Metric,
				"target_label":  l,
				"replacement":   "",
				"action":        "replace", // an empty value removes the label, scoped to this metric
			})
		}
		if len(mp.Drop)+len(mp.Normalize) > 0 {
			notes = append(notes, fmt.Sprintf("#   %s: aggregate away %s (%s) in the collector", mp.Metric, strings.Join(append(slices.Clone(mp.Drop), mp.Normalize...), ", "), mp.Aggregation))
		}
	}
	b, err := yaml.Marshal(map[string]any{"metric_relabel_configs": rules})
	header := "# Generated by cardinality-doctor: add under each scrape job.\n# Only drops that merge no series are done here. These need aggregation at ingest:\n" + strings.Join(notes, "\n") + "\n"
	return append([]byte(header), b...), err
}
