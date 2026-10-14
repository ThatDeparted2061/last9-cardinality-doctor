package analyze

import (
	"encoding/json"
	"math"
	"os"
	"sort"
	"strconv"
)

// OTLP reads an OpenTelemetry metrics export in OTLP/JSON (an
// ExportMetricsServiceRequest, as written by the collector's file exporter).
// A series is a metric name plus its resource and data-point attributes,
// which is how they become Prometheus series. Growth splits at the midpoint
// of the data points' timestamps.
type OTLP struct {
	series map[string][]Series
}

type otlpAttr struct {
	Key   string `json:"key"`
	Value struct {
		String *string  `json:"stringValue"`
		Int    *string  `json:"intValue"`
		Double *float64 `json:"doubleValue"`
		Bool   *bool    `json:"boolValue"`
	} `json:"value"`
}

func (a otlpAttr) str() string {
	switch v := a.Value; {
	case v.String != nil:
		return *v.String
	case v.Int != nil:
		return *v.Int
	case v.Double != nil:
		return strconv.FormatFloat(*v.Double, 'g', -1, 64)
	case v.Bool != nil:
		return strconv.FormatBool(*v.Bool)
	}
	return ""
}

type otlpPoints struct {
	DataPoints []struct {
		Attributes   []otlpAttr `json:"attributes"`
		TimeUnixNano string     `json:"timeUnixNano"`
	} `json:"dataPoints"`
}

func OpenOTLP(path string) (*OTLP, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var req struct {
		ResourceMetrics []struct {
			Resource struct {
				Attributes []otlpAttr `json:"attributes"`
			} `json:"resource"`
			ScopeMetrics []struct {
				Metrics []struct {
					Name      string      `json:"name"`
					Sum       *otlpPoints `json:"sum"`
					Gauge     *otlpPoints `json:"gauge"`
					Histogram *otlpPoints `json:"histogram"`
				} `json:"metrics"`
			} `json:"scopeMetrics"`
		} `json:"resourceMetrics"`
	}
	if err := json.Unmarshal(b, &req); err != nil {
		return nil, err
	}
	type pt struct {
		name string
		lbls map[string]string
		t    int64
	}
	var pts []pt
	minT, maxT := int64(math.MaxInt64), int64(math.MinInt64)
	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				for _, ps := range []*otlpPoints{m.Sum, m.Gauge, m.Histogram} {
					if ps == nil {
						continue
					}
					for _, dp := range ps.DataPoints {
						l := map[string]string{"__name__": m.Name}
						for _, a := range rm.Resource.Attributes {
							l[a.Key] = a.str()
						}
						for _, a := range dp.Attributes {
							l[a.Key] = a.str()
						}
						t, _ := strconv.ParseInt(dp.TimeUnixNano, 10, 64)
						minT, maxT = min(minT, t), max(maxT, t)
						pts = append(pts, pt{m.Name, l, t})
					}
				}
			}
		}
	}
	split := minT + (maxT-minT)/2
	o := &OTLP{series: map[string][]Series{}}
	idx := map[string]int{}
	for _, p := range pts {
		key := keyOf(p.lbls)
		i, ok := idx[key]
		if !ok {
			i = len(o.series[p.name])
			idx[key] = i
			o.series[p.name] = append(o.series[p.name], Series{Labels: p.lbls})
		}
		s := &o.series[p.name][i]
		if p.t < split || minT == maxT {
			s.Before = true
		}
		if p.t >= split {
			s.Now = true
		}
	}
	return o, nil
}

func keyOf(l map[string]string) string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := []byte{}
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, 0)
		b = append(b, l[k]...)
		b = append(b, 0)
	}
	return string(b)
}

func (o *OTLP) Metrics() ([]string, error) {
	var out []string
	for k := range o.series {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func (o *OTLP) Each(metric string, fn func(Series)) error {
	for _, s := range o.series[metric] {
		fn(s)
	}
	return nil
}
