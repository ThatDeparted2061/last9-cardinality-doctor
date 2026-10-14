// Package analyze ranks metrics and labels by cardinality and growth and
// classifies unbounded labels.
package analyze

import (
	"hash/maphash"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Series is one series as a source reports it: its labels (including
// __name__) and whether it had samples before and after the growth split.
type Series struct {
	Labels      map[string]string
	Before, Now bool
}

// Source streams series one metric at a time.
type Source interface {
	Metrics() ([]string, error)
	Each(metric string, fn func(Series)) error
}

// Label is one label of one metric.
type Label struct {
	Name     string   `json:"name"`
	Distinct int      `json:"distinct_values"`
	Class    string   `json:"class,omitempty"` // user_id, uuid, timestamp, url_with_ids, ip, email, hash, redundant
	SameAs   string   `json:"redundant_with,omitempty"`
	Examples []string `json:"examples,omitempty"`
	// SeriesWithout is how many series remain if this label is dropped (or,
	// for url_with_ids, normalised). Zero when not computed.
	SeriesWithout int `json:"series_without,omitempty"`
}

// Metric is one metric's cardinality profile.
type Metric struct {
	Name         string   `json:"name"`
	Series       int      `json:"series"`
	SeriesBefore int      `json:"series_before_split"`
	SeriesNow    int      `json:"series_after_split"`
	NewNow       int      `json:"new_series_after_split"`
	Growth       float64  `json:"growth"` // new series after the split / series before it
	Labels       []Label  `json:"labels"`
	Offenders    []string `json:"offenders,omitempty"`
}

// Options tune classification.
type Options struct {
	MinDistinct int // a label needs this many values to be called unbounded
}

var (
	reUUID  = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	reEpoch = regexp.MustCompile(`^1[0-9]{9}([0-9]{3})?$`) // 2001-2286, seconds or millis
	reIPv4  = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)
	reEmail = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[a-zA-Z]{2,}$`)
	reHash  = regexp.MustCompile(`^[0-9a-fA-F]{12,}$`)
	reID    = regexp.MustCompile(`^[A-Za-z_.-]{0,12}[0-9]{4,}$`)
	reIDSeg = regexp.MustCompile(`/([0-9]+|[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}|[0-9a-fA-F]{12,})(/|$)`)
)

// classify returns the dominant ID-like pattern among sample values, if any.
func classify(values []string) string {
	if len(values) == 0 {
		return ""
	}
	counts := map[string]int{}
	for _, v := range values {
		switch {
		case reUUID.MatchString(v):
			counts["uuid"]++
		case reEpoch.MatchString(v):
			counts["timestamp"]++
		case reIPv4.MatchString(v):
			counts["ip"]++
		case reEmail.MatchString(v):
			counts["email"]++
		case strings.Contains(v, "/") && reIDSeg.MatchString(v):
			counts["url_with_ids"]++
		case reHash.MatchString(v):
			counts["hash"]++
		case reID.MatchString(v):
			counts["user_id"]++
		}
	}
	best, n := "", 0
	for k, c := range counts {
		if c > n {
			best, n = k, c
		}
	}
	if n*10 >= len(values)*8 { // 80% of values share the pattern
		return best
	}
	return ""
}

// NormalizePath replaces ID-like path segments with {id}.
func NormalizePath(v string) string {
	for {
		w := reIDSeg.ReplaceAllString(v, "/{id}$2")
		if w == v {
			return w
		}
		v = w
	}
}

type labelAcc struct {
	values  map[string]struct{}
	samples []string
}

// Analyze profiles every metric from src.
func Analyze(src Source, o Options) ([]Metric, error) {
	names, err := src.Metrics()
	if err != nil {
		return nil, err
	}
	var out []Metric
	for _, name := range names {
		m, err := analyzeMetric(src, name, o)
		if err != nil {
			return nil, err
		}
		out = append(out, m.detach())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Series > out[j].Series })
	return out, nil
}

// detach copies every string out of the source. A TSDB source hands out
// strings backed by its mmapped index, which vanish when the block closes.
func (m Metric) detach() Metric {
	m.Name = strings.Clone(m.Name)
	m.Offenders = cloneAll(m.Offenders)
	m.Labels = slices.Clone(m.Labels)
	for i := range m.Labels {
		l := &m.Labels[i]
		l.Name, l.SameAs, l.Examples = strings.Clone(l.Name), strings.Clone(l.SameAs), cloneAll(l.Examples)
	}
	return m
}

func cloneAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strings.Clone(s)
	}
	return out
}

func analyzeMetric(src Source, name string, o Options) (Metric, error) {
	m := Metric{Name: name}
	accs := map[string]*labelAcc{}
	// Pass 1: distinct values, value samples, growth.
	err := src.Each(name, func(s Series) {
		m.Series++
		if s.Before {
			m.SeriesBefore++
		}
		if s.Now {
			m.SeriesNow++
			if !s.Before {
				m.NewNow++
			}
		}
		for k, v := range s.Labels {
			if k == "__name__" {
				continue
			}
			a := accs[k]
			if a == nil {
				a = &labelAcc{values: map[string]struct{}{}}
				accs[k] = a
			}
			if _, ok := a.values[v]; !ok {
				a.values[v] = struct{}{}
				if len(a.samples) < 2000 {
					a.samples = append(a.samples, v)
				}
			}
		}
	})
	if err != nil {
		return m, err
	}
	if m.SeriesBefore > 0 {
		m.Growth = float64(m.NewNow) / float64(m.SeriesBefore)
	}
	var flagged []string
	for k, a := range accs {
		l := Label{Name: k, Distinct: len(a.values)}
		if l.Distinct >= o.MinDistinct {
			l.Class = classify(a.samples)
		}
		l.Examples = a.samples[:min(3, len(a.samples))]
		if l.Class != "" && l.Class != "ip" {
			flagged = append(flagged, k)
		}
		m.Labels = append(m.Labels, l)
	}
	sort.Slice(m.Labels, func(i, j int) bool { return m.Labels[i].Distinct > m.Labels[j].Distinct })
	redundant(src, name, &m, accs)

	// Pass 2: series left without each flagged label.
	if len(flagged) > 0 {
		var seed = maphash.MakeSeed()
		sets := map[string]map[uint64]struct{}{}
		for _, f := range flagged {
			sets[f] = map[uint64]struct{}{}
		}
		err := src.Each(name, func(s Series) {
			keys := make([]string, 0, len(s.Labels))
			for k := range s.Labels {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, f := range flagged {
				var h maphash.Hash
				h.SetSeed(seed)
				for _, k := range keys {
					v := s.Labels[k]
					if k == f {
						if classOf(m, f) != "url_with_ids" {
							continue
						}
						v = NormalizePath(v)
					}
					h.WriteString(k)
					h.WriteByte(0)
					h.WriteString(v)
					h.WriteByte(0)
				}
				sets[f][h.Sum64()] = struct{}{}
			}
		})
		if err != nil {
			return m, err
		}
		for i := range m.Labels {
			if set, ok := sets[m.Labels[i].Name]; ok {
				m.Labels[i].SeriesWithout = len(set)
				m.Offenders = append(m.Offenders, m.Labels[i].Name)
			}
		}
	}
	return m, nil
}

func classOf(m Metric, label string) string {
	for _, l := range m.Labels {
		if l.Name == label {
			return l.Class
		}
	}
	return ""
}

// redundant marks labels that are one-to-one with another label of the same
// metric (e.g. pod_ip with pod): dropping them loses no information and
// merges no series, so a plain relabel drop is safe.
func redundant(src Source, name string, m *Metric, accs map[string]*labelAcc) {
	type pair struct{ a, b string }
	var pairs []pair
	for a, x := range accs {
		for b, y := range accs {
			if a < b && len(x.values) == len(y.values) && len(x.values) > 1 && len(x.values) <= 200_000 {
				pairs = append(pairs, pair{a, b})
			}
		}
	}
	if len(pairs) == 0 {
		return
	}
	fwd := make([]map[string]string, len(pairs))
	ok := make([]bool, len(pairs))
	for i := range pairs {
		fwd[i], ok[i] = map[string]string{}, true
	}
	src.Each(name, func(s Series) {
		for i, p := range pairs {
			if !ok[i] {
				continue
			}
			va, vb := s.Labels[p.a], s.Labels[p.b]
			if prev, seen := fwd[i][va]; seen && prev != vb {
				ok[i] = false
				continue
			}
			fwd[i][va] = vb
		}
	})
	for i, p := range pairs {
		if !ok[i] {
			continue
		}
		// Equal distinct counts plus a function a->b means a bijection. Keep
		// the more readable label (shorter values first are usually IDs/IPs).
		drop, keep := p.a, p.b
		if reIPv4.MatchString(firstValue(accs[p.b])) || strings.HasSuffix(p.b, "_ip") || strings.HasSuffix(p.b, "_id") {
			drop, keep = p.b, p.a
		}
		for j := range m.Labels {
			if m.Labels[j].Name == drop && m.Labels[j].Class == "" || m.Labels[j].Name == drop && m.Labels[j].Class == "ip" {
				m.Labels[j].Class, m.Labels[j].SameAs = "redundant", keep
			}
		}
	}
}

func firstValue(a *labelAcc) string {
	if len(a.samples) == 0 {
		return ""
	}
	return a.samples[0]
}
