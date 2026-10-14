package analyze_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/analyze"
	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/gen"
)

// The analysis must outlive the TSDB it came from (the MCP server closes the
// blocks after analysing), and must find the planted offenders.
func TestAnalysisOutlivesTheBlocks(t *testing.T) {
	dir := t.TempDir()
	if _, err := gen.Generate(context.Background(), gen.Config{Dir: dir, Scale: 0.01, Start: time.Now().Add(-30 * time.Minute), Seed: 1}); err != nil {
		t.Fatal(err)
	}
	src, err := analyze.OpenTSDB(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	ms, err := analyze.Analyze(src, analyze.Options{MinDistinct: 1000})
	src.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(ms); err != nil { // touches every string
		t.Fatal(err)
	}
	classes := map[string]string{}
	for _, m := range ms {
		for _, l := range m.Labels {
			if l.Class != "" {
				classes[m.Name+"/"+l.Name] = l.Class
			}
		}
	}
	for k, want := range map[string]string{
		"http_requests_total/user_id":                     "user_id",
		"http_request_duration_seconds_bucket/route_path": "url_with_ids",
		"session_events_total/session_id":                 "uuid",
		"container_cpu_usage_seconds_total/pod_ip":        "redundant",
	} {
		if classes[k] != want {
			t.Errorf("%s: class %q, want %q", k, classes[k], want)
		}
	}
}
