package fix

import "testing"

func TestUsedLabels(t *testing.T) {
	u, err := UsedLabels([]Dashboard{
		{"ratio", `sum by (service) (rate(http_requests_total{status=~"5.."}[5m])) / sum by (service) (rate(http_requests_total[5m]))`},
		{"raw join", `node_load1 * on (instance) group_left(team) node_info`},
		{"without", `sum without (le) (rate(lat_bucket[5m]))`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !u["http_requests_total"]["service"] || !u["http_requests_total"]["status"] || u["http_requests_total"]["user_id"] || u["http_requests_total"]["*"] {
		t.Fatalf("aggregated operands only use their grouping and matchers: %v", u["http_requests_total"])
	}
	if !u["node_load1"]["instance"] || !u["node_info"]["team"] {
		t.Fatalf("on()/group_left labels are used: %v %v", u["node_load1"], u["node_info"])
	}
	if !u["lat_bucket"]["*"] {
		t.Fatalf("without() keeps every other label: %v", u["lat_bucket"])
	}
}
