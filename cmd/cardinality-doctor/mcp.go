package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/ThatDeparted2061/last9-cardinality-doctor/internal/analyze"
)

// mcp serves the analysis to an agent over the Model Context Protocol
// (JSON-RPC 2.0 on stdio), so it can answer "why did my series count spike?".
func mcp(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	dir := fs.String("tsdb", "data/tsdb", "Prometheus TSDB directory")
	otlp := fs.String("otlp", "", "OTLP/JSON export to read instead")
	var cost costFlags
	cost.register(fs)
	fs.Parse(args)

	var once sync.Once
	var ms []analyze.Metric
	var aerr error
	analysis := func() ([]analyze.Metric, error) {
		once.Do(func() {
			src, closer, err := source(*dir, *otlp)
			if err != nil {
				aerr = err
				return
			}
			defer closer()
			ms, aerr = analyze.Analyze(src, analyze.Options{MinDistinct: 1000})
		})
		return ms, aerr
	}
	tools := []map[string]any{
		{"name": "cardinality_report", "description": "Rank metrics by active series, with growth, estimated cost and unbounded labels (user IDs, URLs with IDs, timestamps, UUIDs).",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{"top": map[string]any{"type": "integer", "description": "metrics to return (default 10)"}}}},
		{"name": "why_did_series_spike", "description": "Explain which metrics and labels added the most new series between the earlier and the later half of the data.",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}},
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<24)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if json.Unmarshal(in.Bytes(), &req) != nil || req.ID == nil {
			continue // notifications need no answer
		}
		var result any
		switch req.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}},
				"serverInfo": map[string]any{"name": "cardinality-doctor", "version": "0.1.0"}}
		case "tools/list":
			result = map[string]any{"tools": tools}
		case "tools/call":
			ms, err := analysis()
			var buf bytes.Buffer
			switch {
			case err != nil:
				fmt.Fprintf(&buf, "analysis failed: %v", err)
			case req.Params.Name == "cardinality_report":
				top := 10
				if v, ok := req.Params.Arguments["top"].(float64); ok && v > 0 {
					top = int(v)
				}
				render(&buf, ms[:min(top, len(ms))], cost)
			case req.Params.Name == "why_did_series_spike":
				spike(&buf, ms)
			default:
				fmt.Fprintf(&buf, "unknown tool %q", req.Params.Name)
			}
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": buf.String()}}, "isError": err != nil}
		default:
			out.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "method not found"}})
			continue
		}
		out.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}
	return in.Err()
}

func spike(w *bytes.Buffer, ms []analyze.Metric) {
	sorted := append([]analyze.Metric(nil), ms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NewNow > sorted[j].NewNow })
	total := 0
	for _, m := range sorted {
		total += m.NewNow
	}
	fmt.Fprintf(w, "%s new series appeared in the later half of the data.\n\n", human(float64(total)))
	for _, m := range sorted[:min(5, len(sorted))] {
		if m.NewNow == 0 {
			break
		}
		fmt.Fprintf(w, "- %s: %s new series (%+.0f%%)", m.Name, human(float64(m.NewNow)), m.Growth*100)
		for _, l := range m.Labels {
			if l.Class != "" && l.Class != "redundant" {
				fmt.Fprintf(w, "; driven by %s (%s, %s distinct values, e.g. %s)", l.Name, l.Class, human(float64(l.Distinct)), first(l.Examples))
			}
		}
		fmt.Fprintln(w)
	}
}
