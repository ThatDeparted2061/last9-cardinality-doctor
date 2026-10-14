**4.9M active series cut to 169k (96.6% fewer), and all 9 dashboard queries return exactly the same results before and after. The doctor found the four labels behind the blow-up, wrote the OpenTelemetry Collector config that removes them, and proved the config with the real PromQL engine before anyone deployed it.**

# cardinality-doctor

[![CI](https://github.com/ThatDeparted2061/last9-cardinality-doctor/actions/workflows/ci.yml/badge.svg)](https://github.com/ThatDeparted2061/last9-cardinality-doctor/actions/workflows/ci.yml)

High cardinality is the problem Last9 is built around. Someone adds `user_id` to a counter, or puts raw URLs in a histogram, and the metrics bill grows tenfold overnight. This tool reads a Prometheus TSDB (or an OTLP export) and does four things:

1. **Ranks** metrics by active series and growth, and names the labels responsible: user IDs, UUIDs, URLs with IDs in them, Unix timestamps, IPs, emails, hashes, and labels that are 1:1 copies of another label.
2. **Writes the fix.** It produces an OpenTelemetry Collector config that aggregates the offending labels away at ingest, plus Prometheus `metric_relabel_configs` for the drops that are safe at scrape time.
3. **Never breaks a dashboard.** You give it your dashboard queries. It parses them with the PromQL parser, and no label they group by, filter on or join on is touched.
4. **Proves it** (`dry-run`). It applies the plan to a copy of the data, runs every dashboard query with the PromQL engine on both copies, and compares the results.

It also runs as an **MCP server**, so an agent can answer "why did our series count spike last night?".

## Results

`make demo` generates a TSDB with ~4.9M series and planted offenders, then runs the whole pipeline:

**4,904,400 → 168,865 active series (96.6% fewer)**, and all 9 dashboard queries return identical results at 8 instants each.

| | Before | After |
|---|---:|---:|
| Active series | 4,904,400 | 168,865 |
| Samples/month at a 30 s scrape | 424B | 15B |
| At $0.10 per million samples | $42,374/mo | $1,459/mo |
| TSDB on disk (20 min of data) | 898 MB | 24 MB |

| Dashboard query | Series | Equal |
|---|---:|:---:|
| Request rate by service | 20 | yes |
| Error ratio by service | 20 | yes |
| Requests by route and status | 50 | yes |
| p99 latency by service | 20 | yes |
| Session events by tenant | 25 | yes |
| Last successful batch run per job | 200 | yes |
| CPU by pod | 10,000 | yes |
| Busy CPU by node | 500 | yes |
| Consumer lag by topic and group | 1,600 | yes |

Timings on an M-series laptop: generate 48 s, analyse 18 s, dry-run 107 s.

What it found (`bench/results/analyze.txt`):

```
METRIC                                    SERIES  GROWTH  FINDINGS
http_requests_total                       2.0M    +82%    user_id: 900.0k values, user_id (e.g. u-100946) -> 4.0k series without it
http_request_duration_seconds_bucket      1.4M    +82%    route_path: 116.0k values, url_with_ids (e.g. /api/orders/10000/items/32) -> 240 series without it
session_events_total                      1.0M    +82%    session_id: 1.0M values, uuid (e.g. 000145ac-…) -> 25 series without it
batch_job_last_success_timestamp_seconds  300.0k  +82%    run_id: 1.5k values, timestamp (e.g. 1791924720) -> 200 series without it
kafka_consumergroup_lag                   102.4k  +0%     ok
node_cpu_seconds_total                    32.0k   +0%     ok
container_cpu_usage_seconds_total         30.0k   +0%     pod_ip is 1:1 with pod (safe to drop)
```

The well-behaved metrics (`kafka_consumergroup_lag`, `node_cpu_seconds_total`) are large but stable, so they are left alone. Big is not the problem; unbounded is.

## How it decides

**Unbounded labels.** A label is a candidate when it has at least 1,000 values that match a pattern no bounded label has: IDs such as `u-100946` or `cust_48213` (a short prefix and 4+ digits), UUIDs, epoch seconds or milliseconds, IPs, emails, hex hashes, or URL paths with numeric or UUID segments. For each candidate it counts how many series would remain without the label (`-> 4.0k series without it`).

**Growth.** It splits the data in two and counts series that exist only in the later half. Churning labels show up as growth; stable metrics show +0%.

**Redundant labels.** `pod_ip` is 1:1 with `pod`, so dropping it merges no series. That is the only kind of drop done in Prometheus relabelling, because a relabel that merges series produces duplicate-sample errors.

**Dashboard protection.** `examples/dashboards.yaml` holds the queries people actually look at. Each one is walked as a PromQL AST. A label is protected if a selector matches on it, or if an aggregation or vector match directly above the raw series keeps it. `sum by (service) (rate(http_requests_total[5m]))` therefore protects `service` but lets `user_id` go.

**The aggregation.** Counters and histograms are summed. Timestamp gauges (`*_timestamp_seconds`, `last_*`) use `max`, so "when did it last succeed" survives as the latest time. That matches the dashboard's `max by (job)`.

## The fix it writes

`bench/results/fixes/otel-collector.yaml`, abridged:

```yaml
processors:
  transform/cardinality:            # /api/orders/10000/items/32 -> /api/orders/{id}/items/{id}
    metric_statements:
      - context: datapoint
        statements:
          - replace_pattern(datapoint.attributes["route_path"], "/([0-9]+|…uuid…|…hash…)(/|$)", "/{id}$$2") where metric.name == "http_request_duration_seconds"
  metricstransform/cardinality:     # aggregate the offending labels away
    transforms:
      - include: http_requests_total
        action: update
        operations: [{action: aggregate_labels, aggregation_type: sum, label_set: [method, route, service, status]}]
      - include: batch_job_last_success_timestamp_seconds
        action: update
        operations: [{action: aggregate_labels, aggregation_type: max, label_set: [job]}]
service:
  pipelines:
    metrics:
      processors: [cumulativetodelta, transform/cardinality, metricstransform/cardinality, deltatocumulative, batch]
```

Cumulative counters can't simply be summed across series that start and stop (a churned `user_id` series would make the sum drop). The pipeline therefore converts to deltas, aggregates, and converts back.

## The dry run

`dry-run` reads every series and applies the plan exactly as the collector would: drop, normalise, aggregate. It writes the result as a second TSDB, then runs each dashboard query on both TSDBs with Prometheus' own engine. Results are compared at instants that sit away from churn boundaries, because there `rate()` extrapolates per series and a sum of extrapolations is not the extrapolation of a sum. Results must match to a relative 1e-9.

## As an MCP tool

```bash
cardinality-doctor mcp -tsdb /prometheus/data
```

It exposes two tools, `cardinality_report` and `why_did_series_spike`. A typical answer:

```
42.7k new series appeared in the later half of the data.
- http_requests_total: 18.1k new series (+83%); driven by user_id (user_id, 40.0k distinct values, e.g. u-148081)
- http_request_duration_seconds_bucket: 12.9k new series (+82%); driven by route_path (url_with_ids, 2.3k distinct values, …)
```

## Run it

```bash
make demo          # ~4.9M series: generate, analyse, fix, dry-run (≈3 min, ≈2 GB RAM, ≈1 GB disk)
make demo-quick    # ~640k series in ≈20 s
make test

# On your own data (a Prometheus data dir or snapshot, or an OTLP/JSON export):
bin/cardinality-doctor analyze -tsdb /prometheus/data -price 0.10
bin/cardinality-doctor fix     -tsdb /prometheus/data -dashboards my-dashboards.yaml -out fixes
bin/cardinality-doctor dry-run -tsdb /prometheus/data -dashboards my-dashboards.yaml -plan fixes/plan.json
```

## Limits

- The dataset is synthetic, with offenders planted where real ones usually are. The analysis, the configs and the PromQL comparison are real.
- Dashboards are only protected if you list their queries. Pulling them from the Grafana API is the obvious next step.
- `deltatocumulative` keeps state in the collector, so aggregation has to happen on one collector per metric stream (route by metric name with the load-balancing exporter).
- Cost figures use an illustrative $0.10 per million samples at a 30 s scrape, not any vendor's price.
