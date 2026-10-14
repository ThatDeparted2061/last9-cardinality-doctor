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
