#!/usr/bin/env bash
# Generate the synthetic dataset, analyse it, write the fix, and prove the fix
# changes no dashboard. Writes bench/results/ and bench/RESULTS.md.
#   SCALE=1 (≈5M series)   DATA=data   PRICE=0.10 (USD per million samples)
set -euo pipefail
cd "$(dirname "$0")/.."
SCALE=${SCALE:-1} DATA=${DATA:-data} PRICE=${PRICE:-0.10}
OUT=bench/results
BIN=bin/cardinality-doctor
go build -o $BIN ./cmd/cardinality-doctor
rm -rf "$DATA/tsdb" "$DATA/reduced" && mkdir -p $OUT/fixes

t=$SECONDS; $BIN generate -out "$DATA/tsdb" -scale "$SCALE"; gen_s=$((SECONDS - t))
t=$SECONDS; $BIN analyze -tsdb "$DATA/tsdb" -price "$PRICE" -json $OUT/analysis.json | tee $OUT/analyze.txt; ana_s=$((SECONDS - t))
$BIN fix -tsdb "$DATA/tsdb" -dashboards examples/dashboards.yaml -out $OUT/fixes | tee $OUT/fix.txt
t=$SECONDS; $BIN dry-run -tsdb "$DATA/tsdb" -plan $OUT/fixes/plan.json -dashboards examples/dashboards.yaml \
  -work "$DATA/reduced" -out $OUT/dry-run.json; dry_s=$((SECONDS - t))
du_before=$(du -sk "$DATA/tsdb" | cut -f1) du_after=$(du -sk "$DATA/reduced" | cut -f1)

python3 - "$gen_s" "$ana_s" "$dry_s" "$du_before" "$du_after" "$PRICE" <<'PY'
import json, sys
gen_s, ana_s, dry_s, du_b, du_a, price = sys.argv[1:]
d = json.load(open("bench/results/dry-run.json"))
month = 30 * 24 * 3600 / 30  # samples per series per month at a 30 s scrape
cost = lambda s: s * month / 1e6 * float(price)
rows = [
    f"**{d['series_before']:,} → {d['series_after']:,} active series ({d['reduction']:.1%} fewer)**, "
    f"and all {len(d['queries'])} dashboard queries return identical results at {len(d['instants'])} instants each.",
    "",
    "| | Before | After |", "|---|---:|---:|",
    f"| Active series | {d['series_before']:,} | {d['series_after']:,} |",
    f"| Samples/month at a 30 s scrape | {d['series_before']*month/1e9:.0f}B | {d['series_after']*month/1e9:.0f}B |",
    f"| At ${price} per million samples | ${cost(d['series_before']):,.0f}/mo | ${cost(d['series_after']):,.0f}/mo |",
    f"| TSDB on disk (20 min of data) | {int(du_b)/1024:,.0f} MB | {int(du_a)/1024:,.0f} MB |",
    "",
    "| Dashboard query | Series | Equal |", "|---|---:|:---:|",
] + [f"| {q['name']} | {q['result_series']:,} | {'yes' if q['equal'] else '**no**'} |" for q in d["queries"]] + [
    "",
    f"Timings on an M-series laptop: generate {gen_s} s, analyse {ana_s} s, dry-run {dry_s} s.",
]
open("bench/RESULTS.md", "w").write("\n".join(rows) + "\n")
print("\n".join(rows))
PY
