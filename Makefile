.PHONY: demo demo-quick test build
build:
	go build -o bin/cardinality-doctor ./cmd/cardinality-doctor
demo:          ## ~5M series: generate, analyse, fix, dry-run; writes bench/RESULTS.md
	bench/run.sh
demo-quick:    ## ~640k series
	SCALE=0.1 bench/run.sh
test:
	go test ./...
