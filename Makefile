include github.com/msales/make/golang

# Run all benchmarks
bench:
	@go test -bench=. $(shell go list ./... | grep -v /vendor/)
.PHONY: bench
