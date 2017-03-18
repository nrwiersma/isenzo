.PHONY: ci test vet bench

ci: test vet

test:
	go test $(shell go list ./... | grep -v /vendor/)

vet:
	go vet $(shell go list ./... | grep -v /vendor/)

bench:
	go test -bench=. $(shell go list ./... | grep -v /vendor/)
