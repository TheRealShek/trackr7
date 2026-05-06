-include .env
export

.PHONY: test test-integration bench bench-integration vet tidy all

test:
	go test ./... -v -count=1 -race

test-integration:
	go test ./... -v -count=1 -race

bench:
	go test -bench=. -benchmem ./...

bench-integration:
	go test -bench=. -benchmem ./...

vet:
	go vet ./...

tidy:
	go mod tidy

all: vet test
