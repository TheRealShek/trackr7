# Testing trackr7

This document outlines how to run tests for the trackr7 library.

## Prerequisites

- **Go**: 1.26.2
- **Postgres**: 15+ (for integration tests)
- **Kafka / Redpanda**: (for writer/cache integration tests in later phases)

## Running Tests

Trackr7 uses standard Go testing tools. We separate fast unit tests from slower integration tests using an environment variable.

### Makefile Targets

```bash
make test
make test-integration
make bench
make bench-integration
make vet
make tidy
make all
```

### Unit Tests Only

To run fast, mock-free unit tests (skips DB/Kafka integration tests):

```bash
go test ./... -v -count=1
```

To run tests with the race detector enabled (recommended):

```bash
go test ./... -v -count=1 -race
```

To test a specific package:

```bash
go test ./auth/ -v -count=1 -race
```

### Integration Tests

Tests that require a real database are gated behind the `TRACKR7_TEST_DSN` environment variable. If this variable is not set, these tests will cleanly `t.Skip()`.

To run integration tests, point `TRACKR7_TEST_DSN` to a running Postgres instance. The test suite will create and drop its own tables within the provided database.

```bash
export TRACKR7_TEST_DSN="postgres://user:password@localhost:5432/trackr7_test"
go test ./... -v -count=1 -race
```

### Why skip without DSN?

We skip integration tests when `TRACKR7_TEST_DSN` is absent to ensure the default `go test ./...` command completes instantly for local development and doesn't fail spuriously if a local Postgres instance isn't running. CI pipelines are responsible for setting the DSN and running the full suite.

## Running Benchmarks

```bash
# Unit benchmarks only (no DSN needed)
go test -bench=. -benchmem ./...

# With integration benchmarks
export TRACKR7_TEST_DSN="postgres://user:password@localhost:5432/trackr7_test"
go test -bench=. -benchmem ./...
```

Integration benchmarks skip cleanly if `TRACKR7_TEST_DSN` is unset, same as integration tests.
