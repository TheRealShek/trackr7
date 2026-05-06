# trackr7

trackr7 is an embeddable Go library for real-time entity location tracking. It provides a Kafka-backed pipeline for high-throughput ingestion, batch database writes, and an in-memory latest-location cache.

## Docs

- **[Architecture plan](Docs/HLD.md)**: Read this if you're embedding trackr7 into your service or making architectural decisions. It explains the design, package structure, and API contracts.
- **[Testing guide](Docs/TESTING.md)**: Read this to understand how to run tests and benchmarks, including setup for integration tests with `TRACKR7_TEST_DSN`.
- **[Development guide](Docs/DEVELOPMENT.md)**: Read this to set up a local development environment with Docker and run the full test suite locally.

## What it does

- Ingests location events via HTTP and produces to Kafka.
- Batch writes location data from Kafka to Postgres using UNNEST for performance.
- Maintains an in-memory cache of the latest location for every entity.
- Provides API key management and rate limiting for ingestion.
- Designed to be embedded in existing Go services rather than run as a standalone sidecar.

## Architecture

```
Client -> [ingest.Handler] -> Kafka (location.raw)
                                  │
                                  ├──> [writer.Worker] ──> Postgres (batch)
                                  └──> [cache.Store]   ──> In-memory lookup
```

## Quick Start

> **IMPORTANT**: trackr7 does not manage database schema. You must create the required tables before starting the library. See the [Schema Requirements](#schema-requirements) section below.

A complete, runnable example is available in [`example/main.go`](example/main.go). It demonstrates:

1. Database configuration with `DBConfig.WithDefaults()` and `DBConfig.Validate()`
2. Setting up the API key cache with rate limiting
3. Wiring the ingest handler (HTTP → Kafka)
4. Wiring the persistence worker (Kafka → Postgres)
5. Wiring the in-memory cache (Kafka → Memory)
6. Exposing the `/ping` and `/ready` endpoints

To run the example:

```bash
export TRACKR7_DSN="postgres://user:password@localhost/trackr"
export TRACKR7_KAFKA_BROKERS="localhost:9092"
go run -tags example example/main.go
```

The example includes observability hooks and a sample `stdLogger` for logging events.

## Building & Testing

The project includes a `Makefile` for common development tasks. Copy `.env.example` to `.env` and set `TRACKR7_TEST_DSN` for integration tests.

```bash
make vet                 # Run go vet
make test                # Run unit tests with race detector
make test-integration    # Run all tests including integration
make bench               # Run benchmarks
make bench-integration   # Run benchmarks with integration tests
make tidy                # Run go mod tidy
make all                 # Run vet + test
```

See the [testing guide](Docs/TESTING.md) for details.

## Configuration

### Kafka

- **Partitioning**: Messages **must be keyed by `entity_id`**. This ensures all pings for a specific entity land in the same Kafka partition, preserving chronological order.
- **Namespace**: Required for consumer isolation. It prevents consumer group collisions between different environments (e.g., staging vs. prod).

### DBConfig

The `db.DBConfig` struct allows overriding table names and column mappings.

- **`WithDefaults()`**: Fills zero-value strings with library defaults. **Mandatory**.
- **`Validate()`**: Checks for nil pools, invalid identifiers, and duplicate mappings. **Mandatory**.

## Guarantees

| Guarantee              | Status   | Note                                                  |
| ---------------------- | -------- | ----------------------------------------------------- |
| At-least-once delivery | Yes      | Kafka offsets committed only after Postgres success   |
| De-duplication         | Yes      | Relies on `ON CONFLICT (uuid) DO NOTHING` in Postgres |
| Ordering               | Partial  | Guaranteed per `entity_id` via Kafka partitioning     |
| Cache consistency      | Eventual | Cache reflects latest message read from Kafka         |
| Schema ownership       | User     | User is responsible for DDL and migrations            |

## Constraints

- **Go**: Requires Go 1.26.2.
- **Identifiers**: All table and column names must match `^[a-z0-9_.]+$`.
- **No Quoting**: trackr7 does not quote identifiers in generated SQL. Uppercase or special characters will cause SQL syntax errors.
- **Database**: Requires Postgres 12+ (for BRIN indexes) and a Kafka-compatible broker (e.g., Redpanda).
- **Memory**: The `cache.Store` keeps the latest location for every active entity in memory.
- **Consistency**: The cache is **eventually consistent**. Callers should use the returned `fetchedAt` timestamp to verify data freshness.
- **Validation**: Incorrect `DBConfig` mappings lead to undefined behavior or silent runtime failures.

## Security

### Admin Authentication

`admin.KeyManager` is a direct management interface with no authentication layer. Callers must protect any endpoint or code path that calls `CreateKey`, `RevokeKey`, or `ListKeys` with their own authentication and authorization controls. trackr7 does not do this for you.

### Key Revocation Window

Key revocation is eventual. A revoked key remains valid for up to `refreshEvery` duration (default 5 minutes) until the cache refreshes. For immediate revocation wire `admin.RevokeKey` to `auth.KeyCache.Evict(keyID)` after DB write. If you do not wire this, plan around the window.

### Kafka Trust Boundary

`writer` and `cache` trust all messages on the Kafka topic. Treat the Kafka broker and `location.raw` topic as a hard security boundary, same trust level as your database credentials. Use Kafka ACLs and TLS. Do not expose the broker publicly.

### Audit Log Integrity

`audit_log` is append-only by convention only. A database admin can delete rows silently. trackr7 provides no cryptographic integrity guarantees on audit records. For tamper-evidence use DB-native controls: restricted roles, logical replication to an immutable sink, or WORM storage.

## Schema Requirements

The user must provide the following tables. Reference SQL is available in `schema/migrations/001_init.sql`.

- **locations**: Stores the history of pings. Required columns: `uuid`, `entity_id`, `entity_type`, `lat`, `lng`, `ts`.
- **api_keys**: Stores ingestion credentials. Required columns: `key_id`, `key_hash`, `entity_type`, `revoked`, `created_at`.
- **audit_log**: Stores key lifecycle events. Required columns: `key_id`, `action`, `ts`.

## Packages

- **ingest**: HTTP handler that validates API keys and produces to Kafka.
- **writer**: Background worker that batches Kafka messages into Postgres.
- **cache**: Background worker that maintains an in-memory latest-location map.
- **auth**: Key caching, validation, and rate limiting logic.
- **db**: Runtime configuration, identifier validation, and column mapping.
- **schema**: Canonical Go types and reference SQL documentation.

## Non-Goals

- trackr7 is not a standalone service; it has no `main` package.
- It is not an exactly-once delivery system.
- It does not provide a multi-node distributed cache; the cache is local to the Go process.
