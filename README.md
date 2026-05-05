# trackr7

trackr7 is an embeddable Go library for real-time entity location tracking. It provides a Kafka-backed pipeline for high-throughput ingestion, batch database writes, and an in-memory latest-location cache.

## What it does

* Ingests location events via HTTP and produces to Kafka.
* Batch writes location data from Kafka to Postgres using UNNEST for performance.
* Maintains an in-memory cache of the latest location for every entity.
* Provides API key management and rate limiting for ingestion.
* Designed to be embedded in existing Go services rather than run as a standalone sidecar.

## Architecture

```
Client -> [ingest.Handler] -> Kafka (location.raw)
                                  │
                                  ├──> [writer.Worker] ──> Postgres (batch)
                                  └──> [cache.Store]   ──> In-memory lookup
```

## Quick Start

> **IMPORTANT**: trackr7 does not manage database schema. You must create the required tables before starting the library. See the [Schema Requirements](#schema-requirements) section below.

The following example demonstrates wiring the ingestion handler, the database writer, and the location store.

```go
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/TheRealShek/trackr7/auth"
	"github.com/TheRealShek/trackr7/cache"
	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/ingest"
	"github.com/TheRealShek/trackr7/writer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	// 1. Setup Database Configuration
	// MANDATORY: Call WithDefaults() and Validate().
	pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	dbCfg := db.DBConfig{
		Pool:           pool,
		LocationsTable: "trackr.locations",
	}.WithDefaults()

	if err := dbCfg.Validate(); err != nil {
		panic(err)
	}

	// 2. Setup Ingestion (HTTP -> Kafka)
	// Kafka messages must be keyed by entity_id to preserve ordering.
	kw := &kafka.Writer{Addr: kafka.TCP("localhost:9092"), Topic: "location.raw"}
	keyCache := auth.NewKeyCache(dbCfg, 5*time.Minute)
	
	ingestH, _ := ingest.NewHandler(ingest.Config{
		Kafka: kw,
		DB:    dbCfg,
		Auth:  keyCache,
	})

	// 3. Setup Persistence (Kafka -> Postgres)
	worker, _ := writer.NewWorker(writer.Config{
		KafkaBrokers: []string{"localhost:9092"},
		Topic:        "location.raw",
		Namespace:    "prod-cluster-1",
		DB:           dbCfg,
	})
	go worker.Run(ctx)

	// 4. Setup Cache (Kafka -> Memory)
	store, _ := cache.NewStore(cache.Config{
		KafkaBrokers: []string{"localhost:9092"},
		Topic:        "location.raw",
		Namespace:    "prod-cluster-1",
	})
	go store.Run(ctx)

	// 5. Expose Endpoints
	mux := http.NewServeMux()
	mux.Handle("POST /ping", ingestH)
	mux.HandleFunc("GET /ready", store.ReadinessHandler())
	mux.HandleFunc("GET /location/{id}", func(w http.ResponseWriter, r *http.Request) {
		// Cache is eventually consistent. fetchedAt indicates message freshness.
		loc, fetchedAt, ok := store.Get(r.PathValue("id"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"location":   loc,
			"fetched_at": fetchedAt,
		})
	})

	http.ListenAndServe(":8080", mux)
}
```

## Configuration

### Kafka
* **Partitioning**: Messages **must be keyed by `entity_id`**. This ensures all pings for a specific entity land in the same Kafka partition, preserving chronological order.
* **Namespace**: Required for consumer isolation. It prevents consumer group collisions between different environments (e.g., staging vs. prod).

### DBConfig
The `db.DBConfig` struct allows overriding table names and column mappings.
* **`WithDefaults()`**: Fills zero-value strings with library defaults. **Mandatory**.
* **`Validate()`**: Checks for nil pools, invalid identifiers, and duplicate mappings. **Mandatory**.

## Guarantees

| Guarantee | Status | Note |
|---|---|---|
| At-least-once delivery | Yes | Kafka offsets committed only after Postgres success |
| De-duplication | Yes | Relies on `ON CONFLICT (uuid) DO NOTHING` in Postgres |
| Ordering | Partial | Guaranteed per `entity_id` via Kafka partitioning |
| Cache consistency | Eventual | Cache reflects latest message read from Kafka |
| Schema ownership | User | User is responsible for DDL and migrations |

## Constraints

* **Identifiers**: All table and column names must match `^[a-z0-9_.]+$`.
* **No Quoting**: trackr7 does not quote identifiers in generated SQL. Uppercase or special characters will cause SQL syntax errors.
* **Database**: Requires Postgres 12+ (for BRIN indexes) and a Kafka-compatible broker (e.g., Redpanda).
* **Memory**: The `cache.Store` keeps the latest location for every active entity in memory.
* **Consistency**: The cache is **eventually consistent**. Callers should use the returned `fetchedAt` timestamp to verify data freshness.
* **Validation**: Incorrect `DBConfig` mappings lead to undefined behavior or silent runtime failures.

## Schema Requirements

The user must provide the following tables. Reference SQL is available in `schema/migrations/001_init.sql`.

* **locations**: Stores the history of pings. Required columns: `uuid`, `entity_id`, `entity_type`, `lat`, `lng`, `ts`.
* **api_keys**: Stores ingestion credentials. Required columns: `key_id`, `key_hash`, `entity_type`, `revoked`, `created_at`.
* **audit_log**: Stores key lifecycle events. Required columns: `key_id`, `action`, `ts`.

## Packages

* **ingest**: HTTP handler that validates API keys and produces to Kafka.
* **writer**: Background worker that batches Kafka messages into Postgres.
* **cache**: Background worker that maintains an in-memory latest-location map.
* **auth**: Key caching, validation, and rate limiting logic.
* **db**: Runtime configuration, identifier validation, and column mapping.
* **schema**: Canonical Go types and reference SQL documentation.

## Non-Goals

* trackr7 is not a standalone service; it has no `main` package.
* It is not an exactly-once delivery system.
* It does not provide a multi-node distributed cache; the cache is local to the Go process.
