# trackr7 — Complete Plan

> Embeddable Go library. Real-time entity location tracking. Kafka-backed. User owns server, connections, lifecycle. trackr7 owns correctness.

---

## Package Structure

```
trackr7/
  schema/              → shared types, exported errors, reference SQL (not executed)
  db/                  → DBConfig, column maps, WithDefaults(), Validate()
  auth/                → KeyCache, Middleware, rate limiting
  ingest/              → http.Handler — auth, validate, produce
  writer/              → Worker — owns Kafka reader, batch DB write
  cache/               → Store — owns Kafka reader, in-memory latest loc
  admin/               → KeyManager — create/revoke/list keys
  cmd/trackr7-admin/   → thin CLI over admin package
```

---

## System Flow

```
User's HTTP server
      │
      │ mux.Handle("/ping", ingest.NewHandler(cfg))
      ▼
┌─────────────────────────────┐
│       ingest.Handler        │
│                             │
│  MaxBytesReader(1KB)        │
│  → auth.Middleware          │
│  → validate payload         │
│  → stamp server UTC ts      │
│  → produce → Kafka          │
│  → 202                      │
└──────────────┬──────────────┘
               │
               ▼
      Kafka topic: location.raw
      partitioned by: entity_id
               │
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐  ┌──────────────────────────────┐
│writer.Worker│  │         cache.Store           │
│             │  │                               │
│ owns reader │  │ owns reader                   │
│ group:      │  │ group:                        │
│ trackr7.    │  │ trackr7.                      │
│ writer.     │  │ cache.                        │
│ <namespace> │  │ <namespace>                   │
│             │  │                               │
│ batch 500   │  │ map[entity_id]entry           │
│ or 1000ms   │  │ entry { Location, FetchedAt } │
│ UNNEST flush│  │ sync.RWMutex                  │
│ commit after│  │ atomic.Bool readiness         │
│ DB success  │  │ 503 until HWM caught up       │
└─────────────┘  └──────────────────────────────┘
```

---

## DB Schema

> **Schema ownership: the user, not trackr7.**
>
> trackr7 does not ship migrations, does not run SQL, does not call `CREATE TABLE`.
> The user owns tables, migrations, foreign keys, extra columns, and lifecycle.
> Reference SQL below is documentation only — it shows the minimum columns and types trackr7 expects.
> A copy lives at `schema/migrations/001_init.sql` for convenience. It is not embedded and not executed by the library.

> **Column type contract:** trackr7 cannot verify column types at runtime without reflection overhead.
> If the user's actual column types don't match the reference below, behavior is undefined —
> queries may silently return wrong data or fail at runtime.
> This is the user's responsibility.

**`locations`** — trackr7 writes all 6 columns (writer). Reads none from DB directly.

```sql
uuid        TEXT PRIMARY KEY,
entity_id   TEXT NOT NULL,
entity_type TEXT NOT NULL,
lat         FLOAT8 NOT NULL,
lng         FLOAT8 NOT NULL,
ts          BIGINT NOT NULL   -- server UTC ms, display-only
```

Recommended indexes: B-tree on `entity_id`, BRIN on `ts`

**`api_keys`** — trackr7 reads: key_id, key_hash, entity_type, revoked (auth). Writes all 5 columns (admin).

```sql
key_id      UUID PRIMARY KEY,
key_hash    TEXT NOT NULL,
entity_type TEXT NOT NULL,
revoked     BOOLEAN DEFAULT false,
created_at  BIGINT NOT NULL
```

**`audit_log`** — trackr7 writes all 3 columns (admin). Append-only, no PK.

```sql
key_id   UUID,
action   TEXT,    -- created | revoked
ts       BIGINT
```

All table names are configurable via `DBConfig`. All column names are configurable via column mapping structs.
Table names accept fully qualified Postgres schema paths (e.g. `trackr.locations`, `myapp.api_keys`).
Library uses names as-is in queries — no assumptions about schema.

---

## Kafka Message

```json
{
  "uuid": "client-generated, mandatory",
  "entity_id": "string",
  "entity_type": "string",
  "lat": "float64",
  "lng": "float64",
  "ts": "server-stamped UTC unix ms",
  "v": 1
}
```

---

## Public API Surface

**schema**

```
Location  { UUID, EntityID, EntityType, Lat, Lng, TS }
KeyInfo   { KeyID, EntityType, Revoked, CreatedAt }
```

**db**

```
DBConfig {
    Pool              *pgxpool.Pool
    MaxConns          int                -- 0 = use Pool as-is; >0 = library creates sub-pool with that limit
    LocationsTable    string             -- default "locations"; accepts schema-qualified e.g. "trackr.locations"
    APIKeysTable      string             -- default "api_keys"
    AuditLogTable     string             -- default "audit_log"
    LocationColumns   LocationColumnMap
    APIKeyColumns     APIKeyColumnMap
    AuditLogColumns   AuditLogColumnMap
}

LocationColumnMap {
    UUID       string    -- default "uuid"
    EntityID   string    -- default "entity_id"
    EntityType string    -- default "entity_type"
    Lat        string    -- default "lat"
    Lng        string    -- default "lng"
    TS         string    -- default "ts"
}

APIKeyColumnMap {
    KeyID      string    -- default "key_id"
    KeyHash    string    -- default "key_hash"
    EntityType string    -- default "entity_type"
    Revoked    string    -- default "revoked"
    CreatedAt  string    -- default "created_at"
}

AuditLogColumnMap {
    KeyID   string       -- default "key_id"
    Action  string       -- default "action"
    TS      string       -- default "ts"
}

WithDefaults() DBConfig              -- fills zero-value strings with defaults
Validate() error                     -- returns ErrInvalidConfig on:
                                     --   nil Pool
                                     --   empty table/column name after defaults
                                     --   duplicate column names within a table mapping
                                     --   identifier fails regex: ^[a-z0-9_.]+$
                                     --   (rejects uppercase, quotes, spaces, special chars)

-- trackr7 does not quote identifiers in generated SQL.
-- Only lowercase snake_case identifiers are supported (dots allowed for schema qualification).
-- trackr7 does not validate schema correctness at runtime.
-- Incorrect column mapping leads to undefined behavior.
```

**auth**

```
NewKeyCache(cfg db.DBConfig, refreshEvery time.Duration, rateLimit rate.Limit, rateBurst int, options ...auth.Config) (*KeyCache, error)
Config { DBTimeout time.Duration }   -- 0 = no library-enforced timeout
KeyCache.Middleware(next http.Handler) http.Handler
KeyCache.Evict(keyID string)
```

KeyCache internals:

```
KeyMetadata { KeyID, Hash[32]byte, EntityType, Limiter, Revoked }
miss  → DB lookup → populate → proceed
hit   → check revoked → rate limit → proceed
TTL refresh: background goroutine, every refreshEvery
Revocation: DB update + immediate Evict()
```

**ingest**

```
Config {
    Kafka      *kafka.Writer
    DB         db.DBConfig
    Auth       *auth.KeyCache
    MaxBodyKB  int            -- default 1
    Logger     Logger
}
NewHandler(cfg Config) (http.Handler, error)
```

**writer**

```
Config {
    KafkaBrokers  []string
    KafkaDialer   *kafka.Dialer   -- optional, TLS/SASL
    Topic         string
    Namespace     string          -- required, non-empty
    DB            db.DBConfig
    BatchSize     int             -- default 500
    FlushEvery    time.Duration   -- default 1s
    Logger        Logger
}
NewWorker(cfg Config) (*Worker, error)   -- error if Namespace blank
Worker.Run(ctx context.Context) error
```

**cache**

```
Config {
    KafkaBrokers  []string
    KafkaDialer   *kafka.Dialer
    Topic         string
    Namespace     string          -- required, non-empty
    Logger        Logger
}
NewStore(cfg Config) (*Store, error)   -- error if Namespace blank
Store.Run(ctx context.Context) error
Store.Get(entityID string) (schema.Location, FetchedAt time.Time, ok bool)
Store.ReadinessHandler() http.HandlerFunc   -- 503 until HWM reached
```

**admin**

```
NewKeyManager(pool *pgxpool.Pool, cfg db.DBConfig) (*KeyManager, error)
KeyManager.CreateKey(ctx, entityType string) (plaintext string, err error)
KeyManager.RevokeKey(ctx, keyID string) error
KeyManager.ListKeys(ctx context.Context) ([]schema.KeyInfo, error)
-- never returns hash
```

---

## Logger Interface

```
type Logger interface {
    Info(msg string, fields ...any)
    Error(msg string, fields ...any)
}
```

Nil-safe. Library is silent if user passes nil.

---

## Exported Errors

```
ErrUnauthorized       -- 401 path
ErrRateLimited        -- 429 path
ErrNamespaceRequired  -- config validation
ErrInvalidConfig      -- DBConfig validation (nil pool, empty table names after defaults, etc.)
```

Callers can `errors.Is()` on all of these.

---

## User Integration (complete example)

```go
// User owns schema — runs their own migrations before starting.
// Reference SQL lives at schema/migrations/001_init.sql (documentation only).
pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))

// Configure how trackr7 talks to the user's database.
dbCfg := db.DBConfig{
    Pool:     pool,
    MaxConns: 5, // trackr7 gets at most 5 connections from the shared pool

    // User's tables live in a custom Postgres schema with different names.
    LocationsTable: "trackr.pings",
    APIKeysTable:   "trackr.keys",
    AuditLogTable:  "trackr.key_audit",

    // User's location table has different column names.
    LocationColumns: db.LocationColumnMap{
        UUID:       "id",
        EntityID:   "device_id",
        EntityType: "device_type",
        Lat:        "latitude",
        Lng:        "longitude",
        TS:         "recorded_at",
    },
    // APIKeyColumns and AuditLogColumns zero value = defaults applied.
}

keyCache, _ := auth.NewKeyCache(dbCfg, 5*time.Minute, rate.Every(time.Second), 10)

ingestH, _ := ingest.NewHandler(ingest.Config{
    Kafka: kafkaWriter, DB: dbCfg, Auth: keyCache,
})

store, _ := cache.NewStore(cache.Config{
    KafkaBrokers: []string{"localhost:9092"},
    Topic:        "location.raw",
    Namespace:    "myapp-prod",
})
go store.Run(ctx)

worker, _ := writer.NewWorker(writer.Config{
    KafkaBrokers: []string{"localhost:9092"},
    Topic:        "location.raw",
    Namespace:    "myapp-prod",
    DB:           dbCfg,
})
go worker.Run(ctx)

mux.Handle("/ping", ingestH)
mux.HandleFunc("/ready", store.ReadinessHandler())
mux.HandleFunc("/location/{id}", func(w http.ResponseWriter, r *http.Request) {
    loc, fetchedAt, ok := store.Get(r.PathValue("id"))
    if !ok { http.NotFound(w, r); return }
    // caller decides if fetchedAt is fresh enough
    json.NewEncoder(w).Encode(map[string]any{
        "location":   loc,
        "fetched_at": fetchedAt,
    })
})
```

---

## Design Constraints

| Constraint              | Detail                                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Schema ownership        | User owns all DDL. Library ships reference SQL as docs, never executes it.                                         |
| Table name flexibility  | All table names configurable via `DBConfig`. Accept schema-qualified paths (`schema.table`).                       |
| Column name flexibility | Every column trackr7 reads or writes is configurable via column mapping structs.                                   |
| Column type contract    | Library trusts user's column types match reference SQL. Mismatch = undefined behavior. No runtime type validation. |
| Pool isolation          | `MaxConns > 0` → library creates a sub-pool to prevent writer batch writes from exhausting user's shared pool.     |

---

## Guarantees

| Guarantee                | Status                                                                              |
| ------------------------ | ----------------------------------------------------------------------------------- |
| At-least-once delivery   | ✓ commit after DB write                                                             |
| Dedupe                   | ✓ `ON CONFLICT (uuid) DO NOTHING`                                                   |
| Ordering                 | Kafka partition only. `ts` display-only, never used for ordering                    |
| Crash recovery           | replay from last committed offset                                                   |
| Revocation               | eventual, max `refreshEvery` window. documented                                     |
| Rate limiting            | per-key token bucket, single instance only                                          |
| Cache consistency        | eventually consistent. self-corrects on next ping. `FetchedAt` exposed to caller    |
| Consumer group isolation | library-owned group IDs. misuse impossible                                          |
| Horizontal scale         | not v1 — Redis needed for rate limit + shared cache                                 |
| Column type safety       | NOT guaranteed. User must match reference SQL types. Mismatch = undefined behavior. |
| Schema migrations        | NOT managed. User's responsibility. Library documents expected schema only.         |

---

## Dependencies

| Dep                      | Why                                   |
| ------------------------ | ------------------------------------- |
| `segmentio/kafka-go`     | pure Go, no CGO                       |
| `jackc/pgx/v5`           | UNNEST batch                          |
| `golang.org/x/time/rate` | token bucket                          |
| `google/uuid`            | client UUID gen (user-side)           |
| Redpanda                 | single binary Kafka-compatible broker |

Nothing else until real problem demands it.

---

## Build Phases

| Phase | Package                                  | Output                                                                                               |
| ----- | ---------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| 1     | `schema/` + `db/`                        | schema: types, exported errors, reference SQL. db: DBConfig, column maps, WithDefaults(), Validate() |
| 2     | `auth/`                                  | KeyCache, Middleware, rate limiter                                                                   |
| 3     | `ingest/`                                | `http.Handler`, auth wired                                                                           |
| 4     | `writer/`                                | Worker, owns reader, UNNEST batch                                                                    |
| 5     | `cache/`                                 | Store, owns reader, `FetchedAt`, readiness probe                                                     |
| 6     | `admin/`                                 | KeyManager                                                                                           |
| 7     | `cmd/trackr7-admin/`                     | thin CLI                                                                                             |
| 8     | Metrics hooks + audit log                | observability                                                                                        |
| 9     | Benchmark + godoc + README + example app | release ready                                                                                        |

---
