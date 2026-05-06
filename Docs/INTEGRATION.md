# Integration Guide

This guide is for a Go backend engineer embedding `trackr7` into an existing service for the first time. Read it once from top to bottom, wire the pieces in this order, and you will have a working setup. For a runnable reference, see [`example/main.go`](../example/main.go).

## Prerequisites

- Go `1.26.2`
- Postgres `12+`
- Kafka-compatible broker such as Kafka or Redpanda
- Your schema must be applied before starting `trackr7`

`trackr7` is a library, not a service. It does not create tables, run migrations, or own your process lifecycle. You provide the Postgres pool, Kafka connectivity, HTTP server, and shutdown flow.

Apply the reference schema before starting:

```bash
psql "$TRACKR7_DSN" < schema/migrations/001_init.sql
```

The SQL in `schema/migrations/001_init.sql` is documentation for the expected schema. If your schema diverges from it, table names, column names, and types must still match what you configure through `db.DBConfig`.

## Step-by-Step Wiring

### 1. Apply schema first

Do this before anything else because every package assumes the tables already exist. `auth` reads API keys, `writer` inserts locations, and `admin` writes audit records. If the schema is missing, startup may succeed but runtime operations will fail.

### 2. Create a Postgres pool

Create your `*pgxpool.Pool` early because it is the shared dependency for `db.DBConfig`, `auth`, `writer`, and `admin`. The example does this before any `trackr7` constructor so config validation has a real pool to work with.

### 3. Build `db.DBConfig`, then call `WithDefaults()` and `Validate()`

This comes before the rest because `db.DBConfig` is the schema contract for the library. Start with at least `Pool`, call `WithDefaults()` to fill standard table and column names, then call `Validate()` to catch bad identifiers and missing values up front.

Typical pattern:

```go
dbCfg := db.DBConfig{Pool: pool}.WithDefaults()
if err := dbCfg.Validate(); err != nil { ... }
```

If you skip this and rely on zero values, you will eventually get invalid SQL or runtime failures. `example/main.go` uses this exact pattern.

### 4. Create `auth.KeyCache`

Create auth before the ingest handler because ingest depends on auth middleware to inject `entity_type` into the request context. Without auth in place, the ingest handler cannot authenticate requests or build correct Kafka messages.

Use `auth.NewKeyCache(dbCfg, refreshEvery, rateLimit, rateBurst, options...)`. Start its background loop with `go keyCache.Run(ctx)` during service startup.

Why it comes here:

- `ingest.NewHandler` requires `Auth`
- key validation should be ready before you expose `/ping`
- revocation behavior depends on this cache existing

### 5. Create `ingest` handler

Create the ingest handler after auth because `ingest.NewHandler` wraps its inner handler with `auth.Middleware`. This is the HTTP entrypoint that accepts pings, validates payloads, and produces Kafka messages.

At this point you have authenticated HTTP ingestion, but nothing is consuming the Kafka topic yet.

### 6. Create `writer.Worker`

Create the writer after ingest because writer consumes the Kafka messages that ingest produces. It persists accepted messages into Postgres in batches and commits Kafka offsets only after a successful DB write.

Why it comes before cache in this guide:

- persistence is the primary durable side effect
- if you are bringing the system up incrementally, you usually want database writes working before adding the in-memory read path

### 7. Create `cache.Store`

Create the cache store after writer. It reads the same Kafka topic and maintains the latest location for each entity in memory. This is the fast read path and readiness signal, not the source of truth.

Start it with `go store.Run(ctx)`. If you expose readiness, use `store.ReadinessHandler()`.

### 8. Mount HTTP handlers last

Mount handlers after the dependencies behind them exist. That way, once the routes are reachable, auth, Kafka production, writer, and cache are already wired.

Typical routes:

- `POST /ping` -> ingest handler
- `GET /ready` -> `store.ReadinessHandler()`

For a complete runnable assembly with the same components, see [`example/main.go`](../example/main.go).

## Configuration Reference

This section lists every public config field used to embed `trackr7`.

### `db.DBConfig`

- `Pool *pgxpool.Pool`: required Postgres pool used by DB-backed packages.
- `MaxConns int`: currently not enforced; use the supplied pool settings to control connection limits.
- `LocationsTable string`: locations table name; default `locations`.
- `APIKeysTable string`: API keys table name; default `api_keys`.
- `AuditLogTable string`: audit log table name; default `audit_log`.
- `LocationColumns db.LocationColumnMap`: location column mapping; zero values default to `uuid`, `entity_id`, `entity_type`, `lat`, `lng`, `ts`.
- `APIKeyColumns db.APIKeyColumnMap`: API key column mapping; zero values default to `key_id`, `key_hash`, `entity_type`, `revoked`, `created_at`.
- `AuditLogColumns db.AuditLogColumnMap`: audit log column mapping; zero values default to `key_id`, `action`, `ts`.

### `db.LocationColumnMap`

- `UUID string`: location UUID column; default `uuid`.
- `EntityID string`: entity ID column; default `entity_id`.
- `EntityType string`: entity type column; default `entity_type`.
- `Lat string`: latitude column; default `lat`.
- `Lng string`: longitude column; default `lng`.
- `TS string`: event timestamp column; default `ts`.

### `db.APIKeyColumnMap`

- `KeyID string`: API key ID column; default `key_id`.
- `KeyHash string`: SHA-256 key hash column; default `key_hash`.
- `EntityType string`: entity type column; default `entity_type`.
- `Revoked string`: revoked flag column; default `revoked`.
- `CreatedAt string`: creation timestamp column; default `created_at`.

### `db.AuditLogColumnMap`

- `KeyID string`: audit key ID column; default `key_id`.
- `Action string`: audit action column; default `action`.
- `TS string`: audit timestamp column; default `ts`.

### `auth.Config`

- `DBTimeout time.Duration`: optional timeout for auth DB calls when the caller context has no deadline; default `0` means no library-enforced timeout.

### `auth.NewKeyCache` constructor inputs

- `cfg db.DBConfig`: validated schema and pool configuration.
- `refreshEvery time.Duration`: full key cache refresh interval; controls revocation window if you do not evict immediately.
- `rateLimit rate.Limit`: per-key token refill rate.
- `rateBurst int`: per-key burst capacity.
- `options ...auth.Config`: optional auth-specific settings such as `DBTimeout`.

### `ingest.Config`

- `Kafka ingest.Producer`: required Kafka producer used to publish accepted pings.
- `Auth *auth.KeyCache`: required auth cache used for bearer token validation.
- `DB db.DBConfig`: included for package consistency; ingest does not query the DB.
- `MaxBodyKB int`: maximum request body size in KB; default `1`.
- `Logger schema.Logger`: optional logger; nil-safe.
- `OnPingAccepted func()`: optional hook fired after a `202`.
- `OnPingRejected func(reason string)`: optional hook fired on request rejection.
- `OnKafkaError func(err error)`: optional hook fired when Kafka production fails.

### `writer.Config`

- `KafkaBrokers []string`: required broker list for the Kafka consumer.
- `KafkaDialer *kafka.Dialer`: optional custom dialer for TLS or SASL.
- `Topic string`: required Kafka topic to consume.
- `Namespace string`: required consumer-group namespace; no default.
- `DB *pgxpool.Pool`: required Postgres pool used for inserts.
- `DBConfig db.DBConfig`: DB schema mapping used to build insert SQL.
- `DBTimeout time.Duration`: optional timeout for batch inserts when caller context has no deadline; default `0`.
- `BatchSize int`: maximum messages per flush; default `500`.
- `FlushEvery time.Duration`: maximum time between flushes; default `1s`.
- `Logger schema.Logger`: optional logger; nil-safe.
- `OnBatchFlushed func(count int, duration time.Duration)`: optional success hook.
- `OnFlushError func(err error)`: optional DB or commit error hook.
- `OnMessageSkipped func(reason string)`: optional hook for skipped messages.

### `cache.Config`

- `KafkaBrokers []string`: required broker list for the Kafka consumer.
- `KafkaDialer *kafka.Dialer`: optional custom dialer for TLS or SASL.
- `Topic string`: required Kafka topic to consume.
- `Namespace string`: required consumer-group namespace; no default.
- `Logger schema.Logger`: optional logger; nil-safe.
- `OnCacheUpdated func(entityID string)`: optional hook fired when an entity location is updated.
- `OnMessageSkipped func(reason string)`: optional hook for skipped messages.

`admin` does not expose a config struct. You create it with `admin.NewKeyManager(pool, dbCfg)`.

## Key Management

Use `admin.KeyManager` to create, revoke, and list API keys.

Typical setup:

```go
km, err := admin.NewKeyManager(pool, dbCfg)
if err != nil { ... }
km.SetCache(keyCache)
```

Create a key with `CreateKey(ctx, entityType)`. This returns the plaintext key exactly once. Store or deliver it immediately; the database stores only its SHA-256 hash.

Revoke a key with `RevokeKey(ctx, keyID)`. For immediate revocation, the key cache must be wired so the in-memory entry is evicted right after the DB write. The supported integration path is `km.SetCache(keyCache)`, as shown in [`example/main.go`](../example/main.go). Without that wiring, revocation is only visible after the next auth cache refresh.

Use `ListKeys(ctx)` when you need to inspect active and revoked keys without exposing key hashes.

## Security Checklist

Before production:

- Protect every endpoint or code path that calls `admin.KeyManager`. `trackr7` does not authenticate or authorize admin actions for you.
- Treat Kafka as a hard trust boundary. Use ACLs and TLS on the broker and on the `location.raw` topic.
- Plan around the revocation window. If you do not wire `SetCache(keyCache)`, a revoked key can remain valid until the next `refreshEvery`.
- Treat database admin access as security-sensitive. `audit_log` is append-only by convention only; DB admins can alter or delete rows.
- Set explicit deadlines on your top-level contexts. `auth` and `writer` can apply library timeouts when configured, but your service should still own request and shutdown budgets.

## Common Mistakes

- Leaving `Namespace` blank in `writer.Config` or `cache.Config`. It is required and prevents consumer-group collisions.
- Skipping `db.DBConfig.WithDefaults()` before `Validate()`. That leaves required identifiers empty unless you set every field manually.
- Forgetting to wire `km.SetCache(keyCache)`. Revocation will then wait for the next auth refresh instead of taking effect immediately.
- Starting the service before applying schema. Auth, writer, and admin all assume the tables already exist.
- Using column types that do not match what the code writes and reads. `trackr7` does not validate schema correctness at runtime.

For a complete runnable assembly of these pieces, use [`example/main.go`](../example/main.go) as the concrete reference implementation.
