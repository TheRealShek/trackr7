# AGENTS.md

## Project overview

trackr7 is an embeddable Go library for real-time entity location tracking. It is a **library, not a service** — no main packages exist except `cmd/trackr7-admin`. The user owns the server, connections, and lifecycle. trackr7 owns correctness.

Full design: `Docs/HLD.md` — read it before making architectural decisions.

## Package structure

```
schema/       → shared types (Location, KeyInfo), exported errors, reference SQL (not executed)
db/           → DBConfig, column maps, WithDefaults(), Validate()
auth/         → KeyCache, Middleware, rate limiting
ingest/       → http.Handler — auth, validate, produce to Kafka
writer/       → Worker — owns Kafka reader, batch DB write via UNNEST
cache/        → Store — owns Kafka reader, in-memory latest location
admin/        → KeyManager — create/revoke/list API keys
cmd/trackr7-admin/ → thin CLI over admin package
```

## Build and test commands

```bash
go mod tidy                           # resolve dependencies
go vet ./...                          # static analysis
go test ./... -v -count=1             # run all tests
go test ./db/ -v -count=1             # run db package tests only (no DB needed)
```

Integration tests require `TRACKR7_TEST_DSN` environment variable pointing to a Postgres instance.

## Code conventions

- **No global state.** All dependencies injected via Config structs.
- **No mocks** unless an interface demands it.
- **Table-driven tests** alongside each package.
- **Comments explain why**, not what.
- **Code readable over clever.**
- One package per session unless told otherwise.

## Critical design rules

- **Library, not service.** No `main` packages except `cmd/trackr7-admin`.
- **User owns schema.** trackr7 does not ship migrations, does not run SQL, does not call CREATE TABLE. Reference SQL at `schema/migrations/001_init.sql` is documentation only.
- **DBConfig is mandatory.** All packages that touch the DB accept `db.DBConfig`. Callers must call `WithDefaults()` then `Validate()` before use.
- **Identifiers must match `^[a-z0-9_.]+$`.** trackr7 does not quote identifiers in generated SQL. Uppercase, quotes, spaces, or special characters will break queries.
- **Configurable table and column names.** Never hardcode table or column names. Always read from DBConfig.
- **Schema-qualified paths allowed.** Table names like `"trackr.locations"` must pass through as-is.
- **HLD is the source of truth.** If a change affects architecture, API surface, package structure, or design constraints, ask permission to update `Docs/HLD.md` before or alongside the code change. Do not silently diverge from the HLD.

## Exported errors (schema/errors.go)

All sentinel errors live in `schema/` to avoid import cycles:

- `ErrUnauthorized` — 401
- `ErrRateLimited` — 429
- `ErrNamespaceRequired` — config validation
- `ErrInvalidConfig` — DBConfig validation

Use `fmt.Errorf("%w: ...", schema.ErrSomething)` so callers can `errors.Is()`.

## Dependencies

- `jackc/pgx/v5` — Postgres driver (UNNEST batch)
- `segmentio/kafka-go` — pure Go Kafka client
- `golang.org/x/time/rate` — token bucket rate limiting

Do not add dependencies without a real problem demanding it.

## Build phases

| Phase | Package                          | Status      |
| ----- | -------------------------------- | ----------- |
| 1     | `schema/` + `db/`                | Done        |
| 2     | `auth/`                          | Done        |
| 3     | `ingest/`                        | Done        |
| 4     | `writer/`                        | Done        |
| 5     | `cache/`                         | Done        |
| 6     | `admin/`                         | Done        |
| 7     | `cmd/trackr7-admin/`             | Not started |
| 8     | `benchmarks + README`            | Done        |
| 9     | `final polish + godoc + example` | Done        |

Portfolio-ready: Pending (TRACKR7_TEST_DSN not set for integration runs).

## Things to avoid

- Do not invent API surface. Match `Docs/HLD.md` exactly.
- Do not add `Migrate()` or any DDL execution.
- Do not use `//go:embed` for SQL (reference only, not embedded).
- No nil checks or error suppression as first response to a problem.
- No global variables or package-level init().
- Do not hardcode column names — always use DBConfig column maps.

## Output constraints

- Be concise: only essential, high-signal information.
- No explanations or extra context unless asked.
- Prefer short statements or minimal bullets.
