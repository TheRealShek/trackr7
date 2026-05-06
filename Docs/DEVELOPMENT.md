# Development Guide

This guide covers local setup for developing and testing trackr7.

## Prerequisites

- **Go**: Check `go.mod` for the minimum version (currently 1.21+)
- **Docker**: For running local Postgres
- **make**: For running common development tasks

## Local Postgres via Docker

Spin up a local Postgres instance:

```bash
docker run -d \
  --name trackr7-postgres \
  -e POSTGRES_USER=trackr7 \
  -e POSTGRES_PASSWORD=trackr7 \
  -e POSTGRES_DB=trackr7_test \
  -p 5432:5432 \
  postgres:15
```

Useful commands:

```bash
docker stop trackr7-postgres     # Stop the container
docker start trackr7-postgres    # Start the container
docker logs trackr7-postgres     # View logs
docker exec -it trackr7-postgres psql -U trackr7 -d trackr7_test  # Connect to DB
```

## Schema Setup

Apply the reference schema to your local database:

```bash
docker exec -i trackr7-postgres psql -U trackr7 -d trackr7_test < schema/migrations/001_init.sql
```

## Environment Setup

Copy the example environment file and update it:

```bash
cp .env.example .env
```

Edit `.env`:

```
TRACKR7_TEST_DSN=postgres://trackr7:trackr7@localhost:5432/trackr7_test
```

## Running Tests

```bash
make test                    # Unit tests only
make test-integration        # Full suite with database
make bench                   # Benchmarks (no database)
make bench-integration       # Benchmarks with database
```

See [TESTING.md](TESTING.md) for full test documentation.

## Common Issues

**Port 5432 in use**

Another Postgres instance is running locally. Stop it or change the port in `.env` and Docker command.

**Connection refused**

The container is not started. Run:

```bash
docker start trackr7-postgres
```

**Table does not exist**

The schema was not applied. Re-run the schema setup command in the [Schema Setup](#schema-setup) section above.
