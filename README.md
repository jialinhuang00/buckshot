# url-shortener-demo

> One redirect. Five systems.

A local URL shortener that traces every click through PostgreSQL, Redis, Kafka, and ClickHouse.
Real-time dashboard via SSE. Analytics via ClickHouse aggregation.

## Prerequisites

- Go 1.21+
- Docker + Docker Compose

## Start

```bash
git clone ...
cd url-shortener-demo

make setup         # T0 — start Docker infra + init schemas (one-shot)
                   #      or `make ready` if you also want bin/ compiled
make run-shortener # T1 — API on :8080  (long-running)
make run-consumer  # T2 — Kafka → ClickHouse  (long-running)
make run-flush     # T3 — Redis → PG every 5s  (long-running)

# T4 — utilities, run sequentially
make seed          #   seed 50,000 URLs (one-shot, completes in seconds)
make mimic         #   simulate click traffic — needs shortener up (Ctrl+C to stop)
```

Open http://localhost:8080 for the dashboard, http://localhost:8080/analytics for analytics.

**Minimum to demo a redirect**: `make setup` + `make run-shortener` + `make seed` + `curl :8080/<key>`.
Other workers (consumer/flush) only enrich the dashboard/analytics views.

## Config

Defaults match docker-compose.yml — no config needed to run locally.
Override by exporting env vars before starting services:

| Var | Default |
|---|---|
| `POSTGRES_DSN` | `postgres://user:password@127.0.0.1:5433/urlshortener?sslmode=disable` |
| `REDIS_ADDR` | `localhost:6380` |
| `KAFKA_ADDR` | `localhost:9092` |
| `CLICKHOUSE_ADDR` | `localhost:9000` |
| `HTTP_ADDR` | `:8080` |
| `SHORTENER_BASE_URL` | `http://localhost:8080` |

## Reset

```bash
make run-all-down  # graceful kill all run-* Go binaries (SIGTERM, then SIGKILL stragglers)
make db-prune      # drop PG/CH tables, FLUSHALL Redis, recreate Kafka topic + schemas
```

`make run-all-down` doesn't touch Docker infra. To stop infra too: `make infra-down`.
