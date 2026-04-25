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

make setup         # start Docker infra + init schemas
make run-shortener # terminal 1 — API on :8080
make run-consumer  # terminal 2 — Kafka → ClickHouse
make run-flush     # terminal 3 — Redis → PostgreSQL every 5s
make seed          # seed 50,000 URLs
make mimic         # simulate click traffic (Ctrl+C to stop)
```

Open http://localhost:8080 for the dashboard, http://localhost:8080/analytics for analytics.

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

Stop all services before resetting.

```bash
make db-prune  # drop everything, recreate schema
```
