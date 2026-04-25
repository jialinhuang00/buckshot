# url-shortener-demo

> One redirect. Five systems.

A local URL shortener that traces every click through five components: a Go HTTP server (`shortener`), PostgreSQL (source of truth), Redis (cache + bloom filter + live click counter), Kafka (event bus), and ClickHouse (analytics OLAP store). Real-time dashboard via SSE. Analytics via ClickHouse aggregation.

For the full architecture, see [CLAUDE.md](./CLAUDE.md).

## Prerequisites

- Go 1.21+
- Docker + Docker Compose

Ports `5433` (PG) and `6380` (Redis) are used to avoid colliding with any local PG/Redis you might already run on the standard ports.

## Start

Open 4 terminals (T0 is one-shot, T1–T3 stay open):

```bash
git clone ...
cd url-shortener-demo

# T0 — one-shot setup
make setup         # start Docker infra + init schemas
                   # or `make ready` if you also want bin/ compiled

# T1, T2, T3 — long-running services, each in its own terminal
make run-shortener # T1 — API on :8080
make run-consumer  # T2 — Kafka → ClickHouse
make run-flush     # T3 — Redis → PostgreSQL every 5s

# T4 — utilities, run sequentially
make seed          # seed 50,000 URLs (one-shot, completes in seconds)
make mimic         # simulate click traffic — needs shortener up (Ctrl+C to stop)
```

**Minimum to demo a redirect**: `make setup` + `make run-shortener` + `make seed` + a `curl`.
Other workers (consumer/flush) only enrich the dashboard / analytics views.

## Try the API

**Create a short URL**

```bash
curl -s -X POST :8080/shorten \
  -H "Content-Type: application/json" \
  -d '{"long_url":"https://example.com"}' | jq .
# → { "short_url": "http://localhost:8080/aB3cD2" }
```

**Hit a seeded key**

```bash
# Pick one
KEY=$(docker exec -i url-shortener-demo-postgres-1 \
  psql -U user -d urlshortener -tAc \
  "SELECT short_key FROM url_mappings ORDER BY random() LIMIT 1")

# Follow the redirect (302)
curl -v :8080/$KEY
```

**Reject a non-existent key (bloom filter blocks it before PG)**

```bash
curl -v :8080/zzzzzz   # → 404
```

## Web UI

| URL | What it shows |
|---|---|
| http://localhost:8080/ | Dashboard — top 100 keys with live click counts (PG total + Redis live delta), SSE-pushed |
| http://localhost:8080/analytics | Analytics — clicks per interval (30s / 1m / 15m / 30m), top 10, device split. Query ClickHouse |

The dashboard updates per-click via SSE (event-driven). Analytics is pulled on demand from ClickHouse, so its data lags by however long the consumer takes to drain Kafka into CH (under load: a few seconds).

## Config

Defaults match `docker-compose.yml` — no config needed to run locally.
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
