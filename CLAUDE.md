# CLAUDE.md

## Project

Local URL shortener demo. Go + PostgreSQL + Redis + Kafka + ClickHouse.

## Ports

| Service | Port |
|---|---|
| shortener API | 8080 |
| PostgreSQL (Docker) | 5433 |
| Redis (Docker) | 6380 |
| Kafka | 9092 |
| ClickHouse HTTP | 8123 |
| ClickHouse TCP | 9000 |

Note: PostgreSQL is on 5433 (not 5432) — local machine has its own PG on 5432.
Note: Redis is on 6380 (not 6379) — local machine has its own Redis on 6379.

## Services

| Binary | Command | Role |
|---|---|---|
| shortener | `make run-shortener` | POST /shorten, GET /:key, GET /stats, dashboard, GET /stream |
| consumer | `make run-consumer` | Kafka → ClickHouse |
| flush | `make run-flush` | Redis INCR → PostgreSQL every 5s |

## Workflows

```
make setup     # first time: infra-up + db-init + ch-init + seed
make rerun     # re-run: db-reset + seed + traffic
```

## Start sequence

```
make infra-up      # start Docker containers
make db-init       # create PostgreSQL schema (includes seed_ranges coordinator)
make ch-init       # create ClickHouse schema
make run-shortener # start API (terminal 1)
make run-consumer  # start Kafka consumer (terminal 2)
make run-flush     # start flush worker (terminal 3)
make seed          # seed 50,000 URLs — writes to PG + Redis bloom filter
make traffic       # simulate continuous skewed click traffic (Ctrl+C to stop)
```

## Key files

| File | What |
|---|---|
| `docker-compose.yml` | All infra containers (KRaft Kafka, no Zookeeper) |
| `Makefile` | All commands |
| `scripts/init.sql` | PostgreSQL schema + seed_ranges coordinator |
| `scripts/init_clickhouse.sql` | ClickHouse schema |
| `shortener/main.go` | API entry point |
| `shortener/hub/hub.go` | SSE broadcast hub — pushes clicked key to all clients |
| `shortener/handler/stream.go` | GET /stream — SSE endpoint for real-time dashboard |
| `shortener/handler/dashboard.go` | Dashboard HTML (chips + bar chart, SSE-driven) |
| `shortener/store/redis.go` | Cache + Bloom filter (bitset) + INCR |
| `shortener/keygen/counter.go` | DB-backed atomic counter → base62 |
| `consumer/main.go` | Kafka consumer → ClickHouse |
| `flush/main.go` | Redis INCR flush → PostgreSQL every 5s |
| `cmd/seed/main.go` | Distributed seed — workers claim ranges from seed_ranges |
| `cmd/traffic/main.go` | Continuous traffic simulation (80/20 Zipf-like) |

## Architecture

```
Write path
  POST /shorten
    → id_counter (PG atomic RETURNING)
    → base62 short_key
    → PG url_mappings
    → Redis cache url:<key>  (TTL 10min)
    → Redis Bloom SetBit ×5

Read path
  GET /:key
    → Bloom filter miss → 404
    → Redis cache hit  → long_url
                  miss → PG → populate cache
    → Redis INCR clicks:<key>
    → hub.Broadcast(key) → SSE dashboard
    → Kafka click-events
    → 302 redirect

Background
  consumer: Kafka → ClickHouse click_events
  flush:    Redis clicks:<key> every 5s → PG url_stats

Dashboard
  GET /stats  → top 100 (PG total + Redis delta)
  GET /stream → SSE per-click push → browser +1
               resync every 10s (re-rank, fade dropped)
```

## DB schema (PostgreSQL)

- `url_mappings` — short_key → long_url
- `url_stats` — short_key → total_clicks (flushed from Redis)
- `id_counter` — single row, atomic counter for key generation
- `seed_ranges` — distributed seed coordinator (next_start, total)

## Seed design

Multiple workers atomically claim ranges via `UPDATE seed_ranges SET next_start = next_start + batch ... RETURNING next_start - batch`. No overlap, no locks beyond PG row-level locking. `total` advances on each `db-reset` so index never reuses old values.

## Reset

```
make db-reset   # TRUNCATE rows, keep schema, advance seed_ranges.total by 50000
make db-prune   # DROP all tables, recreate schema, seed_ranges resets to 0
```

## Redis keys

| Key | Type | What |
|---|---|---|
| `bloom:urls` | bitset | Bloom filter — blocks nonexistent keys before PG lookup |
| `url:<key>` | string | Cache: short_key → long_url (TTL 10min) |
| `clicks:<key>` | string | Click counter, flushed to PG every 5s |

## Performance notes

Kafka and ClickHouse consume significant CPU even at idle (background merge, leader election). Stop them if not needed: `docker compose stop kafka clickhouse`.
