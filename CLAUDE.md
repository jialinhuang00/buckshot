# CLAUDE.md

## Project

Local URL shortener demo. Go + PostgreSQL + Redis + Kafka + ClickHouse.

> One redirect. Five systems.

## Roles & Ports

```
┌─ Docker Compose (infra) ────────────────────────────┐
│  PostgreSQL   5433 (host) → 5432 (container)        │
│  Redis        6380 (host) → 6379 (container)        │
│  Kafka        9092:9092                              │
│  ClickHouse   8123:8123 (HTTP)  9000:9000 (TCP)     │
└─────────────────────────────────────────────────────┘

┌─ Go process (local) ────────────────────────────────┐
│  shortener   :8080   API + SSE + dashboard          │
│  consumer    —       Kafka → ClickHouse             │
│  flush       —       Redis → PostgreSQL             │
│  traffic     —       打 shortener（模擬流量）        │
│  seed        —       寫 PG + Redis（一次性）         │
└─────────────────────────────────────────────────────┘
```

Port offset: 本機已有 PG(:5432) 和 Redis(:6379)，Docker 映射到 5433 / 6380 避免衝突。

```
traffic → shortener(:8080)
            ├── PG(:5433)      url_mappings, url_stats
            ├── Redis(:6380)   bloom, cache, INCR
            ├── Kafka(:9092)   click-events topic (Producer)
            └── SSE → browser

flush    → Redis(:6380) → PG(:5433)
consumer → Kafka(:9092) → ClickHouse(:9000)   (Consumer)
browser  → shortener(:8080) → ClickHouse(:8123)
```

## Services

| Binary | Command | Role |
|---|---|---|
| shortener | `make run-shortener` | sole entry point — POST /shorten, GET /:key, GET /stats, dashboard, analytics |
| consumer | `make run-consumer` | background worker — Kafka click-events → ClickHouse click_events |
| flush | `make run-flush` | background worker — Redis clicks:\<key\> → PG url_stats every 5s |

shortener is the sole entry point. consumer and flush are background workers — no HTTP, self-scheduled, run until process dies.

- flush:    ticker-driven — wakes every 5s regardless of traffic
- consumer: hybrid — event-driven (rows channel) for throughput + ticker-driven (2s) as timeout flush

```
shortener:  start → connect PG + Redis + Kafka + ClickHouse → serve HTTP :8080
  POST /shorten → counter.Next() (PG atomic) → base62 short_key
              → PG url_mappings (save)
              → Redis url:<key> (cache pre-warm)
              → Redis bloom filter (BloomAdd)
              → return short_url
  GET /:key   → Redis bloom check → miss → 404
              → Redis cache hit  → long_url
                           miss  → PG lookup → cache set
              → Redis INCR clicks:<key>
              → hub.Broadcast(key) → SSE clients +1
              → Kafka produce click-event (fire-and-forget goroutine)
              → 302 redirect

consumer:   start → connect Kafka + ClickHouse → read click-events topic
  goroutine: ReadMessage → parse JSON → detect device → rows channel
  main loop: select rows (batch up to 500) | ticker 2s → PrepareBatch → Append × N → Send → ClickHouse
  note: PrepareBatch uses ClickHouse native binary protocol (TCP :9000), not text SQL.
        INSERT header sent once; Append streams rows as binary blocks; Send finalizes.
        Not valid in PostgreSQL — ClickHouse-specific.

flush:      start → connect Redis + PG → ticker every 5s
  SCAN clicks:* → GetDel (atomic) → INSERT url_stats ON CONFLICT DO UPDATE total_clicks += val
```

## Start sequence

```
make setup         # first time: infra-up + db-init + ch-init
make run-shortener # start API (terminal 1)
make run-consumer  # start Kafka consumer (terminal 2)
make run-flush     # start flush worker (terminal 3)
make seed          # seed 50,000 URLs (repeatable, auto-extends)
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
| `shortener/handler/dashboard.go` | Serves static dashboard HTML |
| `shortener/handler/analytics.go` | GET /analytics, GET /analytics/data |
| `shortener/handler/stats.go` | GET /stats, GET /stats/:key, GET /stats/:key/stream |
| `shortener/store/clickhouse.go` | ClickHouse queries (clicks/interval, top keys, device split) |
| `shortener/store/redis.go` | Cache + Bloom filter (bitset) + INCR |
| `shortener/keygen/counter.go` | DB-backed atomic counter → base62 |
| `shortener/static/dashboard.html` | Dashboard UI (chips / bar / rows, SSE-driven) |
| `shortener/static/analytics.html` | Analytics UI (clicks/interval, top 10, device split) |
| `consumer/main.go` | Kafka consumer → ClickHouse |
| `flush/main.go` | Redis INCR flush → PostgreSQL every 5s |
| `cmd/seed/main.go` | Distributed seed — workers claim ranges from seed_ranges |
| `cmd/traffic/main.go` | Continuous traffic simulation (80/20 Zipf-like, reloads keys every 30s) |

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

Dashboard  http://localhost:8080/
  GET /stats  → top 100 (PG total + Redis delta), re-sorted after adding delta
  GET /stream → SSE per-click push → browser +1
               resync every 10s (FLIP animation, fade dropped keys)

Analytics  http://localhost:8080/analytics
  GET /analytics/data?g=30s|1m|15m|30m
    → clicks per interval (ClickHouse)
    → top 10 keys + long_url (ClickHouse + PG join)
    → device split (ClickHouse)
```

## DB schema (PostgreSQL)

- `url_mappings` — short_key → long_url
- `url_stats` — short_key → total_clicks (flushed from Redis)
- `id_counter` — single row, atomic counter for key generation
- `seed_ranges` — distributed seed coordinator (next_start, total)

## ClickHouse schema

- `click_events` — short_key, ts, ip, country, device (append-only, MergeTree)

## Seed design

Multiple workers atomically claim ranges via `UPDATE seed_ranges SET next_start = next_start + batch ... RETURNING next_start - batch`. No overlap. `make seed` auto-extends `total` by 50000 each run, so repeated calls keep adding data.

## Reset

```
make db-reset   # TRUNCATE PG rows + CH + Redis + Kafka topic (keep schema)
make db-prune   # DROP all PG tables + CH + Redis + Kafka topic, recreate schema
```

Both reset Kafka topic to avoid stale messages being re-consumed into fresh ClickHouse.

**Stop consumer before resetting.** `kafka-go` buffers fetched messages in memory. If `run-consumer` is running during reset, buffered messages get written to the freshly-created ClickHouse — analytics will show stale data even before `make traffic` runs. Stop consumer first, restart after reset.

## Redis keys

| Key | Type | What |
|---|---|---|
| `bloom:urls` | bitset | Bloom filter — blocks nonexistent keys before PG lookup |
| `url:<key>` | string | Cache: short_key → long_url (TTL 10min) |
| `clicks:<key>` | string | Click counter, flushed to PG every 5s |

## Performance notes

Kafka and ClickHouse consume significant CPU even at idle (background merge, leader election). Stop them if not needed: `docker compose stop kafka clickhouse`.
