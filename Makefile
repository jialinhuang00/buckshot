# URL Shortener Demo — command reference

.PHONY: setup infra-up infra-down infra-status db-init db-shell ch-init db-reset db-prune run-shortener run-consumer run-flush seed seed-parallel traffic build test-shorten test-redirect

# ── Workflows ──────────────────────────────────────────────────────
# First time setup: start infra, create schema, seed data.
setup: infra-up db-init ch-init seed

# ── Infra ──────────────────────────────────────────────────────────
infra-up:
	docker compose up -d

infra-down:
	docker compose down

infra-status:
	docker ps

# ── DB setup ───────────────────────────────────────────────────────
db-init:
	docker exec -i url-shortener-demo-postgres-1 psql -U user -d urlshortener < scripts/init.sql

db-shell:
	docker exec -it url-shortener-demo-postgres-1 psql -U user -d urlshortener

ch-init:
	curl -s http://localhost:8123 --data-binary @scripts/init_clickhouse.sql

# ── DB reset ───────────────────────────────────────────────────────
# Truncate all rows, keep schema. seed_ranges.total advances by 50000
# so the next seed run continues from where it left off (no index reuse).
db-reset:
	docker exec -i url-shortener-demo-postgres-1 psql -U user -d urlshortener -c \
		"TRUNCATE url_mappings, url_stats, id_counter RESTART IDENTITY CASCADE; \
		 INSERT INTO id_counter VALUES (1, 100000000); \
		 UPDATE seed_ranges SET next_start = 0, total = 0;"
	curl -s -X POST http://localhost:8123 --data "TRUNCATE TABLE click_events"
	docker exec -i url-shortener-demo-redis-1 redis-cli FLUSHALL

# Drop all tables and recreate schema from scratch. Resets seed_ranges to 0.
db-prune:
	docker exec -i url-shortener-demo-postgres-1 psql -U user -d urlshortener -c \
		"DROP TABLE IF EXISTS url_mappings, url_stats, id_counter, seed_ranges CASCADE;"
	curl -s -X POST http://localhost:8123 --data "DROP TABLE IF EXISTS click_events"
	docker exec -i url-shortener-demo-redis-1 redis-cli FLUSHALL
	$(MAKE) db-init ch-init

# ── Run services ───────────────────────────────────────────────────
run-shortener:
	go run ./shortener/...

run-consumer:
	go run ./consumer/...

run-flush:
	go run ./flush/...

# ── Seed & traffic ─────────────────────────────────────────────────
seed:
	docker exec -i url-shortener-demo-postgres-1 psql -U user -d urlshortener -c \
		"UPDATE seed_ranges SET total = next_start + 50000;"
	go run ./cmd/seed/...

# Run 3 seed workers in parallel — each atomically claims its own range from seed_ranges.
# Workers compete for batches; the coordinator row prevents overlap.
seed-parallel:
	go build -o /tmp/seed-bin ./cmd/seed/...
	/tmp/seed-bin & /tmp/seed-bin & /tmp/seed-bin & wait

traffic:
	go run ./cmd/traffic/...

# ── Build ──────────────────────────────────────────────────────────
build:
	go build -o bin/shortener ./shortener/...
	go build -o bin/consumer ./consumer/...
	go build -o bin/flush ./flush/...

# ── Test ───────────────────────────────────────────────────────────
test-shorten:
	curl -s -X POST http://localhost:8080/shorten \
		-H "Content-Type: application/json" \
		-d '{"long_url":"https://example.com/very/long/path"}' | jq .

test-redirect:
	curl -v http://localhost:8080/$(KEY) 2>&1 | grep -E "Location|< HTTP"
