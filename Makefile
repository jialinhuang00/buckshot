# URL Shortener Demo — command reference

.PHONY: setup ready infra-up infra-down infra-status db-init db-shell ch-init db-prune run-shortener run-consumer run-flush run-all-down seed seed-parallel mimic build test-shorten test-redirect

# ── Workflows ──────────────────────────────────────────────────────
# First time setup: start infra, create schema.
setup: infra-up db-init ch-init

# Everything ready: infra up, schema in, binaries compiled in bin/.
# Use this if you want ./bin/<name> instead of go run.
ready: setup build

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
# Drop all tables, recreate schema, clear ClickHouse + Redis + Kafka topic.
# Stop all services before running.
db-prune:
	docker exec -i url-shortener-demo-postgres-1 psql -U user -d urlshortener -c \
		"DROP TABLE IF EXISTS url_mappings, url_stats, id_counter, seed_ranges CASCADE;"
	curl -s -X POST http://localhost:8123 --data "DROP TABLE IF EXISTS click_events"
	docker exec -i url-shortener-demo-redis-1 redis-cli FLUSHALL
	docker exec -i url-shortener-demo-kafka-1 kafka-topics --bootstrap-server localhost:9092 --delete --topic click-events || true
	sleep 3
	docker exec -i url-shortener-demo-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic click-events --partitions 1 --replication-factor 1 || true
	$(MAKE) db-init ch-init

# ── Run services ───────────────────────────────────────────────────
run-shortener:
	go run ./cmd/shortener/

run-consumer:
	go run ./cmd/consumer/

run-flush:
	go run ./cmd/flush/

# ── Seed & mimic ───────────────────────────────────────────────────
seed:
	docker exec -i url-shortener-demo-postgres-1 psql -U user -d urlshortener -c \
		"UPDATE seed_ranges SET total = next_start + 50000;"
	go run ./cmd/seed/...

# Run 3 seed workers in parallel — each atomically claims its own range from seed_ranges.
# Workers compete for batches; the coordinator row prevents overlap.
seed-parallel:
	docker exec -i url-shortener-demo-postgres-1 psql -U user -d urlshortener -c \
		"UPDATE seed_ranges SET total = next_start + 50000;"
	go build -o /tmp/seed-bin ./cmd/seed/...
	/tmp/seed-bin & P1=$$!; /tmp/seed-bin & P2=$$!; /tmp/seed-bin & P3=$$!; trap "kill $$P1 $$P2 $$P3 2>/dev/null" INT TERM; wait

mimic:
	go run ./cmd/mimic/...

# ── Build ──────────────────────────────────────────────────────────
build:
	go build -o bin/shortener ./cmd/shortener/
	go build -o bin/consumer ./cmd/consumer/
	go build -o bin/flush ./cmd/flush/

# ── Stop running services ──────────────────────────────────────────
# Send SIGTERM to all running cmd/* binaries (graceful), wait 5s,
# then SIGKILL any stragglers. Doesn't touch docker infra.
run-all-down:
	@pids=$$(ps aux | grep -E "go-build.*(shortener|consumer|flush|mimic|seed)" | grep -v grep | awk '{print $$2}'); \
	if [ -z "$$pids" ]; then echo "nothing to stop"; exit 0; fi; \
	echo "SIGTERM: $$pids"; kill $$pids 2>/dev/null || true; \
	sleep 5; \
	stragglers=$$(ps aux | grep -E "go-build.*(shortener|consumer|flush|mimic|seed)" | grep -v grep | awk '{print $$2}'); \
	if [ -n "$$stragglers" ]; then \
		echo "SIGKILL stragglers: $$stragglers"; \
		kill -9 $$stragglers 2>/dev/null || true; \
	fi; \
	echo "done."

# ── Test ───────────────────────────────────────────────────────────
test-shorten:
	curl -s -X POST http://localhost:8080/shorten \
		-H "Content-Type: application/json" \
		-d '{"long_url":"https://example.com/very/long/path"}' | jq .

test-redirect:
	curl -v http://localhost:8080/$(KEY) 2>&1 | grep -E "Location|< HTTP"
