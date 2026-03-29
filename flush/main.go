package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Flush periodically drains Redis INCR click counters into PostgreSQL url_stats.
// Runs every 30 seconds.

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})

	pool, err := pgxpool.New(context.Background(), "postgres://user:password@127.0.0.1:5433/urlshortener?sslmode=disable")
	if err != nil {
		log.Fatal("postgres:", err)
	}

	log.Println("flush worker running, interval: 5s")
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		flush(context.Background(), rdb, pool)
	}
}

func flush(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool) {
	// Scan all clicks:* keys
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "clicks:*", 100).Result()
		if err != nil {
			log.Println("scan error:", err)
			return
		}

		for _, key := range keys {
			shortKey := key[len("clicks:"):]

			// Atomic get-and-delete
			val, err := rdb.GetDel(ctx, key).Int64()
			if err != nil || val == 0 {
				continue
			}

			// Upsert into url_stats
			_, err = pool.Exec(ctx,
				`INSERT INTO url_stats (short_key, total_clicks)
				 VALUES ($1, $2)
				 ON CONFLICT (short_key)
				 DO UPDATE SET total_clicks = url_stats.total_clicks + EXCLUDED.total_clicks`,
				shortKey, val,
			)
			if err != nil {
				log.Printf("flush %s +%d failed: %v\n", shortKey, val, err)
			} else {
				log.Printf("flushed %s +%d\n", shortKey, val)
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}
}
