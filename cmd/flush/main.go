package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"url-shortener-demo/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Flush periodically drains Redis INCR click counters into PostgreSQL url_stats.
// Runs every 5 seconds.

func main() {
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: config.RedisAddr()})
	defer rdb.Close()

	pool, err := pgxpool.New(context.Background(), config.PostgresDSN())
	if err != nil {
		log.Fatal("postgres:", err)
	}
	defer pool.Close()

	log.Println("flush worker running, interval: 5s")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("flush: final drain before exit")
			flush(context.Background(), rdb, pool)
			log.Println("flush: clean exit")
			return
		case <-ticker.C:
			flush(ctx, rdb, pool)
		}
	}
}

func flush(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool) {
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "clicks:*", 100).Result()
		if err != nil {
			log.Println("scan error:", err)
			return
		}

		for _, key := range keys {
			shortKey := key[len("clicks:"):]

			val, err := rdb.GetDel(ctx, key).Int64()
			if err != nil || val == 0 {
				continue
			}

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
