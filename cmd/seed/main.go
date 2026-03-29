package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	bloomKey    = "bloom:urls"
	bloomSize   = 1_000_000
	bloomHashes = 5
)

const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func toBase62(n int64) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 8)
	for n > 0 {
		buf = append([]byte{base62Chars[n%62]}, buf...)
		n /= 62
	}
	return string(buf)
}

func randomShortKey(i int64) string {
	mixed := i*6364136223846793005 + int64(rand.Int31())
	if mixed < 0 {
		mixed = -mixed
	}
	s := toBase62(mixed % 56800235584) // 62^6
	for len(s) < 6 {
		s = "0" + s
	}
	return s
}

var urlTemplates = []func(i int64) string{
	func(i int64) string {
		repos := []string{"golang/go", "kubernetes/kubernetes", "moby/moby", "cli/cli", "gin-gonic/gin", "redis/redis", "facebook/react", "vercel/next.js"}
		return fmt.Sprintf("https://github.com/%s/issues/%d", repos[i%int64(len(repos))], 1000+i)
	},
	func(i int64) string {
		repos := []string{"golang/go", "torvalds/linux", "microsoft/vscode", "anthropics/anthropic-sdk-go"}
		return fmt.Sprintf("https://github.com/%s/pull/%d", repos[i%int64(len(repos))], 500+i)
	},
	func(i int64) string {
		topics := []string{"how-to-use-goroutines", "difference-between-interface-and-struct", "how-does-kafka-replication-work", "postgres-index-types-explained"}
		return fmt.Sprintf("https://stackoverflow.com/questions/%d/%s", 10000000+i, topics[i%int64(len(topics))])
	},
	func(i int64) string {
		ids := []string{"dQw4w9WgXcQ", "ysz5S6PUM-U", "oHg5SJYRHA0", "ZZ5LpwO-An4", "9bZkp7q19f0"}
		return fmt.Sprintf("https://www.youtube.com/watch?v=%s&t=%ds", ids[i%int64(len(ids))], i%3600)
	},
	func(i int64) string {
		subs := []string{"r/golang", "r/kubernetes", "r/programming", "r/devops", "r/distributed"}
		return fmt.Sprintf("https://www.reddit.com/%s/comments/%s/discussion_%d/", subs[i%int64(len(subs))], toBase62(i+1000), i)
	},
	func(i int64) string {
		slugs := []string{"understanding-kafka-internals", "raft-consensus-explained", "postgres-vs-mysql-in-2024", "building-a-url-shortener", "redis-data-structures-cheatsheet"}
		return fmt.Sprintf("https://medium.com/@author%d/%s-%d", i%200, slugs[i%int64(len(slugs))], i)
	},
	func(i int64) string {
		slugs := []string{"go-generics-in-practice", "k8s-networking-deep-dive", "lsm-tree-vs-b-tree"}
		return fmt.Sprintf("https://dev.to/user%d/%s-%d", i%300, slugs[i%int64(len(slugs))], i)
	},
	func(i int64) string {
		sections := []string{"technology", "science", "business", "world"}
		return fmt.Sprintf("https://www.nytimes.com/2024/03/28/%s/article-%d.html", sections[i%int64(len(sections))], i)
	},
	func(i int64) string {
		articles := []string{"Raft_(algorithm)", "Apache_Kafka", "B-tree", "CAP_theorem", "Consistent_hashing", "Bloom_filter"}
		return fmt.Sprintf("https://en.wikipedia.org/wiki/%s", articles[i%int64(len(articles))])
	},
	func(i int64) string {
		return fmt.Sprintf("https://x.com/user%d/status/%d", i%500, 1700000000000+i*1234567)
	},
}

func randomLongURL(i int64) string {
	return urlTemplates[i%int64(len(urlTemplates))](i)
}

// claimRange atomically claims [start, end) from the coordinator row.
// Returns (start, end, true) if a range was claimed, or (0, 0, false) if work is done.
func claimRange(ctx context.Context, pool *pgxpool.Pool, batch int64) (int64, int64, bool) {
	var start int64

	// Try to claim a full batch.
	err := pool.QueryRow(ctx, `
		UPDATE seed_ranges
		SET next_start = next_start + $1
		WHERE id = 1 AND next_start + $1 <= total
		RETURNING next_start - $1
	`, batch).Scan(&start)
	if err == nil {
		return start, start + batch, true
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Fatal("claim range:", err)
	}

	// Try to claim the remaining partial batch.
	// CTE captures the old next_start before the update overwrites it.
	var end int64
	err = pool.QueryRow(ctx, `
		WITH old AS (SELECT next_start, total FROM seed_ranges WHERE id = 1 FOR UPDATE)
		UPDATE seed_ranges
		SET next_start = old.total
		FROM old
		WHERE seed_ranges.id = 1 AND old.next_start < old.total
		RETURNING old.next_start, old.total
	`).Scan(&start, &end)
	if err == nil {
		return start, end, true
	}

	return 0, 0, false
}

func main() {
	batch := flag.Int64("batch", 1000, "IDs to claim per round")
	flag.Parse()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://user:password@127.0.0.1:5433/urlshortener?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("redis ping failed:", err)
	}

	total := int64(0)
	for {
		start, end, ok := claimRange(ctx, pool, *batch)
		if !ok {
			log.Printf("no more work — done. total inserted: %d", total)
			return
		}

		log.Printf("claiming range [%d, %d)", start, end)

		for i := start; i < end; i++ {
			shortKey := randomShortKey(i)
			longURL := randomLongURL(i)

			_, err := pool.Exec(ctx,
				`INSERT INTO url_mappings (short_key, long_url) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
				shortKey, longURL,
			)
			if err != nil {
				log.Printf("insert %s: %v", shortKey, err)
				continue
			}

			bloomAdd(ctx, rdb, shortKey)
		}

		total += end - start
		log.Printf("inserted %d so far", total)
	}
}

func bloomAdd(ctx context.Context, rdb *redis.Client, key string) {
	h := sha256.Sum256([]byte(key))
	for i := 0; i < bloomHashes; i++ {
		offset := i * 4
		var val int64
		for j := 0; j < 8; j++ {
			val = val<<8 | int64(h[(offset+j)%32])
		}
		if val < 0 {
			val = -val
		}
		if err := rdb.SetBit(ctx, bloomKey, val%bloomSize, 1).Err(); err != nil {
			log.Printf("bloom setbit error: %v", err)
			return
		}
	}
}
