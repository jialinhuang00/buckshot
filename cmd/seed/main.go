package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
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

var realURLs = []string{
	// ── GitHub ──────────────────────────────────────────────
	"https://github.com/golang/go",
	"https://github.com/gin-gonic/gin",
	"https://github.com/redis/go-redis",
	"https://github.com/jackc/pgx",
	"https://github.com/segmentio/kafka-go",
	"https://github.com/kubernetes/kubernetes",
	"https://github.com/moby/moby",
	"https://github.com/cli/cli",
	"https://github.com/anthropics/anthropic-sdk-go",
	"https://github.com/neovim/neovim",
	"https://github.com/torvalds/linux",
	"https://github.com/microsoft/vscode",

	// ── Go docs ──────────────────────────────────────────────
	"https://go.dev/tour/welcome/1",
	"https://go.dev/blog/context",
	"https://go.dev/blog/concurrency-is-not-parallelism",
	"https://go.dev/blog/pipelines",
	"https://go.dev/blog/laws-of-reflection",
	"https://go.dev/blog/error-handling-and-go",
	"https://pkg.go.dev/context",
	"https://pkg.go.dev/sync",
	"https://pkg.go.dev/net/http",

	// ── YouTube ──────────────────────────────────────────────
	"https://www.youtube.com/watch?v=oV9rvDllKEg", // Go concurrency patterns — Rob Pike
	"https://www.youtube.com/watch?v=f6kdp27TYZs", // Kafka in 6 minutes
	"https://www.youtube.com/watch?v=LjoXhSMvAKE", // Redis crash course
	"https://www.youtube.com/watch?v=HyWYpM_S-2c", // Raft distributed consensus
	"https://www.youtube.com/watch?v=vLDAMKoqkQE", // Designing data-intensive apps
	"https://www.youtube.com/watch?v=U9mJuokhRAE", // System design interview

	// ── jialin00.com ────────────────────────────────────────
	"https://jialin00.com",
	"https://sd.jialin00.com",

	// ── dev.to ───────────────────────────────────────────────
	"https://dev.to/ben/what-s-a-webhook-5bj",
	"https://dev.to/thegeoffstevens/go-vs-rust-which-is-better-for-systems-programming-4fmb",
	"https://dev.to/gveloper/why-i-switched-from-python-to-go-2hoo",

	// ── Wikipedia: CS concepts ───────────────────────────────
	"https://en.wikipedia.org/wiki/Raft_(algorithm)",
	"https://en.wikipedia.org/wiki/Apache_Kafka",
	"https://en.wikipedia.org/wiki/B-tree",
	"https://en.wikipedia.org/wiki/CAP_theorem",
	"https://en.wikipedia.org/wiki/Consistent_hashing",
	"https://en.wikipedia.org/wiki/Bloom_filter",
	"https://en.wikipedia.org/wiki/Paxos_(computer_science)",

	// ── Wikipedia: movies ────────────────────────────────────
	"https://en.wikipedia.org/wiki/Oppenheimer_(film)",
	"https://en.wikipedia.org/wiki/Dune_(2021_film)",
	"https://en.wikipedia.org/wiki/Interstellar_(film)",
	"https://en.wikipedia.org/wiki/Everything_Everywhere_All_at_Once",
	"https://en.wikipedia.org/wiki/Parasite_(2019_film)",
	"https://en.wikipedia.org/wiki/The_Shawshank_Redemption",
	"https://en.wikipedia.org/wiki/Spirited_Away",

	// ── Letterboxd ───────────────────────────────────────────
	"https://letterboxd.com/film/oppenheimer-2023/",
	"https://letterboxd.com/film/dune-2021/",
	"https://letterboxd.com/film/interstellar/",
	"https://letterboxd.com/film/parasite-2019/",
	"https://letterboxd.com/film/everything-everywhere-all-at-once/",
}

func randomLongURL(i int64) string {
	return realURLs[i%int64(len(realURLs))]
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

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380", PoolSize: 3})
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
	pipe := rdb.Pipeline()
	for i := 0; i < bloomHashes; i++ {
		offset := i * 4
		var val int64
		for j := 0; j < 8; j++ {
			val = val<<8 | int64(h[(offset+j)%32])
		}
		if val < 0 {
			val = -val
		}
		pipe.SetBit(ctx, bloomKey, val%bloomSize, 1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("bloom pipeline error: %v", err)
	}
}
