package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Simulate N concurrent redirect requests against seeded URLs.
const (
	concurrency = 50
	requests    = 10_000
	baseURL     = "http://localhost:8080"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	// Load all short keys from DB so traffic uses real keys, not hardcoded patterns.
	pool, err := pgxpool.New(context.Background(), "postgres://user:password@127.0.0.1:5433/urlshortener?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	rows, err := pool.Query(context.Background(), `SELECT short_key FROM url_mappings`)
	if err != nil {
		log.Fatal("query keys:", err)
	}
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			log.Fatal(err)
		}
		keys = append(keys, k)
	}
	rows.Close()

	if len(keys) == 0 {
		log.Fatal("no keys found — run make seed first")
	}
	log.Printf("loaded %d keys from DB", len(keys))

	// Hot set: top 1% of keys get 80% of traffic (Zipf-like).
	hotN := max(1, len(keys)/100)
	hot := keys[:hotN]
	all := keys

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // don't follow redirects
		},
	}

	log.Println("traffic running continuously, Ctrl+C to stop")
	total := 0
	for {
		start := time.Now()
		for i := 0; i < requests; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()

				var shortKey string
				if rand.Float32() < 0.8 {
					shortKey = hot[rand.Intn(len(hot))]
				} else {
					shortKey = all[rand.Intn(len(all))]
				}

				resp, err := client.Get(baseURL + "/" + shortKey)
				if err != nil {
					return
				}
				resp.Body.Close()
			}()
		}
		wg.Wait()
		total += requests
		elapsed := time.Since(start)
		log.Printf("batch done: %d total | %.0f req/s", total, float64(requests)/elapsed.Seconds())
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
