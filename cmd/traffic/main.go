package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	concurrency = 50
	requests    = 10_000
	baseURL     = "http://localhost:8080"
	reloadEvery = 30 * time.Second
)

// keySet holds the current snapshot of keys, swapped atomically.
type keySet struct {
	all []string
	hot []string
}

func loadKeys(ctx context.Context, pool *pgxpool.Pool) *keySet {
	rows, err := pool.Query(ctx, `
		SELECT m.short_key FROM url_mappings m
		LEFT JOIN url_stats s ON s.short_key = m.short_key
		ORDER BY COALESCE(s.total_clicks, 0) DESC
	`)
	if err != nil {
		log.Printf("reload keys: %v", err)
		return nil
	}
	defer rows.Close()

	var all []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			continue
		}
		all = append(all, k)
	}
	if len(all) == 0 {
		return nil
	}
	hotN := max(1, len(all)/100)
	return &keySet{all: all, hot: all[:hotN]}
}

func main() {
	pool, err := pgxpool.New(context.Background(), "postgres://user:password@127.0.0.1:5433/urlshortener?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	ks := loadKeys(context.Background(), pool)
	if ks == nil {
		log.Fatal("no keys found — run make seed first")
	}
	log.Printf("loaded %d keys from DB", len(ks.all))

	var ptr atomic.Pointer[keySet]
	ptr.Store(ks)

	// background reload
	go func() {
		ticker := time.NewTicker(reloadEvery)
		defer ticker.Stop()
		for range ticker.C {
			next := loadKeys(context.Background(), pool)
			if next == nil {
				continue
			}
			ptr.Store(next)
			log.Printf("reloaded keys: %d total", len(next.all))
		}
	}()

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrency)

	log.Println("traffic running continuously, Ctrl+C to stop")
	total := 0
	for {
		start := time.Now()
		for i := 0; i < requests; i++ {
			wg.Add(1)
			semaphore <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-semaphore }()

				cur := ptr.Load()
				var shortKey string
				if rand.Float32() < 0.8 {
					shortKey = cur.hot[rand.Intn(len(cur.hot))]
				} else {
					shortKey = cur.all[rand.Intn(len(cur.all))]
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
