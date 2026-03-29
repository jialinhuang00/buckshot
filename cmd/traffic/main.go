package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
	rows, err := pool.Query(ctx, `SELECT short_key FROM url_mappings`)
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

	// atomic pointer to current keySet
	var ptr unsafe.Pointer
	atomic.StorePointer(&ptr, unsafe.Pointer(ks))

	// background reload
	go func() {
		ticker := time.NewTicker(reloadEvery)
		defer ticker.Stop()
		for range ticker.C {
			next := loadKeys(context.Background(), pool)
			if next == nil {
				continue
			}
			atomic.StorePointer(&ptr, unsafe.Pointer(next))
			log.Printf("reloaded keys: %d total", len(next.all))
		}
	}()

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	log.Println("traffic running continuously, Ctrl+C to stop")
	total := 0
	for {
		cur := (*keySet)(atomic.LoadPointer(&ptr))
		start := time.Now()
		for i := 0; i < requests; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()

				cur := (*keySet)(atomic.LoadPointer(&ptr))
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
		_ = cur
		log.Printf("batch done: %d total | %.0f req/s", total, float64(requests)/elapsed.Seconds())
	}
}

