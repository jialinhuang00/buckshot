package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"url-shortener-demo/internal/config"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/segmentio/kafka-go"
)

type ClickEvent struct {
	ShortKey  string    `json:"short_key"`
	Timestamp time.Time `json:"timestamp"`
	IP        string    `json:"ip"`
	UserAgent string    `json:"user_agent"`
}

const (
	batchSize    = 500
	batchTimeout = 2 * time.Second
	numWriters   = 4
)

type row struct {
	shortKey string
	ts       time.Time
	ip       string
	device   string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.ClickHouseAddr()},
		Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		log.Fatal("clickhouse:", err)
	}
	defer ch.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{config.KafkaAddr()},
		Topic:    "click-events",
		GroupID:  "analytics-consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})

	log.Printf("consumer running, %d parallel writers, batch=%d, timeout=%s",
		numWriters, batchSize, batchTimeout)

	rows := make(chan row, batchSize*4)
	batchCh := make(chan []row, numWriters*2)

	// Reader goroutine: Kafka → rows
	go func() {
		defer close(rows)
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Println("consumer: reader stopping")
					return
				}
				log.Println("read error:", err)
				continue
			}
			var event ClickEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				continue
			}
			device := "desktop"
			for _, m := range []string{"Mobile", "Android", "iPhone"} {
				if strings.Contains(event.UserAgent, m) {
					device = "mobile"
					break
				}
			}
			select {
			case rows <- row{event.ShortKey, event.Timestamp, event.IP, device}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Batcher goroutine: rows → batchCh
	go func() {
		defer close(batchCh)
		batch := make([]row, 0, batchSize)
		ticker := time.NewTicker(batchTimeout)
		defer ticker.Stop()

		send := func() {
			if len(batch) == 0 {
				return
			}
			out := make([]row, len(batch))
			copy(out, batch)
			batch = batch[:0]
			select {
			case batchCh <- out:
			case <-ctx.Done():
			}
		}

		for {
			select {
			case row, ok := <-rows:
				if !ok {
					send() // final partial batch
					return
				}
				batch = append(batch, row)
				if len(batch) >= batchSize {
					send()
					ticker.Reset(batchTimeout)
				}
			case <-ticker.C:
				send()
			}
		}
	}()

	// N writer goroutines: batchCh → ClickHouse
	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for batch := range batchCh {
				flush(ch, batch, id)
			}
		}(i)
	}

	wg.Wait()
	_ = r.Close()
	log.Println("consumer: drained, clean exit")
}

func flush(ch clickhouse.Conn, batch []row, writerID int) {
	if len(batch) == 0 {
		return
	}
	b, err := ch.PrepareBatch(context.Background(),
		`INSERT INTO click_events (short_key, ts, ip, device)`)
	if err != nil {
		log.Printf("[w%d] prepare batch error: %v", writerID, err)
		return
	}
	for _, r := range batch {
		if err := b.Append(r.shortKey, r.ts, r.ip, r.device); err != nil {
			log.Printf("[w%d] append error: %v", writerID, err)
		}
	}
	if err := b.Send(); err != nil {
		log.Printf("[w%d] batch send error: %v", writerID, err)
		return
	}
	log.Printf("[w%d] flushed %d events", writerID, len(batch))
}
