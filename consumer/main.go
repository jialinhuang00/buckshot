package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

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
)

type row struct {
	shortKey string
	ts       time.Time
	ip       string
	device   string
}

func main() {
	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		log.Fatal("clickhouse:", err)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "click-events",
		GroupID:  "analytics-consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
	defer r.Close()

	log.Println("consumer running, waiting for click events...")

	// goroutine reads Kafka → rows channel
	rows := make(chan row, batchSize*4)
	go func() {
		for {
			msg, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Println("read error:", err)
				continue
			}
			var event ClickEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				continue
			}
			device := "desktop"
			for _, m := range []string{"Mobile", "Android", "iPhone"} {
				if contains(event.UserAgent, m) {
					device = "mobile"
					break
				}
			}
			rows <- row{event.ShortKey, event.Timestamp, event.IP, device}
		}
	}()

	flush := func(batch []row) {
		if len(batch) == 0 {
			return
		}
		b, err := ch.PrepareBatch(context.Background(),
			`INSERT INTO click_events (short_key, ts, ip, country, device)`)
		if err != nil {
			log.Println("prepare batch error:", err)
			return
		}
		for _, r := range batch {
			if err := b.Append(r.shortKey, r.ts, r.ip, "TW", r.device); err != nil {
				log.Println("append error:", err)
			}
		}
		if err := b.Send(); err != nil {
			log.Println("batch send error:", err)
			return
		}
		log.Printf("flushed %d events to clickhouse", len(batch))
	}

	batch := make([]row, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case r := <-rows:
			batch = append(batch, r)
			if len(batch) >= batchSize {
				flush(batch)
				batch = batch[:0]
				ticker.Reset(batchTimeout)
			}
		case <-ticker.C:
			flush(batch)
			batch = batch[:0]
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
