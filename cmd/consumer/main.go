package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
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

	log.Println("consumer running, waiting for click events...")

	rows := make(chan row, batchSize*4)
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

	flush := func(batch []row) {
		if len(batch) == 0 {
			return
		}
		b, err := ch.PrepareBatch(context.Background(),
			`INSERT INTO click_events (short_key, ts, ip, device)`)
		if err != nil {
			log.Println("prepare batch error:", err)
			return
		}
		for _, r := range batch {
			if err := b.Append(r.shortKey, r.ts, r.ip, r.device); err != nil {
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
		case row, ok := <-rows:
			if !ok {
				flush(batch)
				_ = r.Close()
				log.Println("consumer: drained, clean exit")
				return
			}
			batch = append(batch, row)
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
