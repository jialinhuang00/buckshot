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

		// Simple device detection from user agent
		device := "desktop"
		if len(event.UserAgent) > 0 {
			for _, m := range []string{"Mobile", "Android", "iPhone"} {
				if contains(event.UserAgent, m) {
					device = "mobile"
					break
				}
			}
		}

		err = ch.Exec(context.Background(),
			`INSERT INTO click_events (short_key, ts, ip, country, device) VALUES (?, ?, ?, ?, ?)`,
			event.ShortKey, event.Timestamp, event.IP, "TW", device,
		)
		if err != nil {
			log.Println("clickhouse insert error:", err)
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
