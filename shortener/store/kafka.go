package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type ClickEvent struct {
	ShortKey  string    `json:"short_key"`
	Timestamp time.Time `json:"timestamp"`
	IP        string    `json:"ip"`
	UserAgent string    `json:"user_agent"`
}

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(broker, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
	}
}

func (k *KafkaProducer) Close() error { return k.writer.Close() }

// Publish sends a click event fire-and-forget (does not block redirect).
func (k *KafkaProducer) Publish(event ClickEvent) {
	b, err := json.Marshal(event)
	if err != nil {
		return
	}
	// Non-blocking: publish in background, redirect does not wait
	go func() {
		_ = k.writer.WriteMessages(context.Background(), kafka.Message{Value: b})
	}()
}
