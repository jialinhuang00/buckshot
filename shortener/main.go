package main

import (
	"log"
	"url-shortener-demo/shortener/handler"
	"url-shortener-demo/shortener/keygen"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
)

func main() {
	pg, err := store.NewPGStore("postgres://user:password@127.0.0.1:5433/urlshortener?sslmode=disable")
	if err != nil {
		log.Fatal("postgres:", err)
	}

	redis := store.NewRedisStore("localhost:6380")
	kafka := store.NewKafkaProducer("localhost:9092", "click-events")
	counter := keygen.NewCounter(pg.Pool())
	stats := handler.NewStatsHandler(redis, pg.Pool())

	ch, err := store.NewClickHouseStore("localhost:9000")
	if err != nil {
		log.Fatal("clickhouse:", err)
	}
	analytics := handler.NewAnalyticsHandler(ch, pg.Pool())

	r := gin.Default()
	r.GET("/", handler.Dashboard)
	r.POST("/shorten", handler.NewShortenHandler(pg, redis, counter).Handle)
	r.GET("/stats", stats.List)
	r.GET("/stats/:key", stats.Snapshot)
	r.GET("/stats/:key/stream", stats.Stream)
	r.GET("/stream", handler.Stream)
	r.GET("/analytics", analytics.Page)
	r.GET("/analytics/data", analytics.Data)
	r.GET("/:key", handler.NewRedirectHandler(pg, redis, kafka).Handle)

	log.Println("shortener running on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
