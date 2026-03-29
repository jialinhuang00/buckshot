package main

import (
	"log"
	"url-shortener-demo/internal/config"
	"url-shortener-demo/shortener/handler"
	"url-shortener-demo/shortener/keygen"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
)

func main() {
	pg, err := store.NewPGStore(config.PostgresDSN())
	if err != nil {
		log.Fatal("postgres:", err)
	}

	redis := store.NewRedisStore(config.RedisAddr())
	kafka := store.NewKafkaProducer(config.KafkaAddr(), "click-events")
	counter := keygen.NewCounter(pg.Pool())
	stats := handler.NewStatsHandler(redis, pg.Pool())

	ch, err := store.NewClickHouseStore(config.ClickHouseAddr())
	if err != nil {
		log.Fatal("clickhouse:", err)
	}
	analytics := handler.NewAnalyticsHandler(ch, pg.Pool())

	r := gin.Default()
	r.GET("/", handler.Dashboard)
	r.POST("/shorten", handler.NewShortenHandler(pg, redis, counter, config.ShortenerBaseURL()).Handle)
	r.GET("/stats", stats.List)
	r.GET("/stats/:key", stats.Snapshot)
	r.GET("/stats/:key/stream", stats.Stream)
	r.GET("/stream", handler.Stream)
	r.GET("/analytics", analytics.Page)
	r.GET("/analytics/data", analytics.Data)
	r.GET("/:key", handler.NewRedirectHandler(pg, redis, kafka).Handle)

	log.Println("shortener running on", config.HTTPAddr())
	if err := r.Run(config.HTTPAddr()); err != nil {
		log.Fatal(err)
	}
}
