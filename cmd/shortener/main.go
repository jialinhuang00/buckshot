package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"url-shortener-demo/internal/config"
	"url-shortener-demo/shortener/handler"
	"url-shortener-demo/shortener/keygen"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	r.GET("/archi", handler.Archi)
	r.POST("/shorten", handler.NewShortenHandler(pg, redis, counter, config.ShortenerBaseURL()).Handle)
	r.GET("/stats", stats.List)
	r.GET("/stats/:key", stats.Snapshot)
	r.GET("/stats/:key/stream", stats.Stream)
	r.GET("/stream", handler.Stream)
	r.GET("/analytics", analytics.Page)
	r.GET("/analytics/data", analytics.Data)
	r.GET("/:key", handler.NewRedirectHandler(pg, redis, kafka).Handle)

	srv := &http.Server{Addr: config.HTTPAddr(), Handler: r}

	go func() {
		log.Println("shortener running on", config.HTTPAddr())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("shortener: shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Println("server shutdown error:", err)
	}

	pg.Close()
	_ = redis.Close()
	_ = kafka.Close()
	_ = ch.Close()
	log.Println("shortener: clean exit")
}
