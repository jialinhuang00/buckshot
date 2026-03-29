package handler

import (
	"net/http"
	"time"
	"url-shortener-demo/shortener/hub"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
)

type RedirectHandler struct {
	pg    *store.PGStore
	redis *store.RedisStore
	kafka *store.KafkaProducer
}

func NewRedirectHandler(pg *store.PGStore, redis *store.RedisStore, kafka *store.KafkaProducer) *RedirectHandler {
	return &RedirectHandler{pg: pg, redis: redis, kafka: kafka}
}

func (h *RedirectHandler) Handle(c *gin.Context) {
	shortKey := c.Param("key")

	// 1. Bloom filter — definitely not exist → 404
	exists, err := h.redis.BloomCheck(c.Request.Context(), shortKey)
	if err == nil && !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}

	// 2. Cache lookup
	longURL, err := h.redis.CacheGet(c.Request.Context(), shortKey)
	if err != nil {
		// Cache miss — query DB
		longURL, err = h.pg.Get(c.Request.Context(), shortKey)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		// Write to cache
		_ = h.redis.CacheSet(c.Request.Context(), shortKey, longURL)
	}

	// 3. Fire-and-forget: INCR click counter + broadcast to SSE clients + publish to Kafka
	_ = h.redis.IncrClick(c.Request.Context(), shortKey)
	hub.Global.Broadcast(shortKey)
	h.kafka.Publish(store.ClickEvent{
		ShortKey:  shortKey,
		Timestamp: time.Now(),
		IP:        c.ClientIP(),
		UserAgent: c.Request.UserAgent(),
	})

	// 4. Redirect
	c.Redirect(http.StatusFound, longURL) // 302
}
