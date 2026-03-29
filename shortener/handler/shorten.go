package handler

import (
	"net/http"
	"url-shortener-demo/shortener/keygen"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
)

type ShortenRequest struct {
	LongURL string `json:"long_url" binding:"required"`
}

type ShortenHandler struct {
	pg      *store.PGStore
	redis   *store.RedisStore
	counter *keygen.Counter
}

func NewShortenHandler(pg *store.PGStore, redis *store.RedisStore, counter *keygen.Counter) *ShortenHandler {
	return &ShortenHandler{pg: pg, redis: redis, counter: counter}
}

func (h *ShortenHandler) Handle(c *gin.Context) {
	var req ShortenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	shortKey, err := h.counter.Next(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "counter failed"})
		return
	}

	if err := h.pg.Save(c.Request.Context(), shortKey, req.LongURL); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save"})
		return
	}

	// Pre-warm cache and add to Bloom filter
	_ = h.redis.CacheSet(c.Request.Context(), shortKey, req.LongURL)
	_ = h.redis.BloomAdd(c.Request.Context(), shortKey)

	c.JSON(http.StatusCreated, gin.H{
		"short_key": shortKey,
		"short_url": "http://localhost:8080/" + shortKey,
	})
}
