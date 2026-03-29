package handler

import (
	"fmt"
	"net/http"
	"sort"
	"time"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type StatsHandler struct {
	redis *store.RedisStore
	pool  *pgxpool.Pool
}

func NewStatsHandler(redis *store.RedisStore, pool *pgxpool.Pool) *StatsHandler {
	return &StatsHandler{redis: redis, pool: pool}
}

// GET /stats — list all keys with click counts (pg total + live redis delta)
func (h *StatsHandler) List(c *gin.Context) {
	rows, err := h.pool.Query(c.Request.Context(),
		`SELECT s.short_key, s.total_clicks, m.long_url
		 FROM url_stats s
		 JOIN url_mappings m ON m.short_key = s.short_key
		 ORDER BY s.total_clicks DESC LIMIT 100`,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	type entry struct {
		ShortKey    string `json:"short_key"`
		LongURL     string `json:"long_url"`
		TotalClicks int64  `json:"total_clicks"`
	}
	var result []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ShortKey, &e.TotalClicks, &e.LongURL); err != nil {
			continue
		}
		delta, _ := h.redis.GetClickCount(c.Request.Context(), e.ShortKey)
		e.TotalClicks += delta
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalClicks > result[j].TotalClicks
	})
	c.JSON(http.StatusOK, result)
}

// GET /stats/:key — snapshot (total from PG + live delta from Redis)
func (h *StatsHandler) Snapshot(c *gin.Context) {
	shortKey := c.Param("key")

	var pgTotal int64
	var longURL string
	_ = h.pool.QueryRow(c.Request.Context(),
		`SELECT COALESCE(s.total_clicks, 0), m.long_url
		 FROM url_stats s
		 JOIN url_mappings m ON m.short_key = s.short_key
		 WHERE s.short_key = $1`,
		shortKey,
	).Scan(&pgTotal, &longURL)

	delta, _ := h.redis.GetClickCount(c.Request.Context(), shortKey)

	c.JSON(http.StatusOK, gin.H{
		"short_key":    shortKey,
		"long_url":     longURL,
		"total_clicks": pgTotal + delta,
		"live_delta":   delta,
		"pg_total":     pgTotal,
	})
}

// GET /stats/:key/stream — SSE, pushes updated click count every 2 seconds
func (h *StatsHandler) Stream(c *gin.Context) {
	shortKey := c.Param("key")

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	ctx := c.Request.Context()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var pgTotal int64
			_ = h.pool.QueryRow(ctx,
				`SELECT COALESCE(total_clicks, 0) FROM url_stats WHERE short_key = $1`,
				shortKey,
			).Scan(&pgTotal)

			delta, _ := h.redis.GetClickCount(ctx, shortKey)
			total := pgTotal + delta

			fmt.Fprintf(c.Writer, "data: %d\n\n", total)
			c.Writer.Flush()
		}
	}
}
