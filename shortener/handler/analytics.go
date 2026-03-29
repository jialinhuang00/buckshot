package handler

import (
	"net/http"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AnalyticsHandler struct {
	ch   *store.ClickHouseStore
	pool *pgxpool.Pool
}

func NewAnalyticsHandler(ch *store.ClickHouseStore, pool *pgxpool.Pool) *AnalyticsHandler {
	return &AnalyticsHandler{ch: ch, pool: pool}
}

func (h *AnalyticsHandler) Page(c *gin.Context) {
	c.File("shortener/static/analytics.html")
}

// GET /analytics/data?g=30s|1m|15m|30m — returns all analytics in one payload
func (h *AnalyticsHandler) Data(c *gin.Context) {
	ctx := c.Request.Context()
	granularity := c.DefaultQuery("g", "30m")

	hourly, err := h.ch.ClicksPerInterval(ctx, granularity)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	topKeys, err := h.ch.TopKeys(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// enrich top keys with long_url from PG
	if len(topKeys) > 0 {
		keys := make([]string, len(topKeys))
		for i, k := range topKeys {
			keys[i] = k.ShortKey
		}
		rows, err := h.pool.Query(ctx,
			`SELECT short_key, long_url FROM url_mappings WHERE short_key = ANY($1)`, keys)
		if err == nil {
			urlMap := map[string]string{}
			for rows.Next() {
				var sk, lu string
				if err := rows.Scan(&sk, &lu); err == nil {
					urlMap[sk] = lu
				}
			}
			rows.Close()
			for i := range topKeys {
				topKeys[i].LongURL = urlMap[topKeys[i].ShortKey]
			}
		}
	}

	devices, err := h.ch.DeviceSplit(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"hourly":  hourly,
		"top":     topKeys,
		"devices": devices,
	})
}
