package handler

import (
	"net/http"
	"url-shortener-demo/shortener/store"

	"github.com/gin-gonic/gin"
)

type AnalyticsHandler struct {
	ch *store.ClickHouseStore
}

func NewAnalyticsHandler(ch *store.ClickHouseStore) *AnalyticsHandler {
	return &AnalyticsHandler{ch: ch}
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
