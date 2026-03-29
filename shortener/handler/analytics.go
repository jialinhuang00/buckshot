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

// GET /analytics/data — returns all analytics in one payload
func (h *AnalyticsHandler) Data(c *gin.Context) {
	ctx := c.Request.Context()

	hourly, err := h.ch.ClicksPerHour(ctx)
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
