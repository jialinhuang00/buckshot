package handler

import (
	"fmt"
	"url-shortener-demo/shortener/hub"

	"github.com/gin-gonic/gin"
)

// GET /stream — SSE endpoint that pushes each clicked short_key to the browser.
// Client maintains its own count map and increments on each event.
func Stream(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")

	ch := hub.Global.Subscribe()
	defer hub.Global.Unsubscribe(ch)

	ctx := c.Request.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-ch:
			fmt.Fprintf(c.Writer, "data: %s\n\n", key)
			c.Writer.Flush()
		}
	}
}
