package handler

import "github.com/gin-gonic/gin"

func Dashboard(c *gin.Context) {
	c.File("shortener/static/dashboard.html")
}

// GET /archi — animated architecture walk. The page is one static file with no
// calls back to this server; it is built by the hiroki-archi-flow skill.
func Archi(c *gin.Context) {
	c.File("shortener/static/archi.html")
}
