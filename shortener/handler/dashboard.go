package handler

import "github.com/gin-gonic/gin"

func Dashboard(c *gin.Context) {
	c.File("shortener/static/dashboard.html")
}
