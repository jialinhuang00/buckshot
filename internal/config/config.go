package config

import "os"

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func PostgresDSN() string {
	return getEnv("POSTGRES_DSN", "postgres://user:password@127.0.0.1:5433/urlshortener?sslmode=disable")
}

func RedisAddr() string {
	return getEnv("REDIS_ADDR", "localhost:6380")
}

func KafkaAddr() string {
	return getEnv("KAFKA_ADDR", "localhost:9092")
}

func ClickHouseAddr() string {
	return getEnv("CLICKHOUSE_ADDR", "localhost:9000")
}

func HTTPAddr() string {
	return getEnv("HTTP_ADDR", ":8080")
}

func ShortenerBaseURL() string {
	return getEnv("SHORTENER_BASE_URL", "http://localhost:8080")
}
