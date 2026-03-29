package keygen

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

type Counter struct {
	pool *pgxpool.Pool
}

func NewCounter(pool *pgxpool.Pool) *Counter {
	return &Counter{pool: pool}
}

// Next atomically increments the DB counter and returns the next short key.
// Uses UPDATE ... RETURNING which is atomic under PostgreSQL ACID guarantees —
// no two concurrent callers get the same value.
func (c *Counter) Next(ctx context.Context) (string, error) {
	var id int64
	err := c.pool.QueryRow(ctx,
		`UPDATE id_counter SET next_val = next_val + 1 WHERE id = 1 RETURNING next_val`,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("counter: %w", err)
	}
	return toBase62(id), nil
}

func toBase62(n int64) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 8)
	for n > 0 {
		buf = append([]byte{base62Chars[n%62]}, buf...)
		n /= 62
	}
	return string(buf)
}
