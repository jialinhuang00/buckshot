package store

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(dsn string) (*PGStore, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, err
	}
	return &PGStore{pool: pool}, nil
}

func (s *PGStore) Pool() *pgxpool.Pool { return s.pool }

func (s *PGStore) Close() { s.pool.Close() }

func (s *PGStore) Save(ctx context.Context, shortKey, longURL string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO url_mappings (short_key, long_url) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		shortKey, longURL,
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, shortKey string) (string, error) {
	var longURL string
	err := s.pool.QueryRow(ctx,
		`SELECT long_url FROM url_mappings WHERE short_key = $1`,
		shortKey,
	).Scan(&longURL)
	return longURL, err
}
