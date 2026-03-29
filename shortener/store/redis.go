package store

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	bloomKey    = "bloom:urls"
	bloomSize   = 1_000_000 // 1M bits
	bloomHashes = 5
	cacheTTL    = 10 * time.Minute
)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(addr string) *RedisStore {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	return &RedisStore{rdb: rdb}
}

// Cache: short_key → long_url
func (r *RedisStore) CacheSet(ctx context.Context, shortKey, longURL string) error {
	return r.rdb.Set(ctx, "url:"+shortKey, longURL, cacheTTL).Err()
}

func (r *RedisStore) CacheGet(ctx context.Context, shortKey string) (string, error) {
	return r.rdb.Get(ctx, "url:"+shortKey).Result()
}

// Click counter (INCR)
func (r *RedisStore) IncrClick(ctx context.Context, shortKey string) error {
	return r.rdb.Incr(ctx, "clicks:"+shortKey).Err()
}

func (r *RedisStore) GetAndResetClick(ctx context.Context, shortKey string) (int64, error) {
	return r.rdb.GetDel(ctx, "clicks:"+shortKey).Int64()
}

// GetClickCount reads the current live delta without resetting.
func (r *RedisStore) GetClickCount(ctx context.Context, shortKey string) (int64, error) {
	val, err := r.rdb.Get(ctx, "clicks:"+shortKey).Int64()
	if err != nil {
		return 0, nil // key doesn't exist → 0 clicks
	}
	return val, nil
}

// Bloom filter using Redis bitset
// Uses bloomHashes different bit positions derived from SHA-256.
func bloomPositions(key string) []int64 {
	h := sha256.Sum256([]byte(key))
	var positions []int64
	for i := 0; i < bloomHashes; i++ {
		// Take 8 bytes at a time, offset by i*4 bytes
		offset := i * 4
		var val int64
		for j := 0; j < 8; j++ {
			val = val<<8 | int64(h[(offset+j)%32])
		}
		if val < 0 {
			val = -val
		}
		positions = append(positions, val%bloomSize)
	}
	return positions
}

func (r *RedisStore) BloomAdd(ctx context.Context, shortKey string) error {
	pipe := r.rdb.Pipeline()
	for _, pos := range bloomPositions(shortKey) {
		pipe.SetBit(ctx, bloomKey, pos, 1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// BloomCheck returns true if key MIGHT exist (could be false positive),
// false if key DEFINITELY does not exist.
func (r *RedisStore) BloomCheck(ctx context.Context, shortKey string) (bool, error) {
	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, bloomHashes)
	for i, pos := range bloomPositions(shortKey) {
		cmds[i] = pipe.GetBit(ctx, bloomKey, pos)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	for _, cmd := range cmds {
		if cmd.Val() == 0 {
			return false, nil // definitely not in set
		}
	}
	return true, nil // might be in set
}
