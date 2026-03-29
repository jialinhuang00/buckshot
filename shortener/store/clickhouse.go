package store

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseStore struct {
	conn clickhouse.Conn
}

func NewClickHouseStore(addr string) (*ClickHouseStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		return nil, err
	}
	return &ClickHouseStore{conn: conn}, nil
}

type HourlyClick struct {
	Hour   string `json:"hour"`
	Clicks uint64 `json:"clicks"`
}

type TopKey struct {
	ShortKey string `json:"short_key"`
	Clicks   uint64 `json:"clicks"`
}

type DeviceSplit struct {
	Device string `json:"device"`
	Clicks uint64 `json:"clicks"`
}

// ClicksPerHour returns click counts grouped by hour for the last 24 hours.
func (s *ClickHouseStore) ClicksPerHour(ctx context.Context) ([]HourlyClick, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT toString(toStartOfHour(ts)) AS hour, count() AS clicks
		FROM click_events
		WHERE ts >= now() - INTERVAL 24 HOUR
		GROUP BY hour
		ORDER BY hour
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []HourlyClick
	for rows.Next() {
		var r HourlyClick
		if err := rows.Scan(&r.Hour, &r.Clicks); err != nil {
			continue
		}
		result = append(result, r)
	}
	return result, nil
}

// TopKeys returns the top 10 most clicked keys in the last 24 hours.
func (s *ClickHouseStore) TopKeys(ctx context.Context) ([]TopKey, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT short_key, count() AS clicks
		FROM click_events
		WHERE ts >= now() - INTERVAL 24 HOUR
		GROUP BY short_key
		ORDER BY clicks DESC
		LIMIT 10
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []TopKey
	for rows.Next() {
		var r TopKey
		if err := rows.Scan(&r.ShortKey, &r.Clicks); err != nil {
			continue
		}
		result = append(result, r)
	}
	return result, nil
}

// DeviceSplit returns click counts grouped by device type.
func (s *ClickHouseStore) DeviceSplit(ctx context.Context) ([]DeviceSplit, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT device, count() AS clicks
		FROM click_events
		WHERE ts >= now() - INTERVAL 24 HOUR
		GROUP BY device
		ORDER BY clicks DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DeviceSplit
	for rows.Next() {
		var r DeviceSplit
		if err := rows.Scan(&r.Device, &r.Clicks); err != nil {
			continue
		}
		result = append(result, r)
	}
	return result, nil
}
