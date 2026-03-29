CREATE TABLE IF NOT EXISTS url_mappings (
    short_key   VARCHAR(10)  PRIMARY KEY,
    long_url    TEXT         NOT NULL,
    user_id     BIGINT,
    created_at  TIMESTAMPTZ  DEFAULT now(),
    expires_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS url_stats (
    short_key    VARCHAR(10) PRIMARY KEY,
    total_clicks BIGINT      DEFAULT 0
);

CREATE TABLE IF NOT EXISTS id_counter (
    id       INT     PRIMARY KEY DEFAULT 1,
    next_val BIGINT  NOT NULL DEFAULT 100000000,
    CHECK (id = 1)   -- single row only
);
INSERT INTO id_counter (id, next_val) VALUES (1, 100000000) ON CONFLICT DO NOTHING;

-- Seed range coordinator: each seed worker atomically claims a batch of IDs.
-- Workers UPDATE next_start += batch, get back their exclusive start offset.
-- Multiple workers can run in parallel with no overlap.
CREATE TABLE IF NOT EXISTS seed_ranges (
    id         INT    PRIMARY KEY DEFAULT 1,
    next_start BIGINT NOT NULL DEFAULT 0,
    total      BIGINT NOT NULL DEFAULT 50000,
    CHECK (id = 1)  -- single row only
);
INSERT INTO seed_ranges (id, next_start, total) VALUES (1, 0, 50000) ON CONFLICT DO NOTHING;
