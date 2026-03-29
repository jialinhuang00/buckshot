CREATE TABLE IF NOT EXISTS click_events (
    short_key  String,
    ts         DateTime,
    ip         String,
    country    String,
    device     String
) ENGINE = MergeTree()
ORDER BY (short_key, ts);
