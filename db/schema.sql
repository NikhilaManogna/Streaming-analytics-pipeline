CREATE TABLE IF NOT EXISTS analytics_metrics (
    timestamp TIMESTAMP PRIMARY KEY,
    total_revenue NUMERIC(14, 2) NOT NULL,
    active_users INTEGER NOT NULL,
    event_count BIGINT NOT NULL,
    anomaly_detected BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analytics_metrics_created_at
    ON analytics_metrics (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_analytics_metrics_anomaly
    ON analytics_metrics (anomaly_detected, timestamp DESC);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    kafka_timestamp TIMESTAMP NULL,
    raw_payload TEXT NOT NULL,
    invalid_reason VARCHAR(120) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_events_created_at
    ON dead_letter_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dead_letter_events_reason
    ON dead_letter_events (invalid_reason, created_at DESC);