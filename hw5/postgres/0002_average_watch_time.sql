CREATE TABLE IF NOT EXISTS average_watch_time (
    date              DATE             NOT NULL PRIMARY KEY,
    avg_watch_seconds DOUBLE PRECISION NOT NULL,
    total_views       BIGINT           NOT NULL,
    computed_at       TIMESTAMPTZ      NOT NULL DEFAULT now()
);
