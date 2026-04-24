CREATE TABLE IF NOT EXISTS cinema.average_watch_time
(
    date              Date,
    avg_watch_seconds Float64,
    total_views       UInt64,
    computed_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY date;
