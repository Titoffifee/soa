CREATE TABLE IF NOT EXISTS cinema.daily_active_users
(
    date        Date,
    dau         UInt64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY date;
