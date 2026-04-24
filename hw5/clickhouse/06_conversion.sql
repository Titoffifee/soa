CREATE TABLE IF NOT EXISTS cinema.conversion
(
    date            Date,
    started         UInt64,
    finished        UInt64,
    conversion_rate Float64,
    computed_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY date;
