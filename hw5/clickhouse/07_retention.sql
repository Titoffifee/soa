CREATE TABLE IF NOT EXISTS cinema.retention
(
    cohort_date Date,
    day         UInt8,
    retained    UInt64,
    cohort_size UInt64,
    rate        Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day);
