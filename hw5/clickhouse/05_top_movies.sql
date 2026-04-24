CREATE TABLE IF NOT EXISTS cinema.top_movies
(
    date        Date,
    movie_id    String,
    view_count  UInt64,
    rank        UInt32,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (date, movie_id);
