CREATE TABLE IF NOT EXISTS top_movies (
    date        DATE        NOT NULL,
    movie_id    TEXT        NOT NULL,
    view_count  BIGINT      NOT NULL,
    rank        INT         NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, movie_id)
);
