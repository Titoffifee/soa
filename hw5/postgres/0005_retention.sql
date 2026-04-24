CREATE TABLE IF NOT EXISTS retention (
    cohort_date DATE             NOT NULL,
    day         INT              NOT NULL,
    retained    BIGINT           NOT NULL,
    cohort_size BIGINT           NOT NULL,
    rate        DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (cohort_date, day)
);
