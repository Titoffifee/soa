CREATE TABLE IF NOT EXISTS conversion (
    date             DATE             NOT NULL PRIMARY KEY,
    started          BIGINT           NOT NULL,
    finished         BIGINT           NOT NULL,
    conversion_rate  DOUBLE PRECISION NOT NULL,
    computed_at      TIMESTAMPTZ      NOT NULL DEFAULT now()
);
