CREATE TABLE IF NOT EXISTS daily_active_users (
    date        DATE        NOT NULL PRIMARY KEY,
    dau         BIGINT      NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
