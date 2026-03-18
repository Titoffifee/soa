CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS bookings (
    id               UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_id        BIGINT         NOT NULL,
    user_id          VARCHAR(100)   NOT NULL,
    passenger_name   VARCHAR(200)   NOT NULL,
    passenger_email  VARCHAR(200)   NOT NULL,
    seat_count       INT            NOT NULL CHECK (seat_count > 0),
    total_price      NUMERIC(10, 2) NOT NULL CHECK (total_price > 0),
    status           VARCHAR(20)    NOT NULL DEFAULT 'CONFIRMED',
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT bookings_status_check CHECK (status IN ('CONFIRMED','CANCELLED'))
);
