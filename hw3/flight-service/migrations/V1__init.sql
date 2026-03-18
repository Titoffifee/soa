CREATE TABLE IF NOT EXISTS flights (
    id             BIGSERIAL PRIMARY KEY,
    flight_number  VARCHAR(10)    NOT NULL,
    departure_date DATE           NOT NULL,
    airline        VARCHAR(100)   NOT NULL,
    origin         CHAR(3)        NOT NULL,
    destination    CHAR(3)        NOT NULL,
    departure_time TIMESTAMPTZ    NOT NULL,
    arrival_time   TIMESTAMPTZ    NOT NULL,
    total_seats    INT            NOT NULL CHECK (total_seats > 0),
    available_seats INT           NOT NULL CHECK (available_seats >= 0),
    price          NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    status         VARCHAR(20)    NOT NULL DEFAULT 'SCHEDULED',
    CONSTRAINT flights_number_date_unique UNIQUE (flight_number, departure_date),
    CONSTRAINT flights_available_lte_total CHECK (available_seats <= total_seats),
    CONSTRAINT flights_status_check CHECK (status IN ('SCHEDULED','DEPARTED','CANCELLED','COMPLETED'))
);

CREATE TABLE IF NOT EXISTS seat_reservations (
    id         BIGSERIAL PRIMARY KEY,
    flight_id  BIGINT      NOT NULL REFERENCES flights(id),
    booking_id UUID        NOT NULL,
    seat_count INT         NOT NULL CHECK (seat_count > 0),
    status     VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT seat_reservations_booking_unique UNIQUE (booking_id),
    CONSTRAINT seat_reservations_status_check CHECK (status IN ('ACTIVE','RELEASED','EXPIRED'))
);
