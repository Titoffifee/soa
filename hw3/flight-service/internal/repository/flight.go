package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrNotEnoughSeats = errors.New("not enough seats")

type Flight struct {
	ID             int64
	FlightNumber   string
	DepartureDate  time.Time
	Airline        string
	Origin         string
	Destination    string
	DepartureTime  time.Time
	ArrivalTime    time.Time
	TotalSeats     int32
	AvailableSeats int32
	Price          float64
	Status         string
}

type SeatReservation struct {
	ID        int64
	FlightID  int64
	BookingID string
	SeatCount int32
	Status    string
	CreatedAt time.Time
}

type FlightRepository struct {
	db *pgxpool.Pool
}

func NewFlightRepository(db *pgxpool.Pool) *FlightRepository {
	return &FlightRepository{db: db}
}

func (r *FlightRepository) SearchFlights(ctx context.Context, origin, destination, date string) ([]*Flight, error) {
	query := `SELECT id, flight_number, departure_date, airline, origin, destination,
		departure_time, arrival_time, total_seats, available_seats, price, status
		FROM flights WHERE origin = $1 AND destination = $2 AND status = 'SCHEDULED'`
	args := []any{origin, destination}
	if date != "" {
		query += " AND departure_date = $3"
		args = append(args, date)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var flights []*Flight
	for rows.Next() {
		f := &Flight{}
		if err := rows.Scan(&f.ID, &f.FlightNumber, &f.DepartureDate, &f.Airline,
			&f.Origin, &f.Destination, &f.DepartureTime, &f.ArrivalTime,
			&f.TotalSeats, &f.AvailableSeats, &f.Price, &f.Status); err != nil {
			return nil, err
		}
		flights = append(flights, f)
	}
	return flights, nil
}

func (r *FlightRepository) GetFlight(ctx context.Context, id int64) (*Flight, error) {
	f := &Flight{}
	err := r.db.QueryRow(ctx, `SELECT id, flight_number, departure_date, airline, origin, destination,
		departure_time, arrival_time, total_seats, available_seats, price, status
		FROM flights WHERE id = $1`, id).Scan(
		&f.ID, &f.FlightNumber, &f.DepartureDate, &f.Airline,
		&f.Origin, &f.Destination, &f.DepartureTime, &f.ArrivalTime,
		&f.TotalSeats, &f.AvailableSeats, &f.Price, &f.Status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return f, err
}

func (r *FlightRepository) ReserveSeats(ctx context.Context, flightID int64, seatCount int32, bookingID string) (*SeatReservation, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var existing SeatReservation
	err = tx.QueryRow(ctx, `SELECT id, flight_id, booking_id, seat_count, status, created_at
		FROM seat_reservations WHERE booking_id = $1`, bookingID).Scan(
		&existing.ID, &existing.FlightID, &existing.BookingID,
		&existing.SeatCount, &existing.Status, &existing.CreatedAt,
	)
	if err == nil {
		return &existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	var available int32
	err = tx.QueryRow(ctx, `SELECT available_seats FROM flights WHERE id = $1 FOR UPDATE`, flightID).Scan(&available)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if available < seatCount {
		return nil, ErrNotEnoughSeats
	}

	_, err = tx.Exec(ctx, `UPDATE flights SET available_seats = available_seats - $1 WHERE id = $2`, seatCount, flightID)
	if err != nil {
		return nil, err
	}

	res := &SeatReservation{}
	err = tx.QueryRow(ctx, `INSERT INTO seat_reservations (flight_id, booking_id, seat_count, status)
		VALUES ($1, $2, $3, 'ACTIVE') RETURNING id, flight_id, booking_id, seat_count, status, created_at`,
		flightID, bookingID, seatCount).Scan(
		&res.ID, &res.FlightID, &res.BookingID, &res.SeatCount, &res.Status, &res.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	return res, tx.Commit(ctx)
}

func (r *FlightRepository) ReleaseReservation(ctx context.Context, bookingID string) (*SeatReservation, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	res := &SeatReservation{}
	err = tx.QueryRow(ctx, `SELECT id, flight_id, booking_id, seat_count, status, created_at
		FROM seat_reservations WHERE booking_id = $1 AND status = 'ACTIVE'`, bookingID).Scan(
		&res.ID, &res.FlightID, &res.BookingID, &res.SeatCount, &res.Status, &res.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(ctx, `UPDATE flights SET available_seats = available_seats + $1 WHERE id = $2`, res.SeatCount, res.FlightID)
	if err != nil {
		return nil, err
	}

	err = tx.QueryRow(ctx, `UPDATE seat_reservations SET status = 'RELEASED' WHERE id = $1
		RETURNING id, flight_id, booking_id, seat_count, status, created_at`, res.ID).Scan(
		&res.ID, &res.FlightID, &res.BookingID, &res.SeatCount, &res.Status, &res.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	return res, tx.Commit(ctx)
}
