package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Booking struct {
	ID             string
	FlightID       int64
	UserID         string
	PassengerName  string
	PassengerEmail string
	SeatCount      int32
	TotalPrice     float64
	Status         string
	CreatedAt      time.Time
}

var ErrNotFound = errors.New("not found")

type BookingRepository struct {
	db *pgxpool.Pool
}

func NewBookingRepository(db *pgxpool.Pool) *BookingRepository {
	return &BookingRepository{db: db}
}

func (r *BookingRepository) Create(ctx context.Context, b *Booking) (*Booking, error) {
	created := &Booking{}
	err := r.db.QueryRow(ctx, `INSERT INTO bookings
		(id, flight_id, user_id, passenger_name, passenger_email, seat_count, total_price, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'CONFIRMED')
		RETURNING id, flight_id, user_id, passenger_name, passenger_email, seat_count, total_price, status, created_at`,
		b.ID, b.FlightID, b.UserID, b.PassengerName, b.PassengerEmail, b.SeatCount, b.TotalPrice,
	).Scan(&created.ID, &created.FlightID, &created.UserID, &created.PassengerName,
		&created.PassengerEmail, &created.SeatCount, &created.TotalPrice, &created.Status, &created.CreatedAt)
	return created, err
}

func (r *BookingRepository) GetByID(ctx context.Context, id string) (*Booking, error) {
	b := &Booking{}
	err := r.db.QueryRow(ctx, `SELECT id, flight_id, user_id, passenger_name, passenger_email,
		seat_count, total_price, status, created_at FROM bookings WHERE id = $1`, id).Scan(
		&b.ID, &b.FlightID, &b.UserID, &b.PassengerName, &b.PassengerEmail,
		&b.SeatCount, &b.TotalPrice, &b.Status, &b.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

func (r *BookingRepository) ListByUser(ctx context.Context, userID string) ([]*Booking, error) {
	rows, err := r.db.Query(ctx, `SELECT id, flight_id, user_id, passenger_name, passenger_email,
		seat_count, total_price, status, created_at FROM bookings WHERE user_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bookings []*Booking
	for rows.Next() {
		b := &Booking{}
		if err := rows.Scan(&b.ID, &b.FlightID, &b.UserID, &b.PassengerName, &b.PassengerEmail,
			&b.SeatCount, &b.TotalPrice, &b.Status, &b.CreatedAt); err != nil {
			return nil, err
		}
		bookings = append(bookings, b)
	}
	return bookings, nil
}

func (r *BookingRepository) Cancel(ctx context.Context, id string) (*Booking, error) {
	b := &Booking{}
	err := r.db.QueryRow(ctx, `UPDATE bookings SET status = 'CANCELLED' WHERE id = $1 AND status = 'CONFIRMED'
		RETURNING id, flight_id, user_id, passenger_name, passenger_email, seat_count, total_price, status, created_at`, id).Scan(
		&b.ID, &b.FlightID, &b.UserID, &b.PassengerName, &b.PassengerEmail,
		&b.SeatCount, &b.TotalPrice, &b.Status, &b.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}
