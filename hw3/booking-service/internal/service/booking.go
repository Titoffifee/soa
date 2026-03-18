package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/soa/booking-service/internal/grpcclient"
	"github.com/soa/booking-service/internal/repository"
	pb "github.com/soa/booking-service/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrNotFound = errors.New("not found")
var ErrAlreadyCancelled = errors.New("booking already cancelled")
var ErrFlightNotFound = errors.New("flight not found")
var ErrNotEnoughSeats = errors.New("not enough seats")

type BookingService struct {
	repo   *repository.BookingRepository
	flight *grpcclient.FlightClient
}

func NewBookingService(repo *repository.BookingRepository, flight *grpcclient.FlightClient) *BookingService {
	return &BookingService{repo: repo, flight: flight}
}

type CreateBookingInput struct {
	UserID         string
	FlightID       int64
	PassengerName  string
	PassengerEmail string
	SeatCount      int32
}

func (s *BookingService) SearchFlights(ctx context.Context, origin, destination, date string) ([]*pb.Flight, error) {
	resp, err := s.flight.SearchFlights(ctx, origin, destination, date)
	if err != nil {
		return nil, fmt.Errorf("search flights: %w", err)
	}
	return resp.Flights, nil
}

func (s *BookingService) GetFlight(ctx context.Context, flightID int64) (*pb.Flight, error) {
	resp, err := s.flight.GetFlight(ctx, flightID)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, ErrFlightNotFound
		}
		return nil, fmt.Errorf("get flight: %w", err)
	}
	return resp.Flight, nil
}

func (s *BookingService) CreateBooking(ctx context.Context, in CreateBookingInput) (*repository.Booking, error) {
	flightResp, err := s.flight.GetFlight(ctx, in.FlightID)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, ErrFlightNotFound
		}
		return nil, fmt.Errorf("get flight: %w", err)
	}
	flight := flightResp.Flight

	bookingID := uuid.New().String()

	_, err = s.flight.ReserveSeats(ctx, in.FlightID, in.SeatCount, bookingID)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.ResourceExhausted:
				return nil, ErrNotEnoughSeats
			case codes.NotFound:
				return nil, ErrFlightNotFound
			}
		}
		return nil, fmt.Errorf("reserve seats: %w", err)
	}

	totalPrice := float64(in.SeatCount) * flight.Price

	b, err := s.repo.Create(ctx, &repository.Booking{
		ID:             bookingID,
		FlightID:       in.FlightID,
		UserID:         in.UserID,
		PassengerName:  in.PassengerName,
		PassengerEmail: in.PassengerEmail,
		SeatCount:      in.SeatCount,
		TotalPrice:     totalPrice,
	})
	if err != nil {
		return nil, fmt.Errorf("create booking: %w", err)
	}
	return b, nil
}

func (s *BookingService) GetBooking(ctx context.Context, id string) (*repository.Booking, error) {
	b, err := s.repo.GetByID(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, ErrNotFound
	}
	return b, err
}

func (s *BookingService) ListBookings(ctx context.Context, userID string) ([]*repository.Booking, error) {
	return s.repo.ListByUser(ctx, userID)
}

func (s *BookingService) CancelBooking(ctx context.Context, id string) (*repository.Booking, error) {
	b, err := s.repo.GetByID(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if b.Status != "CONFIRMED" {
		return nil, ErrAlreadyCancelled
	}

	if _, err = s.flight.ReleaseReservation(ctx, id); err != nil {
		return nil, fmt.Errorf("release reservation: %w", err)
	}

	cancelled, err := s.repo.Cancel(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, ErrNotFound
	}
	return cancelled, err
}
