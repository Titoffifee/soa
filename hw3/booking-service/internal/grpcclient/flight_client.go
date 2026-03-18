package grpcclient

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/soa/booking-service/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	maxRetries     = 3
	baseBackoff    = 100 * time.Millisecond
)

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

func withRetry[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		var result T
		result, err = fn()
		if err == nil {
			return result, nil
		}
		if !isRetryable(err) {
			return zero, err
		}
		backoff := baseBackoff * (1 << attempt)
		log.Printf("retry attempt %d after %v: %v", attempt+1, backoff, err)
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return zero, err
}

type FlightClient struct {
	client pb.FlightServiceClient
	apiKey string
}

func NewFlightClient() (*FlightClient, error) {
	addr := os.Getenv("FLIGHT_SERVICE_ADDR")
	if addr == "" {
		addr = "flight-service:50051"
	}
	apiKey := os.Getenv("API_KEY")

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &FlightClient{client: pb.NewFlightServiceClient(conn), apiKey: apiKey}, nil
}

func (c *FlightClient) outCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-api-key", c.apiKey)
}

func (c *FlightClient) SearchFlights(ctx context.Context, origin, destination, date string) (*pb.SearchFlightsResponse, error) {
	return withRetry(ctx, func() (*pb.SearchFlightsResponse, error) {
		return c.client.SearchFlights(c.outCtx(ctx), &pb.SearchFlightsRequest{
			Origin:      origin,
			Destination: destination,
			Date:        date,
		})
	})
}

func (c *FlightClient) GetFlight(ctx context.Context, flightID int64) (*pb.GetFlightResponse, error) {
	return withRetry(ctx, func() (*pb.GetFlightResponse, error) {
		return c.client.GetFlight(c.outCtx(ctx), &pb.GetFlightRequest{FlightId: flightID})
	})
}

func (c *FlightClient) ReserveSeats(ctx context.Context, flightID int64, seatCount int32, bookingID string) (*pb.ReserveSeatsResponse, error) {
	return withRetry(ctx, func() (*pb.ReserveSeatsResponse, error) {
		return c.client.ReserveSeats(c.outCtx(ctx), &pb.ReserveSeatsRequest{
			FlightId:  flightID,
			SeatCount: seatCount,
			BookingId: bookingID,
		})
	})
}

func (c *FlightClient) ReleaseReservation(ctx context.Context, bookingID string) (*pb.ReleaseReservationResponse, error) {
	return withRetry(ctx, func() (*pb.ReleaseReservationResponse, error) {
		return c.client.ReleaseReservation(c.outCtx(ctx), &pb.ReleaseReservationRequest{BookingId: bookingID})
	})
}
