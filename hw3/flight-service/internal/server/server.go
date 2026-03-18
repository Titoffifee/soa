package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/soa/flight-service/internal/cache"
	"github.com/soa/flight-service/internal/repository"
	pb "github.com/soa/flight-service/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FlightServer struct {
	pb.UnimplementedFlightServiceServer
	repo   *repository.FlightRepository
	cache  *cache.RedisCache
	apiKey string
}

func NewFlightServer(repo *repository.FlightRepository, c *cache.RedisCache, apiKey string) *FlightServer {
	return &FlightServer{repo: repo, cache: c, apiKey: apiKey}
}

func (s *FlightServer) auth(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("x-api-key")
	if len(vals) == 0 || vals[0] != s.apiKey {
		return status.Error(codes.Unauthenticated, "invalid api key")
	}
	return nil
}

func (s *FlightServer) SearchFlights(ctx context.Context, req *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	if req.Origin == "" || req.Destination == "" {
		return nil, status.Error(codes.InvalidArgument, "origin and destination are required")
	}

	if cached, ok := s.cache.GetSearch(ctx, req.Origin, req.Destination, req.Date); ok {
		var flights []*pb.Flight
		if err := json.Unmarshal(cached, &flights); err == nil {
			return &pb.SearchFlightsResponse{Flights: flights}, nil
		}
	}

	flights, err := s.repo.SearchFlights(ctx, req.Origin, req.Destination, req.Date)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &pb.SearchFlightsResponse{}
	for _, f := range flights {
		resp.Flights = append(resp.Flights, flightToProto(f))
	}
	s.cache.SetSearch(ctx, req.Origin, req.Destination, req.Date, resp.Flights)
	return resp, nil
}

func (s *FlightServer) GetFlight(ctx context.Context, req *pb.GetFlightRequest) (*pb.GetFlightResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}

	if cached, ok := s.cache.GetFlight(ctx, req.FlightId); ok {
		var f pb.Flight
		if err := json.Unmarshal(cached, &f); err == nil {
			return &pb.GetFlightResponse{Flight: &f}, nil
		}
	}

	f, err := s.repo.GetFlight(ctx, req.FlightId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if f == nil {
		return nil, status.Error(codes.NotFound, "flight not found")
	}
	proto := flightToProto(f)
	s.cache.SetFlight(ctx, req.FlightId, proto)
	return &pb.GetFlightResponse{Flight: proto}, nil
}

func (s *FlightServer) ReserveSeats(ctx context.Context, req *pb.ReserveSeatsRequest) (*pb.ReserveSeatsResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	if req.FlightId == 0 || req.SeatCount <= 0 || req.BookingId == "" {
		return nil, status.Error(codes.InvalidArgument, "flight_id, seat_count and booking_id are required")
	}
	res, err := s.repo.ReserveSeats(ctx, req.FlightId, req.SeatCount, req.BookingId)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "flight not found")
		}
		if errors.Is(err, repository.ErrNotEnoughSeats) {
			return nil, status.Error(codes.ResourceExhausted, "not enough seats")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.cache.DeleteFlight(ctx, req.FlightId)
	s.cache.InvalidateSearchByFlight(ctx, "", "")
	return &pb.ReserveSeatsResponse{Reservation: reservationToProto(res)}, nil
}

func (s *FlightServer) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.ReleaseReservationResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	if req.BookingId == "" {
		return nil, status.Error(codes.InvalidArgument, "booking_id is required")
	}
	res, err := s.repo.ReleaseReservation(ctx, req.BookingId)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "active reservation not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.cache.DeleteFlight(ctx, res.FlightID)
	s.cache.InvalidateSearchByFlight(ctx, "", "")
	return &pb.ReleaseReservationResponse{Reservation: reservationToProto(res)}, nil
}

func flightToProto(f *repository.Flight) *pb.Flight {
	return &pb.Flight{
		Id:             f.ID,
		FlightNumber:   f.FlightNumber,
		Airline:        f.Airline,
		Origin:         f.Origin,
		Destination:    f.Destination,
		DepartureTime:  timestamppb.New(f.DepartureTime),
		ArrivalTime:    timestamppb.New(f.ArrivalTime),
		TotalSeats:     f.TotalSeats,
		AvailableSeats: f.AvailableSeats,
		Price:          f.Price,
		Status:         pb.FlightStatus(pb.FlightStatus_value["FLIGHT_STATUS_"+f.Status]),
	}
}

func reservationToProto(r *repository.SeatReservation) *pb.SeatReservation {
	return &pb.SeatReservation{
		Id:        r.ID,
		FlightId:  r.FlightID,
		BookingId: r.BookingID,
		SeatCount: r.SeatCount,
		Status:    pb.ReservationStatus(pb.ReservationStatus_value["RESERVATION_STATUS_"+r.Status]),
		CreatedAt: timestamppb.New(r.CreatedAt),
	}
}
