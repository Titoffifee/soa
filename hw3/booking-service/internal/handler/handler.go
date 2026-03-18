package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "github.com/soa/booking-service/proto"
	"github.com/soa/booking-service/internal/repository"
	"github.com/soa/booking-service/internal/service"
)

type Handler struct {
	svc *service.BookingService
}

func New(svc *service.BookingService) *Handler {
	return &Handler{svc: svc}
}

type flightResponse struct {
	ID             int64   `json:"id"`
	FlightNumber   string  `json:"flight_number"`
	Airline        string  `json:"airline"`
	Origin         string  `json:"origin"`
	Destination    string  `json:"destination"`
	DepartureTime  string  `json:"departure_time"`
	ArrivalTime    string  `json:"arrival_time"`
	TotalSeats     int32   `json:"total_seats"`
	AvailableSeats int32   `json:"available_seats"`
	Price          float64 `json:"price"`
	Status         string  `json:"status"`
}

type bookingResponse struct {
	ID             string  `json:"id"`
	FlightID       int64   `json:"flight_id"`
	UserID         string  `json:"user_id"`
	PassengerName  string  `json:"passenger_name"`
	PassengerEmail string  `json:"passenger_email"`
	SeatCount      int32   `json:"seat_count"`
	TotalPrice     float64 `json:"total_price"`
	Status         string  `json:"status"`
	CreatedAt      string  `json:"created_at"`
}

func protoFlightToResponse(f *pb.Flight) flightResponse {
	statusName := pb.FlightStatus_name[int32(f.Status)]
	statusName = strings.TrimPrefix(statusName, "FLIGHT_STATUS_")
	return flightResponse{
		ID:             f.Id,
		FlightNumber:   f.FlightNumber,
		Airline:        f.Airline,
		Origin:         f.Origin,
		Destination:    f.Destination,
		DepartureTime:  f.DepartureTime.AsTime().Format(time.RFC3339),
		ArrivalTime:    f.ArrivalTime.AsTime().Format(time.RFC3339),
		TotalSeats:     f.TotalSeats,
		AvailableSeats: f.AvailableSeats,
		Price:          f.Price,
		Status:         statusName,
	}
}

func bookingToResponse(b *repository.Booking) bookingResponse {
	return bookingResponse{
		ID:             b.ID,
		FlightID:       b.FlightID,
		UserID:         b.UserID,
		PassengerName:  b.PassengerName,
		PassengerEmail: b.PassengerEmail,
		SeatCount:      b.SeatCount,
		TotalPrice:     b.TotalPrice,
		Status:         b.Status,
		CreatedAt:      b.CreatedAt.Format(time.RFC3339),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")

	switch {
	case r.Method == http.MethodGet && path == "/flights":
		h.searchFlights(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/flights/"):
		h.getFlight(w, r)
	case r.Method == http.MethodPost && path == "/bookings":
		h.createBooking(w, r)
	case r.Method == http.MethodGet && path == "/bookings":
		h.listBookings(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/bookings/") && !strings.HasSuffix(path, "/cancel"):
		h.getBooking(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/cancel"):
		h.cancelBooking(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) searchFlights(w http.ResponseWriter, r *http.Request) {
	origin := r.URL.Query().Get("origin")
	destination := r.URL.Query().Get("destination")
	if origin == "" || destination == "" {
		writeError(w, http.StatusBadRequest, "origin and destination are required")
		return
	}
	date := r.URL.Query().Get("date")
	flights, err := h.svc.SearchFlights(r.Context(), origin, destination, date)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	resp := make([]flightResponse, 0, len(flights))
	for _, f := range flights {
		resp = append(resp, protoFlightToResponse(f))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) getFlight(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/flights/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid flight id")
		return
	}
	flight, err := h.svc.GetFlight(r.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrFlightNotFound) {
			writeError(w, http.StatusNotFound, "flight not found")
			return
		}
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, protoFlightToResponse(flight))
}

func (h *Handler) createBooking(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID         string `json:"user_id"`
		FlightID       int64  `json:"flight_id"`
		PassengerName  string `json:"passenger_name"`
		PassengerEmail string `json:"passenger_email"`
		SeatCount      int32  `json:"seat_count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.UserID == "" || req.FlightID == 0 || req.PassengerName == "" || req.PassengerEmail == "" || req.SeatCount <= 0 {
		writeError(w, http.StatusBadRequest, "user_id, flight_id, passenger_name, passenger_email, seat_count are required")
		return
	}

	b, err := h.svc.CreateBooking(r.Context(), service.CreateBookingInput{
		UserID:         req.UserID,
		FlightID:       req.FlightID,
		PassengerName:  req.PassengerName,
		PassengerEmail: req.PassengerEmail,
		SeatCount:      req.SeatCount,
	})
	if err != nil {
		switch {
		case errors.Is(err, service.ErrFlightNotFound):
			writeError(w, http.StatusNotFound, "flight not found")
		case errors.Is(err, service.ErrNotEnoughSeats):
			writeError(w, http.StatusConflict, "not enough seats")
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, bookingToResponse(b))
}

func (h *Handler) getBooking(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/bookings/")
	b, err := h.svc.GetBooking(r.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			writeError(w, http.StatusNotFound, "booking not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, bookingToResponse(b))
}

func (h *Handler) listBookings(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user_id is required")
		return
	}
	bookings, err := h.svc.ListBookings(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := make([]bookingResponse, 0, len(bookings))
	for _, b := range bookings {
		resp = append(resp, bookingToResponse(b))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) cancelBooking(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/cancel")
	id := strings.TrimPrefix(path, "/bookings/")
	b, err := h.svc.CancelBooking(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrNotFound):
			writeError(w, http.StatusNotFound, "booking not found")
		case errors.Is(err, service.ErrAlreadyCancelled):
			writeError(w, http.StatusConflict, "booking already cancelled")
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, bookingToResponse(b))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
