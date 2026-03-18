package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/soa/booking-service/internal/grpcclient"
	"github.com/soa/booking-service/internal/handler"
	"github.com/soa/booking-service/internal/repository"
	"github.com/soa/booking-service/internal/service"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	defer pool.Close()

	flightClient, err := grpcclient.NewFlightClient()
	if err != nil {
		log.Fatalf("failed to create flight client: %v", err)
	}

	repo := repository.NewBookingRepository(pool)
	svc := service.NewBookingService(repo, flightClient)
	h := handler.New(svc)

	log.Printf("booking-service HTTP listening on :%s", port)
	if err := http.ListenAndServe(":"+port, h); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
