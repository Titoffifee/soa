package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/soa/flight-service/internal/cache"
	"github.com/soa/flight-service/internal/repository"
	"github.com/soa/flight-service/internal/server"
	pb "github.com/soa/flight-service/proto"
	"google.golang.org/grpc"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		log.Fatal("API_KEY is not set")
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	defer pool.Close()

	redisCache := cache.NewRedisCache(redisAddr)
	repo := repository.NewFlightRepository(pool)
	flightServer := server.NewFlightServer(repo, redisCache, apiKey)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterFlightServiceServer(grpcServer, flightServer)

	log.Printf("flight-service gRPC listening on :%s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
