FROM golang:1.22-alpine AS builder

RUN apk add --no-cache protobuf protobuf-dev git
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

WORKDIR /app
COPY booking-service/ .
COPY flight-service/proto ./proto

RUN protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/flight.proto

RUN go mod tidy
RUN CGO_ENABLED=0 go build -o booking-service ./cmd/server

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/booking-service .
COPY booking-service/migrations ./migrations
EXPOSE 8080
CMD ["./booking-service"]
