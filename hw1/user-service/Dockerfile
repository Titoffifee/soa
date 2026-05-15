FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/user-service ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=build /out/user-service /user-service

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["/user-service"]
