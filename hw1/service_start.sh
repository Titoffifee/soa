docker compose up --build -d

curl -i http://localhost:8080/health

docker compose down -v
