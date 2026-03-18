#!/bin/bash
set -e

docker compose -f docker-compose.yml down -v
docker compose -f docker-compose.yml build
docker compose -f docker-compose.yml up
