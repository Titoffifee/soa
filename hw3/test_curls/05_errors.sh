#!/bin/bash

BASE="http://localhost:8080"

curl -s "$BASE/flights/9999" | python3 -m json.tool

curl -s "$BASE/bookings/00000000-0000-0000-0000-000000000000" | python3 -m json.tool

curl -s -X POST "$BASE/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user1","flight_id":9999,"passenger_name":"Test","passenger_email":"test@test.com","seat_count":1}' \
  | python3 -m json.tool

curl -s -X POST "$BASE/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user1","flight_id":1,"passenger_name":"Test","passenger_email":"test@test.com","seat_count":9999}' \
  | python3 -m json.tool

curl -s -X POST "$BASE/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user1"}' \
  | python3 -m json.tool

curl -s -X POST "$BASE/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user1","flight_id":4,"passenger_name":"Test","passenger_email":"test@test.com","seat_count":1}' \
  | python3 -m json.tool
