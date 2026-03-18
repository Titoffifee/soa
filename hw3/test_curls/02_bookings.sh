#!/bin/bash

BASE="http://localhost:8080"

curl -s -X POST "$BASE/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user1","flight_id":1,"passenger_name":"Ivan Ivanov","passenger_email":"ivan@test.com","seat_count":2}' \
  | python3 -m json.tool

curl -s -X POST "$BASE/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user2","flight_id":1,"passenger_name":"Petr Petrov","passenger_email":"petr@test.com","seat_count":3}' \
  | python3 -m json.tool

curl -s "$BASE/bookings?user_id=user1" | python3 -m json.tool

curl -s "$BASE/bookings?user_id=user2" | python3 -m json.tool
