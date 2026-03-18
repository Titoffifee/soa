#!/bin/bash

BASE="http://localhost:8080"

BOOKING=$(curl -s -X POST "$BASE/bookings" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user1","flight_id":1,"passenger_name":"Ivan Ivanov","passenger_email":"ivan@test.com","seat_count":2}')
echo "$BOOKING" | python3 -m json.tool
BOOKING_ID=$(echo "$BOOKING" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

curl -s "$BASE/flights/1" | python3 -m json.tool

curl -s -X POST "$BASE/bookings/$BOOKING_ID/cancel" | python3 -m json.tool

curl -s "$BASE/flights/1" | python3 -m json.tool

curl -s -X POST "$BASE/bookings/$BOOKING_ID/cancel" | python3 -m json.tool
