#!/bin/bash

BASE="http://localhost:8080"

curl -s "$BASE/flights?origin=ABC&destination=DEF&date=2026-03-18" | python3 -m json.tool

curl -s "$BASE/flights?origin=ABC&destination=DEF" | python3 -m json.tool

curl -s "$BASE/flights/1" | python3 -m json.tool

curl -s "$BASE/flights/9999" | python3 -m json.tool
