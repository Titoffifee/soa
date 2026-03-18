#!/bin/bash
set -e

bash test_curls/01_flights.sh
bash test_curls/02_bookings.sh
bash test_curls/03_cancel.sh
bash test_curls/04_cache.sh
bash test_curls/05_errors.sh
