INSERT INTO flights (flight_number, departure_date, airline, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status)
VALUES
    ('SU1',   '2026-03-18', 'A', 'ABC', 'DEF', '2026-03-18 00:00:00+03', '2026-03-18 01:20:00+03', 10, 10, 1000.00, 'SCHEDULED'),
    ('SU2',   '2026-03-18', 'A', 'DEF', 'ABC', '2026-03-18 06:30:00+03', '2026-03-18 07:50:00+03', 10,  5,  950.00, 'SCHEDULED'),
    ('TEST1', '2026-03-18', 'B', 'ABC', 'GHI', '2026-03-18 08:00:00+03', '2026-03-18 10:30:00+03',  8,  8, 1200.00, 'SCHEDULED'),
    ('TEST2', '2026-03-18', 'B', 'GHI', 'ABC', '2026-03-18 15:00:00+03', '2026-03-18 17:30:00+03',  8,  0, 1200.00, 'SCHEDULED'),
    ('ABC',   '2026-03-18', 'C', 'DEF', 'GHI', '2026-03-18 12:00:00+03', '2026-03-18 13:20:00+03',  5,  5,  500.00, 'CANCELLED');
