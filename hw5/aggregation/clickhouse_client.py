import logging
from datetime import date

import clickhouse_connect

from config import Settings

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self._settings.clickhouse_host,
                port=self._settings.clickhouse_port,
                database=self._settings.clickhouse_database,
            )
            logger.info(f"ClickHouse connected to {self._settings.clickhouse_host}:{self._settings.clickhouse_port}")
        return self._client

    def query_dau(self, target_date: date) -> int:
        result = self._get_client().query(
            "SELECT uniq(user_id) FROM cinema.movie_events WHERE toDate(timestamp) = %(date)s",
            parameters={"date": target_date},
        )
        return int(result.first_row[0])

    def query_avg_watch_time(self, target_date: date) -> tuple[float, int]:
        result = self._get_client().query(
            """
            SELECT avg(progress_seconds), count()
            FROM cinema.movie_events
            WHERE toDate(timestamp) = %(date)s
              AND event_type = 'VIEW_FINISHED'
              AND progress_seconds IS NOT NULL
            """,
            parameters={"date": target_date},
        )
        row = result.first_row
        avg_seconds = float(row[0]) if row[0] is not None else 0.0
        total_views = int(row[1])
        return avg_seconds, total_views

    def query_top_movies(self, target_date: date, limit: int = 10) -> list[tuple[str, int, int]]:
        result = self._get_client().query(
            """
            SELECT movie_id, count() AS view_count
            FROM cinema.movie_events
            WHERE toDate(timestamp) = %(date)s
              AND event_type = 'VIEW_STARTED'
            GROUP BY movie_id
            ORDER BY view_count DESC
            LIMIT %(limit)s
            """,
            parameters={"date": target_date, "limit": limit},
        )
        return [(str(row[0]), int(row[1]), rank) for rank, row in enumerate(result.result_rows, start=1)]

    def query_conversion(self, target_date: date) -> tuple[int, int, float]:
        result = self._get_client().query(
            """
            SELECT
                countIf(event_type = 'VIEW_STARTED')  AS started,
                countIf(event_type = 'VIEW_FINISHED') AS finished,
                if(countIf(event_type = 'VIEW_STARTED') > 0,
                   countIf(event_type = 'VIEW_FINISHED') / countIf(event_type = 'VIEW_STARTED'),
                   0.0) AS conversion_rate
            FROM cinema.movie_events
            WHERE toDate(timestamp) = %(date)s
            """,
            parameters={"date": target_date},
        )
        row = result.first_row
        started = int(row[0])
        finished = int(row[1])
        conversion_rate = float(row[2])
        return started, finished, conversion_rate

    def query_retention(self, target_date: date) -> list[tuple[date, int, int, int, float]]:
        result = self._get_client().query(
            """
            WITH
                first_seen AS (
                    SELECT user_id, toDate(min(timestamp)) AS cohort_date
                    FROM cinema.movie_events
                    GROUP BY user_id
                ),
                cohort AS (
                    SELECT cohort_date, uniq(user_id) AS cohort_size
                    FROM first_seen
                    WHERE cohort_date <= %(date)s
                    GROUP BY cohort_date
                ),
                activity AS (
                    SELECT
                        fs.cohort_date,
                        toUInt8(toDate(e.timestamp) - fs.cohort_date) AS day,
                        uniq(e.user_id) AS retained
                    FROM cinema.movie_events e
                    JOIN first_seen fs ON e.user_id = fs.user_id
                    WHERE toDate(e.timestamp) - fs.cohort_date IN (1, 7)
                      AND fs.cohort_date <= %(date)s
                    GROUP BY fs.cohort_date, day
                )
            SELECT
                a.cohort_date,
                a.day,
                a.retained,
                c.cohort_size,
                if(c.cohort_size > 0, a.retained / c.cohort_size, 0.0) AS rate
            FROM activity a
            JOIN cohort c ON a.cohort_date = c.cohort_date
            ORDER BY a.cohort_date, a.day
            """,
            parameters={"date": target_date},
        )
        return [
            (row[0], int(row[1]), int(row[2]), int(row[3]), float(row[4]))
            for row in result.result_rows
        ]

    def write_dau(self, target_date: date, dau: int) -> None:
        self._get_client().command(
            "INSERT INTO cinema.daily_active_users (date, dau) VALUES (%(date)s, %(dau)s)",
            parameters={"date": target_date, "dau": dau},
        )

    def write_avg_watch_time(self, target_date: date, avg_seconds: float, total_views: int) -> None:
        self._get_client().command(
            """
            INSERT INTO cinema.average_watch_time (date, avg_watch_seconds, total_views)
            VALUES (%(date)s, %(avg)s, %(total)s)
            """,
            parameters={"date": target_date, "avg": avg_seconds, "total": total_views},
        )

    def write_top_movies(self, target_date: date, rows: list[tuple[str, int, int]]) -> None:
        for movie_id, view_count, rank in rows:
            self._get_client().command(
                """
                INSERT INTO cinema.top_movies (date, movie_id, view_count, rank)
                VALUES (%(date)s, %(movie_id)s, %(view_count)s, %(rank)s)
                """,
                parameters={"date": target_date, "movie_id": movie_id, "view_count": view_count, "rank": rank},
            )

    def write_conversion(self, target_date: date, started: int, finished: int, conversion_rate: float) -> None:
        self._get_client().command(
            """
            INSERT INTO cinema.conversion (date, started, finished, conversion_rate)
            VALUES (%(date)s, %(started)s, %(finished)s, %(rate)s)
            """,
            parameters={"date": target_date, "started": started, "finished": finished, "rate": conversion_rate},
        )

    def write_retention(self, rows: list[tuple[date, int, int, int, float]]) -> None:
        for cohort_date, day, retained, cohort_size, rate in rows:
            self._get_client().command(
                """
                INSERT INTO cinema.retention (cohort_date, day, retained, cohort_size, rate)
                VALUES (%(cohort_date)s, %(day)s, %(retained)s, %(cohort_size)s, %(rate)s)
                """,
                parameters={
                    "cohort_date": cohort_date,
                    "day": day,
                    "retained": retained,
                    "cohort_size": cohort_size,
                    "rate": rate,
                },
            )
