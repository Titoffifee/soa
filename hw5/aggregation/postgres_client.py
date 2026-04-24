import logging
import time
from datetime import date

import psycopg

from config import Settings

logger = logging.getLogger(__name__)


class PostgresClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._conn = None

    def _dsn(self) -> str:
        s = self._settings
        return (
            f"host={s.postgres_host} port={s.postgres_port} "
            f"dbname={s.postgres_database} user={s.postgres_user} "
            f"password={s.postgres_password}"
        )

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn())
            logger.info(f"PostgreSQL connected to {self._settings.postgres_host}:{self._settings.postgres_port}")
        return self._conn

    def _execute_with_retry(self, fn, retries: int = 3, delay: float = 2.0):
        for attempt in range(1, retries + 1):
            try:
                return fn()
            except Exception as exc:
                logger.warning(f"PostgreSQL error (attempt {attempt}/{retries}): {exc}")
                self._conn = None
                if attempt < retries:
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

    def upsert_dau(self, target_date: date, dau: int) -> None:
        def _run():
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO daily_active_users (date, dau, computed_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (date) DO UPDATE
                        SET dau = EXCLUDED.dau,
                            computed_at = EXCLUDED.computed_at
                    """,
                    (target_date, dau),
                )
            conn.commit()

        self._execute_with_retry(_run)

    def upsert_avg_watch_time(self, target_date: date, avg_seconds: float, total_views: int) -> None:
        def _run():
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO average_watch_time (date, avg_watch_seconds, total_views, computed_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (date) DO UPDATE
                        SET avg_watch_seconds = EXCLUDED.avg_watch_seconds,
                            total_views       = EXCLUDED.total_views,
                            computed_at       = EXCLUDED.computed_at
                    """,
                    (target_date, avg_seconds, total_views),
                )
            conn.commit()

        self._execute_with_retry(_run)

    def upsert_top_movies(self, target_date: date, rows: list[tuple[str, int, int]]) -> None:
        def _run():
            conn = self._get_conn()
            with conn.cursor() as cur:
                for movie_id, view_count, rank in rows:
                    cur.execute(
                        """
                        INSERT INTO top_movies (date, movie_id, view_count, rank, computed_at)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (date, movie_id) DO UPDATE
                            SET view_count  = EXCLUDED.view_count,
                                rank        = EXCLUDED.rank,
                                computed_at = EXCLUDED.computed_at
                        """,
                        (target_date, movie_id, view_count, rank),
                    )
            conn.commit()

        self._execute_with_retry(_run)

    def upsert_conversion(self, target_date: date, started: int, finished: int, conversion_rate: float) -> None:
        def _run():
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversion (date, started, finished, conversion_rate, computed_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (date) DO UPDATE
                        SET started         = EXCLUDED.started,
                            finished        = EXCLUDED.finished,
                            conversion_rate = EXCLUDED.conversion_rate,
                            computed_at     = EXCLUDED.computed_at
                    """,
                    (target_date, started, finished, conversion_rate),
                )
            conn.commit()

        self._execute_with_retry(_run)

    def upsert_retention(self, rows: list[tuple[date, int, int, int, float]]) -> None:
        def _run():
            conn = self._get_conn()
            with conn.cursor() as cur:
                for cohort_date, day, retained, cohort_size, rate in rows:
                    cur.execute(
                        """
                        INSERT INTO retention (cohort_date, day, retained, cohort_size, rate, computed_at)
                        VALUES (%s, %s, %s, %s, %s, now())
                        ON CONFLICT (cohort_date, day) DO UPDATE
                            SET retained    = EXCLUDED.retained,
                                cohort_size = EXCLUDED.cohort_size,
                                rate        = EXCLUDED.rate,
                                computed_at = EXCLUDED.computed_at
                        """,
                        (cohort_date, day, retained, cohort_size, rate),
                    )
            conn.commit()

        self._execute_with_retry(_run)
