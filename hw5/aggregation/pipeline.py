import logging
import time
from datetime import date

from clickhouse_client import ClickHouseClient
from metrics import average_watch_time, conversion, daily_active_users, retention, top_movies
from postgres_client import PostgresClient

logger = logging.getLogger(__name__)


class AggregationPipeline:
    def __init__(self, clickhouse: ClickHouseClient, postgres: PostgresClient) -> None:
        self._clickhouse = clickhouse
        self._postgres = postgres

    def run(self, target_date: date) -> dict:
        started_at = time.monotonic()
        logger.info(f"Aggregation started for date={target_date}")

        results = {}

        results.update(daily_active_users.compute(target_date, self._clickhouse, self._postgres))
        results.update(average_watch_time.compute(target_date, self._clickhouse, self._postgres))
        results.update(top_movies.compute(target_date, self._clickhouse, self._postgres))
        results.update(conversion.compute(target_date, self._clickhouse, self._postgres))
        results.update(retention.compute(target_date, self._clickhouse, self._postgres))

        elapsed = time.monotonic() - started_at
        logger.info(f"Aggregation finished for date={target_date} in {elapsed:.2f}s")
        results["elapsed_seconds"] = round(elapsed, 3)
        return results
