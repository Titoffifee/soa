import json
import logging
from datetime import date

from metrics import average_watch_time, conversion, daily_active_users, retention, top_movies
from postgres_client import PostgresClient
from s3_client import S3Client

logger = logging.getLogger(__name__)


class S3Exporter:
    def __init__(self, postgres: PostgresClient, s3: S3Client) -> None:
        self._postgres = postgres
        self._s3 = s3

    def export(self, target_date: date) -> dict:
        logger.info(f"S3 export started for date={target_date}")

        payload: dict = {"date": target_date.isoformat()}
        payload.update(daily_active_users.read(target_date, self._postgres))
        payload.update(average_watch_time.read(target_date, self._postgres))
        payload.update(top_movies.read(target_date, self._postgres))
        payload.update(conversion.read(target_date, self._postgres))
        payload.update(retention.read(target_date, self._postgres))

        body = json.dumps(payload, indent=2, default=str)
        key = f"daily/{target_date.isoformat()}/aggregates.json"

        self._s3.ensure_bucket()
        self._s3.put_object(key, body)

        logger.info(f"S3 export finished for date={target_date} key={key}")
        return {"s3_key": key}
