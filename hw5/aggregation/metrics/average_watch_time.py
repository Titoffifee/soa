import logging
from datetime import date

from clickhouse_client import ClickHouseClient
from postgres_client import PostgresClient

logger = logging.getLogger(__name__)


def compute(
    target_date: date,
    clickhouse: ClickHouseClient,
    postgres: PostgresClient,
) -> dict:
    average_seconds, total_views = clickhouse.query_avg_watch_time(target_date)

    clickhouse.write_avg_watch_time(target_date, average_seconds, total_views)
    postgres.upsert_avg_watch_time(target_date, average_seconds, total_views)

    logger.info(f"Average watch time for {target_date}: {average_seconds:.1f}s over {total_views} views")
    return {"avg_watch_seconds": average_seconds, "total_views": total_views}


def read(target_date: date, postgres: PostgresClient) -> dict:
    conn = postgres._get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT avg_watch_seconds, total_views, computed_at FROM average_watch_time WHERE date = %s",
            (target_date,),
        )
        row = cur.fetchone()
    if row is None:
        return {}
    return {
        "average_watch_time": {
            "avg_watch_seconds": row[0],
            "total_views": row[1],
            "computed_at": row[2].isoformat(),
        }
    }
