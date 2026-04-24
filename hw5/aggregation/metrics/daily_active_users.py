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
    daily_active_users = clickhouse.query_dau(target_date)

    clickhouse.write_dau(target_date, daily_active_users)
    postgres.upsert_dau(target_date, daily_active_users)

    logger.info(f"DAU for {target_date}: {daily_active_users}")
    return {"dau": daily_active_users}


def read(target_date: date, postgres: PostgresClient) -> dict:
    conn = postgres._get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT dau, computed_at FROM daily_active_users WHERE date = %s",
            (target_date,),
        )
        row = cur.fetchone()
    if row is None:
        return {}
    return {
        "dau": {
            "value": row[0],
            "computed_at": row[1].isoformat(),
        }
    }
