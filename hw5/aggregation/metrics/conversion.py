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
    started, finished, conversion_rate = clickhouse.query_conversion(target_date)

    clickhouse.write_conversion(target_date, started, finished, conversion_rate)
    postgres.upsert_conversion(target_date, started, finished, conversion_rate)

    logger.info(f"Conversion for {target_date}: started={started} finished={finished} rate={conversion_rate:.3f}")
    return {"started": started, "finished": finished, "conversion_rate": conversion_rate}


def read(target_date: date, postgres: PostgresClient) -> dict:
    conn = postgres._get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT started, finished, conversion_rate, computed_at FROM conversion WHERE date = %s",
            (target_date,),
        )
        row = cur.fetchone()
    if row is None:
        return {}
    return {
        "conversion": {
            "started": row[0],
            "finished": row[1],
            "conversion_rate": row[2],
            "computed_at": row[3].isoformat(),
        }
    }
