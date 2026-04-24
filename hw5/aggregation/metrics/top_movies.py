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
    rows = clickhouse.query_top_movies(target_date)

    clickhouse.write_top_movies(target_date, rows)
    postgres.upsert_top_movies(target_date, rows)

    logger.info(f"Top movies for {target_date}: {len(rows)} entries")
    return {"top_movies_count": len(rows)}


def read(target_date: date, postgres: PostgresClient) -> dict:
    conn = postgres._get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT movie_id, view_count, rank, computed_at FROM top_movies WHERE date = %s ORDER BY rank",
            (target_date,),
        )
        rows = cur.fetchall()
    return {
        "top_movies": [
            {
                "movie_id": r[0],
                "view_count": r[1],
                "rank": r[2],
                "computed_at": r[3].isoformat(),
            }
            for r in rows
        ]
    }
