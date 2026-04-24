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
    rows = clickhouse.query_retention(target_date)

    clickhouse.write_retention(rows)
    postgres.upsert_retention(rows)

    d1_rows = [(cohort_date, day, retained, cohort_size, rate) for cohort_date, day, retained, cohort_size, rate in rows if day == 1]
    d7_rows = [(cohort_date, day, retained, cohort_size, rate) for cohort_date, day, retained, cohort_size, rate in rows if day == 7]

    avg_d1 = sum(r[4] for r in d1_rows) / len(d1_rows) if d1_rows else 0.0
    avg_d7 = sum(r[4] for r in d7_rows) / len(d7_rows) if d7_rows else 0.0

    logger.info(f"Retention for {target_date}: cohorts={len(set(r[0] for r in rows))} avg_d1={avg_d1:.3f} avg_d7={avg_d7:.3f}")
    return {"retention_cohorts": len(rows), "avg_retention_d1": avg_d1, "avg_retention_d7": avg_d7}


def read(target_date: date, postgres: PostgresClient) -> dict:
    conn = postgres._get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cohort_date, day, retained, cohort_size, rate, computed_at FROM retention WHERE cohort_date = %s ORDER BY day",
            (target_date,),
        )
        rows = cur.fetchall()
    return {
        "retention": [
            {
                "cohort_date": r[0].isoformat(),
                "day": r[1],
                "retained": r[2],
                "cohort_size": r[3],
                "rate": r[4],
                "computed_at": r[5].isoformat(),
            }
            for r in rows
        ]
    }
