import logging
from datetime import date, datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException

from clickhouse_client import ClickHouseClient
from config import settings
from pipeline import AggregationPipeline
from postgres_client import PostgresClient
from s3_client import S3Client
from s3_export import S3Exporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

clickhouse_client = ClickHouseClient(settings)
postgres_client = PostgresClient(settings)
pipeline = AggregationPipeline(clickhouse_client, postgres_client)
s3_client = S3Client(settings)
s3_exporter = S3Exporter(postgres_client, s3_client)

app = FastAPI(title="Aggregation Service", version="1.0.0")
scheduler = BackgroundScheduler()


def _scheduled_run():
    target_date = datetime.now(timezone.utc).date()
    logger.info(f"Scheduled aggregation triggered for date={target_date}")
    try:
        pipeline.run(target_date)
    except Exception as exc:
        logger.error(f"Scheduled aggregation failed: {exc}")


def _scheduled_export():
    target_date = datetime.now(timezone.utc).date()
    logger.info(f"Scheduled S3 export triggered for date={target_date}")
    try:
        s3_exporter.export(target_date)
    except Exception as exc:
        logger.error(f"Scheduled S3 export failed: {exc}")


@app.on_event("startup")
def startup():
    scheduler.add_job(
        _scheduled_run,
        trigger="interval",
        seconds=settings.schedule_interval_seconds,
        id="aggregation",
        replace_existing=True,
    )
    scheduler.add_job(
        _scheduled_export,
        trigger="interval",
        seconds=settings.schedule_interval_seconds,
        id="s3_export",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started with interval={settings.schedule_interval_seconds}s")


@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown(wait=False)


@app.post("/aggregate")
def trigger_aggregation(target_date: date | None = None):
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()
    try:
        result = pipeline.run(target_date)
    except Exception as exc:
        logger.error(f"Manual aggregation failed for date={target_date}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    return {"date": target_date.isoformat(), "metrics": result}


@app.post("/export")
def trigger_export(target_date: date | None = None):
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()
    try:
        result = s3_exporter.export(target_date)
    except Exception as exc:
        logger.error(f"Manual S3 export failed for date={target_date}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    return {"date": target_date.isoformat(), **result}


@app.get("/health")
def health():
    return {"status": "ok"}
