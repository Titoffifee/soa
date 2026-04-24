import logging
from datetime import date

from fastapi import FastAPI, HTTPException, BackgroundTasks

from config import Settings
from generator import generate_events
from kafka_client import KafkaAvroProducer
from models import MovieEventIn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

settings = Settings()
producer = KafkaAvroProducer(settings)

app = FastAPI(title="Movie Events Producer", version="1.0.0")


@app.post("/events", status_code=201)
def post_event(event_in: MovieEventIn):
    record = event_in.to_record()
    try:
        producer.publish(record.to_avro_dict(), partition_key=record.user_id)
        producer.flush(timeout=10.0)
    except Exception as exc:
        logger.error(f"Failed to publish event {record.event_id}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to publish event: {exc}")

    logger.info(f"Published event_id={record.event_id} event_type={record.event_type}")
    return {"event_id": record.event_id, "status": "published"}


@app.get("/generate")
def generate(
    background_tasks: BackgroundTasks,
    count: int = None,
    delay_ms: int = None,
    target_date: date = None,
):
    num_sessions = count if count is not None else settings.generator_default_sessions
    session_delay = delay_ms if delay_ms is not None else settings.generator_default_delay_ms

    def _run():
        logger.info(f"Generator started: sessions={num_sessions} delay_ms={session_delay} target_date={target_date}")
        events = generate_events(num_sessions, session_delay, target_date)
        published = 0
        errors = 0
        for ev in events:
            try:
                producer.publish(ev.to_avro_dict(), partition_key=ev.user_id)
                published += 1
            except Exception as exc:
                logger.error(f"Generator failed to publish event {ev.event_id}: {exc}")
                errors += 1
        producer.flush(timeout=30.0)
        logger.info(f"Generator finished: published={published} errors={errors}")

    background_tasks.add_task(_run)
    return {
        "status": "generating",
        "sessions": num_sessions,
        "delay_ms": session_delay,
        "target_date": target_date.isoformat() if target_date else None,
    }


@app.get("/health")
def health():
    return {"status": "ok"}
