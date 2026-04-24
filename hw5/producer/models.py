from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import uuid

from pydantic import BaseModel, Field


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEventRecord(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    movie_id: str
    event_type: EventType
    timestamp: int = Field(
        default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000),
    )
    device_type: DeviceType
    session_id: str
    progress_seconds: Optional[int] = None

    def to_avro_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }


class MovieEventIn(BaseModel):
    event_id: Optional[str] = Field(default=None)
    user_id: str
    movie_id: str
    event_type: EventType
    timestamp: Optional[datetime] = Field(default=None)
    device_type: DeviceType
    session_id: str
    progress_seconds: Optional[int] = Field(default=None, ge=0)

    def to_record(self) -> MovieEventRecord:
        ts = self.timestamp or datetime.now(timezone.utc)
        ts_ms = int(ts.timestamp() * 1000)
        return MovieEventRecord(
            event_id=self.event_id or str(uuid.uuid4()),
            user_id=self.user_id,
            movie_id=self.movie_id,
            event_type=self.event_type,
            timestamp=ts_ms,
            device_type=self.device_type,
            session_id=self.session_id,
            progress_seconds=self.progress_seconds,
        )
