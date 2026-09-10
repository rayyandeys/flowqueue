from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


class JobPriority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class JobCreate(BaseModel):
    task_name: str = Field(min_length=1, max_length=100)
    payload: dict = Field(default_factory=dict)
    priority: JobPriority = JobPriority.NORMAL
    max_retries: int = Field(default=3, ge=0, le=10)


class JobResponse(BaseModel):
    id: UUID
    task_name: str
    payload: dict
    priority: str
    status: str
    retry_count: int
    max_retries: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)