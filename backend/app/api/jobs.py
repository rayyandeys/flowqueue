from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Job
from app.queue.redis_client import enqueue_job
from app.schemas import JobCreate, JobResponse


router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
)


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_job(
    job_data: JobCreate,
    db: Session = Depends(get_db),
) -> Job:
    job = Job(
        task_name=job_data.task_name,
        payload=job_data.payload,
        priority=job_data.priority.value,
        status="queued",
        max_retries=job_data.max_retries,
    )

    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        enqueue_job(
            job_id=str(job.id),
            task_name=job.task_name,
            priority=job.priority,
        )
    except Exception as exc:
        job.status = "failed"
        db.commit()

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to enqueue job in Redis: {exc}",
        ) from exc

    return job


@router.get(
    "",
    response_model=list[JobResponse],
)
def list_jobs(
    db: Session = Depends(get_db),
) -> list[Job]:
    statement = select(Job).order_by(Job.created_at.desc())
    jobs = db.scalars(statement).all()

    return list(jobs)


@router.get(
    "/{job_id}",
    response_model=JobResponse,
)
def get_job(
    job_id: UUID,
    db: Session = Depends(get_db),
) -> Job:
    job = db.get(Job, job_id)

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    return job