from redis import Redis

from app.config import settings


PRIORITY_STREAMS = {
    "critical": "flowqueue:jobs:critical",
    "high": "flowqueue:jobs:high",
    "normal": "flowqueue:jobs:normal",
    "low": "flowqueue:jobs:low",
}


redis_client = Redis.from_url(
    settings.redis_url,
    decode_responses=True,
)


def ping_redis() -> bool:
    return bool(redis_client.ping())


def get_stream_for_priority(priority: str) -> str:
    return PRIORITY_STREAMS.get(
        priority,
        PRIORITY_STREAMS["normal"],
    )


def enqueue_job(
    job_id: str,
    task_name: str,
    priority: str,
) -> str:
    stream_name = get_stream_for_priority(
        priority
    )

    message_id = redis_client.xadd(
        stream_name,
        {
            "job_id": job_id,
            "task_name": task_name,
            "priority": priority,
        },
    )

    return str(message_id)