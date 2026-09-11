import os
import socket

from redis import Redis


PRIORITY_STREAMS = [
    "flowqueue:jobs:critical",
    "flowqueue:jobs:high",
    "flowqueue:jobs:normal",
    "flowqueue:jobs:low",
]

DEAD_LETTER_STREAM = "flowqueue:dead-letter"

CONSUMER_GROUP = "flowqueue-workers"

HOSTNAME = socket.gethostname()
PROCESS_ID = os.getpid()

CONSUMER_NAME = f"{HOSTNAME}-{PROCESS_ID}"


redis_url = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0",
)

redis_client = Redis.from_url(
    redis_url,
    decode_responses=True,
    socket_timeout=None,
    socket_connect_timeout=5,
    health_check_interval=30,
)


def get_stream_for_priority(
    priority: str,
) -> str:
    stream_map = {
        "critical": "flowqueue:jobs:critical",
        "high": "flowqueue:jobs:high",
        "normal": "flowqueue:jobs:normal",
        "low": "flowqueue:jobs:low",
    }

    return stream_map.get(
        priority,
        stream_map["normal"],
    )