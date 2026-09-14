import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from redis.exceptions import (
    ConnectionError,
    ResponseError,
    TimeoutError,
)
from sqlalchemy import text

from database import SessionLocal
from redis_client import (
    CONSUMER_GROUP,
    CONSUMER_NAME,
    DEAD_LETTER_STREAM,
    PRIORITY_STREAMS,
    get_stream_for_priority,
    redis_client,
)
from tasks import TASKS


RECOVERY_INTERVAL_SECONDS = 5
CLAIM_IDLE_TIME_MS = 10000

WORKER_CONCURRENCY = max(
    1,
    int(os.getenv("WORKER_CONCURRENCY", "5")),
)


def create_consumer_groups() -> None:
    for stream_name in PRIORITY_STREAMS:
        try:
            redis_client.xgroup_create(
                stream_name,
                CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )

            print(
                f"Consumer group created for {stream_name}"
            )

        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                print(
                    f"Consumer group already exists for {stream_name}"
                )
            else:
                raise


def get_job(
    job_id: str,
) -> dict | None:
    with SessionLocal() as db:
        result = db.execute(
            text(
                """
                SELECT
                    id,
                    task_name,
                    payload,
                    priority,
                    status,
                    retry_count,
                    max_retries
                FROM jobs
                WHERE id = CAST(:job_id AS uuid)
                """
            ),
            {
                "job_id": job_id,
            },
        )

        row = result.mappings().first()

        if row is None:
            return None

        job = dict(row)

        payload = job["payload"]

        if isinstance(payload, str):
            payload = json.loads(
                payload
            )

        job["payload"] = payload

        return job


def update_job_state(
    job_id: str,
    status: str,
    retry_count: int | None = None,
) -> None:
    with SessionLocal() as db:
        if retry_count is None:
            db.execute(
                text(
                    """
                    UPDATE jobs
                    SET status = :status
                    WHERE id = CAST(:job_id AS uuid)
                    """
                ),
                {
                    "status": status,
                    "job_id": job_id,
                },
            )

        else:
            db.execute(
                text(
                    """
                    UPDATE jobs
                    SET
                        status = :status,
                        retry_count = :retry_count
                    WHERE id = CAST(:job_id AS uuid)
                    """
                ),
                {
                    "status": status,
                    "retry_count": retry_count,
                    "job_id": job_id,
                },
            )

        db.commit()


def acknowledge_message(
    stream_name: str,
    message_id: str,
) -> None:
    redis_client.xack(
        stream_name,
        CONSUMER_GROUP,
        message_id,
    )


def enqueue_retry(
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

    return str(
        message_id
    )


def move_to_dead_letter_queue(
    job: dict,
    error_message: str,
) -> str:
    dead_letter_id = redis_client.xadd(
        DEAD_LETTER_STREAM,
        {
            "job_id": str(
                job["id"]
            ),
            "task_name": job["task_name"],
            "priority": job["priority"],
            "retry_count": str(
                job["retry_count"]
            ),
            "error": error_message,
            "failed_at": datetime.now(
                timezone.utc
            ).isoformat(),
        },
    )

    return str(
        dead_letter_id
    )


def calculate_backoff(
    retry_count: int,
) -> int:
    base_delay = 2
    max_delay = 30

    delay = base_delay ** retry_count

    return min(
        delay,
        max_delay,
    )


def process_message(
    stream_name: str,
    message_id: str,
    message: dict,
    recovered: bool = False,
) -> None:
    job_id = message.get(
        "job_id"
    )

    if not job_id:
        print(
            f"Message {message_id} has no job_id."
        )

        acknowledge_message(
            stream_name,
            message_id,
        )

        return

    priority = message.get(
        "priority",
        "normal",
    )

    if recovered:
        print(
            f"\nRecovered stale {priority.upper()} job {job_id}"
        )

    else:
        print(
            f"\nReceived {priority.upper()} job {job_id}"
        )

    job = get_job(
        job_id
    )

    if job is None:
        print(
            f"Job {job_id} does not exist in PostgreSQL."
        )

        acknowledge_message(
            stream_name,
            message_id,
        )

        return

    task_name = job["task_name"]
    payload = job["payload"]

    task = TASKS.get(
        task_name
    )

    if task is None:
        update_job_state(
            job_id,
            "dead_letter",
        )

        move_to_dead_letter_queue(
            job,
            f"Unknown task: {task_name}",
        )

        acknowledge_message(
            stream_name,
            message_id,
        )

        print(
            f"Unknown task moved to DLQ: {task_name}"
        )

        return

    update_job_state(
        job_id,
        "running",
    )

    print(
        f"Job {job_id} is now running."
    )

    try:
        task(
            payload
        )

        update_job_state(
            job_id,
            "succeeded",
        )

        acknowledge_message(
            stream_name,
            message_id,
        )

        print(
            f"Job {job_id} succeeded and was acknowledged."
        )

    except Exception as exc:
        current_retry_count = (
            job["retry_count"]
        )

        max_retries = (
            job["max_retries"]
        )

        next_retry_count = (
            current_retry_count + 1
        )

        print(
            f"Job {job_id} failed: {exc}"
        )

        if (
            next_retry_count
            <= max_retries
        ):
            delay = calculate_backoff(
                next_retry_count
            )

            update_job_state(
                job_id,
                "retrying",
                next_retry_count,
            )

            print(
                f"Retry {next_retry_count}/{max_retries}"
            )

            print(
                f"Waiting {delay} seconds before retry..."
            )

            time.sleep(
                delay
            )

            enqueue_retry(
                job_id=job_id,
                task_name=job["task_name"],
                priority=job["priority"],
            )

            acknowledge_message(
                stream_name,
                message_id,
            )

            print(
                f"Job {job_id} re-enqueued "
                f"to {job['priority'].upper()} queue."
            )

        else:
            update_job_state(
                job_id,
                "dead_letter",
                next_retry_count,
            )

            job["retry_count"] = (
                next_retry_count
            )

            dead_letter_id = (
                move_to_dead_letter_queue(
                    job,
                    str(exc),
                )
            )

            acknowledge_message(
                stream_name,
                message_id,
            )

            print(
                f"Job {job_id} exhausted all retries."
            )

            print(
                f"Moved to DLQ as {dead_letter_id}"
            )


def recover_stream(
    stream_name: str,
) -> None:
    try:
        result = redis_client.xautoclaim(
            stream_name,
            CONSUMER_GROUP,
            CONSUMER_NAME,
            min_idle_time=CLAIM_IDLE_TIME_MS,
            start_id="0-0",
            count=10,
        )

        if not result:
            return

        messages = result[1]

        if not messages:
            return

        for message_id, message in messages:
            print(
                f"Found stale pending message "
                f"{message_id} in {stream_name}"
            )

            process_message(
                stream_name,
                message_id,
                message,
                recovered=True,
            )

    except ResponseError as exc:
        print(
            f"Recovery error on {stream_name}: {exc}"
        )


def recover_stale_messages() -> None:
    for stream_name in PRIORITY_STREAMS:
        recover_stream(
            stream_name
        )


def read_next_job() -> tuple | None:
    for stream_name in PRIORITY_STREAMS:
        response = redis_client.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=CONSUMER_NAME,
            streams={
                stream_name: ">"
            },
            count=1,
            block=250,
        )

        if response:
            _, messages = response[0]

            if messages:
                message_id, message = (
                    messages[0]
                )

                return (
                    stream_name,
                    message_id,
                    message,
                )

    return None


def worker_loop() -> None:
    print(
        f"\nWorker started as '{CONSUMER_NAME}'."
    )

    print(
        "Priority order:"
    )

    print(
        "   CRITICAL -> HIGH -> NORMAL -> LOW"
    )

    print(
        f"Worker concurrency: {WORKER_CONCURRENCY}"
    )

    print(
        f"Crash recovery checks every "
        f"{RECOVERY_INTERVAL_SECONDS} seconds."
    )

    print(
        f"Pending messages idle for "
        f"{CLAIM_IDLE_TIME_MS / 1000:.0f}+ seconds "
        f"can be reclaimed."
    )

    print(
        "Press CTRL+C to stop the worker.\n"
    )

    last_recovery_check = 0.0

    slots = threading.Semaphore(
        WORKER_CONCURRENCY
    )

    active_jobs = 0
    active_jobs_lock = threading.Lock()

    executor = ThreadPoolExecutor(
        max_workers=WORKER_CONCURRENCY,
        thread_name_prefix="flowqueue-worker",
    )

    def run_job(
        stream_name: str,
        message_id: str,
        message: dict,
    ) -> None:
        nonlocal active_jobs

        try:
            process_message(
                stream_name,
                message_id,
                message,
            )

        finally:
            with active_jobs_lock:
                active_jobs -= 1

            slots.release()

    try:
        while True:
            try:
                current_time = (
                    time.time()
                )

                with active_jobs_lock:
                    current_active_jobs = (
                        active_jobs
                    )

                if (
                    current_active_jobs == 0
                    and current_time
                    - last_recovery_check
                    >= RECOVERY_INTERVAL_SECONDS
                ):
                    recover_stale_messages()

                    last_recovery_check = (
                        current_time
                    )

                slots.acquire()

                try:
                    job_message = (
                        read_next_job()
                    )

                except Exception:
                    slots.release()
                    raise

                if job_message is None:
                    slots.release()
                    continue

                (
                    stream_name,
                    message_id,
                    message,
                ) = job_message

                with active_jobs_lock:
                    active_jobs += 1

                try:
                    executor.submit(
                        run_job,
                        stream_name,
                        message_id,
                        message,
                    )

                except Exception:
                    with active_jobs_lock:
                        active_jobs -= 1

                    slots.release()
                    raise

            except TimeoutError:
                print(
                    "Redis read timeout. Retrying..."
                )

                time.sleep(
                    1
                )

            except ConnectionError:
                print(
                    "Redis connection lost. Retrying..."
                )

                time.sleep(
                    2
                )

    finally:
        executor.shutdown(
            wait=True
        )


def main() -> None:
    create_consumer_groups()

    try:
        worker_loop()

    except KeyboardInterrupt:
        print(
            "\nWorker stopped."
        )


if __name__ == "__main__":
    main()