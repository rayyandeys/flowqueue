import argparse
import json
import time
from urllib.request import Request, urlopen


BASE_URL = "http://127.0.0.1:8000"


def request_json(path, method="GET", payload=None):
    data = None
    headers = {}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(
        f"{BASE_URL}{path}",
        data=data,
        headers=headers,
        method=method,
    )

    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    job_ids = []

    print(f"\nSubmitting {args.jobs} jobs...")

    enqueue_start = time.perf_counter()

    for i in range(args.jobs):
        job = request_json(
            "/jobs",
            method="POST",
            payload={
                "task_name": "send_email",
                "payload": {
                    "recipient": f"benchmark-{i}@example.com",
                    "sleep_seconds": args.sleep,
                },
                "priority": "normal",
                "max_retries": 3,
            },
        )

        job_ids.append(job["id"])

    enqueue_time = time.perf_counter() - enqueue_start

    print(f"Queued {args.jobs} jobs in {enqueue_time:.3f}s")
    print(
        f"API enqueue throughput: "
        f"{args.jobs / enqueue_time:.2f} jobs/sec"
    )

    processing_start = time.perf_counter()

    remaining = set(job_ids)

    while remaining:
        completed = []

        for job_id in remaining:
            job = request_json(f"/jobs/{job_id}")

            if job["status"] == "succeeded":
                completed.append(job_id)

            elif job["status"] == "failed":
                raise RuntimeError(
                    f"Benchmark job {job_id} failed"
                )

        for job_id in completed:
            remaining.remove(job_id)

        if remaining:
            print(
                f"\rCompleted: {args.jobs - len(remaining)}/{args.jobs}",
                end="",
                flush=True,
            )
            time.sleep(0.25)

    processing_time = time.perf_counter() - processing_start
    total_time = enqueue_time + processing_time

    print(f"\rCompleted: {args.jobs}/{args.jobs}")
    print(f"Processing time: {processing_time:.3f}s")
    print(f"Total benchmark time: {total_time:.3f}s")
    print(
        f"End-to-end throughput: "
        f"{args.jobs / total_time:.2f} jobs/sec"
    )


if __name__ == "__main__":
    main()