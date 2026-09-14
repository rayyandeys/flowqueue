import argparse
import json
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


TERMINAL_STATUSES = {"succeeded", "failed", "dead", "cancelled"}


def request_json(method, url, payload=None, timeout=30):
    data = None
    headers = {}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method=method,
    )

    start = time.perf_counter()

    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = json.loads(response.read().decode("utf-8"))

    latency = time.perf_counter() - start
    return body, latency


def create_job(base_url, job_number, sleep_seconds):
    payload = {
        "task_name": "send_email",
        "payload": {
            "recipient": f"benchmark-{job_number}@example.com",
            "sleep_seconds": sleep_seconds,
        },
        "priority": "critical",
    }

    body, latency = request_json(
        "POST",
        f"{base_url}/jobs",
        payload,
    )

    return body["id"], latency


def percentile(values, percentile_value):
    if not values:
        return 0.0

    ordered = sorted(values)
    index = int((len(ordered) - 1) * percentile_value)
    return ordered[index]


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark the FlowQueue API and worker."
    )

    parser.add_argument(
        "--url",
        default="https://flowqueue-api.onrender.com",
        help="FlowQueue API base URL",
    )

    parser.add_argument(
        "--jobs",
        type=int,
        default=100,
        help="Number of jobs to submit",
    )

    parser.add_argument(
        "--clients",
        type=int,
        default=10,
        help="Number of concurrent API clients",
    )

    parser.add_argument(
        "--sleep",
        type=int,
        default=0,
        help="Simulated task duration in seconds",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="Maximum time to wait for all jobs",
    )

    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    print("\nFlowQueue Benchmark")
    print("=" * 55)
    print(f"API:               {base_url}")
    print(f"Jobs:              {args.jobs}")
    print(f"Concurrent clients:{args.clients:>7}")
    print(f"Task sleep:        {args.sleep:>7}s")
    print("=" * 55)

    # Wake the Render service before measuring.
    print("\nWarming up API...")

    try:
        request_json("GET", base_url)
    except Exception as exc:
        print(f"Warm-up request failed: {exc}")
        return

    print("API ready.")

    benchmark_start = time.perf_counter()
    submission_start = time.perf_counter()

    job_ids = []
    submission_latencies = []
    submission_errors = []

    print(f"\nSubmitting {args.jobs} jobs...")

    with ThreadPoolExecutor(max_workers=args.clients) as executor:
        futures = [
            executor.submit(
                create_job,
                base_url,
                i,
                args.sleep,
            )
            for i in range(args.jobs)
        ]

        for future in as_completed(futures):
            try:
                job_id, latency = future.result()
                job_ids.append(job_id)
                submission_latencies.append(latency)
            except Exception as exc:
                submission_errors.append(str(exc))

    submission_end = time.perf_counter()
    submission_duration = submission_end - submission_start

    if not job_ids:
        print("\nNo jobs were successfully created.")
        if submission_errors:
            print("First error:", submission_errors[0])
        return

    print(
        f"Created {len(job_ids)}/{args.jobs} jobs "
        f"in {submission_duration:.2f}s."
    )

    print("\nWaiting for jobs to finish...")

    remaining = set(job_ids)
    final_statuses = {}
    deadline = time.perf_counter() + args.timeout

    while remaining and time.perf_counter() < deadline:
        completed_this_round = []

        for job_id in list(remaining):
            try:
                body, _ = request_json(
                    "GET",
                    f"{base_url}/jobs/{job_id}",
                )

                status = body.get("status", "unknown")

                if status in TERMINAL_STATUSES:
                    final_statuses[job_id] = status
                    completed_this_round.append(job_id)

            except Exception:
                pass

        for job_id in completed_this_round:
            remaining.discard(job_id)

        finished = len(job_ids) - len(remaining)

        print(
            f"\rCompleted: {finished}/{len(job_ids)}",
            end="",
            flush=True,
        )

        if remaining:
            time.sleep(0.25)

    benchmark_end = time.perf_counter()
    total_duration = benchmark_end - benchmark_start

    print("\n")

    success_count = sum(
        1 for status in final_statuses.values()
        if status == "succeeded"
    )

    failed_count = sum(
        1 for status in final_statuses.values()
        if status != "succeeded"
    )

    timed_out = len(remaining)

    average_latency = (
        statistics.mean(submission_latencies)
        if submission_latencies
        else 0
    )

    p95_latency = percentile(submission_latencies, 0.95)

    submission_throughput = (
        len(job_ids) / submission_duration
        if submission_duration > 0
        else 0
    )

    completion_throughput = (
        success_count / total_duration
        if total_duration > 0
        else 0
    )

    print("RESULTS")
    print("=" * 55)
    print(f"Successfully submitted : {len(job_ids)}")
    print(f"Submission errors      : {len(submission_errors)}")
    print(f"Succeeded jobs         : {success_count}")
    print(f"Failed jobs            : {failed_count}")
    print(f"Timed out              : {timed_out}")
    print()
    print(f"Submission time        : {submission_duration:.3f}s")
    print(f"Total benchmark time   : {total_duration:.3f}s")
    print()
    print(f"API submit throughput  : {submission_throughput:.2f} jobs/sec")
    print(f"End-to-end throughput  : {completion_throughput:.2f} jobs/sec")
    print(f"Mean POST latency      : {average_latency * 1000:.2f} ms")
    print(f"P95 POST latency       : {p95_latency * 1000:.2f} ms")
    print("=" * 55)


if __name__ == "__main__":
    main()