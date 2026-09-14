import argparse
import json
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


TERMINAL_STATUSES = {
    "succeeded",
    "failed",
    "dead",
    "dead_letter",
    "cancelled",
}


def request_json(
    method,
    url,
    payload=None,
    timeout=60,
):
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

    with urllib.request.urlopen(
        request,
        timeout=timeout,
    ) as response:
        raw_body = response.read().decode("utf-8")

        if raw_body:
            body = json.loads(raw_body)
        else:
            body = {}

    latency = time.perf_counter() - start

    return body, latency


def wake_service(
    url,
    name,
    attempts=5,
):
    print(f"Warming up {name}...")

    for attempt in range(
        1,
        attempts + 1,
    ):
        try:
            body, latency = request_json(
                "GET",
                url,
                timeout=90,
            )

            print(
                f"{name} ready "
                f"({latency:.2f}s)."
            )

            return True

        except Exception as exc:
            print(
                f"{name} warm-up attempt "
                f"{attempt}/{attempts} failed: {exc}"
            )

            if attempt < attempts:
                time.sleep(2)

    return False


def create_job(
    base_url,
    job_number,
    sleep_seconds,
    priority,
):
    payload = {
        "task_name": "send_email",
        "payload": {
            "recipient": (
                f"benchmark-{job_number}"
                f"@example.com"
            ),
            "sleep_seconds": sleep_seconds,
        },
        "priority": priority,
    }

    body, latency = request_json(
        "POST",
        f"{base_url}/jobs",
        payload,
    )

    return body["id"], latency


def get_job_status(
    base_url,
    job_id,
):
    try:
        body, latency = request_json(
            "GET",
            f"{base_url}/jobs/{job_id}",
        )

        return (
            job_id,
            body.get(
                "status",
                "unknown",
            ),
            latency,
            None,
        )

    except Exception as exc:
        return (
            job_id,
            None,
            None,
            str(exc),
        )


def percentile(
    values,
    percentile_value,
):
    if not values:
        return 0.0

    ordered = sorted(values)

    position = (
        len(ordered) - 1
    ) * percentile_value

    lower_index = int(position)
    upper_index = min(
        lower_index + 1,
        len(ordered) - 1,
    )

    fraction = (
        position - lower_index
    )

    return (
        ordered[lower_index]
        * (1 - fraction)
        + ordered[upper_index]
        * fraction
    )


def print_metric(
    name,
    value,
):
    print(
        f"{name:<28}: {value}"
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the FlowQueue API "
            "and worker."
        )
    )

    parser.add_argument(
        "--url",
        default=(
            "https://flowqueue-api"
            ".onrender.com"
        ),
        help="FlowQueue API base URL",
    )

    parser.add_argument(
        "--worker-url",
        default=(
            "https://flowqueue-worker"
            ".onrender.com"
        ),
        help="FlowQueue worker health URL",
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
        help=(
            "Concurrent API submission "
            "clients"
        ),
    )

    parser.add_argument(
        "--pollers",
        type=int,
        default=20,
        help=(
            "Concurrent status polling "
            "requests"
        ),
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0,
        help=(
            "Simulated task duration "
            "in seconds"
        ),
    )

    parser.add_argument(
        "--priority",
        default="critical",
        choices=[
            "critical",
            "high",
            "normal",
            "low",
        ],
        help="Priority for benchmark jobs",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help=(
            "Maximum seconds to wait "
            "for completion"
        ),
    )

    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.25,
        help=(
            "Delay between polling "
            "rounds"
        ),
    )

    args = parser.parse_args()

    base_url = (
        args.url.rstrip("/")
    )

    worker_url = (
        args.worker_url.rstrip("/")
    )

    print()
    print("FlowQueue Benchmark")
    print("=" * 60)

    print_metric(
        "API",
        base_url,
    )

    print_metric(
        "Worker",
        worker_url,
    )

    print_metric(
        "Jobs",
        args.jobs,
    )

    print_metric(
        "Concurrent clients",
        args.clients,
    )

    print_metric(
        "Concurrent pollers",
        args.pollers,
    )

    print_metric(
        "Task sleep",
        f"{args.sleep}s",
    )

    print_metric(
        "Priority",
        args.priority,
    )

    print("=" * 60)
    print()

    api_ready = wake_service(
        base_url,
        "API",
    )

    if not api_ready:
        print(
            "\nAPI could not be warmed up."
        )
        return

    worker_ready = wake_service(
        worker_url,
        "Worker",
    )

    if not worker_ready:
        print(
            "\nWorker could not be warmed up."
        )
        return

    print()
    print(
        f"Submitting {args.jobs} jobs..."
    )

    benchmark_start = (
        time.perf_counter()
    )

    submission_start = (
        benchmark_start
    )

    job_ids = []
    submission_latencies = []
    submission_errors = []

    with ThreadPoolExecutor(
        max_workers=args.clients
    ) as executor:
        futures = [
            executor.submit(
                create_job,
                base_url,
                job_number,
                args.sleep,
                args.priority,
            )
            for job_number
            in range(args.jobs)
        ]

        for future in as_completed(
            futures
        ):
            try:
                (
                    job_id,
                    latency,
                ) = future.result()

                job_ids.append(
                    job_id
                )

                submission_latencies.append(
                    latency
                )

            except Exception as exc:
                submission_errors.append(
                    str(exc)
                )

    submission_end = (
        time.perf_counter()
    )

    submission_duration = (
        submission_end
        - submission_start
    )

    if not job_ids:
        print()
        print(
            "No jobs were successfully "
            "created."
        )

        if submission_errors:
            print(
                "First error:",
                submission_errors[0],
            )

        return

    print(
        f"Created {len(job_ids)}/"
        f"{args.jobs} jobs "
        f"in {submission_duration:.3f}s."
    )

    print()
    print(
        "Checking queue state "
        "after submission..."
    )

    remaining = set(job_ids)
    final_statuses = {}

    poll_latencies = []
    poll_errors = 0

    completed_by_submission_end = 0

    with ThreadPoolExecutor(
        max_workers=args.pollers
    ) as executor:
        futures = [
            executor.submit(
                get_job_status,
                base_url,
                job_id,
            )
            for job_id in list(
                remaining
            )
        ]

        for future in as_completed(
            futures
        ):
            (
                job_id,
                status,
                latency,
                error,
            ) = future.result()

            if latency is not None:
                poll_latencies.append(
                    latency
                )

            if error is not None:
                poll_errors += 1
                continue

            if status in TERMINAL_STATUSES:
                final_statuses[
                    job_id
                ] = status

                remaining.discard(
                    job_id
                )

    completed_by_submission_end = (
        len(job_ids)
        - len(remaining)
    )

    jobs_remaining_after_submit = (
        len(remaining)
    )

    print(
        f"Already complete: "
        f"{completed_by_submission_end}"
    )

    print(
        f"Remaining to drain: "
        f"{jobs_remaining_after_submit}"
    )

    drain_start = (
        time.perf_counter()
    )

    deadline = (
        drain_start
        + args.timeout
    )

    print()
    print(
        "Waiting for remaining jobs "
        "to finish..."
    )

    while (
        remaining
        and time.perf_counter()
        < deadline
    ):
        completed_this_round = []

        with ThreadPoolExecutor(
            max_workers=args.pollers
        ) as executor:
            futures = [
                executor.submit(
                    get_job_status,
                    base_url,
                    job_id,
                )
                for job_id
                in list(remaining)
            ]

            for future in as_completed(
                futures
            ):
                (
                    job_id,
                    status,
                    latency,
                    error,
                ) = future.result()

                if latency is not None:
                    poll_latencies.append(
                        latency
                    )

                if error is not None:
                    poll_errors += 1
                    continue

                if status in TERMINAL_STATUSES:
                    final_statuses[
                        job_id
                    ] = status

                    completed_this_round.append(
                        job_id
                    )

        for job_id in completed_this_round:
            remaining.discard(
                job_id
            )

        finished = (
            len(job_ids)
            - len(remaining)
        )

        print(
            f"\rCompleted: "
            f"{finished}/"
            f"{len(job_ids)}",
            end="",
            flush=True,
        )

        if remaining:
            time.sleep(
                args.poll_interval
            )

    benchmark_end = (
        time.perf_counter()
    )

    drain_duration = (
        benchmark_end
        - drain_start
    )

    total_duration = (
        benchmark_end
        - benchmark_start
    )

    print()
    print()

    succeeded = sum(
        1
        for status
        in final_statuses.values()
        if status == "succeeded"
    )

    failed = sum(
        1
        for status
        in final_statuses.values()
        if status != "succeeded"
    )

    timed_out = len(
        remaining
    )

    success_rate = (
        succeeded
        / len(job_ids)
        * 100
        if job_ids
        else 0
    )

    mean_post_latency = (
        statistics.mean(
            submission_latencies
        )
        if submission_latencies
        else 0
    )

    p95_post_latency = percentile(
        submission_latencies,
        0.95,
    )

    mean_poll_latency = (
        statistics.mean(
            poll_latencies
        )
        if poll_latencies
        else 0
    )

    submission_throughput = (
        len(job_ids)
        / submission_duration
        if submission_duration > 0
        else 0
    )

    end_to_end_throughput = (
        succeeded
        / total_duration
        if total_duration > 0
        else 0
    )

    post_submit_drain_throughput = (
        jobs_remaining_after_submit
        / drain_duration
        if (
            jobs_remaining_after_submit
            > 0
            and drain_duration > 0
        )
        else 0
    )

    print("RESULTS")
    print("=" * 60)

    print_metric(
        "Successfully submitted",
        len(job_ids),
    )

    print_metric(
        "Submission errors",
        len(submission_errors),
    )

    print_metric(
        "Succeeded jobs",
        succeeded,
    )

    print_metric(
        "Failed jobs",
        failed,
    )

    print_metric(
        "Timed out",
        timed_out,
    )

    print_metric(
        "Success rate",
        f"{success_rate:.2f}%",
    )

    print()

    print_metric(
        "Submission time",
        f"{submission_duration:.3f}s",
    )

    print_metric(
        "Queue drain time",
        f"{drain_duration:.3f}s",
    )

    print_metric(
        "Total benchmark time",
        f"{total_duration:.3f}s",
    )

    print()

    print_metric(
        "API submit throughput",
        (
            f"{submission_throughput:.2f} "
            f"jobs/sec"
        ),
    )

    print_metric(
        "End-to-end throughput",
        (
            f"{end_to_end_throughput:.2f} "
            f"jobs/sec"
        ),
    )

    print_metric(
        "Post-submit drain rate",
        (
            f"{post_submit_drain_throughput:.2f} "
            f"jobs/sec"
        ),
    )

    print()

    print_metric(
        "Mean POST latency",
        (
            f"{mean_post_latency * 1000:.2f} "
            f"ms"
        ),
    )

    print_metric(
        "P95 POST latency",
        (
            f"{p95_post_latency * 1000:.2f} "
            f"ms"
        ),
    )

    print_metric(
        "Mean status-poll latency",
        (
            f"{mean_poll_latency * 1000:.2f} "
            f"ms"
        ),
    )

    print_metric(
        "Polling errors",
        poll_errors,
    )

    print("=" * 60)


if __name__ == "__main__":
    main()