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
        return response.status, json.loads(response.read().decode("utf-8"))


def test_health():
    status_code, body = request_json("/health")

    assert status_code == 200
    assert body["status"] == "healthy"


def test_job_executes_successfully():
    status_code, job = request_json(
        "/jobs",
        method="POST",
        payload={
            "task_name": "send_email",
            "payload": {
                "recipient": "integration-test@example.com",
                "sleep_seconds": 1,
            },
            "priority": "high",
            "max_retries": 3,
        },
    )

    assert status_code in (200, 201)
    assert job["status"] == "queued"
    assert job["priority"] == "high"

    job_id = job["id"]

    deadline = time.time() + 20

    while time.time() < deadline:
        _, current_job = request_json(f"/jobs/{job_id}")

        if current_job["status"] == "succeeded":
            assert current_job["retry_count"] == 0
            return

        if current_job["status"] == "failed":
            raise AssertionError(
                f"Job unexpectedly failed: {current_job}"
            )

        time.sleep(0.5)

    raise AssertionError(
        f"Job {job_id} did not succeed within 20 seconds"
    )