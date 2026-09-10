import time


def send_email(payload: dict) -> None:
    recipient = payload.get("recipient")

    if not recipient:
        raise ValueError(
            "send_email requires a recipient"
        )

    should_fail = payload.get(
        "should_fail",
        False,
    )

    sleep_seconds = payload.get(
        "sleep_seconds",
        3,
    )

    print(
        f"📧 Sending email to {recipient}"
    )

    print(
        f"⏳ Simulating task for {sleep_seconds} seconds..."
    )

    time.sleep(sleep_seconds)

    if should_fail:
        raise RuntimeError(
            "Simulated email service failure"
        )

    print(
        f"✅ Email sent to {recipient}"
    )


TASKS = {
    "send_email": send_email,
}