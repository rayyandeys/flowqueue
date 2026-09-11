import os
import runpy
import sys
import threading
from pathlib import Path

import uvicorn
from fastapi import FastAPI


WORKER_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WORKER_DIR))

app = FastAPI(
    title="FlowQueue Worker",
    version="1.0.0",
)


@app.get("/")
def root():
    return {
        "service": "flowqueue-worker",
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "worker",
    }


def start_health_server():
    port = int(os.getenv("PORT", "10000"))

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
    )


if __name__ == "__main__":
    health_thread = threading.Thread(
        target=start_health_server,
        daemon=True,
    )
    health_thread.start()

    # Run the real FlowQueue worker in the foreground.
    # If the worker crashes, this process exits and Render can restart it.
    runpy.run_path(
        str(WORKER_DIR / "main.py"),
        run_name="__main__",
    )