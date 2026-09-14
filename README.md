# FlowQueue

A distributed priority task queue built with FastAPI, Redis Streams, PostgreSQL, and concurrent Python workers.

FlowQueue provides persistent job tracking, priority-aware scheduling, configurable worker concurrency, automatic retries with exponential backoff, dead-letter handling, and stale-job recovery.

## Live Deployment

- API: https://flowqueue-api.onrender.com
- API Documentation: https://flowqueue-api.onrender.com/docs
- Worker Health Service: https://flowqueue-worker.onrender.com

> The services are hosted on Render's free tier and may require a short cold start after periods of inactivity.

---

## Why FlowQueue?

Many applications need to move expensive or asynchronous work outside the request-response cycle.

FlowQueue implements a lightweight distributed task-processing architecture where:

1. clients submit jobs through a REST API,
2. job metadata is persisted in PostgreSQL,
3. jobs are routed into priority-specific Redis Streams,
4. concurrent workers process jobs asynchronously,
5. failures are automatically retried,
6. exhausted jobs are moved to a dead-letter queue,
7. stale pending jobs can be recovered after worker interruption.

---

## Architecture

```mermaid
flowchart LR
    A[Client] --> B[FastAPI]
    B --> C[(PostgreSQL)]
    B --> D[Redis Streams]

    D --> E1[Critical Queue]
    D --> E2[High Queue]
    D --> E3[Normal Queue]
    D --> E4[Low Queue]

    E1 --> F[Worker Pool]
    E2 --> F
    E3 --> F
    E4 --> F

    F --> G[Task Execution]

    G -->|Success| H[Mark Succeeded]
    H --> C

    G -->|Failure| I[Retry + Exponential Backoff]
    I --> D

    I -->|Retries Exhausted| J[Dead-Letter Queue]

    F --> K[Stale Message Recovery]