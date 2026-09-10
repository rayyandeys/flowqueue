from fastapi import FastAPI

import app.models
from app.api.jobs import router as jobs_router
from app.database import Base, engine


Base.metadata.create_all(bind=engine)


app = FastAPI(
    title="FlowQueue API",
    description="Distributed background job orchestration platform",
    version="0.1.0",
)

app.include_router(jobs_router)


@app.get("/")
async def root():
    return {
        "service": "FlowQueue",
        "status": "running",
        "version": "0.1.0",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
    }