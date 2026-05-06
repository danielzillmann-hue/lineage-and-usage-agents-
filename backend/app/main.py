import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import buckets, runs

logging.basicConfig(level=get_settings().log_level)

app = FastAPI(
    title="Lineage and Usage Agents",
    description="Oracle warehouse multi-agent analyzer — Inventory, Lineage, Usage.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(buckets.router, prefix="/api/buckets", tags=["buckets"])
app.include_router(buckets.get_demo_router(), prefix="/api", tags=["demo"])
app.include_router(runs.router, prefix="/api/runs", tags=["runs"])


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "lineage-and-usage-agents", "version": "0.1.0"}
