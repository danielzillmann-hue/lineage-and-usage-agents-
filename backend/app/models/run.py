from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class AgentName(str, Enum):
    INVENTORY = "inventory"
    LINEAGE = "lineage"
    USAGE = "usage"
    SUMMARY = "summary"
    TRANSFORM = "transform"
    ORCHESTRATION = "orchestration"


class AgentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class OracleConnection(BaseModel):
    host: str
    port: int = 1521
    service: str
    user: str
    password: str


class RunRequest(BaseModel):
    # Either or both. Oracle connection is the primary input for the live demo;
    # the bucket is optional for ETL XMLs and any extracts. outputs_prefix
    # points at where the pipeline output CSVs live (often a different prefix
    # in the same bucket).
    oracle: OracleConnection | None = None
    bucket: str | None = None
    prefix: str = ""
    outputs_prefix: str | None = None  # if None, no separate outputs scan
    documents_prefix: str | None = None  # delivery specs for cross-check
    agents: list[AgentName] = Field(default_factory=lambda: list(AgentName))
    label: str | None = None


class AgentRunState(BaseModel):
    name: AgentName
    status: AgentStatus = AgentStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    artifact_path: str | None = None


class Run(BaseModel):
    id: str
    bucket: str | None = None
    prefix: str = ""
    oracle_dsn: str | None = None
    label: str | None = None
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    created_at: datetime
    updated_at: datetime
    agents: list[AgentRunState]


class BucketPreview(BaseModel):
    bucket: str
    prefix: str
    ddl_files: int
    dictionary_files: int
    awr_files: int
    etl_files: int = 0
    output_files: int = 0
    other_files: int
    total_bytes: int
    sample_paths: list[str]


class StreamEvent(BaseModel):
    event: Literal["status", "log", "thinking", "result", "error", "done"]
    agent: AgentName | None = None
    message: str | None = None
    data: dict | None = None
    ts: datetime = Field(default_factory=datetime.utcnow)
