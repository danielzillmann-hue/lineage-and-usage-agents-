from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class AgentName(str, Enum):
    INVENTORY = "inventory"
    LINEAGE = "lineage"
    USAGE = "usage"
    SUMMARY = "summary"


class AgentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class RunRequest(BaseModel):
    bucket: str
    prefix: str = ""
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
    bucket: str
    prefix: str
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
    other_files: int
    total_bytes: int
    sample_paths: list[str]


class StreamEvent(BaseModel):
    event: Literal["status", "log", "thinking", "result", "error", "done"]
    agent: AgentName | None = None
    message: str | None = None
    data: dict | None = None
    ts: datetime = Field(default_factory=datetime.utcnow)
