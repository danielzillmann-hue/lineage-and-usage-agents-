"""Domain models for the parsed Oracle warehouse and the agents' outputs."""

from enum import Enum

from pydantic import BaseModel, Field


class Layer(str, Enum):
    RAW = "raw"
    STAGING = "staging"
    INTEGRATION = "integration"
    REPORTING = "reporting"
    UNKNOWN = "unknown"


class Domain(str, Enum):
    MEMBER = "member"
    ACCOUNT = "account"
    PRODUCT = "product"
    ADVISER = "adviser"
    TRANSACTION = "transaction"
    HOLDING = "holding"
    FEE = "fee"
    REFERENCE = "reference"
    AUDIT = "audit"
    OTHER = "other"


class Column(BaseModel):
    name: str
    data_type: str
    nullable: bool = True
    is_pk: bool = False
    is_fk: bool = False
    fk_target: str | None = None
    comment: str | None = None


class Table(BaseModel):
    schema_name: str
    name: str
    kind: str  # TABLE / VIEW / MVIEW
    columns: list[Column] = Field(default_factory=list)
    row_count: int | None = None
    bytes: int | None = None
    last_analyzed: str | None = None
    layer: Layer = Layer.UNKNOWN
    domain: Domain = Domain.OTHER
    comment: str | None = None
    source_text: str | None = None  # for views and mviews

    @property
    def fqn(self) -> str:
        return f"{self.schema_name}.{self.name}"


class Procedure(BaseModel):
    schema_name: str
    name: str
    kind: str  # PROCEDURE / FUNCTION / PACKAGE / PACKAGE_BODY / TRIGGER
    source: str
    last_compiled: str | None = None


class Inventory(BaseModel):
    tables: list[Table] = Field(default_factory=list)
    procedures: list[Procedure] = Field(default_factory=list)
    flags: list["InventoryFlag"] = Field(default_factory=list)


class InventoryFlag(BaseModel):
    severity: str  # info / warn / critical
    title: str
    detail: str
    object_fqn: str | None = None


class LineageEdge(BaseModel):
    source_fqn: str
    source_column: str | None = None
    target_fqn: str
    target_column: str | None = None
    operation: str  # SELECT / VIEW / CTAS / INSERT / MERGE / PROC
    transform: str | None = None  # short description if non-trivial
    origin_object: str | None = None  # the object whose code declared this edge
    confidence: float = 1.0


class LineageGraph(BaseModel):
    edges: list[LineageEdge] = Field(default_factory=list)
    unresolved: list[str] = Field(default_factory=list)


class ObjectUsage(BaseModel):
    fqn: str
    read_count: int = 0
    write_count: int = 0
    distinct_users: int = 0
    last_read: str | None = None
    last_write: str | None = None


class UsageReport(BaseModel):
    objects: list[ObjectUsage] = Field(default_factory=list)
    hot_tables: list[str] = Field(default_factory=list)
    write_only_orphans: list[str] = Field(default_factory=list)
    dead_objects: list[str] = Field(default_factory=list)
    reporting_reachable_sources: list[str] = Field(default_factory=list)
    reporting_unreachable_sources: list[str] = Field(default_factory=list)


class ExecutiveSummary(BaseModel):
    headline: str
    bullets: list[str] = Field(default_factory=list)
    findings: list["Finding"] = Field(default_factory=list)
    metrics: dict[str, str | int | float] = Field(default_factory=dict)


class Finding(BaseModel):
    severity: str  # info / warn / critical
    title: str
    detail: str
    object_fqns: list[str] = Field(default_factory=list)
    recommendation: str | None = None


class RunResults(BaseModel):
    inventory: Inventory | None = None
    lineage: LineageGraph | None = None
    usage: UsageReport | None = None
    summary: ExecutiveSummary | None = None


Inventory.model_rebuild()
ExecutiveSummary.model_rebuild()
