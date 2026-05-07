"""Domain models for the warehouse and the agents' outputs."""

from enum import Enum

from pydantic import BaseModel, Field


class Layer(str, Enum):
    RAW = "raw"
    STAGING = "staging"
    INTEGRATION = "integration"
    REPORTING = "reporting"
    OUTPUT = "output"        # CSV / file artefacts at the end of a pipeline
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
    INVESTMENT = "investment"
    PIPELINE = "pipeline"     # for ETL pipeline objects
    OTHER = "other"


class Sensitivity(str, Enum):
    PII = "pii"             # personal identifying — DOB, name, contact
    FINANCIAL = "financial" # account balances, holdings, fees, transactions
    TAX = "tax"             # TFN, tax-related
    INTERNAL = "internal"   # business but not externally sensitive
    PUBLIC = "public"       # safe to share


class ColumnNature(str, Enum):
    DATA = "data"            # business data column (default)
    KEY = "key"              # identifier (PK or FK)
    AUDIT = "audit"          # load_dt, created_by, modified_dt, etc.
    CALCULATED = "calculated" # derived in source via formula/function
    REFERENCE = "reference"  # lookup/dimension column (small, stable enums)


class Column(BaseModel):
    name: str
    data_type: str
    nullable: bool = True
    is_pk: bool = False
    is_fk: bool = False
    fk_target: str | None = None
    comment: str | None = None
    sensitivity: Sensitivity = Sensitivity.INTERNAL
    nature: ColumnNature = ColumnNature.DATA
    annotation_notes: str | None = None  # short LLM rationale
    # PII propagation: upstream "schema.table.column" entries that flowed
    # PII (or other sensitive) values into this column via the lineage graph.
    inherited_sensitivity_from: list[str] = Field(default_factory=list)
    # Human-edit overrides — set when a user accepts/changes a classification.
    user_overridden: bool = False


class Table(BaseModel):
    schema_name: str
    name: str
    kind: str  # TABLE / VIEW / MVIEW / CSV / PIPELINE
    columns: list[Column] = Field(default_factory=list)
    row_count: int | None = None
    bytes: int | None = None
    last_analyzed: str | None = None
    layer: Layer = Layer.UNKNOWN
    domain: Domain = Domain.OTHER
    comment: str | None = None
    source_text: str | None = None  # view source / pipeline XML

    @property
    def fqn(self) -> str:
        return f"{self.schema_name}.{self.name}"


class Procedure(BaseModel):
    schema_name: str
    name: str
    kind: str  # PROCEDURE / FUNCTION / PACKAGE / TRIGGER
    source: str
    last_compiled: str | None = None


class PipelineStep(BaseModel):
    """One step inside an ETL pipeline DAG."""
    id: str
    kind: str  # extract | transform | join | aggregate | load
    inputs: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    operations: list[str] = Field(default_factory=list)
    source_tables: list[str] = Field(default_factory=list)
    source_query: str | None = None
    output_path: str | None = None


class PipelineRunStats(BaseModel):
    runs_total: int = 0
    runs_success: int = 0
    runs_failed: int = 0
    first_run: str | None = None
    last_run: str | None = None


class ETLPipeline(BaseModel):
    name: str
    file: str
    output_csv: str | None = None
    source_tables: list[str] = Field(default_factory=list)
    steps: list[PipelineStep] = Field(default_factory=list)
    column_count: int = 0
    runs: PipelineRunStats | None = None
    connection_host: str | None = None
    connection_service: str | None = None
    # Cross-checked from the outputs bucket — proof the pipeline produced data
    # even when the audit log says it didn't.
    csv_exists: bool = False
    csv_last_modified: str | None = None
    csv_size_bytes: int | None = None


class InventoryFlag(BaseModel):
    severity: str  # info / warn / critical
    title: str
    detail: str
    object_fqn: str | None = None


class OrphanRun(BaseModel):
    pipeline_name: str
    csv_generated: str | None = None
    runs: PipelineRunStats


class DecommissionAssessment(BaseModel):
    """Per-object verdict on whether it can be safely decommissioned."""
    object_fqn: str
    score: int                    # 0-100 (higher = safer to retire)
    verdict: str                  # safe | review | blocked
    last_read: str | None = None
    days_since_last_read: int | None = None
    downstream_pipeline_count: int = 0
    downstream_view_count: int = 0
    archive_eligible: bool = False  # per Indigo policy: 10y active / 7y exited
    drivers: list[str] = Field(default_factory=list)


class MigrationWave(BaseModel):
    """One wave in the sequenced migration plan."""
    wave: int
    description: str
    table_fqns: list[str] = Field(default_factory=list)
    pipeline_names: list[str] = Field(default_factory=list)


class BusinessRule(BaseModel):
    """An embedded business rule discovered in PL/SQL, ETL, or DDL."""
    rule_type: str  # enum | range | not_null | calculated | filter | constraint
    source_object: str   # fqn of where the rule was found
    column: str | None = None
    expression: str
    natural_language: str
    confidence: float = 0.8


class DeliverySpec(BaseModel):
    """A documented delivery destination for an output CSV.

    Extracted from delivery-specification documents (PDFs / Markdown) found
    alongside the pipeline outputs. Lets us cross-check which CSVs ship
    somewhere the org has formally specified vs which run "off-spec".
    """
    csv_name: str
    kind: str = "internal"   # internal / external / unknown
    destination: str | None = None
    protocol: str | None = None       # SFTP / REST / SMTP / etc.
    endpoint: str | None = None       # full URL or SFTP path
    auth: str | None = None
    frequency: str | None = None
    details: str | None = None
    source_doc: str | None = None     # filename of the spec
    confidence: float = 0.9


class MultiWriterTarget(BaseModel):
    """A target table written by more than one pipeline.

    Mirrors the Transformation Agent's multi_writer_registry classification
    so it can ingest this directly and skip the single-threaded pre-pass.
    """
    target_fqn: str
    writer_pipelines: list[str] = Field(default_factory=list)
    pattern: str  # disjoint | lifecycle | update_back | unknown
    rationale: str | None = None


class Inventory(BaseModel):
    tables: list[Table] = Field(default_factory=list)
    procedures: list[Procedure] = Field(default_factory=list)
    pipelines: list[ETLPipeline] = Field(default_factory=list)
    orphan_runs: list[OrphanRun] = Field(default_factory=list)  # audit-log entries with no XML
    flags: list[InventoryFlag] = Field(default_factory=list)

    # Migration-program signals
    decommission: list[DecommissionAssessment] = Field(default_factory=list)
    sequencing: list[MigrationWave] = Field(default_factory=list)
    rules: list[BusinessRule] = Field(default_factory=list)
    multi_writers: list[MultiWriterTarget] = Field(default_factory=list)
    deliveries: list[DeliverySpec] = Field(default_factory=list)
    undocumented_outputs: list[str] = Field(default_factory=list)


class LineageEdge(BaseModel):
    source_fqn: str
    source_column: str | None = None
    target_fqn: str
    target_column: str | None = None
    operation: str  # SELECT / VIEW / CTAS / extract / transform / join / aggregate / load / pipeline
    transform: str | None = None
    origin_object: str | None = None
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


class PipelineUsage(BaseModel):
    pipeline_name: str
    runs_total: int = 0
    runs_success: int = 0
    runs_failed: int = 0
    last_run: str | None = None
    success_rate: float = 0.0
    output_csv: str | None = None
    has_definition: bool = True
    csv_exists: bool = False
    ran_without_logging: bool = False  # CSV produced but audit log silent


class UsageReport(BaseModel):
    objects: list[ObjectUsage] = Field(default_factory=list)
    hot_tables: list[str] = Field(default_factory=list)
    write_only_orphans: list[str] = Field(default_factory=list)
    dead_objects: list[str] = Field(default_factory=list)
    reporting_reachable_sources: list[str] = Field(default_factory=list)
    reporting_unreachable_sources: list[str] = Field(default_factory=list)
    pipelines: list[PipelineUsage] = Field(default_factory=list)
    never_run_pipelines: list[str] = Field(default_factory=list)
    runs_without_definition: list[str] = Field(default_factory=list)


class Finding(BaseModel):
    severity: str
    title: str
    detail: str
    object_fqns: list[str] = Field(default_factory=list)
    recommendation: str | None = None


class ExecutiveSummary(BaseModel):
    headline: str
    bullets: list[str] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)
    metrics: dict[str, str | int | float] = Field(default_factory=dict)


class RunResults(BaseModel):
    inventory: Inventory | None = None
    lineage: LineageGraph | None = None
    usage: UsageReport | None = None
    summary: ExecutiveSummary | None = None


Inventory.model_rebuild()
ExecutiveSummary.model_rebuild()
