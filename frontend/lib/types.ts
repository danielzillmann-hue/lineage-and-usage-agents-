// Mirrors backend/app/models — keep in sync with Pydantic.

export type AgentName = "inventory" | "lineage" | "usage" | "summary" | "transform" | "orchestration";
export type AgentStatus = "pending" | "running" | "completed" | "failed";
export type RunStatus = "pending" | "running" | "completed" | "failed";
export type Layer = "raw" | "staging" | "integration" | "reporting" | "output" | "unknown";
export type Domain =
  | "member" | "account" | "product" | "adviser"
  | "transaction" | "holding" | "fee" | "reference"
  | "audit" | "investment" | "pipeline" | "other";
export type Severity = "info" | "warn" | "critical";

export interface OracleConnection {
  host: string;
  port: number;
  service: string;
  user: string;
  password: string;
}

export interface DemoDefaults {
  oracle: OracleConnection;
  bucket: string;
  prefix: string;
  outputs_prefix: string;
  documents_prefix: string;
}

export interface DeliverySpec {
  csv_name: string;
  kind: "internal" | "external" | "unknown" | string;
  destination?: string | null;
  protocol?: string | null;
  endpoint?: string | null;
  auth?: string | null;
  frequency?: string | null;
  details?: string | null;
  source_doc?: string | null;
  confidence: number;
}

export interface TestConnectionResponse {
  ok: boolean;
  schema_name?: string | null;
  table_count?: number | null;
  pipeline_runs?: number | null;
  error?: string | null;
}

export interface BucketPreview {
  bucket: string;
  prefix: string;
  ddl_files: number;
  dictionary_files: number;
  awr_files: number;
  etl_files: number;
  output_files: number;
  other_files: number;
  total_bytes: number;
  sample_paths: string[];
}

export interface AgentRunState {
  name: AgentName;
  status: AgentStatus;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  artifact_path: string | null;
}

export interface Run {
  id: string;
  bucket?: string | null;
  prefix?: string;
  oracle_dsn?: string | null;
  label: string | null;
  status: RunStatus;
  created_at: string;
  updated_at: string;
  agents: AgentRunState[];
}

export interface RunRequest {
  oracle?: OracleConnection;
  bucket?: string;
  prefix?: string;
  outputs_prefix?: string;
  documents_prefix?: string;
  agents?: AgentName[];
  label?: string;
}

export type Sensitivity = "pii" | "financial" | "tax" | "internal" | "public";
export type ColumnNature = "data" | "key" | "audit" | "calculated" | "reference";

export interface Column {
  name: string;
  data_type: string;
  nullable: boolean;
  is_pk: boolean;
  is_fk: boolean;
  fk_target?: string | null;
  comment?: string | null;
  sensitivity: Sensitivity;
  nature: ColumnNature;
  annotation_notes?: string | null;
  inherited_sensitivity_from?: string[];
  user_overridden?: boolean;
}

export interface ColumnAnnotationUpdate {
  table_fqn: string;
  column_name: string;
  sensitivity?: Sensitivity;
  nature?: ColumnNature;
  annotation_notes?: string | null;
}

export interface Table {
  schema_name: string;
  name: string;
  kind: string;
  columns: Column[];
  row_count?: number | null;
  bytes?: number | null;
  layer: Layer;
  domain: Domain;
  comment?: string | null;
  source_text?: string | null;
}

export interface Procedure {
  schema_name: string;
  name: string;
  kind: string;
  source: string;
  last_compiled?: string | null;
}

export interface InventoryFlag {
  severity: Severity;
  title: string;
  detail: string;
  object_fqn?: string | null;
}

export interface PipelineRunStats {
  runs_total: number;
  runs_success: number;
  runs_failed: number;
  first_run?: string | null;
  last_run?: string | null;
}

export interface PipelineStep {
  id: string;
  kind: string;
  inputs: string[];
  columns: string[];
  operations: string[];
  source_tables: string[];
  source_query?: string | null;
  output_path?: string | null;
}

export interface ETLPipeline {
  name: string;
  file: string;
  output_csv?: string | null;
  source_tables: string[];
  steps: PipelineStep[];
  column_count: number;
  runs?: PipelineRunStats | null;
  connection_host?: string | null;
  connection_service?: string | null;
  csv_exists: boolean;
  csv_last_modified?: string | null;
  csv_size_bytes?: number | null;
}

export interface OrphanRun {
  pipeline_name: string;
  csv_generated?: string | null;
  runs: PipelineRunStats;
}

export interface DecommissionAssessment {
  object_fqn: string;
  score: number;
  verdict: "safe" | "review" | "blocked";
  last_read?: string | null;
  days_since_last_read?: number | null;
  downstream_pipeline_count: number;
  downstream_view_count: number;
  archive_eligible: boolean;
  drivers: string[];
}

export interface MigrationWave {
  wave: number;
  description: string;
  table_fqns: string[];
  pipeline_names: string[];
}

export interface BusinessRule {
  rule_type: "enum" | "range" | "not_null" | "calculated" | "filter" | "constraint";
  source_object: string;
  column?: string | null;
  expression: string;
  natural_language: string;
  confidence: number;
}

export interface MultiWriterTarget {
  target_fqn: string;
  writer_pipelines: string[];
  pattern: "disjoint" | "lifecycle" | "update_back" | "unknown";
  rationale?: string | null;
}

export interface Inventory {
  tables: Table[];
  procedures: Procedure[];
  pipelines: ETLPipeline[];
  orphan_runs: OrphanRun[];
  flags: InventoryFlag[];
  decommission: DecommissionAssessment[];
  sequencing: MigrationWave[];
  rules: BusinessRule[];
  multi_writers: MultiWriterTarget[];
  deliveries?: DeliverySpec[];
  undocumented_outputs?: string[];
}

export interface LineageEdge {
  source_fqn: string;
  source_column?: string | null;
  target_fqn: string;
  target_column?: string | null;
  operation: string;
  transform?: string | null;
  origin_object?: string | null;
  confidence: number;
}

export interface LineageGraph {
  edges: LineageEdge[];
  unresolved: string[];
}

export interface ObjectUsage {
  fqn: string;
  read_count: number;
  write_count: number;
  distinct_users: number;
  last_read?: string | null;
  last_write?: string | null;
}

export interface PipelineUsage {
  pipeline_name: string;
  runs_total: number;
  runs_success: number;
  runs_failed: number;
  last_run?: string | null;
  success_rate: number;
  output_csv?: string | null;
  has_definition: boolean;
  csv_exists: boolean;
  ran_without_logging: boolean;
}

export interface UsageReport {
  objects: ObjectUsage[];
  hot_tables: string[];
  write_only_orphans: string[];
  dead_objects: string[];
  reporting_reachable_sources: string[];
  reporting_unreachable_sources: string[];
  pipelines: PipelineUsage[];
  never_run_pipelines: string[];
  runs_without_definition: string[];
}

export interface Finding {
  severity: Severity;
  title: string;
  detail: string;
  object_fqns: string[];
  recommendation?: string | null;
}

export interface ExecutiveSummary {
  headline: string;
  bullets: string[];
  findings: Finding[];
  metrics: Record<string, string | number>;
}

export interface RunResults {
  inventory?: Inventory;
  lineage?: LineageGraph;
  usage?: UsageReport;
  summary?: ExecutiveSummary;
}

export interface StreamEvent {
  event: "status" | "log" | "thinking" | "result" | "error" | "done";
  agent?: AgentName | null;
  message?: string | null;
  data?: Record<string, unknown> | null;
  ts: string;
}
