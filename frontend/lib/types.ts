// Mirrors backend/app/models — keep in sync with Pydantic.

export type AgentName = "inventory" | "lineage" | "usage" | "summary";
export type AgentStatus = "pending" | "running" | "completed" | "failed";
export type RunStatus = "pending" | "running" | "completed" | "failed";
export type Layer = "raw" | "staging" | "integration" | "reporting" | "unknown";
export type Domain =
  | "member" | "account" | "product" | "adviser"
  | "transaction" | "holding" | "fee" | "reference" | "audit" | "other";
export type Severity = "info" | "warn" | "critical";

export interface BucketPreview {
  bucket: string;
  prefix: string;
  ddl_files: number;
  dictionary_files: number;
  awr_files: number;
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
  bucket: string;
  prefix: string;
  label: string | null;
  status: RunStatus;
  created_at: string;
  updated_at: string;
  agents: AgentRunState[];
}

export interface RunRequest {
  bucket: string;
  prefix?: string;
  agents?: AgentName[];
  label?: string;
}

export interface Column {
  name: string;
  data_type: string;
  nullable: boolean;
  is_pk: boolean;
  is_fk: boolean;
  fk_target?: string | null;
  comment?: string | null;
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
}

export interface InventoryFlag {
  severity: Severity;
  title: string;
  detail: string;
  object_fqn?: string | null;
}

export interface Inventory {
  tables: Table[];
  procedures: Procedure[];
  flags: InventoryFlag[];
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

export interface UsageReport {
  objects: ObjectUsage[];
  hot_tables: string[];
  write_only_orphans: string[];
  dead_objects: string[];
  reporting_reachable_sources: string[];
  reporting_unreachable_sources: string[];
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
