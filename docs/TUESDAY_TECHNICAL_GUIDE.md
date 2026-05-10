# Technical Deep Dive — Lineage & Usage Agents

**A study guide for Daniel's demo on Tuesday 2026-05-12.**

This is NOT the marketing overview (see `PRODUCT_OVERVIEW.md` for that). This is a technical reference so you can confidently answer questions about how the system actually works, where code lives, and what it can and can't do.

---

## Table of Contents

1. [End-to-End Architecture](#end-to-end-architecture)
2. [The Insignia ETL XML Parser](#the-insignia-etl-xml-parser)
3. [The Intermediate Representation (IR)](#the-intermediate-representation-ir)
4. [SQL Emitter & Dataform Generation](#sql-emitter--dataform-generation)
5. [Oracle → BigQuery Dialect Translation](#oracle--bigquery-dialect-translation)
6. [The Six Agents](#the-six-agents)
7. [Verification: Post-Migration Validation](#verification-post-migration-validation)
8. [Known Limitations & What NOT to Claim](#known-limitations--what-not-to-claim)
9. [Q&A Prep — Likely Demo Questions](#qa-prep--likely-demo-questions)

---

## End-to-End Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  User Interface (Next.js, Cloud Run)                         │
│  New Analysis / Live Run / Results (8 tabs)                  │
│                                                              │
└────────────────────────────┬─────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  FastAPI Backend │ (Python 3.12, Cloud Run)
                    │ (app/routes)     │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        │                    │                    │
        ▼                    ▼                    ▼
    ┌─────────┐      ┌──────────────┐    ┌─────────────┐
    │ Agents  │      │   Services   │    │  Parsers &  │
    │ (6x)    │      │ (Oracle, GCS)│    │  Transform  │
    │         │      │              │    │   Library   │
    └────┬────┘      └──────┬───────┘    └──────┬──────┘
         │                  │                    │
         │                  │                    │
         ├──────┬───────────┤                    │
         │      │           │                    │
      1. INV 2. LIN 3. USE  4. SUM              │
         │      │           │                    │
         │      └─────┬─────┤                    │
         │            │     │                    │
         │            │  5. TRANS                │
         │            │     ├──────────┬─────────┘
         │            │     │          │
         │            │     ▼          ▼
         │            │  ┌──────────────────────────────┐
         │            │  │ transformation-core library  │
         │            │  │ (Parser → IR → SQL emitter)  │
         │            │  │ + sql_helpers (Oracle→BQ)    │
         │            │  │ + dataform_project (assembly)│
         │            │  └──────┬─────────────────────┬─┘
         │            │         │                     │
         │            │         ▼                     ▼
         │            │    ┌──────────┐        ┌─────────────┐
         │            │    │ SQLX src │        │ Config YAML │
         │            │    │ (*.sqlx) │        │ + metadata  │
         │            │    └──────────┘        └─────────────┘
         │            │
         │         6. ORCH
         │            │
         │            ▼
         │    ┌──────────────────┐
         │    │ GitHub Workflow  │
         │    │ (dataform.yaml)  │
         │    └──────────────────┘
         │
         ▼
    ┌──────────────────────────┐
    │  GCS Results Bucket      │
    │  gs://results_bucket/    │
    │  runs/<run_id>/          │
    │    inventory.json        │
    │    lineage.json          │
    │    usage.json            │
    │    summary.json          │
    │    transform/            │
    │    verification/         │
    └──────────────────────────┘
```

**Flow summary:**
1. User provides Oracle connection + GCS bucket with pipeline XMLs → clicks Run
2. **Inventory agent** — live Oracle introspection + XML parsing
3. **Lineage agent** — column-level edges from parsed pipelines + FK graph
4. **Usage agent** — pipeline run history from audit logs
5. **Summary agent** — Gemini 2.5 synthesises findings
6. **Transformation agent** — reads XMLs, calls the transformation-core library, emits SQLX + project config
7. **Orchestration agent** — emits GitHub Actions YAML
8. **Verification agent** (optional, runs after BQ pipeline execution) — row-count + aggregate comparison

Each agent produces JSON artifacts. The frontend queries `/runs/<run_id>/inventory`, `/runs/<run_id>/lineage`, etc.

---

## The Insignia ETL XML Parser

**File:** `backend/app/parsers/etl_xml.py` (~575 lines)

The parser is a deterministic, column-level-aware XML reader. It does NOT invoke an LLM.

### Supported XML Tags

```xml
<pipeline name="...">
  <steps>
    <!-- 1. Extract: SELECT from Oracle table -->
    <extract id="ext_users">
      <query>SELECT user_id, name, email FROM users WHERE status = 'ACTIVE'</query>
    </extract>

    <!-- 2. Extract CSV: read external CSV file -->
    <extract_csv id="csv_tax" path="s3://data/tax_brackets.csv"/>

    <!-- 3. Transform: apply in-memory operations -->
    <transform id="txn_enrich" input="ext_users">
      <calculate_age dob_column="birth_date" result_column="age"/>
      <concat cols="first_name,last_name" result_column="full_name" separator=" "/>
      <drop columns="internal_id,temp_flag"/>
      <math operation="add" col1="salary" val2="bonus" result_column="total_comp"/>
      <filter_text column="email" contains="@company.com"/>
      <calculate_category column="age" low="18" high="65" result_column="working_age"/>
      <rename old_col="orig_col" new_col="final_col"/>
      <simulate_performance/>  <!-- Opaque: adds columns, no spec -->
      <filter .../>  <!-- Row-level filtering, doesn't change columns -->
      <sort .../>    <!-- Row-level sorting, doesn't change columns -->
    </transform>

    <!-- 4. Join: merge two streams -->
    <join id="j_users_org" left="ext_users" right="csv_orgs" on="org_id" how="left"/>

    <!-- 5. Aggregate: GROUP BY + aggregates -->
    <aggregate id="agg_by_dept" input="j_users_org" group_by="department,role">
      <sum column="total_comp" result_column="dept_payroll"/>
      <count result_column="headcount"/>
    </aggregate>

    <!-- 6. Load: write to CSV or Oracle table -->
    <load id="load_csv" input="agg_by_dept" type="csv" path="gs://bucket/output/dept_payroll.csv"/>
    <load id="load_oracle" input="agg_by_dept" type="oracle" table="stg_payroll_by_dept"/>

    <!-- 7. Execute SQL: direct SQL statement (INSERT/UPDATE/DELETE/MERGE/TRUNCATE) -->
    <execute_sql id="sql_update_flags">
      <query>UPDATE ${ref('stg_payroll')} SET processed_flag = 1 WHERE run_date = CURRENT_DATE()</query>
    </execute_sql>
  </steps>
</pipeline>
```

### Data Structures

**StepNode** (`etl_xml.py:36-56`):
```python
@dataclass
class StepNode:
    id: str                          # e.g. "ext_users", "j_accounts_orgs"
    kind: str                        # extract | extract_csv | transform | join | aggregate | load | execute_sql
    inputs: list[str]                # upstream step IDs this depends on
    columns: list[str]               # output column names (ordered)
    column_sources: dict[str, list]  # column_name → [(upstream_step, upstream_col), ...]
    source_tables: list[str]         # for extract/execute_sql: Oracle table names
    source_query: str | None         # raw SQL from <extract> or <execute_sql>
    output_path: str | None          # for load type="csv"
    output_table: str | None         # for load type="oracle"
    external_path: str | None        # for extract_csv
    sql_kind: str | None             # INSERT | UPDATE | DELETE | TRUNCATE | MERGE
    operations: list[str]            # human-readable op descriptions
```

**Pipeline** (`etl_xml.py:59-66`):
```python
@dataclass
class Pipeline:
    name: str                   # pipeline name
    file: str                   # source XML filename
    connection: dict            # <connection> attributes (no password)
    steps: dict[str, StepNode]  # step_id → node
    load_step_id: str | None    # ID of the primary load step
```

### Column Lineage Tracking

For every column in every step, `column_sources` maps it to upstream provenance:
- **Extract:** `column_sources['USER_ID'] = [('USERS', 'USER_ID')]` (Oracle table, original column)
- **Transform (concat):** `column_sources['FULL_NAME'] = [('ext_users', 'FIRST_NAME'), ('ext_users', 'LAST_NAME')]`
- **Join:** Both left and right column sources are merged (deduped by name)
- **Aggregate:** Only group-by columns + aggregate results are in output

### Key Limitations

1. **sqlglot parsing** (`_parse_select_columns`, line 398-418):
   - Extracts column names + table names from SELECT statements
   - If the SQL has complex subqueries or window functions, extraction may be incomplete
   - Column-to-table mapping is best-effort (works for single-table SELECTs; for JOINs we attribute to first table)

2. **`simulate_performance` is opaque** (line 207-208):
   - It's logged as an operation but we don't know what columns it adds
   - Downstream steps referencing those columns will have missing source provenance

3. **Complex WHERE clauses are captured but not analyzed** (line 203-206):
   - Row filtering (WHERE, FILTER) doesn't change the column set, so we just record the operation string

4. **External CSV schema is inferred, not declared** (line 300-308):
   - `extract_csv` nodes have empty column lists initially
   - Pre-pass scanning (`_collect_csv_step_columns`, line 172) infers columns from how they're used downstream (JOIN ON, math col1/col2, etc.)
   - If a CSV column is never explicitly referenced, it won't appear in the IR

### How to Read the Code

- **Entry point:** `parse_pipeline(xml_text, filename)` — line 85
- **Per-step handlers:** `_parse_extract` (121), `_parse_transform` (138), `_parse_join` (222), `_parse_aggregate` (248), `_parse_load` (270), `_parse_execute_sql` (311), `_parse_extract_csv` (300)
- **SQL classification:** `_classify_sql` (332) — returns (kind, target_table, source_tables)
- **Lineage expansion:** `to_lineage_edges(pl)` (424) — walks all steps and emits `ColumnEdge` records

---

## The Intermediate Representation (IR)

**File:** `transformation-core/transformation_core/ir.py` (~262 lines)

The IR decouples Insignia XML from BigQuery SQL. It's immutable after construction (built by the transformer, read-only by the emitter).

### Why an IR?

1. **Source agnostic** — same IR could be populated from Informatica, SSIS, Talend, etc.
2. **Deterministic** — same input always produces same IR (no LLM drift)
3. **Explicit column lineage** — every column knows its source node and expression
4. **Testable** — can validate IR structure independently of SQL rendering

### Node Types (11 total)

Each maps to one CTE in the final SQL:

| Node Type | Purpose | Key Fields |
|-----------|---------|-----------|
| **SourceNode** | FROM table (possibly with custom SQL) | `table_ref`, `custom_sql`, `where`, `joined_tables`, `join_conditions` |
| **ExpressionNode** | SELECT computed columns FROM upstream | `upstream`, `columns`, `pass_upstream` |
| **FilterNode** | WHERE condition | `upstream`, `condition` |
| **LookupNode** | LEFT JOIN dimension table | `upstream`, `lookup_table`, `join_condition`, `join_type` |
| **JoinerNode** | JOIN two streams (Informatica Joiner) | `master_upstream`, `detail_upstream`, `join_type`, `join_condition` |
| **AggregatorNode** | GROUP BY + aggregate functions | `upstream`, `group_by` list, `columns` |
| **UnionNode** | UNION ALL multiple streams | `upstreams` list, `column_mappings` |
| **NormalizerNode** | CROSS JOIN UNNEST (row explosion) | `upstream`, `occurs`, `flat_columns`, `group_columns` |
| **RouterNode** | WHERE-based stream split (becomes WHERE in final SELECT) | `upstream`, `groups` list of RouterGroup |
| **SequenceNode** | ROW_NUMBER() surrogate key | (placeholder in ir.py, not yet heavily used) |
| **TargetNode** | Final SELECT (not a separate node; metadata on DataflowGraph) | (metadata only) |

### ColumnDef — Column Lineage

```python
@dataclass(frozen=True)
class ColumnDef:
    name: str                 # output column name
    expression: str           # SQL expression (e.g., "src.user_id", "CONCAT(first, last)")
    source_node: str | None   # upstream node CTE name
    source_column: str | None # upstream column name
    is_passthrough: bool      # True if unchanged from upstream
    bq_data_type: str         # "INT64", "STRING", "DATETIME", "" if unknown
```

**Example:**
```python
# For a projection "SELECT user_id, CONCAT(first_name, ' ', last_name) AS full_name FROM users":
ColumnDef(name="user_id", expression="src.user_id", source_node="cte_src", source_column="user_id", is_passthrough=True, bq_data_type="INT64")
ColumnDef(name="full_name", expression="CONCAT(src.first_name, ' ', src.last_name)", source_node="cte_src", source_column=None, is_passthrough=False, bq_data_type="STRING")
```

### DataflowGraph — The Complete IR

```python
@dataclass
class DataflowGraph:
    mapping_name: str                # e.g., "regulatory_audit_compliance"
    nodes: list[Node]                # topologically sorted
    target: TargetMapping            # primary output (name, schema, columns)
    all_targets: list[TargetMapping] # for multi-target pipelines
    variables: dict[str, str]        # variables from XML (unused for now)
    table_type: str                  # "table", "view", "incremental", "operations"
    schema: str                      # BigQuery dataset (e.g., "CDWH_Store")
    tags: list[str]                  # Dataform layer tags
    ref_name_map: dict               # snake_case → display name for ${ref()}
```

**Methods:**
- `get_node(cte_name)` → find node by CTE name
- `get_output_columns(cte_name)` → get ColumnDef list for a node
- `get_final_cte()` → name of the last (output) CTE

### How the IR is Built

**File:** `backend/app/transformer/insignia_to_ir.py` (~500+ lines)

Entry point: `parse(xml_text, filename)` → `TransformResult`

Flow for a typical pipeline:
1. **Pre-passes** — collect columns added by post-load UPDATEs, infer CSV columns from downstream usage
2. **Step processing** — walk `<steps>` in order, emit IR nodes:
   - `<extract>` → call `parse_select()` from sql_helpers, append resulting SourceNode(s)
   - `<extract_csv>` → emit SourceNode with inferred columns
   - `<transform>` → emit ExpressionNode
   - `<join>` → merge upstream column lists, emit JoinerNode
   - `<aggregate>` → emit AggregatorNode
   - `<load>` → commit current stage as a primary DataflowGraph
   - `<execute_sql>` → either roll into primary (if INSERT...SELECT) or emit as operations script (if UPDATE/DELETE)
3. **Stage completion** → `_build_primary_graphs()` wraps each completed stage in a DataflowGraph

---

## SQL Emitter & Dataform Generation

### SQL Emitter

**File:** `transformation-core/transformation_core/sql_generator.py` (~1200 lines)

**Entry point:** `SQLGenerator().generate(graph: DataflowGraph) → str`

The emitter walks the IR nodes in topological order and produces a WITH statement (CTE chain).

**Algorithm:**
1. Build CTE column sets for source qualification (so expressions can reference `src.column`)
2. Walk graph.nodes, call `_emit_node(node)` for each:
   - Each returns a CTE definition string like `cte_source AS (SELECT ...)`
3. Collect all CTEs and strip unreachable branches (dead code elimination)
4. Emit final SELECT from the last CTE (the output)

**Example output:**
```sql
WITH
cte_users_src AS (
  SELECT user_id, first_name, last_name, dept_id FROM ${ref('USERS')}
),

cte_enrich_expr AS (
  SELECT *, CONCAT(first_name, ' ', last_name) AS full_name FROM cte_users_src
),

cte_join_depts AS (
  SELECT detail.*, master.dept_name FROM cte_enrich_expr AS detail
  LEFT JOIN ${ref('DEPARTMENTS')} AS master ON detail.dept_id = master.dept_id
),

cte_agg_final AS (
  SELECT dept_id, dept_name, COUNT(*) AS headcount FROM cte_join_depts
  GROUP BY dept_id, dept_name
)

SELECT * FROM cte_agg_final
```

**Key behaviors:**
- Column aliasing is auto-escaped if it's a BigQuery reserved word (backtick-quoted, line 40-54)
- Merge lookups into single CTEs when possible (`_pending_lookups`, lines 69-70)
- `${ref('table_name')}` is used for all Dataform references (resolved at compile time)
- Carriage returns are stripped (`replace('\r', '')`) because Informatica XML expressions sometimes leak them

### Dataform Project Assembly

**File:** `backend/app/transformer/dataform_project.py` (~770 lines)

**Entry point:** `assemble_project(generated: list[GeneratedFile], config) → AssembledProject`

What gets emitted:
1. **Primary SQLX files** — one per pipeline (`definitions/core_account_summary.sqlx`, etc.)
2. **Operations SQLX files** — post-load UPDATEs/DELETEs as sibling operations
3. **Source declarations** — one per inferred source table (`definitions/sources/users.sqlx`, etc.)
4. **workflow_settings.yaml** — Dataform config (project, location, dataset, core version)
5. **package.json** — npm dependencies (@dataform/core)
6. **bootstrap/raw_schema.sql** — DDL for raw dataset (optional)
7. **bootstrap/replication_setup.md** — how-to guide for loading raw tables
8. **README.md** — project overview + validation summary

**Source declarations:**

For every table referenced via `${ref('X')}` minus every table the project itself produces, we generate:
```sqlx
config {
  type: "declaration",
  name: "USERS",
  columns: {
    user_id: "INT64",
    name: "STRING",
    ...
  }
}

SELECT * FROM \`project.raw_dataset.users\`
```

If the table is a view and we have the original Oracle SQL, it becomes:
```sqlx
config {
  type: "view",
  name: "V_ACTIVE_USERS",
  columns: { ... }
}

SELECT ... FROM ${ref('USERS')} WHERE status = 'ACTIVE'
```

CSV stubs (external files referenced by pipelines but not found in Oracle) are rendered as empty declarations so Dataform doesn't fail on unknown tables.

**Validation pass:**

After assembly, `validate_project()` checks:
- All `${ref()}` calls resolve (no undefined tables)
- No cycles in the DAG
- SQL parses without error
- Column names don't collide
- Each file gets a confidence score (see Verification section)

---

## Oracle → BigQuery Dialect Translation

**File:** `backend/app/transformer/sql_helpers.py` (~560 lines)

The translator patches the sqlglot AST before rendering to BigQuery. This handles cases sqlglot's built-in dialect map doesn't cover.

### Patches Applied

**1. SYSDATE → CURRENT_DATETIME()** (line 440-442)

Oracle's SYSDATE is a wall-clock DATE (no timezone). sqlglot maps it to CURRENT_TIMESTAMP() (TIMESTAMP), which then fails when compared to DATETIME columns. Fix: detect `sysdate=True` and replace with `exp.CurrentDatetime()`.

**2. TRUNC date handling** (line 443-465)

| Oracle | BigQuery |
|--------|----------|
| `TRUNC(d)` | `DATETIME_TRUNC(d, DAY)` |
| `TRUNC(d, 'MM')` | `DATETIME_TRUNC(d, MONTH)` |
| `TRUNC(d, 'YYYY')` | `DATETIME_TRUNC(d, YEAR)` |

The AST walk finds `exp.Anonymous` nodes named "TRUNC" and rewrites them. Date-part literals ('MM', 'YYYY') are normalized to BigQuery equivalents (MONTH, YEAR).

**3. Date arithmetic** (line 467-496)

Oracle: `SYSDATE - 365` means "365 days ago"
BigQuery: `DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 365 DAY)` or `DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)` (depends on type)

The patch detects `<date_expr> - <int_literal>` and rewrites to `*_SUB(date_expr, INTERVAL N DAY)`, choosing the function based on the LHS type.

**Type heuristics** (`_date_sub_fn_for`, line 499-526):
- `CurrentDatetime()` → `DATETIME_SUB`
- `CurrentTimestamp()` → `TIMESTAMP_SUB`
- `CurrentDate()` → `DATE_SUB`
- Column name contains "timestamp" → `TIMESTAMP_SUB`
- Default → `DATETIME_SUB` (broadest fit for typical ETL columns)

### Not Handled

These patterns require manual review:
- **Nested TRUNC** — `TRUNC(TRUNC(d, 'MM'), 'YYYY')` may not translate correctly
- **Oracle-specific functions** — `ADD_MONTHS`, `LAST_DAY`, `NEXT_DAY` (would need explicit mapping)
- **Timezone conversions** — `AT TIME ZONE`, `FROM_TZ`, `NEW_TIME` (BigQuery's AT TIME ZONE syntax differs)
- **Number formatting** — `TO_CHAR(x, '999.99')` (no BigQuery equivalent; needs manual TO_STRING)
- **Row-level security predicates** — Oracle's VPD (Virtual Private Database) has no BigQuery equivalent

### Post-Render Text Patches

**Function:** `_patch_bigquery_text()` (line 547-565)

After sqlglot renders to BigQuery SQL, regex patterns fix edge cases:
- `DATE_TRUNC(..., MM)` → `DATE_TRUNC(..., MONTH)` (unquoted date parts)
- `EXTRACT(..., MI)` → `EXTRACT(MINUTE FROM ...)` (if sqlglot missed it)

### Usage in the Pipeline

1. **Extract step**: `parse_select()` calls `_patch_oracle_dialect_for_bigquery()` up-front (line 64), so all subsequent `.sql(dialect="bigquery")` calls emit patched SQL
2. **DML operations**: `render_dml_for_bigquery()` (line 356) applies patches, transpiles, then rewrites table refs to `${ref()}`

---

## The Six Agents

Each agent is a coroutine that reads the prior agents' results and emits progress events.

### 1. Inventory Agent

**File:** `backend/app/agents/inventory.py` (~400 lines)

**Produces:** `Inventory` model with tables, views, columns, FKs, stored procedures, ETL pipelines, pipeline run stats.

**Flow:**
1. **Oracle introspection** — live query to `user_tables`, `user_tab_columns`, `user_constraints` (if req.oracle is set)
2. **Bucket scan** — read ETL XMLs, DDL files, dictionary files, CSV outputs from GCS
3. **XML parsing** — call `parse_pipeline()` on each XML, populate `inv.pipelines`
4. **CSV parsing** — each output CSV becomes a Table at Layer.OUTPUT with columns inferred from CSV header
5. **Cross-reference** — match audit-log pipeline runs to XML-defined pipelines by name
6. **Annotation** — call `annotate_columns()` to add PII classification via Gemini 2.5 Flash

**Data model:**
```python
@dataclass
class Inventory:
    tables: list[Table]          # Oracle + CSV tables
    views: list[Table]           # Oracle views
    procedures: list[Procedure]  # PL/SQL procedures
    pipelines: list[ETLPipeline] # Parsed XMLs
    foreign_keys: dict           # table → [(col, target_table.col), ...]
```

### 2. Lineage Agent

**File:** `backend/app/agents/lineage.py` (~250 lines)

**Produces:** `LineageGraph` — column-level edges from sources → transformations → outputs.

**Flow:**
1. **XML pipeline edges** — re-read the XMLs, call `to_lineage_edges()` for each, emit ColumnEdge records
2. **FK graph edges** — from inventory, add edges for foreign-key relationships
3. **View source SQL tracer** — for each Oracle view, parse the source SQL (sqlglot) and trace columns
4. **Procedure tracer** (fallback) — if XML or views don't cover it, call Gemini 2.5 Pro with the PL/SQL code

**Lineage edge format:**
```python
@dataclass
class LineageEdge:
    source_fqn: str          # "schema.table" or "pipeline.step"
    source_column: str
    target_fqn: str
    target_column: str
    operation: str           # "extract", "join", "aggregate", "fk", etc.
    transform: str | None    # human-readable detail
    origin_object: str       # which pipeline or procedure
    confidence: float        # 1.0 for XML (deterministic), <1.0 for LLM
```

**Frontend uses this to:**
- Draw the lineage graph (nodes = tables/steps, edges = column flows)
- Support "focus" filtering (show only paths through a selected object)
- Expand pipeline internals (toggle to see the step DAG)

### 3. Usage Agent

**File:** `backend/app/agents/usage.py` (~250 lines)

**Produces:** `UsageData` — pipeline run stats, hot tables, dead objects.

**Flow:**
1. **Audit log queries** — live query Oracle audit tables for pipeline execution history
2. **Success rate calculation** — count runs where exit code = 0
3. **Hot table detection** — aggregate table reads/writes across all pipelines
4. **Dead object detection** — tables/views with no reads and no writes in past N days
5. **Undocumented execution** — audit-log entries with no matching XML-defined pipeline

**Output:**
```python
@dataclass
class UsageData:
    pipelines: list[PipelineRunStats]  # per-pipeline: run_count, success_rate, last_run
    hot_tables: list[HotTable]         # frequently accessed
    dead_objects: list[DeadObject]     # not accessed in 90 days
    undocumented_runs: list[str]       # audit log entries with no XML
```

### 4. Summary Agent

**File:** `backend/app/agents/summary.py` (~150 lines)

**Produces:** `ExecutiveSummary` — headline, bullets, findings (critical/warn/info).

**LLM:** Gemini 2.5 Pro (system prompt at line 19-54)

**Input:** JSON digest of inventory + lineage + usage results

**Output:**
```json
{
  "headline": "87 tables, 14 pipelines, 98% success rate — 3 critical findings",
  "bullets": [
    "Pipeline LOAD_FACT_SALES runs daily with 100% success for 6 months",
    "View V_ACTIVE_USERS has no consumers — decommission candidate",
    "TEMP_STAGING table is missing 3 columns that downstream pipelines write"
  ],
  "metrics": {
    "total_tables": 87,
    "total_pipelines": 14,
    "successful_runs": "98%"
  },
  "findings": [
    {
      "severity": "critical",
      "title": "Pipeline runs without logging",
      "detail": "EXTRACT_VENDOR_MASTER executes 4× daily but audit log shows 0 entries",
      "object_fqns": ["SCHEMA.EXTRACT_VENDOR_MASTER"],
      "recommendation": "Enable audit logging; validate data quality before go-live"
    }
  ]
}
```

### 5. Transformation Agent

**File:** `backend/app/agents/transformation.py` (~120 lines)

**Produces:** `TransformationResult` — Dataform SQLX project files.

**Flow:**
1. **XML read** — fetch all ETL XMLs from the source bucket
2. **View + metadata collection** — pull Oracle view source SQL and table PKs/non-nulls from inventory
3. **Project generation** — call `generate_project()` (entry point to transformation-core library)
4. **Assembly** — calls `assemble_project()` which generates source declarations, workflow YAML, README
5. **Upload** — writes all files to `gs://results_bucket/runs/<run_id>/transform/`
6. **Manifest** — emit `_manifest.json` listing all produced tables and sources

**Output files:**
```
definitions/
  core_account_summary.sqlx         [primary]
  fact_regulatory_audit.sqlx        [primary]
  fact_regulatory_audit_anomalies.sqlx  [operations]
  sources/
    users.sqlx                       [declaration]
    departments.sqlx                 [view]
    ...

bootstrap/
  raw_schema.sql                    [DDL for raw dataset]
  replication_setup.md

workflow_settings.yaml
package.json
README.md
_manifest.json
```

### 6. Orchestration Agent

**File:** `backend/app/agents/orchestration.py` (~100 lines)

**Produces:** `.github/workflows/dataform.yaml` — CI/CD workflow.

**Workflow template:**
```yaml
name: Dataform Compile & Deploy

on:
  push:
    branches: [main]
  schedule:
    - cron: '0 2 * * *'  # 2 AM UTC daily

jobs:
  compile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: npm ci
      - run: npx dataform compile
  
  run:
    needs: compile
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}
      - run: npx dataform run --project=${{ env.GCP_PROJECT }}
```

---

## Verification: Post-Migration Validation

**File:** `backend/app/agents/verification.py` (~600 lines)

Verification is **optional** and runs **after** the Transformation agent and after the user has executed the Dataform project on BigQuery.

### Classification

Every migrated table gets a classification:

| Classification | Meaning | Verification |
|---|---|---|
| `oracle_origin` | Present in Oracle inventory | Query Oracle + BQ side-by-side, compare aggregates |
| `view_origin` | Oracle view (snapshot replicated) | Same as oracle_origin |
| `csv_stub` | Referenced by ETL but not in Oracle | Skip (no Oracle source to compare) |
| `bq_derived` | Produced by Dataform (may or may not exist in Oracle) | Try Oracle; gracefully skip if absent |

### Comparison Metrics

For each table:
- **Row count** — `COUNT(*)`
- **Null counts per column** — `COUNT(CASE WHEN col IS NULL THEN 1 END)`
- **Distinct counts** — `COUNT(DISTINCT col)` (exact, not APPROX)
- **Numerics** — SUM, MIN, MAX

**Status outcomes:**
- `matched` — row count + all per-column aggregates agree
- `drifted` — same rows but aggregate values differ (schema drift, logic drift, date boundaries)
- `missing_in_bq` — Oracle has data, BQ is empty (Dataform didn't run?)
- `missing_in_oracle` — BQ has data, Oracle doesn't (expected for bq_derived tables)
- `skipped` — csv_stub or no Oracle connection
- `error` — connection failed or SQL error

### Confidence Scoring

Each generated SQLX file gets a confidence score (0.0–1.0) based on:
- Validation warnings (e.g., unresolved refs drop score)
- Verification status (matched = 1.0, drifted = 0.5, missing = 0.2)
- Source complexity (pure INSERT...SELECT = 1.0, custom SQL = 0.8, LLM-assisted = 0.6)

The UI displays these as color pills:
- Green ✓ (≥0.8)
- Yellow ⚠ (0.5–0.8)
- Red ✗ (<0.5)

### Report Output

Verification produces a JSON report:
```json
{
  "run_id": "abc123",
  "generated_at": "2026-05-12T...",
  "bq_project": "dan-sandpit",
  "raw_dataset": "migration_raw",
  "derived_dataset": "migration_demo",
  "tables": [
    {
      "name": "USERS",
      "bq_dataset": "migration_raw",
      "classification": "oracle_origin",
      "status": "matched",
      "oracle_rows": 12547,
      "bq_rows": 12547,
      "columns_compared": ["USER_ID", "NAME", "EMAIL", ...],
      "column_diffs": []
    },
    {
      "name": "FACT_SALES",
      "bq_dataset": "migration_demo",
      "classification": "bq_derived",
      "status": "drifted",
      "oracle_rows": 4891245,
      "bq_rows": 4891300,
      "columns_compared": ["SALE_ID", "AMOUNT", "REGION"],
      "column_diffs": [
        {"column": "AMOUNT", "metric": "sum", "oracle": "12345678.90", "bq": "12345700.12", "pct_diff": 0.002}
      ]
    }
  ],
  "summary": {
    "total": 23,
    "matched": 20,
    "drifted": 2,
    "missing_in_bq": 1,
    "missing_in_oracle": 0,
    "skipped": 0,
    "errors": 0
  }
}
```

---

## Known Limitations & What NOT to Claim

### Parser Limitations

1. **`simulate_performance` is opaque**
   - We log it as an operation but don't know what columns it adds
   - Downstream code that references those columns will fail at IR build time
   - **Workaround:** manually inspect the XML or ask the author

2. **Complex WHERE clauses are not analyzed**
   - Row filtering is recorded as a string; we don't decompose the predicate
   - If a WHERE uses Oracle-specific functions (e.g., `TRUNC(date_col)` to filter), those aren't patched in the captured query
   - **Workaround:** rely on execute_sql step processor to patch the final DML

3. **External CSV schema is inferred, not declared**
   - `extract_csv` columns come from downstream usage, not the CSV file itself
   - If a CSV column is never explicitly referenced, it won't appear in the final output
   - **Workaround:** manually add columns to the source declaration or edit the SQLX after generation

4. **Aggregate projections without aliases lose names**
   - `SELECT SUM(amount)` without `AS total_sales` gets a placeholder name "col" or "sum"
   - INSERT statements with target columns rename them, but SELECT-only steps lose the info
   - **Workaround:** ensure all expressions in XML have explicit aliases (result_column)

### Dialect Translation Limitations

1. **Nested TRUNC and complex date math**
   - `TRUNC(TRUNC(d, 'MM'), 'YYYY')` may not render correctly
   - `ADD_MONTHS(d, 3)`, `LAST_DAY(d)`, `NEXT_DAY(d)` have no direct BigQuery equivalents
   - **Workaround:** manually patch these in the generated SQLX

2. **Timezone handling**
   - Oracle's `AT TIME ZONE`, `FROM_TZ`, `NEW_TIME` are not handled
   - BigQuery's `AT TIME ZONE` syntax differs
   - **Workaround:** replace with `DATETIME_TRUNC()` or explicit CAST if timezone is not critical

3. **Number/string formatting**
   - `TO_CHAR(x, '999.99')` (Oracle format strings) → no BigQuery equivalent
   - `TO_NUMBER(s, '9.99EEEE')` (scientific notation) → not handled
   - **Workaround:** replace with CAST + STRING_AGG or manual formatting

4. **Row-level security**
   - Oracle's VPD (Virtual Private Database) predicates have no BigQuery equivalent
   - These aren't captured by the parser anyway (they're in Oracle, not the ETL XML)

### IR & Emitter Limitations

1. **Custom SQL with local CTEs**
   - If a source node has `custom_sql` that starts with `WITH`, the emitter unwraps it correctly
   - But nested CTEs within CTEs can cause issues if they reference the outer CTE
   - **Workaround:** flatten the nested CTEs or manually refactor the SQLX

2. **Multiple routers in sequence**
   - If you have two RouterNode steps back-to-back, the emitter may miss some branches
   - This is rare in practice (most pipelines have one router for INSERT/UPDATE/DELETE logic)

3. **Column name collisions across joins**
   - When both sides of a JOIN have the same column name, the first side wins
   - The second side's version is dropped (deduped by case-insensitive name)
   - **Workaround:** ensure upstream pipelines rename conflicting columns

4. **Circular dependencies**
   - The parser doesn't detect cycles (e.g., A reads from B, B reads from A)
   - This is caught at Dataform compile time with a clearer error
   - **Workaround:** fix the XML to break the cycle

### Verification Limitations

1. **Exact COUNT DISTINCT can be slow on large tables (>1B rows)**
   - We use exact `COUNT(DISTINCT col)` instead of APPROX_COUNT_DISTINCT
   - For very large tables, this query may time out
   - **Workaround:** manually set a row-count threshold above which we skip column aggregates

2. **Null handling in Oracle vs BigQuery**
   - Oracle treats empty strings as NULL; BigQuery doesn't
   - Comparison may show differences even if the pipeline is logically correct
   - **Workaround:** document this known difference in the findings

3. **Timestamp precision drift**
   - Oracle TIMESTAMP has microsecond precision; BigQuery has microseconds but may round differently
   - Comparisons of SUM/MIN/MAX on timestamps can show rounding drift
   - **Workaround:** compare at the day/hour level for timestamp columns

### What to Avoid Claiming

1. **"100% automated migration"**
   - You still need to review confidence-scored files and validate data parity
   - Complex procedures, dynamic SQL, and custom business logic still need manual work
   - **Better phrasing:** "6× faster automated *translation* with confidence scoring and validation framework"

2. **"We handle all Oracle SQL"**
   - We handle the most common patterns (TRUNC, SYSDATE, basic date math)
   - Edge cases like `INTERVAL` arithmetic, PL/SQL function definitions, and materialized view logs need review
   - **Better phrasing:** "We translate the most common Oracle SQL patterns; edge cases are flagged for review"

3. **"Data parity is guaranteed"**
   - We validate row counts and aggregates, but logic bugs can slip through
   - If the original pipeline had a bug, we replicate it faithfully
   - **Better phrasing:** "Verification compares row counts and column aggregates to catch gross mismatches"

4. **"No manual work needed"**
   - Post-load operations (UPDATE/DELETE on the same staging table) require careful sequencing
   - If a pipeline has dynamic SQL or conditional branches, you'll need to review the generated code
   - **Better phrasing:** "Reduces manual porting from weeks to days; review queues the high-risk files"

---

## Q&A Prep — Likely Demo Questions

### 1. How do you handle stored procedures?

**Code:** `backend/app/transformer/procedure_converter.py`

For PL/SQL procedures not defined in the XML, we:
1. Extract the source code from `user_source` in Oracle
2. Send it to Gemini 2.5 Pro with a system prompt asking for the logic as SQL
3. Parse the response and build an IR node

**Limitation:** Confidence scoring is on the 0–100 scale (≥90 high, ≥70 medium, <70 low — see `validation.py:_confidence_for`). Generated procedure files take the same deductions as everything else: −25 per validation error, −10 per warning, −5 per parser warning. Procedures with packages, global variables, or complex cursor logic typically land in the medium bucket and get surfaced in the review queue.

**What to say:** "For XML-defined pipelines, lineage is deterministic. For stored procedures without XML, we use Gemini to translate the PL/SQL — those files are flagged for review."

---

### 2. What if the ETL XML has constructs we don't support?

**Code:** `insignia_to_ir.py:192-195`, `parse_pipeline(etl_xml.py:85-118)`

Unsupported tags are logged as warnings and skipped. The pipeline still compiles, but those steps are omitted from the IR.

**Example:** If a `<pivot>` step exists but isn't in our parser, we emit:
```json
"warnings": ["unknown step: <pivot>"]
```

The step is skipped, and the next step reads from its logical predecessor.

**What to say:** "Unknown XML tags are logged as warnings and skipped. The pipeline compiles, but you'll need to manually add those steps to the generated SQLX. We flag this in the review queue."

---

### 3. How do you know the migration is correct?

**Code:** `verification.py` (full agent)

We use three complementary strategies:

1. **Structural validation** (`validation.py`):
   - All `${ref()}` calls resolve
   - No SQL parse errors
   - No circular dependencies
   - Confidence scoring per file

2. **Data-level verification** (verification agent):
   - Side-by-side Oracle ↔ BigQuery row counts and aggregates
   - Per-column SUM/MIN/MAX for numerics
   - Per-column NULL and DISTINCT counts
   - Status: matched / drifted / missing_in_bq / missing_in_oracle

3. **Lineage audit**:
   - Column-level edges traced through the entire pipeline
   - FK relationships validated
   - Dead objects and hot tables identified

**What to say:** "Verification is a three-layer framework: structural validation (refs, syntax, cycles), data-level parity checks (row counts, aggregates), and lineage audit. Mismatches are flagged for review."

---

### 4. Why are 10 BQ tables missing in Oracle?

**Code:** `verification.py:221-226` (classification logic)

These are typically:
- **CSV-sourced outputs** — the original ETL wrote CSVs, not Oracle tables
- **Dataform-derived tables** — produced by the migration pipeline, not the source
- **Staging tables** — temporary ETL intermediates, intentionally not in the data warehouse

Check the `classification` field:
- `csv_stub` — expected, no Oracle data to compare
- `bq_derived` → `missing_in_oracle` — expected (Dataform output)

**What to say:** "Those tables are classified as `bq_derived` because they're produced by Dataform, not by the original ETL. CSV-sourced tables are `csv_stub` and comparison is skipped. This is normal."

---

### 5. What dialect translations are NOT handled?

**Code:** `sql_helpers.py` — `_patch_oracle_dialect_for_bigquery` (line 418) AST walks; `_patch_bigquery_text` (line 547) post-render text fixes.

**Handled:**
- SYSDATE → CURRENT_DATETIME()
- TRUNC(date) / TRUNC(date, 'MM') → DATETIME_TRUNC
- Date arithmetic: `date - 365` → DATETIME_SUB
- TO_DATE, TO_TIMESTAMP (sqlglot native)

**NOT handled (need manual review):**
- ADD_MONTHS, LAST_DAY, NEXT_DAY
- Timezone functions (AT TIME ZONE, FROM_TZ, NEW_TIME)
- Number formatting (TO_CHAR with format strings)
- PL/SQL package functions (DBMS_*, UTL_*)
- VPD (Virtual Private Database) row-level security

**What to say:** "We patch the most common patterns. Format strings, timezone functions, and PL/SQL builtins need manual review — the generated SQLX will have placeholder comments flagging these."

---

### 6. How big is the IR?

**Code:** `ir.py:23-37` (node types), `ir.py:213-215` (union type)

11 node types, each immutable after construction:
1. SourceNode
2. ExpressionNode
3. FilterNode
4. LookupNode
5. JoinerNode
6. AggregatorNode
7. UnionNode
8. NormalizerNode
9. RouterNode
10. SequenceNode
11. TargetNode (metadata, not a CTE)

A typical demo pipeline lands at roughly one IR node per XML step — a 5-step `<extract>/<transform>/<join>/<aggregate>/<load>` produces ~5–7 nodes plus a TargetNode.

**What to say:** "The IR is a small, immutable dataclass model — 11 node types, no mutable state. The emitter walks them sequentially and emits one CTE per node."

---

### 7. What model do you use?

**Code:** `summary.py:70-78` (Gemini 2.5 Pro)

**Deterministic components** (no LLM):
- Inventory (live Oracle + XML parsing)
- Lineage (sqlglot + XML edges)
- Usage (audit-log queries)
- Transformation (deterministic IR → SQL)
- Orchestration (template YAML)
- Verification (SQL aggregates)

**LLM components**:
- **Summary:** Gemini 2.5 Pro (synthesis, findings, recommendations)
- **Annotations:** Gemini 2.5 Flash (PII classification, layer assignment)
- **Procedures (fallback):** Gemini 2.5 Pro (PL/SQL → SQL translation)
- **Chat (Ask tab):** Gemini 2.5 Pro (grounded in run results)

**What to say:** "Most of the system is deterministic Python — parsing, lineage, IR, SQL generation. We use Gemini 2.5 Pro for synthesis (summary, findings) and PL/SQL translation (confidence-scored). The pipeline translation itself is purely code — no LLM uncertainty."

---

### 8. Can I edit the generated SQLX?

**Yes.** The generated project is a standard Dataform repo. You can:
- Edit any `.sqlx` file
- Add new files (views, ops, assertions)
- Push to GitHub and Dataform will compile

**Code reference:** Not in the system (this is a UX flow). The "Push to GitHub" button in the UI uses a GitHub token to open a PR with the entire `definitions/` tree.

**What to say:** "The generated SQLX is a real Dataform project. You can edit it before pushing to GitHub, or after. Dataform compiles on every push, so you'll get immediate feedback on syntax errors or ref resolution issues."

---

### 9. What happens if the pipeline runs during the migration?

The system captures a snapshot at run time. If the pipeline runs afterwards:
- **Verification agent** will show the old data (if it ran before verification)
- **Dataform** will run the new pipeline and overwrite the BQ table
- You can re-run verification after Dataform finishes to see the new parity

**What to say:** "We snapshot the source warehouse at run time. If the pipeline runs afterwards, data parity may show drift — this is expected. Re-run verification after the Dataform pipeline executes to see the updated comparison."

---

### 10. How long does the end-to-end run take?

**Typical:** 3–5 minutes for a mid-sized warehouse (50–100 tables, 20–30 pipelines)

**Breakdown:**
- Inventory: 30–60 sec (Oracle introspection + XML parsing)
- Lineage: 20–40 sec (column tracing + LLM procedures)
- Usage: 10–20 sec (audit-log queries)
- Summary: 15–30 sec (Gemini synthesis)
- Transformation: 30–90 sec (IR building + SQL generation + Dataform assembly + validation)
- Orchestration: 5 sec (YAML generation)

**Bottleneck:** Transformation (IR → SQL) on very large pipelines (>100 steps each) can take 2–3 min.

**What to say:** "End-to-end is typically 3–5 minutes. Transformation is the bottleneck — large, complex pipelines can take longer. The live run UI shows per-agent progress, so you can watch it happen."

---

### 11. Do you support Informatica?

**Partially.** The transformation-core library was originally built for Informatica XML, and it still supports:
- Standard Informatica mappings (sources, expressions, filters, joins, aggregates, targets)
- Lookups, Joiners, Normalizers
- Routers (INSERT/UPDATE/DELETE logic)

For Insignia/Direnc pipelines, we have a dedicated parser (`insignia_to_ir.py`). Both produce the same IR shape, so the emitter is source-agnostic.

**What to say:** "The IR and SQL emitter are source-agnostic. We have parsers for Insignia XMLs and Informatica mappings. Adding support for SSIS, DataStage, or Talend would be a new parser module — the rest of the pipeline is reusable."

---

### 12. What about data types?

**Code:** `ir.py:50-70` (ColumnDef.bq_data_type)

Every column in the IR has a `bq_data_type` field, but it's often empty because:
- We infer column names from SQL parsing and lineage, not from Oracle metadata
- Oracle types (NUMBER, VARCHAR2, DATE) aren't automatically mapped to BigQuery types

**Roadmap (not shipped):**
- DDL introspection to populate bq_data_type
- Automatic type inference (NUMBER with scale → NUMERIC, DATE → DATETIME, etc.)
- Type assertions in Dataform

For now, the generated SQLX files don't include column types in the config block. Dataform infers types from the output data.

**What to say:** "Column types aren't emitted in the SQLX config block yet — Dataform infers them from the actual data. Wiring Oracle types into the IR is straightforward but wasn't a Tuesday-blocker. It's on the roadmap."

---

## File Index for Quick Reference

| Component | File | Key Functions/Classes |
|-----------|------|----------------------|
| **Parser** | `backend/app/parsers/etl_xml.py` | `parse_pipeline()`, `StepNode`, `Pipeline`, `to_lineage_edges()` |
| **IR** | `transformation-core/transformation_core/ir.py` | `DataflowGraph`, `SourceNode`, `ExpressionNode`, 11 node types, `ColumnDef` |
| **IR Builder** | `backend/app/transformer/insignia_to_ir.py` | `parse()`, `_process_extract()`, `_process_transform()`, etc. |
| **SQL Emitter** | `transformation-core/transformation_core/sql_generator.py` | `SQLGenerator.generate()`, `_emit_source()`, `_emit_node()` |
| **Dialect** | `backend/app/transformer/sql_helpers.py` | `_patch_oracle_dialect_for_bigquery()`, `render_dml_for_bigquery()`, `parse_select()` |
| **Project** | `backend/app/transformer/dataform_project.py` | `assemble_project()`, `_build_source_files()`, `_inject_tags_into_sqlx()` |
| **Validation** | `backend/app/transformer/validation.py` | `validate_project()`, issue detection, confidence scoring |
| **Inventory** | `backend/app/agents/inventory.py` | `run()`, Oracle introspection, XML parsing, annotation |
| **Lineage** | `backend/app/agents/lineage.py` | `run()`, edge collection, FK graph, view tracing |
| **Usage** | `backend/app/agents/usage.py` | `run()`, audit-log analysis, hot tables, dead objects |
| **Summary** | `backend/app/agents/summary.py` | `run()`, Gemini synthesis, findings extraction |
| **Transformation** | `backend/app/agents/transformation.py` | `run()`, entry to project generation, GCS upload |
| **Orchestration** | `backend/app/agents/orchestration.py` | `run()`, YAML workflow generation |
| **Verification** | `backend/app/agents/verification.py` | `run()`, table comparison, classification, parity checks |

---

## Key Takeaways for the Demo

1. **It's deterministic, not LLM-driven** — parsing, lineage, and SQL generation are pure Python with sqlglot. Only summary/chat/procedures use Gemini.

2. **The IR decouples source from target** — same IR can be emitted from different ETL tools.

3. **Dialect translation is pragmatic** — we handle the 80% case (SYSDATE, TRUNC, date math). Edge cases are flagged.

4. **Verification validates correctness** — row counts, per-column aggregates, and status classification. Mismatches are surfaced per-table.

5. **Confidence scores drive review priority** — focus manual work on high-risk files (<0.8 score).

6. **CSV stubs are expected** — not all sources are Oracle tables; external files are handled gracefully.

7. **The project is ready to deploy** — generated Dataform SQLX is real, can be edited, compiles immediately, and runs on a schedule via GitHub Actions.

---

_Last updated 2026-05-09 — read this tonight + tomorrow morning before the Tuesday demo._
