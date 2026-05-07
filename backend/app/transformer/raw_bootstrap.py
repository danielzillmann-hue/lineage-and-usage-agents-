"""Generate the raw-layer bootstrap from the Oracle inventory.

The pipelines we generate read from a `<project>.<source_dataset>` raw
layer that has to be populated *before* anything will run. Real projects
do this with continuous replication (Datastream / Fivetran / BryteFlow).
This module emits two artefacts the user can run as a one-shot demo or
hand to the team that owns replication:

- `bootstrap/raw_schema.sql`     — BQ CREATE TABLE DDL for every source
                                   table the pipelines reference, with
                                   Oracle types mapped to BigQuery types.
- `bootstrap/replication_setup.md` — README walking through Datastream
                                   setup (gcloud commands), manual `bq
                                   load`, and BQ federation as
                                   alternatives.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


_TYPE_RE = re.compile(r"^\s*([A-Za-z_]+)\s*(?:\(([^)]+)\))?", re.IGNORECASE)


@dataclass
class ColumnSchema:
    name: str
    oracle_type: str  # raw string from the inventory, e.g. "VARCHAR2(50)" / "NUMBER(10,2)"
    nullable: bool = True
    is_pk: bool = False


def oracle_to_bq_type(oracle_type: str) -> str:
    """Map an Oracle data-type string to a BigQuery type.

    Handles type strings like:
        VARCHAR2(50)        -> STRING
        NUMBER              -> NUMERIC
        NUMBER(10)          -> INT64
        NUMBER(10,2)        -> NUMERIC
        DATE                -> DATETIME (Oracle DATE has time-of-day)
        TIMESTAMP(6)        -> TIMESTAMP
        BLOB                -> BYTES

    Returns "STRING" as a safe fallback for unknown types.
    """
    if not oracle_type:
        return "STRING"
    m = _TYPE_RE.match(oracle_type.strip())
    if not m:
        return "STRING"
    base = m.group(1).upper()
    args = (m.group(2) or "").strip()

    if base in {"VARCHAR2", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR2",
                "CLOB", "NCLOB", "ROWID", "UROWID", "LONG", "XMLTYPE"}:
        return "STRING"
    if base == "NUMBER":
        # NUMBER without args → NUMERIC. NUMBER(p,s) with s>0 → NUMERIC.
        # NUMBER(p) with s=0 → INT64.
        if args:
            parts = [p.strip() for p in args.split(",")]
            if len(parts) >= 2:
                try:
                    if int(parts[1]) > 0:
                        return "NUMERIC"
                except ValueError:
                    pass
                return "INT64"
            try:
                _ = int(parts[0])
                return "INT64"
            except ValueError:
                pass
            return "NUMERIC"
        return "NUMERIC"
    if base in {"INTEGER", "INT", "SMALLINT", "BIGINT"}:
        return "INT64"
    if base in {"FLOAT", "REAL", "DOUBLE", "BINARY_FLOAT", "BINARY_DOUBLE",
                "DECIMAL", "NUMERIC"}:
        return "FLOAT64"
    if base == "DATE":
        return "DATETIME"  # Oracle DATE includes time-of-day
    if base.startswith("TIMESTAMP"):
        # TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE
        return "TIMESTAMP"
    if base in {"INTERVAL"}:
        return "STRING"  # BQ has no INTERVAL — store as ISO 8601 string
    if base in {"RAW", "BLOB", "BFILE"}:
        return "BYTES"
    if base == "BOOLEAN":
        return "BOOL"
    return "STRING"


def generate_table_ddl(
    table_name: str,
    columns: list[ColumnSchema],
    *,
    project: str,
    dataset: str,
    description: str = "",
) -> str:
    """Build a single `CREATE TABLE IF NOT EXISTS ...` statement for one
    source table.
    """
    lines = [f"CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table_name}` ("]
    col_lines: list[str] = []
    pk_cols: list[str] = []
    for c in columns:
        bq_type = oracle_to_bq_type(c.oracle_type)
        null_clause = "" if c.nullable else " NOT NULL"
        col_lines.append(f"  {c.name} {bq_type}{null_clause}")
        if c.is_pk:
            pk_cols.append(c.name)
    body = ",\n".join(col_lines)
    if pk_cols:
        body += f",\n  PRIMARY KEY ({', '.join(pk_cols)}) NOT ENFORCED"
    lines.append(body)
    if description:
        # Use OPTIONS(description=...) at the end.
        lines.append(f') OPTIONS(description="{description.replace(chr(34), chr(39))}");')
    else:
        lines.append(");")
    return "\n".join(lines)


def generate_raw_schema_sql(
    sources: list[str],
    table_metadata: dict[str, dict],
    *,
    project: str,
    dataset: str,
) -> str:
    """Build the full `bootstrap/raw_schema.sql` file.

    Iterates `sources` (the list of external table names referenced by
    `${ref()}` calls and not produced internally), looks up each in
    `table_metadata` for column schemas, and concatenates the DDLs.
    Tables we have no schema for get a TODO comment instead of being
    skipped silently.
    """
    blocks: list[str] = [
        "-- Raw-layer schema bootstrap.",
        "-- Generated by intelia from the Oracle inventory. Run once against",
        "-- the BigQuery target before `dataform run`. Idempotent — every",
        "-- statement is CREATE TABLE IF NOT EXISTS.",
        "",
        f"CREATE SCHEMA IF NOT EXISTS `{project}.{dataset}`",
        '  OPTIONS(description="Source layer replicated from the Oracle warehouse");',
        "",
    ]
    for name in sources:
        meta = table_metadata.get(name.lower(), {})
        schema_rows = meta.get("schema") or []
        if not schema_rows:
            blocks.append(
                f"-- TODO: no inventory schema captured for `{name}`. "
                "Populate the table manually or rerun the inventory agent.\n"
            )
            continue
        cols = [
            ColumnSchema(
                name=row["name"],
                oracle_type=row["oracle_type"] or "",
                nullable=row.get("nullable", True),
                is_pk=row.get("is_pk", False),
            )
            for row in schema_rows
        ]
        ddl = generate_table_ddl(
            name, cols, project=project, dataset=dataset,
            description=f"Source table replicated from Oracle ({name})",
        )
        blocks.append(ddl)
        blocks.append("")
    return "\n".join(blocks).rstrip() + "\n"


def generate_replication_readme(
    sources: list[str],
    *,
    project: str,
    dataset: str,
    region: str,
) -> str:
    """Build `bootstrap/replication_setup.md` — three options the user can
    pick from to populate the raw layer.
    """
    table_list = "\n".join(f"  - {s}" for s in sorted(sources))
    sources_csv = ", ".join(f"'{s}'" for s in sorted(sources))
    return f"""# Raw-layer replication setup

The transformation pipelines this project generates read from the
`{project}.{dataset}` BigQuery dataset. That dataset is the **raw
layer** — replicated from the Oracle warehouse — and the agents
declare these tables but do *not* populate them. Pick one of the
options below to bring data over.

## Tables required

The pipelines reference {len(sources)} source tables:

{table_list}

The DDL for each is in [`raw_schema.sql`](raw_schema.sql). Run that
once to provision the empty target tables:

```bash
bq query --project_id={project} --location={region} --use_legacy_sql=false \\
  < bootstrap/raw_schema.sql
```

## Option A — Datastream (recommended for production)

Datastream is the GCP-native, low-latency Oracle CDC service.

1. Enable the API:
   ```bash
   gcloud services enable datastream.googleapis.com
   ```
2. Create connection profiles for the source (Oracle) and destination
   (BigQuery). Replace `<HOST>` / `<USER>` / `<PASS>` with your values:
   ```bash
   gcloud datastream connection-profiles create oracle-source \\
     --location={region} --type=oracle --display-name="Insignia Oracle" \\
     --oracle-hostname=<HOST> --oracle-port=1521 \\
     --oracle-username=<USER> --oracle-password=<PASS> \\
     --oracle-database-service=<SERVICE>

   gcloud datastream connection-profiles create bq-target \\
     --location={region} --type=bigquery --display-name="Insignia BQ"
   ```
3. Create the stream (initial backfill + ongoing CDC):
   ```bash
   gcloud datastream streams create insignia-stream \\
     --location={region} \\
     --source=oracle-source --destination=bq-target \\
     --oracle-source-config="<json-with-tables-listed>" \\
     --bigquery-destination-config="data_freshness=900s,dataset={dataset}" \\
     --backfill-all
   ```

The full Oracle source config JSON can list specific schemas/tables
({sources_csv}) so only what the pipelines need gets replicated.

## Option B — Manual `bq load` (one-shot for a demo / POC)

For a Tuesday-style demo where you just need data once, dump the source
tables to CSV/Parquet and load them:

```bash
# Per table, repeat with appropriate names:
bq load --autodetect --source_format=CSV \\
  {project}:{dataset}.accounts \\
  gs://your-bucket/oracle-export/accounts.csv
```

A small Python script using `oracledb` + `google-cloud-bigquery` can
loop over the tables in one pass — see the inventory agent's source for
the connection pattern.

## Option C — BigQuery federation (no copy)

Use BQ external tables that query Oracle live via Cloud SQL. Limited
performance but no replication infrastructure needed:

```sql
CREATE EXTERNAL TABLE `{project}.{dataset}.accounts`
WITH CONNECTION `{project}.{region}.oracle-conn`
OPTIONS (
  format = 'oracle',
  uris = ['oracle://<HOST>:1521/<SERVICE>'],
  source_table = 'ORACLE_SCHEMA.ACCOUNTS'
);
```

This avoids the schema bootstrap entirely — the external table inherits
Oracle's schema. Trade-off is per-query latency.

---

After whichever path you pick, verify with:

```sql
SELECT table_name, row_count
FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;
```

…then `dataform run` will produce the downstream tables.
"""
