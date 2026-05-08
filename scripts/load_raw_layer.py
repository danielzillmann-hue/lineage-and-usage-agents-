#!/usr/bin/env python3
"""Replicate the demo source tables from Oracle into a BigQuery raw layer.

One-shot script for the Tuesday end-to-end demo. The Dataform pipelines
the agents generate read from `<project>.<dataset>` (default
`dan-sandpit.migration_raw`); this populates that dataset with sample
data so `dataform run` actually produces tables.

Usage:
    python scripts/load_raw_layer.py
        # uses the demo defaults baked in below

    python scripts/load_raw_layer.py \\
        --oracle-host 35.201.6.195 \\
        --oracle-user superuser --oracle-pass superpassword \\
        --bq-project dan-sandpit --bq-dataset migration_raw \\
        --limit 100000

Defaults are aligned to Direnc's demo Oracle box. Override anything via
CLI flags or env vars (ORACLE_HOST, ORACLE_USER, ORACLE_PASS, BQ_PROJECT,
BQ_DATASET).

Dependencies:
    pip install oracledb google-cloud-bigquery pandas pyarrow

Authentication:
    BigQuery uses Application Default Credentials. Make sure
    `gcloud auth application-default login` has been run, or set
    GOOGLE_APPLICATION_CREDENTIALS to a service-account key.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable

import oracledb
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

log = logging.getLogger("load_raw_layer")


# Tables the Insignia demo pipelines reference. Override with --tables.
DEFAULT_TABLES = [
    "accounts",
    "account_types",
    "account_investments",
    "members",
    "member_addresses",
    "transactions",
    "tax_brackets",
    "investment_options",
    "market_benchmarks",
    "vw_member_risk_profile",  # view — replicated as a snapshot table
]


@dataclass
class Config:
    oracle_host: str
    oracle_port: int
    oracle_service: str
    oracle_user: str
    oracle_pass: str
    bq_project: str
    bq_dataset: str
    bq_location: str
    tables: list[str]
    limit: int | None
    if_exists: str  # "replace" | "append" | "skip"


# ─── Oracle → BQ type mapping ─────────────────────────────────────────────


def _bq_schema_from_cursor(cursor: oracledb.Cursor) -> list[bigquery.SchemaField]:
    """Translate oracledb's cursor.description into a BQ SchemaField list.

    oracledb metadata gives us (name, type, display_size, internal_size,
    precision, scale, nullable). We map type-codes to BQ types.
    """
    fields: list[bigquery.SchemaField] = []
    for col in cursor.description:
        name = col[0]
        type_code = col[1]
        precision = col[4]
        scale = col[5]
        nullable = bool(col[6])

        bq_type = _oracle_type_code_to_bq(type_code, precision, scale)
        mode = "NULLABLE" if nullable else "REQUIRED"
        fields.append(bigquery.SchemaField(name, bq_type, mode=mode))
    return fields


def _oracle_type_code_to_bq(type_code, precision, scale) -> str:
    """oracledb db-api type codes → BigQuery types."""
    name = getattr(type_code, "name", str(type_code)).upper()
    # oracledb DbType codes: DB_TYPE_NUMBER, DB_TYPE_VARCHAR, DB_TYPE_DATE, etc.
    if "NUMBER" in name:
        if scale is not None and scale > 0:
            return "NUMERIC"
        return "INT64"
    if "BINARY_FLOAT" in name or "BINARY_DOUBLE" in name or "FLOAT" in name:
        return "FLOAT64"
    if "DATE" in name:
        return "DATETIME"  # Oracle DATE has time-of-day
    if "TIMESTAMP" in name:
        return "TIMESTAMP"
    if "BLOB" in name or "RAW" in name:
        return "BYTES"
    if "CLOB" in name or "VARCHAR" in name or "CHAR" in name or "ROWID" in name:
        return "STRING"
    if "BOOLEAN" in name:
        return "BOOL"
    return "STRING"


# ─── Per-table extract + load ─────────────────────────────────────────────


def replicate_table(
    cfg: Config,
    bq_client: bigquery.Client,
    oracle_conn: oracledb.Connection,
    table: str,
) -> tuple[int, str]:
    """Pull one table's rows from Oracle, load into BQ. Returns (row_count, status)."""
    target = f"{cfg.bq_project}.{cfg.bq_dataset}.{table}"

    if cfg.if_exists == "skip":
        try:
            existing = bq_client.get_table(target)
            log.info("  skip — %s already exists (%s rows)", target, existing.num_rows)
            return (existing.num_rows or 0, "skipped")
        except NotFound:
            pass

    cursor = oracle_conn.cursor()
    sql = f"SELECT * FROM {table}"
    if cfg.limit:
        sql += f" FETCH FIRST {cfg.limit} ROWS ONLY"
    log.info("  extracting from Oracle: %s", sql)
    try:
        cursor.execute(sql)
    except oracledb.DatabaseError as e:
        log.warning("  failed: %s", e)
        return (0, f"error: {e}")
    schema = _bq_schema_from_cursor(cursor)
    columns = [f.name for f in schema]
    rows = cursor.fetchall()
    if not rows:
        log.info("  no rows in source — creating empty table with schema only")

    # Build a DataFrame so the BQ client handles type coercion cleanly.
    df = pd.DataFrame(rows, columns=columns)
    df = _normalise_dataframe(df, schema)

    log.info("  loading %d rows into %s", len(df), target)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if cfg.if_exists == "replace"
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    job = bq_client.load_table_from_dataframe(df, target, job_config=job_config)
    job.result(timeout=600)
    if job.errors:
        log.error("  load failed: %s", job.errors)
        return (len(df), f"error: {job.errors}")
    return (len(df), "loaded")


def _normalise_dataframe(df: pd.DataFrame, schema: list[bigquery.SchemaField]) -> pd.DataFrame:
    """Convert numpy/object dtypes to types pandas.to_gbq / load_from_dataframe
    accepts. The big landmines are NaN-vs-None and Oracle's CLOB / LOB
    handling.
    """
    for field in schema:
        col = field.name
        if col not in df.columns:
            continue
        # Oracle CLOB / NCLOB come back as oracledb.LOB objects — read them.
        if field.field_type == "STRING":
            df[col] = df[col].apply(lambda v: v.read() if hasattr(v, "read") else v)
            df[col] = df[col].astype("string").where(df[col].notna(), None)
        elif field.field_type == "BYTES":
            df[col] = df[col].apply(lambda v: v.read() if hasattr(v, "read") else v)
        elif field.field_type in ("INT64",):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif field.field_type in ("FLOAT64", "NUMERIC"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ─── Driver ───────────────────────────────────────────────────────────────


def ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str) -> None:
    """Create the BQ dataset if it doesn't exist."""
    ds_ref = bigquery.DatasetReference(project, dataset)
    try:
        client.get_dataset(ds_ref)
        log.info("dataset %s.%s already exists", project, dataset)
    except NotFound:
        ds = bigquery.Dataset(ds_ref)
        ds.location = location
        client.create_dataset(ds)
        log.info("created dataset %s.%s in %s", project, dataset, location)


def run(cfg: Config) -> None:
    log.info("connecting to Oracle %s:%d/%s as %s",
             cfg.oracle_host, cfg.oracle_port, cfg.oracle_service, cfg.oracle_user)
    dsn = oracledb.makedsn(cfg.oracle_host, cfg.oracle_port, service_name=cfg.oracle_service)
    oracle_conn = oracledb.connect(user=cfg.oracle_user, password=cfg.oracle_pass, dsn=dsn)

    bq_client = bigquery.Client(project=cfg.bq_project, location=cfg.bq_location)
    ensure_dataset(bq_client, cfg.bq_project, cfg.bq_dataset, cfg.bq_location)

    log.info("replicating %d tables to %s.%s",
             len(cfg.tables), cfg.bq_project, cfg.bq_dataset)
    summary: list[tuple[str, int, str, float]] = []
    for table in cfg.tables:
        log.info("\n→ %s", table)
        t0 = time.time()
        try:
            rows, status = replicate_table(cfg, bq_client, oracle_conn, table)
        except Exception as e:  # noqa: BLE001
            log.error("  unhandled error: %s", e)
            rows, status = 0, f"error: {e}"
        summary.append((table, rows, status, time.time() - t0))

    oracle_conn.close()

    log.info("\n%s", "─" * 60)
    log.info(f"{'table':30s} {'rows':>10s}  {'status':12s} {'sec':>6s}")
    for name, rows, status, secs in summary:
        log.info(f"{name:30s} {rows:>10,}  {status:12s} {secs:>6.1f}")
    failures = [s for s in summary if s[2].startswith("error")]
    if failures:
        log.warning("%d tables failed", len(failures))
        sys.exit(1)


def parse_args() -> Config:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--oracle-host", default=os.getenv("ORACLE_HOST", "35.201.6.195"))
    p.add_argument("--oracle-port", type=int, default=int(os.getenv("ORACLE_PORT", "1521")))
    p.add_argument("--oracle-service", default=os.getenv("ORACLE_SERVICE", "XEPDB1"))
    p.add_argument("--oracle-user", default=os.getenv("ORACLE_USER", "superuser"))
    p.add_argument("--oracle-pass", default=os.getenv("ORACLE_PASS", "superpassword"))
    p.add_argument("--bq-project", default=os.getenv("BQ_PROJECT", "dan-sandpit"))
    p.add_argument("--bq-dataset", default=os.getenv("BQ_DATASET", "migration_raw"))
    p.add_argument("--location", default=os.getenv("BQ_LOCATION", "australia-southeast1"))
    p.add_argument(
        "--tables",
        help="Comma-separated list. Defaults to the demo tables.",
        default=",".join(DEFAULT_TABLES),
    )
    p.add_argument("--limit", type=int, default=None,
                   help="Cap rows per table (faster runs). Default: no limit.")
    p.add_argument("--if-exists", choices=["replace", "append", "skip"], default="replace",
                   help="What to do when the BQ table already exists. Default: replace.")
    args = p.parse_args()
    return Config(
        oracle_host=args.oracle_host,
        oracle_port=args.oracle_port,
        oracle_service=args.oracle_service,
        oracle_user=args.oracle_user,
        oracle_pass=args.oracle_pass,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        bq_location=args.location,
        tables=[t.strip() for t in args.tables.split(",") if t.strip()],
        limit=args.limit,
        if_exists=args.if_exists,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    cfg = parse_args()
    run(cfg)


if __name__ == "__main__":
    main()
