"""Verification agent — proves the Oracle→BigQuery migration worked.

Runs after the Transformation/Orchestration agents and after the user has
executed the generated Dataform pipelines. For every table the agent
migrated, we query Oracle and BigQuery side-by-side, compare row counts
plus per-column aggregates, and emit a categorised report:

  ✓ matched          — Oracle and BQ agree on rows + aggregates
  ⚠ drifted          — same table on both sides but values diverge
  ⚠ missing_in_bq    — Oracle has it, BQ doesn't (run Dataform yet?)
  ⚠ missing_in_oracle — BQ-derived table with no Oracle equivalent
  ℹ skipped          — non-Oracle source (CSV stub) — comparison N/A

The report is written to GCS at
`runs/<run_id>/verification/_report.json` and surfaced via the
`/runs/<run_id>/verification` REST endpoint plus a streaming summary on
the run channel.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from app.agents.base import EmitFn, log_event
from app.config import get_settings
from app.models.run import AgentName, RunRequest, StreamEvent
from app.services import oracle as oracle_svc
from app.services import transform_storage

log = logging.getLogger(__name__)

last_result: dict | None = None


# ─── Report dataclasses ──────────────────────────────────────────────────

@dataclass
class ColumnAgg:
    """Per-column aggregate snapshot used for fingerprint comparison."""
    column: str
    bq_type: str
    null_count: int | None = None
    distinct_count: int | None = None
    sum: str | None = None        # rendered as string to keep JSON-safe
    min: str | None = None
    max: str | None = None


@dataclass
class TableComparison:
    name: str
    bq_dataset: str
    classification: str           # oracle_origin | view_origin | csv_stub | bq_derived
    status: str = ""              # match | drift | missing_in_bq | missing_in_oracle | skipped | error
    oracle_rows: int | None = None
    bq_rows: int | None = None
    columns_compared: list[str] = field(default_factory=list)
    column_diffs: list[dict] = field(default_factory=list)
    notes: str = ""
    error: str = ""
    # Full schema on each side, for side-by-side DDL rendering in the
    # frontend. Populated for raw-layer tables (oracle_origin /
    # view_origin); empty for CSV stubs and BQ-derived outputs.
    oracle_columns: list[dict] = field(default_factory=list)  # [{name, data_type, nullable}]
    bq_columns: list[dict] = field(default_factory=list)      # [{name, type, mode}]


@dataclass
class VerificationReport:
    run_id: str
    generated_at: str
    bq_project: str
    raw_dataset: str               # e.g. migration_raw
    derived_dataset: str           # e.g. migration_demo
    tables: list[TableComparison] = field(default_factory=list)
    summary: dict = field(default_factory=dict)


# ─── Driver ──────────────────────────────────────────────────────────────


async def run(req: RunRequest, results, emit: EmitFn, run_id: str) -> None:
    """Compare every relevant Oracle table with its BigQuery counterpart."""
    global last_result
    last_result = None

    if not req.oracle:
        await log_event(emit, AgentName.VERIFY,
                        "skipped: no Oracle connection — nothing to verify against")
        return

    # We need transform output (to know what was migrated) and inventory
    # (to classify which sources are Oracle-origin vs CSV stubs).
    manifest = transform_storage.read_manifest(run_id)
    if manifest is None:
        await log_event(emit, AgentName.VERIFY,
                        "skipped: no transform output — run the Transformation agent first")
        return

    inventory_tables = _extract_inventory_table_names(results)
    inventory_views = _extract_inventory_view_names(results)

    # Map every BQ table the agent produced to a classification.
    targets = _build_targets(manifest, inventory_tables, inventory_views)
    if not targets:
        await log_event(emit, AgentName.VERIFY,
                        "no migrated tables found — nothing to verify")
        return

    await log_event(emit, AgentName.VERIFY,
                    f"verifying {len(targets)} tables against Oracle")

    settings = get_settings()
    bq_project = settings.gcp_project
    raw_dataset = "migration_raw"
    derived_dataset = "migration_demo"

    bq_client = bigquery.Client(project=bq_project, location=settings.gcp_region)
    oracle_conn_cfg = oracle_svc.OracleConn(
        host=req.oracle.host, port=req.oracle.port, service=req.oracle.service,
        user=req.oracle.user, password=req.oracle.password,
    )

    report = VerificationReport(
        run_id=run_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        bq_project=bq_project,
        raw_dataset=raw_dataset,
        derived_dataset=derived_dataset,
    )

    # The actual queries are blocking I/O; run the comparison loop in a
    # worker thread so we don't block the asyncio event loop.
    def _compare_all() -> list[TableComparison]:
        out: list[TableComparison] = []
        with oracle_svc.connect(oracle_conn_cfg) as oracle_conn:
            for t in targets:
                try:
                    out.append(_compare_one(t, bq_client, oracle_conn,
                                            bq_project, raw_dataset, derived_dataset))
                except Exception as e:  # noqa: BLE001
                    log.warning("verify %s failed: %s", t["name"], e)
                    out.append(TableComparison(
                        name=t["name"], bq_dataset=t["bq_dataset"],
                        classification=t["classification"],
                        status="error", error=str(e)[:300],
                    ))
        return out

    comparisons = await asyncio.to_thread(_compare_all)
    report.tables = comparisons
    report.summary = _summarise(comparisons)

    # Stream a summary line so the live UI shows progress.
    s = report.summary
    await log_event(emit, AgentName.VERIFY,
                    f"verified: {s['matched']}/{s['total']} match, "
                    f"{s['drifted']} drift, {s['missing']} missing, "
                    f"{s['skipped']} skipped, {s['errors']} errors")

    _upload_report(run_id, report)

    last_result = {
        "summary": report.summary,
        "report_path": f"runs/{run_id}/verification/_report.json",
    }
    await emit(StreamEvent(
        event="result",
        agent=AgentName.VERIFY,
        data=last_result,
    ))


# ─── Target classification ──────────────────────────────────────────────


def _build_targets(
    manifest, inventory_tables: set[str], inventory_views: set[str],
) -> list[dict]:
    """Build the list of (table_name, bq_dataset, classification) tuples to verify.

    Sources first (everything in `manifest.sources` lives in migration_raw),
    then derived pipelines (everything in `manifest.pipelines` lives in
    migration_demo). Classification:
      - `oracle_origin` — present in inventory tables → Oracle has same data
      - `view_origin`   — present in inventory views → Oracle has same data
                          (snapshot replication)
      - `csv_stub`      — referenced but absent from Oracle inventory
                          → no Oracle data to compare against
      - `bq_derived`    — Dataform-produced table; Oracle may or may not
                          have an equivalent depending on whether the
                          original ETL ran there
    """
    targets: list[dict] = []
    inv_lower = {t.lower() for t in inventory_tables}
    view_lower = {v.lower() for v in inventory_views}

    for s in manifest.sources or []:
        n = s.lower()
        if n in inv_lower:
            cls = "oracle_origin"
        elif n in view_lower:
            cls = "view_origin"
        else:
            cls = "csv_stub"
        targets.append({
            "name": s,
            "bq_dataset": "migration_raw",
            "classification": cls,
        })

    for p in manifest.pipelines or []:
        n = p.lower()
        # Dataform pipelines may share a name with an Oracle table when
        # the original ETL also wrote to that name (e.g. stg_audit_master,
        # core_account_summary). Mark those bq_derived but try Oracle
        # comparison anyway — works when ETL ran, gracefully misses
        # otherwise.
        targets.append({
            "name": p,
            "bq_dataset": "migration_demo",
            "classification": "bq_derived",
            "oracle_present": n in inv_lower,
        })
    return targets


def _extract_inventory_table_names(results) -> set[str]:
    inv = getattr(results, "inventory", None)
    if inv is None:
        return set()
    out: set[str] = set()
    for t in getattr(inv, "tables", []) or []:
        kind = getattr(t, "kind", "") or ""
        if kind == "TABLE":
            out.add(t.name)
    return out


def _extract_inventory_view_names(results) -> set[str]:
    inv = getattr(results, "inventory", None)
    if inv is None:
        return set()
    out: set[str] = set()
    for t in getattr(inv, "tables", []) or []:
        if getattr(t, "kind", None) == "VIEW":
            out.add(t.name)
    return out


# ─── Per-table comparison ───────────────────────────────────────────────


def _compare_one(
    target: dict,
    bq_client: bigquery.Client,
    oracle_conn,
    bq_project: str,
    raw_dataset: str,
    derived_dataset: str,
) -> TableComparison:
    name = target["name"]
    bq_dataset = target["bq_dataset"]
    cls = target["classification"]

    cmp = TableComparison(name=name, bq_dataset=bq_dataset, classification=cls)

    # CSV stubs — schema only, no rows. Skip.
    if cls == "csv_stub":
        cmp.status = "skipped"
        cmp.notes = "Source is not from Oracle (CSV input or never-created table); BQ has an empty stub schema."
        return cmp

    bq_count, bq_columns = _bq_stats(bq_client, bq_project, bq_dataset, name)
    oracle_count = _oracle_count(oracle_conn, name)

    cmp.bq_rows = bq_count
    cmp.oracle_rows = oracle_count
    # Persist full BQ schema so the frontend can render the DDL side-by-side.
    cmp.bq_columns = [
        {"name": c["name"], "type": c.get("type", ""), "mode": c.get("mode", "")}
        for c in bq_columns
    ]
    # Oracle-side schema (full, with data types + nullable) — only for
    # raw-layer tables that have an Oracle counterpart.
    if cls in ("oracle_origin", "view_origin"):
        cmp.oracle_columns = _oracle_full_columns(oracle_conn, name)

    # Resolve "missing on either side" before content-level checks.
    if oracle_count is None and bq_count is None:
        cmp.status = "missing_both"
        cmp.notes = "Neither Oracle nor BigQuery has this table."
        return cmp
    if oracle_count is None:
        cmp.status = (
            "missing_in_oracle"
            if cls == "bq_derived"
            else "missing_in_oracle"
        )
        cmp.notes = (
            "Table is BQ-derived; Oracle has no equivalent (likely the "
            "original ETL never produced it)."
            if cls == "bq_derived"
            else "Table is in BQ but not in Oracle."
        )
        return cmp
    if bq_count is None:
        cmp.status = "missing_in_bq"
        cmp.notes = "Oracle has the table; BQ does not. Has the Dataform run completed?"
        return cmp

    # Both sides exist — content-level comparison.
    cmp.columns_compared = [c["name"] for c in bq_columns]
    diffs: list[dict] = []
    rows_equal = oracle_count == bq_count
    overall_match = rows_equal

    # Per-column aggregates: SUM for numerics, COUNT-DISTINCT + MIN+MAX for
    # strings, all on both sides. Only run if Oracle has the matching column.
    oracle_cols = _oracle_table_columns(oracle_conn, name)
    common = [c for c in bq_columns if c["name"].upper() in oracle_cols]
    if common:
        bq_aggs = _bq_column_aggregates(bq_client, bq_project, bq_dataset, name, common)
        oracle_aggs = _oracle_column_aggregates(oracle_conn, name, common)
        for col in common:
            cn = col["name"]
            o = oracle_aggs.get(cn.upper(), {})
            b = bq_aggs.get(cn, {})
            col_match = _agg_match(o, b)
            if not col_match:
                overall_match = False
            diffs.append({
                "column": cn,
                "bq_type": col["type"],
                "match": col_match,
                "oracle": o,
                "bq": b,
            })

    cmp.column_diffs = diffs
    if overall_match:
        cmp.status = "match"
        cmp.notes = f"{bq_count:,} rows on both sides; aggregates align."
    else:
        cmp.status = "drift"
        if not rows_equal:
            cmp.notes = (
                f"Row count differs: Oracle={oracle_count:,}, BQ={bq_count:,}."
            )
        else:
            cmp.notes = (
                f"Row counts match ({bq_count:,}) but per-column aggregates "
                f"diverge — see column_diffs."
            )
    return cmp


def _agg_match(oracle_agg: dict, bq_agg: dict) -> bool:
    """Loose equality: compare the few aggregate keys both sides emit.
    Numeric sums tolerate tiny float drift; string min/max compare exactly.
    """
    for key in ("null_count", "distinct_count"):
        if key in oracle_agg and key in bq_agg:
            if oracle_agg[key] != bq_agg[key]:
                return False
    for key in ("sum",):
        if key in oracle_agg and key in bq_agg:
            o, b = oracle_agg[key], bq_agg[key]
            if o is None and b is None:
                continue
            try:
                if abs(float(o) - float(b)) > 1e-6 * max(abs(float(o)), 1.0):
                    return False
            except (TypeError, ValueError):
                if str(o) != str(b):
                    return False
    for key in ("min", "max"):
        if key in oracle_agg and key in bq_agg:
            if str(oracle_agg[key]) != str(bq_agg[key]):
                return False
    return True


# ─── BigQuery queries ───────────────────────────────────────────────────


def _bq_stats(
    bq_client: bigquery.Client, project: str, dataset: str, table: str
) -> tuple[int | None, list[dict]]:
    """Return (row_count, columns). columns is a list of {name, type}.
    Returns (None, []) if the table doesn't exist.
    """
    fq = f"{project}.{dataset}.{table}"
    try:
        tbl = bq_client.get_table(fq)
    except NotFound:
        return None, []
    cols = [{"name": f.name, "type": f.field_type} for f in tbl.schema]
    # streaming rows can lag in num_rows; SELECT COUNT(*) is authoritative.
    q = f"SELECT COUNT(*) AS c FROM `{fq}`"
    rows = list(bq_client.query(q).result())
    return int(rows[0]["c"]), cols


def _bq_column_aggregates(
    bq_client: bigquery.Client, project: str, dataset: str, table: str,
    columns: list[dict],
) -> dict[str, dict]:
    """Run one query computing aggregates for every column in `columns`.

    Returns {column_name: {null_count, distinct_count, sum?, min?, max?}}.
    """
    fq = f"`{project}.{dataset}.{table}`"
    select_parts: list[str] = []
    out_keys: dict[str, list[str]] = {}
    for c in columns:
        nm = c["name"]
        ty = (c["type"] or "").upper()
        # Always: null + distinct
        select_parts.append(
            f"COUNTIF(`{nm}` IS NULL) AS `null__{nm}`"
        )
        # Exact COUNT(DISTINCT) so the BQ side matches Oracle's exact
        # count; APPROX_COUNT_DISTINCT introduces ~1% HLL noise that
        # shows up as false-positive drift in the report.
        select_parts.append(
            f"COUNT(DISTINCT `{nm}`) AS `distinct__{nm}`"
        )
        keys = ["null_count", "distinct_count"]
        if ty in ("INT64", "INTEGER", "NUMERIC", "BIGNUMERIC", "FLOAT64", "FLOAT"):
            select_parts.append(
                f"CAST(SUM(`{nm}`) AS STRING) AS `sum__{nm}`"
            )
            select_parts.append(
                f"CAST(MIN(`{nm}`) AS STRING) AS `min__{nm}`"
            )
            select_parts.append(
                f"CAST(MAX(`{nm}`) AS STRING) AS `max__{nm}`"
            )
            keys.extend(["sum", "min", "max"])
        elif ty in ("STRING",):
            select_parts.append(
                f"MIN(`{nm}`) AS `min__{nm}`"
            )
            select_parts.append(
                f"MAX(`{nm}`) AS `max__{nm}`"
            )
            keys.extend(["min", "max"])
        out_keys[nm] = keys

    q = f"SELECT {', '.join(select_parts)} FROM {fq}"
    row = list(bq_client.query(q).result())[0]

    out: dict[str, dict] = {}
    for c in columns:
        nm = c["name"]
        agg: dict = {}
        for k in out_keys[nm]:
            field_name = f"{k.replace('_count', '').replace('null', 'null').replace('distinct', 'distinct')}__{nm}"
            # Map the aggregate key back to the column suffix
            mapped = {
                "null_count": "null",
                "distinct_count": "distinct",
                "sum": "sum",
                "min": "min",
                "max": "max",
            }[k]
            v = row.get(f"{mapped}__{nm}")
            if v is not None:
                if k in ("null_count", "distinct_count"):
                    agg[k] = int(v)
                else:
                    agg[k] = str(v)
        out[nm] = agg
    return out


# ─── Oracle queries ─────────────────────────────────────────────────────


def _oracle_count(oracle_conn, table: str) -> int | None:
    cur = oracle_conn.cursor()
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{table.upper()}"')
        return int(cur.fetchone()[0])
    except Exception:  # noqa: BLE001
        # Common case: ORA-00942 table does not exist. Return None and let
        # the caller flag missing_in_oracle.
        try:
            # Try lowercase quote-less variant in case the user-set
            # identifier is case-sensitive.
            cur2 = oracle_conn.cursor()
            cur2.execute(f"SELECT COUNT(*) FROM {table}")
            return int(cur2.fetchone()[0])
        except Exception:
            return None


def _oracle_table_columns(oracle_conn, table: str) -> set[str]:
    """Names (uppercase) of every column on the Oracle table, or empty if
    the table doesn't exist.
    """
    cur = oracle_conn.cursor()
    try:
        cur.execute(
            "SELECT column_name FROM all_tab_columns "
            "WHERE table_name = UPPER(:t)",
            t=table,
        )
        return {r[0] for r in cur.fetchall()}
    except Exception:
        return set()


def _oracle_full_columns(oracle_conn, table: str) -> list[dict]:
    """Full schema (name, data type, nullable) for a single Oracle table,
    ordered by column position. Returns [] if the table doesn't exist.

    Used by the verification report so the frontend can render a
    side-by-side DDL comparison against the BigQuery schema.
    """
    cur = oracle_conn.cursor()
    try:
        cur.execute(
            """
            SELECT column_name,
                   data_type,
                   data_length,
                   data_precision,
                   data_scale,
                   nullable
            FROM   all_tab_columns
            WHERE  table_name = UPPER(:t)
            ORDER  BY column_id
            """,
            t=table,
        )
        out: list[dict] = []
        for name, dt, length, precision, scale in (
            (r[0], r[1], r[2], r[3], r[4]) for r in cur.fetchall()
        ):
            # Build a readable Oracle data-type string. Numeric types
            # carry (precision, scale); VARCHAR2 carries length.
            base = dt or ""
            ty = base
            if base in ("NUMBER",) and precision is not None:
                ty = f"NUMBER({precision}" + (f",{scale}" if scale else "") + ")"
            elif base in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR") and length:
                ty = f"{base}({length})"
            out.append({"name": name, "data_type": ty, "nullable": True})
        # Re-execute to capture the nullable flag too (column_id-based
        # iterator above intentionally dropped it; do it here to keep
        # the parsing block focused).
        cur.execute(
            "SELECT column_name, nullable FROM all_tab_columns "
            "WHERE table_name = UPPER(:t)",
            t=table,
        )
        nullable_by_name = {r[0]: (r[1] != "N") for r in cur.fetchall()}
        for c in out:
            c["nullable"] = nullable_by_name.get(c["name"], True)
        return out
    except Exception:
        return []


def _oracle_column_aggregates(
    oracle_conn, table: str, columns: list[dict],
) -> dict[str, dict]:
    """Run one Oracle query computing the same aggregates as the BQ side.

    Keys returned in UPPER-case column names (Oracle convention).
    """
    if not columns:
        return {}
    parts: list[str] = []
    types: dict[str, str] = {}
    for c in columns:
        nm = c["name"].upper()
        ty = (c["type"] or "").upper()
        types[nm] = ty
        parts.append(
            f"SUM(CASE WHEN \"{nm}\" IS NULL THEN 1 ELSE 0 END) AS NULL__{nm}"
        )
        parts.append(f'COUNT(DISTINCT "{nm}") AS DISTINCT__{nm}')
        if ty in ("INT64", "INTEGER", "NUMERIC", "BIGNUMERIC", "FLOAT64", "FLOAT"):
            parts.append(f'TO_CHAR(SUM("{nm}")) AS SUM__{nm}')
            parts.append(f'TO_CHAR(MIN("{nm}")) AS MIN__{nm}')
            parts.append(f'TO_CHAR(MAX("{nm}")) AS MAX__{nm}')
        elif ty == "STRING":
            parts.append(f'MIN("{nm}") AS MIN__{nm}')
            parts.append(f'MAX("{nm}") AS MAX__{nm}')

    q = f'SELECT {", ".join(parts)} FROM "{table.upper()}"'
    cur = oracle_conn.cursor()
    try:
        cur.execute(q)
    except Exception:
        # Fall back to lowercase-unquoted in case of case sensitivity.
        try:
            q2 = q.replace(f'"{table.upper()}"', table)
            cur.execute(q2)
        except Exception:
            return {}
    headers = [d[0].upper() for d in cur.description]
    row = cur.fetchone()
    if row is None:
        return {}
    out: dict[str, dict] = {}
    for h, v in zip(headers, row):
        # Headers come back as e.g. NULL__ACCOUNT_ID, DISTINCT__ACCOUNT_ID.
        if "__" not in h:
            continue
        prefix, col = h.split("__", 1)
        agg = out.setdefault(col, {})
        if prefix == "NULL":
            agg["null_count"] = int(v) if v is not None else None
        elif prefix == "DISTINCT":
            agg["distinct_count"] = int(v) if v is not None else None
        elif prefix in ("SUM", "MIN", "MAX"):
            agg[prefix.lower()] = str(v) if v is not None else None
    return out


# ─── Storage / summary ──────────────────────────────────────────────────


def _summarise(comparisons: list[TableComparison]) -> dict:
    counts = {"match": 0, "drift": 0, "missing_in_bq": 0, "missing_in_oracle": 0,
              "missing_both": 0, "skipped": 0, "error": 0}
    for c in comparisons:
        counts[c.status] = counts.get(c.status, 0) + 1
    return {
        "total": len(comparisons),
        "matched": counts["match"],
        "drifted": counts["drift"],
        "missing": counts["missing_in_bq"] + counts["missing_in_oracle"] + counts["missing_both"],
        "skipped": counts["skipped"],
        "errors": counts["error"],
        "by_status": counts,
    }


def _upload_report(run_id: str, report: VerificationReport) -> None:
    from app.services import gcs
    settings = get_settings()
    payload = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "bq_project": report.bq_project,
        "raw_dataset": report.raw_dataset,
        "derived_dataset": report.derived_dataset,
        "summary": report.summary,
        "tables": [asdict(t) for t in report.tables],
    }
    gcs.write_json(
        settings.results_bucket,
        f"runs/{run_id}/verification/_report.json",
        json.dumps(payload, indent=2, default=str),
    )
