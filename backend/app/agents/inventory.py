"""Inventory agent — introspects the live Oracle DB and parses any ETL XMLs.

If an Oracle connection is provided we get tables/columns/FKs/row counts/audit
log directly. If a bucket prefix is provided we additionally scan it for ETL
XML pipelines (deterministic) and CSV outputs (column inferred from XML load).

Falls back to legacy DDL+CSV-dictionary flow if neither is present.
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

import sqlglot
from sqlglot import exp

from app.agents.base import EmitFn, log_event
from app.models.run import AgentName, RunRequest, StreamEvent
from app.models.schema import (
    Column, Domain, ETLPipeline, Inventory, InventoryFlag, Layer,
    PipelineRunStats, PipelineStep, Procedure, Table,
)
from app.parsers.etl_xml import parse_pipeline as parse_xml_pipeline
from app.services import gcs
from app.services import oracle as ora

log = logging.getLogger(__name__)

last_result: Inventory | None = None


async def run(req: RunRequest, results, emit: EmitFn) -> None:
    global last_result
    inv = Inventory()
    oracle_runs: list[ora.PipelineRunStat] = []

    # ─── 1. Live Oracle introspection ───────────────────────────────────
    if req.oracle:
        await log_event(emit, AgentName.INVENTORY, f"Connecting to Oracle at {req.oracle.host}:{req.oracle.port}/{req.oracle.service}")
        try:
            conn = ora.OracleConn(
                host=req.oracle.host, port=req.oracle.port, service=req.oracle.service,
                user=req.oracle.user, password=req.oracle.password,
            )
            snap = ora.snapshot(conn)
            await log_event(
                emit, AgentName.INVENTORY,
                f"Live introspection: schema {snap.schema} · {len(snap.tables)} tables · {len(snap.pipeline_runs)} pipelines in audit log",
            )
            for ot in snap.tables:
                cols = [
                    Column(
                        name=c.name, data_type=c.data_type, nullable=c.nullable,
                        is_pk=c.is_pk, is_fk=c.is_fk, fk_target=c.fk_target,
                    )
                    for c in ot.columns
                ]
                t = Table(
                    schema_name=ot.schema, name=ot.name, kind=ot.kind, columns=cols,
                    row_count=ot.row_count, bytes=ot.bytes, last_analyzed=ot.last_analyzed,
                    layer=_heuristic_layer_from_name(ot.name),
                    domain=_heuristic_domain_from_name(ot.name),
                    source_text=ot.source_text,
                )
                inv.tables.append(t)
            oracle_runs = snap.pipeline_runs
        except Exception as e:  # noqa: BLE001
            log.exception("oracle introspection failed")
            await log_event(emit, AgentName.INVENTORY, f"Oracle connection failed: {e}", kind="error")

    # ─── 2. Bucket scan: ETL XMLs + CSV outputs + legacy DDL/dictionary ─
    if req.bucket:
        await log_event(emit, AgentName.INVENTORY, f"Scanning bucket gs://{req.bucket}/{req.prefix}")
        ddl_files: list[tuple[str, str]] = []
        dict_files: list[tuple[str, str]] = []
        etl_files: list[tuple[str, str]] = []
        output_files: list[tuple[str, str]] = []
        for f in gcs.iter_classified(req.bucket, req.prefix):
            if f.kind == "ddl":
                ddl_files.append((f.name, gcs.read_text(req.bucket, f.name)))
            elif f.kind == "dictionary":
                dict_files.append((f.name, gcs.read_text(req.bucket, f.name)))
            elif f.kind == "etl":
                etl_files.append((f.name, gcs.read_text(req.bucket, f.name)))
            elif f.kind == "output" or (f.name.lower().endswith(".csv") and "output" in f.name.lower()):
                output_files.append((f.name, gcs.read_text(req.bucket, f.name)))

        await log_event(
            emit, AgentName.INVENTORY,
            f"Bucket: {len(etl_files)} ETL XMLs · {len(output_files)} output CSVs · {len(ddl_files)} DDL · {len(dict_files)} dictionary",
        )

        # ETL pipelines
        for name, text in etl_files:
            pl = parse_xml_pipeline(text, name)
            if not pl:
                continue
            inv.pipelines.append(_to_inventory_pipeline(pl))

        # CSV outputs — model each as a Table at Layer.OUTPUT, columns from header
        for name, text in output_files:
            inv.tables.append(_csv_to_table(name, text))

        # Legacy DDL flow (non-Oracle path) — still useful for DDL-only buckets
        if ddl_files and not req.oracle:
            for t in _parse_ddl(ddl_files):
                t.layer = _heuristic_layer_from_name(t.name)
                t.domain = _heuristic_domain_from_name(t.name)
                inv.tables.append(t)
            await log_event(emit, AgentName.INVENTORY, f"Parsed {len(ddl_files)} DDL files → {sum(1 for t in inv.tables if t.kind in ('TABLE','VIEW'))} objects")

    # ─── 2b. Cross-reference pipeline outputs against actual CSV files ──
    if req.bucket and inv.pipelines and req.outputs_prefix is not None:
        try:
            csvs = gcs.list_csv_outputs(req.bucket, req.outputs_prefix)
            csv_by_name: dict[str, gcs.ClassifiedFile] = {}
            for c in csvs:
                short = c.name.split("/")[-1].lower()
                csv_by_name.setdefault(short, c)
            await log_event(
                emit, AgentName.INVENTORY,
                f"Outputs scan: {len(csvs)} CSV files at gs://{req.bucket}/{req.outputs_prefix}",
            )
            for p in inv.pipelines:
                if not p.output_csv:
                    continue
                hit = csv_by_name.get(p.output_csv.lower())
                if hit:
                    p.csv_exists = True
                    p.csv_last_modified = hit.updated
                    p.csv_size_bytes = hit.size
        except Exception as e:  # noqa: BLE001
            log.warning("output CSV scan failed: %s", e)

    # ─── 3. Match audit-log runs against XML-defined pipelines ──────────
    runs = oracle_runs
    matched: set[str] = set()
    if runs:
        runs_by_canonical: dict[str, ora.PipelineRunStat] = {}
        for r in runs:
            for k in {r.pipeline_name, _strip_numeric_prefix(r.pipeline_name)}:
                runs_by_canonical.setdefault(k.lower(), r)
        for p in inv.pipelines:
            for k in {p.name, _strip_numeric_prefix(p.name)}:
                r = runs_by_canonical.get(k.lower())
                if r:
                    p.runs = PipelineRunStats(
                        runs_total=r.runs_total,
                        runs_success=r.runs_success,
                        runs_failed=r.runs_failed,
                        first_run=r.first_run,
                        last_run=r.last_run,
                    )
                    matched.add(r.pipeline_name)
                    break
        # Audit-log entries that don't correspond to any defined pipeline
        for r in runs:
            if r.pipeline_name in matched:
                continue
            stripped = _strip_numeric_prefix(r.pipeline_name)
            # Don't double-emit a numeric-prefixed run if its bare name was matched
            if stripped != r.pipeline_name and any(p.name.lower() == stripped.lower() for p in inv.pipelines):
                continue
            from app.models.schema import OrphanRun
            inv.orphan_runs.append(OrphanRun(
                pipeline_name=r.pipeline_name,
                csv_generated=r.csv_generated,
                runs=PipelineRunStats(
                    runs_total=r.runs_total, runs_success=r.runs_success, runs_failed=r.runs_failed,
                    first_run=r.first_run, last_run=r.last_run,
                ),
            ))

    # ─── 4. Heuristic flags ─────────────────────────────────────────────
    inv.flags = _build_flags(inv)

    last_result = inv
    await emit(StreamEvent(
        event="result", agent=AgentName.INVENTORY,
        data={
            "tables": len(inv.tables),
            "pipelines": len(inv.pipelines),
            "orphan_runs": len(inv.orphan_runs),
            "flags": len(inv.flags),
            "by_layer": _count_by(inv.tables, lambda t: t.layer.value),
            "by_domain": _count_by(inv.tables, lambda t: t.domain.value),
        },
    ))


# ─── helpers ──────────────────────────────────────────────────────────────


def _to_inventory_pipeline(pl) -> ETLPipeline:
    """Convert parsers.etl_xml.Pipeline → models.schema.ETLPipeline."""
    src_tables: list[str] = []
    for s in pl.steps.values():
        for t in s.source_tables:
            if t and t not in src_tables:
                src_tables.append(t)
    output_csv = None
    column_count = 0
    if pl.load_step_id:
        load = pl.steps[pl.load_step_id]
        output_csv = load.output_path
        column_count = len(load.columns)
    steps = [
        PipelineStep(
            id=s.id, kind=s.kind, inputs=s.inputs, columns=s.columns,
            operations=s.operations, source_tables=s.source_tables,
            source_query=s.source_query, output_path=s.output_path,
        )
        for s in pl.steps.values()
    ]
    return ETLPipeline(
        name=pl.name,
        file=pl.file,
        output_csv=output_csv,
        source_tables=src_tables,
        steps=steps,
        column_count=column_count,
        connection_host=pl.connection.get("host"),
        connection_service=pl.connection.get("service"),
    )


def _csv_to_table(blob_name: str, text: str) -> Table:
    """Read just the CSV header to surface a table-like object at Layer.OUTPUT."""
    short = blob_name.split("/")[-1]
    name = short.removesuffix(".csv").upper()
    cols: list[Column] = []
    try:
        reader = csv.reader(io.StringIO(text))
        header = next(reader, [])
        # Count rows roughly without loading entire CSV in memory if huge
        row_count = sum(1 for _ in reader)
        cols = [Column(name=h.strip().upper(), data_type="STRING", nullable=True) for h in header]
    except Exception as e:  # noqa: BLE001
        log.warning("csv parse failed on %s: %s", blob_name, e)
        row_count = None
    return Table(
        schema_name="OUTPUTS", name=name, kind="CSV",
        columns=cols, row_count=row_count,
        layer=Layer.OUTPUT, domain=_heuristic_domain_from_name(name),
        comment=f"CSV produced by ETL pipeline (file: {short})",
    )


# ─── DDL parser (legacy path) ─────────────────────────────────────────────


def _parse_ddl(files: list[tuple[str, str]]) -> list[Table]:
    tables: dict[str, Table] = {}
    for name, text in files:
        try:
            statements = sqlglot.parse(text, dialect="oracle")
        except Exception as e:  # noqa: BLE001
            log.warning("sqlglot parse failed on %s: %s", name, e)
            continue
        for stmt in statements:
            if stmt is None:
                continue
            if isinstance(stmt, exp.Create):
                t = _table_from_create(stmt)
                if t:
                    tables[t.fqn] = t
    return list(tables.values())


def _table_from_create(stmt: exp.Create) -> Table | None:
    kind = (stmt.kind or "").upper()
    if kind not in {"TABLE", "VIEW", "MATERIALIZED VIEW", "MVIEW"}:
        return None
    this = stmt.this
    table_ref = this if isinstance(this, exp.Table) else this.this if hasattr(this, "this") else None
    if not isinstance(table_ref, exp.Table):
        return None
    schema_name = (table_ref.db or "UNKNOWN").upper()
    name = table_ref.name.upper()
    cols: list[Column] = []
    table_pk_columns: set[str] = set()
    if isinstance(this, exp.Schema):
        for item in this.expressions or []:
            pk = None
            if isinstance(item, exp.PrimaryKey):
                pk = item
            elif isinstance(item, exp.Constraint):
                for sub in item.args.get("expressions") or []:
                    if isinstance(sub, exp.PrimaryKey):
                        pk = sub
                        break
            if pk:
                for c in pk.expressions or []:
                    table_pk_columns.add(getattr(c, "name", str(c)).upper())
        for col in this.expressions or []:
            if not isinstance(col, exp.ColumnDef):
                continue
            constraints = col.args.get("constraints") or []
            col_pk = any(
                isinstance(c, exp.PrimaryKeyColumnConstraint)
                or (hasattr(c, "kind") and isinstance(c.kind, exp.PrimaryKeyColumnConstraint))
                for c in constraints
            )
            cols.append(Column(
                name=col.name.upper(),
                data_type=col.args.get("kind").sql() if col.args.get("kind") else "UNKNOWN",
                nullable=not any(
                    isinstance(c, exp.NotNullColumnConstraint)
                    or (hasattr(c, "kind") and isinstance(c.kind, exp.NotNullColumnConstraint))
                    for c in constraints
                ),
                is_pk=col_pk or col.name.upper() in table_pk_columns,
            ))
    source_text = stmt.expression.sql(dialect="oracle") if stmt.expression else None
    return Table(
        schema_name=schema_name, name=name,
        kind="VIEW" if kind in {"VIEW", "MATERIALIZED VIEW", "MVIEW"} else "TABLE",
        columns=cols, source_text=source_text,
    )


# ─── Heuristics ───────────────────────────────────────────────────────────


def _heuristic_layer_from_name(name: str) -> Layer:
    n = name.upper()
    if any(p in n for p in ("RAW_", "STAGE_", "STG_RAW")):
        return Layer.RAW
    if n.startswith(("STG_", "STAGE_")):
        return Layer.STAGING
    if n.startswith(("DIM_", "FACT_", "F_", "D_", "BRIDGE_", "INT_")):
        return Layer.INTEGRATION
    if n.startswith(("RPT_", "MART_", "AGG_", "DSH_")):
        return Layer.REPORTING
    # Live super-fund DB: short business names are source/raw tables.
    if n in {
        "MEMBERS", "ACCOUNTS", "ACCOUNT_TYPES", "ACCOUNT_INVESTMENTS",
        "INVESTMENT_OPTIONS", "TRANSACTIONS", "ETL_EXECUTION_LOGS",
    }:
        return Layer.RAW
    return Layer.UNKNOWN


_DOMAIN_HINTS = (
    ("MEMBER", Domain.MEMBER), ("CLIENT", Domain.MEMBER),
    ("CUSTOMER", Domain.MEMBER), ("INVESTOR", Domain.MEMBER),
    ("ACCOUNT", Domain.ACCOUNT),
    ("HOLDING", Domain.HOLDING), ("FUND", Domain.HOLDING),
    ("PORTFOLIO", Domain.HOLDING), ("INVESTMENT", Domain.INVESTMENT),
    ("TRANSACTION", Domain.TRANSACTION), ("TXN", Domain.TRANSACTION),
    ("FEE", Domain.FEE), ("PENSION", Domain.TRANSACTION),
    ("CONTRIB", Domain.TRANSACTION), ("TAX", Domain.TRANSACTION),
    ("ADVISER", Domain.ADVISER), ("ADVISOR", Domain.ADVISER),
    ("PRODUCT", Domain.PRODUCT),
    ("ETL", Domain.AUDIT), ("AUDIT", Domain.AUDIT), ("LOG", Domain.AUDIT),
    ("BALANCE", Domain.ACCOUNT), ("DAILY", Domain.TRANSACTION),
    ("REF_", Domain.REFERENCE),
)


def _heuristic_domain_from_name(name: str) -> Domain:
    n = name.upper()
    for token, dom in _DOMAIN_HINTS:
        if token in n:
            return dom
    return Domain.OTHER


def _strip_numeric_prefix(s: str) -> str:
    parts = s.split("_", 1)
    if len(parts) == 2 and parts[0].isdigit():
        return parts[1]
    return s


def _build_flags(inv: Inventory) -> list[InventoryFlag]:
    flags: list[InventoryFlag] = []
    for t in inv.tables:
        if t.kind == "TABLE" and not any(c.is_pk for c in t.columns):
            flags.append(InventoryFlag(
                severity="warn",
                title="Table has no primary key",
                detail=f"{t.fqn} has no PK constraint declared.",
                object_fqn=t.fqn,
            ))
    # Pipelines that never ran (audit log silent AND no output CSV)
    for p in inv.pipelines:
        no_audit = p.runs is None or p.runs.runs_total == 0
        if no_audit and p.csv_exists:
            # Output CSV exists despite no audit-log entries → pipeline ran but
            # didn't write to ETL_EXECUTION_LOGS. Classic governance gap.
            flags.append(InventoryFlag(
                severity="critical",
                title="Pipeline ran without logging",
                detail=(
                    f"{p.name} produced {p.output_csv} (last modified {p.csv_last_modified}) "
                    f"but has no entries in ETL_EXECUTION_LOGS. The pipeline executes outside "
                    f"the audit framework — observability and SLA tracking can't see it."
                ),
                object_fqn=p.name,
            ))
        elif no_audit and not p.csv_exists:
            flags.append(InventoryFlag(
                severity="warn",
                title="Pipeline never executed",
                detail=f"{p.name} is defined in {p.file} but has no execution history and no output CSV in the bucket.",
                object_fqn=p.name,
            ))
        elif p.runs and p.runs.runs_failed > 0 and p.runs.runs_total > 0 and (p.runs.runs_failed / p.runs.runs_total) >= 0.05:
            pct = 100 * p.runs.runs_failed / p.runs.runs_total
            flags.append(InventoryFlag(
                severity="critical" if pct >= 10 else "warn",
                title=f"Pipeline failure rate {pct:.1f}%",
                detail=f"{p.name}: {p.runs.runs_failed} of {p.runs.runs_total} runs failed.",
                object_fqn=p.name,
            ))
    # Runs in audit log without a matching XML definition — undocumented ETL
    for orphan in inv.orphan_runs:
        flags.append(InventoryFlag(
            severity="critical",
            title="Pipeline running without source definition",
            detail=f"{orphan.pipeline_name} appears in ETL_EXECUTION_LOGS ({orphan.runs.runs_total} runs) but no XML pipeline definition exists. Undocumented ETL is a governance risk.",
            object_fqn=orphan.pipeline_name,
        ))
    return flags


def _count_by(items, key) -> dict[str, int]:
    out: dict[str, int] = {}
    for it in items:
        k = key(it)
        out[k] = out.get(k, 0) + 1
    return out
