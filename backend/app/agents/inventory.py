"""Inventory agent — reads DDL + data-dictionary CSVs, builds a schema model.

Deterministic SQL parsing handles tables/views; Claude classifies layer + domain
and writes inventory flags (e.g., orphan staging, missing PKs, suspect comments).
"""

from __future__ import annotations

import json
import logging
from typing import Any

import sqlglot
from sqlglot import exp

from app.agents.base import EmitFn, log_event, stream_thinking
from app.config import get_settings
from app.models.run import AgentName, RunRequest
from app.models.schema import Column, Domain, Inventory, InventoryFlag, Layer, Procedure, Table
from app.services import gcs

log = logging.getLogger(__name__)

last_result: Inventory | None = None


_LAYER_PROMPT = """\
You are classifying tables in an Oracle data warehouse into layers and domains.

Rules:
- "raw" — verbatim source extracts; names often contain RAW_, SRC_, _LANDING, _STG_RAW
- "staging" — cleansed/conformed but not modeled; names often STG_, STAGE_, _STAGING
- "integration" — dimensional or 3NF integrated model; names often DIM_, FACT_, F_, D_, INT_
- "reporting" — aggregates/marts feeding BI; names often RPT_, MART_, AGG_, DSH_, _REPORT
- If unclear, return "unknown".

Domains for a wealth-management warehouse: member, account, product, adviser, transaction, holding, fee, reference, audit, other.

Return ONLY a JSON array of objects: [{"fqn": "SCHEMA.NAME", "layer": "...", "domain": "...", "rationale": "..."}, ...]
"""


async def run(req: RunRequest, results, emit: EmitFn) -> None:
    global last_result
    await log_event(emit, AgentName.INVENTORY, "Reading DDL and data-dictionary files from GCS")

    ddl_text, dict_files, proc_files = _gather(req)
    await log_event(
        emit,
        AgentName.INVENTORY,
        f"Loaded {len(ddl_text)} DDL files, {len(dict_files)} dictionary files, {len(proc_files)} procedure files",
    )

    tables = _parse_ddl(ddl_text)
    procedures = _parse_procedures(proc_files)
    _enrich_from_dictionary(tables, dict_files)

    # Deterministic prefix-based pre-classification — Gemini may refine, but we
    # always have a meaningful layer/domain even if the LLM is slow or returns junk.
    for t in tables:
        t.layer = _heuristic_layer(t)
        t.domain = _heuristic_domain(t)

    await log_event(
        emit,
        AgentName.INVENTORY,
        f"Parsed {len(tables)} tables/views and {len(procedures)} procedures — refining classification with Gemini",
    )

    classifications = await _classify_layers_and_domains(tables, emit)
    by_fqn = {t.fqn: t for t in tables}
    applied = 0
    for row in classifications:
        t = by_fqn.get(str(row.get("fqn", "")).upper())
        if not t:
            continue
        try:
            t.layer = Layer(row.get("layer", t.layer.value))
            applied += 1
        except (ValueError, AttributeError):
            pass
        try:
            t.domain = Domain(row.get("domain", t.domain.value))
        except (ValueError, AttributeError):
            pass
    await log_event(
        emit, AgentName.INVENTORY,
        f"Layer/domain classification: Gemini refined {applied} of {len(tables)} (rest from heuristic)"
    )

    flags = _heuristic_flags(tables)

    inv = Inventory(tables=tables, procedures=procedures, flags=flags)
    last_result = inv
    await emit_result(emit, inv)


async def emit_result(emit: EmitFn, inv: Inventory) -> None:
    from app.models.run import StreamEvent

    await emit(
        StreamEvent(
            event="result",
            agent=AgentName.INVENTORY,
            data={
                "tables": len(inv.tables),
                "procedures": len(inv.procedures),
                "flags": len(inv.flags),
                "by_layer": _count_by(inv.tables, lambda t: t.layer.value),
                "by_domain": _count_by(inv.tables, lambda t: t.domain.value),
            },
        )
    )


def _count_by(items, key) -> dict[str, int]:
    out: dict[str, int] = {}
    for it in items:
        k = key(it)
        out[k] = out.get(k, 0) + 1
    return out


def _gather(req: RunRequest) -> tuple[list[tuple[str, str]], list[tuple[str, str]], list[tuple[str, str]]]:
    ddl: list[tuple[str, str]] = []
    dict_: list[tuple[str, str]] = []
    procs: list[tuple[str, str]] = []
    for f in gcs.iter_classified(req.bucket, req.prefix):
        if f.kind == "ddl":
            ddl.append((f.name, gcs.read_text(req.bucket, f.name)))
        elif f.kind == "dictionary":
            dict_.append((f.name, gcs.read_text(req.bucket, f.name)))
        elif f.name.lower().endswith(".pls") or "dba_source" in f.name.lower():
            procs.append((f.name, gcs.read_text(req.bucket, f.name)))
    return ddl, dict_, procs


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
        # First pass: scan table-level constraints for PRIMARY KEY (col, col, ...)
        # sqlglot wraps `CONSTRAINT name PRIMARY KEY (...)` as Constraint(expressions=[PrimaryKey(...)]).
        for item in this.expressions or []:
            pk: exp.PrimaryKey | None = None
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

        # Second pass: build columns; flag PKs from either column- or table-level
        for col in this.expressions or []:
            if not isinstance(col, exp.ColumnDef):
                continue
            constraints = col.args.get("constraints") or []
            col_pk = any(
                isinstance(c, exp.PrimaryKeyColumnConstraint)
                or (hasattr(c, "kind") and isinstance(c.kind, exp.PrimaryKeyColumnConstraint))
                for c in constraints
            )
            cols.append(
                Column(
                    name=col.name.upper(),
                    data_type=col.args.get("kind").sql() if col.args.get("kind") else "UNKNOWN",
                    nullable=not any(
                        isinstance(c, exp.NotNullColumnConstraint)
                        or (hasattr(c, "kind") and isinstance(c.kind, exp.NotNullColumnConstraint))
                        for c in constraints
                    ),
                    is_pk=col_pk or col.name.upper() in table_pk_columns,
                )
            )

    source_text = stmt.expression.sql(dialect="oracle") if stmt.expression else None
    return Table(
        schema_name=schema_name,
        name=name,
        kind="VIEW" if kind in {"VIEW", "MATERIALIZED VIEW", "MVIEW"} else "TABLE",
        columns=cols,
        source_text=source_text,
    )


def _parse_procedures(files: list[tuple[str, str]]) -> list[Procedure]:
    procs: list[Procedure] = []
    for name, text in files:
        # naive — colleague's dump format will pin this down
        procs.append(
            Procedure(
                schema_name="UNKNOWN",
                name=name.split("/")[-1].rsplit(".", 1)[0].upper(),
                kind="PROCEDURE",
                source=text,
            )
        )
    return procs


def _enrich_from_dictionary(tables: list[Table], files: list[tuple[str, str]]) -> None:
    """If we have ALL_TAB_COLUMNS / DBA_SEGMENTS exports, fill row counts and bytes.

    Format-dependent — finalize once the colleague's extract format is known.
    """
    # Placeholder — implementation arrives with extract sample.
    return


_LAYER_RULES: list[tuple[str, Layer]] = [
    # (token in schema or table name, layer) — first match wins
    ("WH_RAW.", Layer.RAW),
    ("WH_STG.", Layer.STAGING),
    ("WH_DW.", Layer.INTEGRATION),
    ("WH_RPT.", Layer.REPORTING),
    ("WH_LEGACY.", Layer.UNKNOWN),
    ("RAW_", Layer.RAW),
    ("STG_", Layer.STAGING),
    ("STAGE_", Layer.STAGING),
    ("DIM_", Layer.INTEGRATION),
    ("FACT_", Layer.INTEGRATION),
    ("F_", Layer.INTEGRATION),
    ("D_", Layer.INTEGRATION),
    ("BRIDGE_", Layer.INTEGRATION),
    ("INT_", Layer.INTEGRATION),
    ("RPT_", Layer.REPORTING),
    ("MART_", Layer.REPORTING),
    ("AGG_", Layer.REPORTING),
    ("DSH_", Layer.REPORTING),
    ("V_", Layer.UNKNOWN),  # views — schema prefix usually wins above
    ("LEG_", Layer.UNKNOWN),
]

_DOMAIN_HINTS: list[tuple[str, Domain]] = [
    ("MEMBER", Domain.MEMBER),
    ("CLIENT", Domain.MEMBER),
    ("CUSTOMER", Domain.MEMBER),
    ("INVESTOR", Domain.MEMBER),
    ("ACCOUNT", Domain.ACCOUNT),
    ("HOLDING", Domain.HOLDING),
    ("FUND", Domain.HOLDING),
    ("PORTFOLIO", Domain.HOLDING),
    ("TRANSACTION", Domain.TRANSACTION),
    ("TXN", Domain.TRANSACTION),
    ("FEE", Domain.FEE),
    ("ADVISER", Domain.ADVISER),
    ("ADVISOR", Domain.ADVISER),
    ("PRODUCT", Domain.PRODUCT),
    ("DATE", Domain.REFERENCE),
    ("REF_", Domain.REFERENCE),
    ("AUDIT", Domain.AUDIT),
]


def _heuristic_layer(t: Table) -> Layer:
    fqn_upper = t.fqn.upper()
    name_upper = t.name.upper()
    for token, layer in _LAYER_RULES:
        if token in fqn_upper or name_upper.startswith(token):
            return layer
    return Layer.UNKNOWN


def _heuristic_domain(t: Table) -> Domain:
    name_upper = t.name.upper()
    for token, dom in _DOMAIN_HINTS:
        if token in name_upper:
            return dom
    return Domain.OTHER


def _heuristic_flags(tables: list[Table]) -> list[InventoryFlag]:
    flags: list[InventoryFlag] = []
    for t in tables:
        if t.kind == "TABLE" and not any(c.is_pk for c in t.columns):
            flags.append(
                InventoryFlag(
                    severity="warn",
                    title="Table has no primary key",
                    detail=f"{t.fqn} has no PK constraint declared in DDL.",
                    object_fqn=t.fqn,
                )
            )
    return flags


async def _classify_layers_and_domains(tables: list[Table], emit: EmitFn) -> list[dict[str, Any]]:
    if not tables:
        return []
    payload = [
        {"fqn": t.fqn, "kind": t.kind, "columns": [c.name for c in t.columns][:10]}
        for t in tables
    ]
    text = await stream_thinking(
        emit,
        AgentName.INVENTORY,
        get_settings().inventory_model,
        system=_LAYER_PROMPT,
        user=json.dumps(payload, indent=2),
        json_mode=True,
    )
    try:
        # response_mime_type=json gives us a clean payload, but be defensive about
        # responseSchema-less calls returning {"items": [...]} or wrapping in fences.
        s = text.strip()
        if s.startswith("```"):
            s = s.strip("`").lstrip("json").strip()
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            for k in ("items", "results", "tables", "data"):
                if isinstance(parsed.get(k), list):
                    return parsed[k]
            return []
        return parsed
    except Exception as e:  # noqa: BLE001
        log.warning("classification parse failed: %s; first 200=%r", e, text[:200])
        return []
