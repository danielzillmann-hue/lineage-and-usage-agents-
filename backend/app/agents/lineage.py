"""Lineage agent — deterministic column-level lineage from ETL XML pipelines,
augmented with FK relationships from the live DB.

For an XML-based input (the super-fund demo), no LLM is needed: the XML
explicitly declares every column transformation. For DDL/PL-SQL we keep the
sqlglot view tracer + LLM PL/SQL fallback.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import sqlglot
from sqlglot import exp

from app.agents.base import EmitFn, log_event, stream_thinking
from app.config import get_settings
from app.models.run import AgentName, RunRequest, StreamEvent
from app.models.schema import LineageEdge, LineageGraph
from app.parsers.etl_xml import (
    Pipeline as ParsedPipeline,
    parse_pipeline as parse_xml_pipeline,
    to_lineage_edges,
)
from app.services import gcs

log = logging.getLogger(__name__)

last_result: LineageGraph | None = None


_PROC_PROMPT = """\
You are a senior data engineer extracting lineage from Oracle PL/SQL.
Output JSON only — an array of edges of the form
{"source_fqn": "S.T", "source_column": "C", "target_fqn": "S.T",
 "target_column": "C", "operation": "INSERT|MERGE|UPDATE|CTAS|PROC",
 "transform": "short note or null", "origin_object": "<proc fqn>",
 "confidence": 0.0-1.0}
"""


async def run(req: RunRequest, results, emit: EmitFn) -> None:
    global last_result
    inv = results.inventory
    edges: list[LineageEdge] = []
    unresolved: list[str] = []

    # ─── 1. Deterministic XML pipeline lineage (the super-fund demo) ────
    if inv and inv.pipelines:
        await log_event(emit, AgentName.LINEAGE, f"Tracing column-level lineage through {len(inv.pipelines)} ETL pipelines")
        # Re-read the XMLs to walk through the parser (the inventory model is lossy)
        if req.bucket:
            xml_files = [
                (f.name, gcs.read_text(req.bucket, f.name))
                for f in gcs.iter_classified(req.bucket, req.prefix)
                if f.kind == "etl"
            ]
            for name, text in xml_files:
                pl = parse_xml_pipeline(text, name)
                if not pl:
                    continue
                for e in to_lineage_edges(pl):
                    edges.append(LineageEdge(
                        source_fqn=_fqn_for(e.source_kind, e.source_object),
                        source_column=e.source_column,
                        target_fqn=_fqn_for(e.target_kind, e.target_object),
                        target_column=e.target_column,
                        operation=e.operation,
                        transform=e.detail,
                        origin_object=pl.name,
                        confidence=1.0,
                    ))
        await log_event(emit, AgentName.LINEAGE, f"Pipeline edges: {len(edges)}")

    # ─── 2. FK-graph edges (data model relationships from live DB) ──────
    if inv:
        fk_count = 0
        for t in inv.tables:
            for c in t.columns:
                if c.is_fk and c.fk_target:
                    ref_table, ref_col = c.fk_target.split(".", 1) if "." in c.fk_target else (c.fk_target, "")
                    edges.append(LineageEdge(
                        source_fqn=f"{t.schema_name}.{ref_table}",
                        source_column=ref_col,
                        target_fqn=t.fqn,
                        target_column=c.name,
                        operation="fk",
                        transform=f"FK {t.fqn}.{c.name} → {c.fk_target}",
                        origin_object=t.fqn,
                        confidence=1.0,
                    ))
                    fk_count += 1
        if fk_count:
            await log_event(emit, AgentName.LINEAGE, f"FK edges from data model: {fk_count}")

    # ─── 3. View source-SQL tracer ──────────────────────────────────────
    if inv:
        view_count = 0
        for t in inv.tables:
            if t.kind == "VIEW" and t.source_text:
                # Pass the view's own schema so unqualified table refs in the SQL
                # resolve back to it (Oracle implicitly does the same at runtime).
                v_edges = _edges_from_view(t.fqn, t.source_text, default_schema=t.schema_name)
                edges.extend(v_edges)
                view_count += len(v_edges)
        if view_count:
            await log_event(emit, AgentName.LINEAGE, f"View column edges traced: {view_count}")

    # ─── 4. PL/SQL procedures (LLM, only if procs are present) ──────────
    if inv and inv.procedures:
        await log_event(emit, AgentName.LINEAGE, f"Tracing {len(inv.procedures)} PL/SQL procedures with Gemini")
        for proc in inv.procedures:
            try:
                proc_edges = await _edges_from_procedure(proc, emit)
                edges.extend(proc_edges)
            except Exception as e:  # noqa: BLE001
                log.warning("proc lineage failed for %s: %s", proc.name, e)
                unresolved.append(f"{proc.schema_name}.{proc.name}")

    graph = LineageGraph(edges=edges, unresolved=unresolved)
    last_result = graph
    await emit(StreamEvent(
        event="result", agent=AgentName.LINEAGE,
        data={
            "edges": len(edges),
            "tables": len({e.source_fqn for e in edges} | {e.target_fqn for e in edges}),
            "operations": _count_by(edges, lambda e: e.operation),
            "unresolved": len(unresolved),
        },
    ))


# ─── helpers ──────────────────────────────────────────────────────────────


def _fqn_for(kind: str, obj: str) -> str:
    """Map a parser node identifier into a stable FQN for the graph."""
    if kind == "oracle":
        # Oracle table — qualify as SOURCE.{table}. Schema isn't always in the
        # XML query so we use a logical SOURCE namespace.
        return f"SOURCE.{obj.upper()}"
    if kind == "load":
        # CSV file output
        short = obj.split("/")[-1]
        return f"OUTPUTS.{short.removesuffix('.csv').upper()}"
    if kind == "step":
        # Pipeline step — keep as-is (already pipeline.step format)
        return f"PIPELINE.{obj}"
    return obj


def _count_by(items, key) -> dict[str, int]:
    out: dict[str, int] = {}
    for it in items:
        k = key(it)
        out[k] = out.get(k, 0) + 1
    return out


def _edges_from_view(view_fqn: str, source_sql: str, default_schema: str = "UNKNOWN") -> list[LineageEdge]:
    try:
        tree = sqlglot.parse_one(source_sql, dialect="oracle")
    except Exception as e:  # noqa: BLE001
        log.warning("view sqlglot parse failed for %s: %s", view_fqn, e)
        return []
    edges: list[LineageEdge] = []
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if not select:
        return []
    sources = {t.alias_or_name.upper(): _table_fqn(t, default_schema) for t in select.find_all(exp.Table)}
    for projection in select.expressions:
        target_col = projection.alias_or_name.upper() if projection.alias_or_name else None
        if not target_col:
            continue
        for col in projection.find_all(exp.Column):
            src_table = (col.table or "").upper()
            src_col = col.name.upper()
            src_fqn = sources.get(src_table) or next(iter(sources.values()), f"{default_schema.upper()}.UNKNOWN")
            edges.append(LineageEdge(
                source_fqn=src_fqn, source_column=src_col,
                target_fqn=view_fqn, target_column=target_col,
                operation="VIEW", origin_object=view_fqn, confidence=0.95,
            ))
    return edges


def _table_fqn(t: exp.Table, default_schema: str = "UNKNOWN") -> str:
    schema = (t.db or default_schema).upper()
    return f"{schema}.{t.name.upper()}"


async def _edges_from_procedure(proc, emit: EmitFn) -> list[LineageEdge]:
    text = await stream_thinking(
        emit, AgentName.LINEAGE, get_settings().lineage_model,
        system=_PROC_PROMPT,
        user=f"Procedure: {proc.schema_name}.{proc.name}\n\nSource:\n{proc.source[:32000]}",
        json_mode=True,
    )
    try:
        s = text.strip()
        if s.startswith("```"):
            s = s.strip("`").lstrip("json").strip()
        parsed: Any = json.loads(s)
        if isinstance(parsed, dict):
            for k in ("edges", "items", "results", "data"):
                if isinstance(parsed.get(k), list):
                    parsed = parsed[k]
                    break
            else:
                return []
        return [LineageEdge.model_validate(r) for r in parsed]
    except Exception as e:  # noqa: BLE001
        log.warning("proc lineage parse failed for %s: %s", proc.name, e)
        return []
