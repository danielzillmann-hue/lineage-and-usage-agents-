"""Lineage agent — deterministic where possible, LLM-augmented for PL/SQL.

Phase 1: parse view source / CTAS / INSERT...SELECT with sqlglot, emit edges with
exact column mappings.
Phase 2: pass procedure source to Claude with the inventory as context, ask for
edges in JSON; merge.
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
from app.models.schema import LineageEdge, LineageGraph

log = logging.getLogger(__name__)

last_result: LineageGraph | None = None


_PROC_PROMPT = """\
You are a senior data engineer extracting lineage from Oracle PL/SQL.

For each INSERT, MERGE, UPDATE, or CREATE-AS-SELECT inside the procedure, output edges
of the form:
  {"source_fqn": "S.T", "source_column": "C", "target_fqn": "S.T", "target_column": "C",
   "operation": "INSERT|MERGE|UPDATE|CTAS|PROC", "transform": "short note or null",
   "origin_object": "<proc fqn>", "confidence": 0.0-1.0}

Rules:
- Use fully qualified names. If schema is missing, use "UNKNOWN".
- Capture transformation hints (CASE, COALESCE, function calls, joins) in "transform".
- One edge per source-column → target-column pair. Skip literals.
- If you cannot determine a column-level mapping, emit a single table-level edge with
  source_column and target_column both null and confidence <= 0.6.
- Return ONLY a JSON array. No prose.
"""


async def run(req: RunRequest, results, emit: EmitFn) -> None:
    global last_result
    inv = results.inventory
    if not inv:
        await log_event(emit, AgentName.LINEAGE, "No inventory in context — running inventory first is recommended", kind="log")
    edges: list[LineageEdge] = []
    unresolved: list[str] = []

    await log_event(emit, AgentName.LINEAGE, "Tracing view definitions with sqlglot")
    if inv:
        for t in inv.tables:
            if t.kind == "VIEW" and t.source_text:
                edges.extend(_edges_from_view(t.fqn, t.source_text))

    await log_event(emit, AgentName.LINEAGE, f"Deterministic pass produced {len(edges)} view edges; analyzing procedures")
    if inv:
        for proc in inv.procedures:
            try:
                edges.extend(await _edges_from_procedure(proc, emit))
            except Exception as e:  # noqa: BLE001
                log.warning("proc lineage failed for %s: %s", proc.name, e)
                unresolved.append(f"{proc.schema_name}.{proc.name}")

    graph = LineageGraph(edges=edges, unresolved=unresolved)
    last_result = graph
    await emit_result(emit, graph)


async def emit_result(emit: EmitFn, graph: LineageGraph) -> None:
    from app.models.run import StreamEvent

    await emit(
        StreamEvent(
            event="result",
            agent=AgentName.LINEAGE,
            data={
                "edges": len(graph.edges),
                "unresolved": len(graph.unresolved),
                "tables": len({e.source_fqn for e in graph.edges} | {e.target_fqn for e in graph.edges}),
            },
        )
    )


def _edges_from_view(view_fqn: str, source_sql: str) -> list[LineageEdge]:
    try:
        tree = sqlglot.parse_one(source_sql, dialect="oracle")
    except Exception as e:  # noqa: BLE001
        log.warning("view sqlglot parse failed for %s: %s", view_fqn, e)
        return []
    edges: list[LineageEdge] = []
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if not select:
        return []
    sources = {t.alias_or_name.upper(): _table_fqn(t) for t in select.find_all(exp.Table)}
    for projection in select.expressions:
        target_col = projection.alias_or_name.upper() if projection.alias_or_name else None
        if not target_col:
            continue
        for col in projection.find_all(exp.Column):
            src_table = (col.table or "").upper()
            src_col = col.name.upper()
            src_fqn = sources.get(src_table) or next(iter(sources.values()), "UNKNOWN.UNKNOWN")
            edges.append(
                LineageEdge(
                    source_fqn=src_fqn,
                    source_column=src_col,
                    target_fqn=view_fqn,
                    target_column=target_col,
                    operation="VIEW",
                    origin_object=view_fqn,
                    confidence=0.95,
                )
            )
    return edges


def _table_fqn(t: exp.Table) -> str:
    schema = (t.db or "UNKNOWN").upper()
    name = t.name.upper()
    return f"{schema}.{name}"


async def _edges_from_procedure(proc, emit: EmitFn) -> list[LineageEdge]:
    text = await stream_thinking(
        emit,
        AgentName.LINEAGE,
        get_settings().lineage_model,
        system=_PROC_PROMPT,
        user=f"Procedure: {proc.schema_name}.{proc.name}\n\nSource:\n{proc.source[:32000]}",
    )
    try:
        start = text.index("[")
        end = text.rindex("]") + 1
        rows: list[dict[str, Any]] = json.loads(text[start:end])
        return [LineageEdge.model_validate(r) for r in rows]
    except Exception as e:  # noqa: BLE001
        log.warning("proc lineage parse failed for %s: %s", proc.name, e)
        return []
