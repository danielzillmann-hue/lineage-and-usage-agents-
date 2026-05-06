"""Deterministic parser for the super-fund demo's ETL XML pipeline format.

Pipeline DAG:
    <extract id=...>     SQL → set of (table, column) at the step level
    <transform id... input=...>
        <calculate_age dob_column result_column/>
        <concat cols result_column/>
        <drop columns/>
        <math op col1 val2 result_column/>
        <filter_text column contains/>            # row-level only
        <simulate_performance/>                   # opaque, adds columns
    <join id... left right on how/>
    <aggregate id... input group_by>
        <sum column result_column/>
        <count result_column/>
    <load input type path/>

We expand to column-level lineage edges:
    (oracle_table, oracle_column) ─[extract]→ (step, column) ─...→ (csv_file, column)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from xml.etree import ElementTree as ET

import sqlglot
from sqlglot import exp

log = logging.getLogger(__name__)


@dataclass
class StepNode:
    id: str
    kind: str  # extract | transform | join | aggregate | load
    inputs: list[str] = field(default_factory=list)
    raw: ET.Element | None = None
    # Column model: list preserves ordering. Each column has provenance — which
    # upstream (step, column) feeds it. When None it's a freshly created column.
    columns: list[str] = field(default_factory=list)
    column_sources: dict[str, list[tuple[str | None, str | None]]] = field(default_factory=dict)
    # For extract steps:
    source_tables: list[str] = field(default_factory=list)
    source_query: str | None = None
    # For load steps:
    output_path: str | None = None
    # Within-step ops, for human-readable transform descriptions
    operations: list[str] = field(default_factory=list)


@dataclass
class Pipeline:
    name: str
    file: str
    connection: dict[str, str] = field(default_factory=dict)
    steps: dict[str, StepNode] = field(default_factory=dict)
    load_step_id: str | None = None


@dataclass
class ColumnEdge:
    """A column-level lineage edge expanded across the pipeline DAG."""
    source_kind: str  # "oracle" | "step" | "load"
    source_object: str  # table FQN or step id
    source_column: str
    target_kind: str
    target_object: str
    target_column: str
    operation: str
    detail: str | None = None
    pipeline: str = ""


# ─── 1. Parse one XML file ────────────────────────────────────────────────


def parse_pipeline(xml_text: str, filename: str) -> Pipeline | None:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log.warning("XML parse failed for %s: %s", filename, e)
        return None
    if root.tag != "pipeline":
        return None

    pl = Pipeline(name=root.attrib.get("name", filename), file=filename)
    conn_el = root.find(".//connection")
    if conn_el is not None:
        pl.connection = {k: v for k, v in conn_el.attrib.items() if k != "password"}

    steps_el = root.find("steps")
    if steps_el is None:
        return pl

    for child in steps_el:
        if child.tag == "extract":
            _parse_extract(child, pl)
        elif child.tag == "transform":
            _parse_transform(child, pl)
        elif child.tag == "join":
            _parse_join(child, pl)
        elif child.tag == "aggregate":
            _parse_aggregate(child, pl)
        elif child.tag == "load":
            _parse_load(child, pl)
    return pl


def _parse_extract(el: ET.Element, pl: Pipeline) -> None:
    step_id = el.attrib.get("id") or f"extract_{len(pl.steps)}"
    node = StepNode(id=step_id, kind="extract")
    q = (el.findtext("query") or "").strip()
    node.source_query = q
    cols, tables = _parse_select_columns(q)
    node.columns = cols
    node.source_tables = tables
    # Provenance: each output column maps to the table it was selected from.
    for c in cols:
        # Pick first table that 'matches' — for a single-table SELECT this is exact.
        # For joins we punt to JOIN steps; here we just attribute to first table.
        src = (tables[0], c) if tables else (None, c)
        node.column_sources[c] = [src]
    pl.steps[step_id] = node


def _parse_transform(el: ET.Element, pl: Pipeline) -> None:
    step_id = el.attrib.get("id") or f"transform_{len(pl.steps)}"
    inp = el.attrib.get("input")
    node = StepNode(id=step_id, kind="transform", inputs=[inp] if inp else [])

    # Start from the upstream step's columns
    upstream = pl.steps.get(inp) if inp else None
    if upstream:
        node.columns = list(upstream.columns)
        for c in upstream.columns:
            node.column_sources[c] = [(inp, c)]

    # Apply each transform op in order
    for op in list(el):
        if op.tag == "calculate_age":
            dob = (op.attrib.get("dob_column") or "").upper()
            res = (op.attrib.get("result_column") or "").upper()
            if res and res not in node.columns:
                node.columns.append(res)
            node.column_sources[res] = [(inp, dob)] if dob else []
            node.operations.append(f"calculate_age({dob}) → {res}")
        elif op.tag == "concat":
            cols = [c.strip().upper() for c in (op.attrib.get("cols") or "").split(",") if c.strip()]
            res = (op.attrib.get("result_column") or "").upper()
            sep = op.attrib.get("separator", " ")
            if res and res not in node.columns:
                node.columns.append(res)
            node.column_sources[res] = [(inp, c) for c in cols]
            node.operations.append(f"concat({','.join(cols)} sep={sep!r}) → {res}")
        elif op.tag == "drop":
            cols = [c.strip().upper() for c in (op.attrib.get("columns") or "").split(",") if c.strip()]
            node.columns = [c for c in node.columns if c not in cols]
            for c in cols:
                node.column_sources.pop(c, None)
            node.operations.append(f"drop({','.join(cols)})")
        elif op.tag == "math":
            col1 = (op.attrib.get("col1") or "").upper()
            res = (op.attrib.get("result_column") or "").upper()
            opname = op.attrib.get("operation", "math")
            val2 = op.attrib.get("val2", op.attrib.get("col2", ""))
            if res and res not in node.columns:
                node.columns.append(res)
            node.column_sources[res] = [(inp, col1)] if col1 else []
            node.operations.append(f"math({opname} {col1} {val2}) → {res}")
        elif op.tag == "filter_text":
            col = (op.attrib.get("column") or "").upper()
            contains = op.attrib.get("contains", "")
            node.operations.append(f"filter_text({col} contains={contains!r})")
        elif op.tag == "rename":
            old = (op.attrib.get("old_col") or "").upper()
            new = op.attrib.get("new_col") or old
            if old in node.columns:
                idx = node.columns.index(old)
                node.columns[idx] = new
                node.column_sources[new] = node.column_sources.pop(old, [(inp, old)])
            node.operations.append(f"rename({old} → {new})")
        elif op.tag == "calculate_category":
            col = (op.attrib.get("column") or "").upper()
            res = (op.attrib.get("result_column") or "").upper()
            low = op.attrib.get("low", "")
            high = op.attrib.get("high", "")
            if res and res not in node.columns:
                node.columns.append(res)
            node.column_sources[res] = [(inp, col)] if col else []
            node.operations.append(f"calculate_category({col} low={low} high={high}) → {res}")
        elif op.tag in ("filter", "sort"):
            # Row-level only: no column-set changes; record condition for the transcript.
            attrs = ", ".join(f"{k}={v!r}" for k, v in op.attrib.items())
            node.operations.append(f"{op.tag}({attrs})")
        elif op.tag == "simulate_performance":
            node.operations.append("simulate_performance(opaque)")
        else:
            # Best-effort fallback: if it has column→result_column attrs, capture them
            col = (op.attrib.get("column") or op.attrib.get("col1") or "").upper()
            res = (op.attrib.get("result_column") or "").upper()
            if res:
                if res not in node.columns:
                    node.columns.append(res)
                node.column_sources[res] = [(inp, col)] if col else []
            node.operations.append(f"{op.tag}({op.attrib})")

    pl.steps[step_id] = node


def _parse_join(el: ET.Element, pl: Pipeline) -> None:
    step_id = el.attrib.get("id") or f"join_{len(pl.steps)}"
    left = el.attrib.get("left")
    right = el.attrib.get("right")
    on = (el.attrib.get("on") or "").upper()
    how = el.attrib.get("how", "inner")
    node = StepNode(id=step_id, kind="join", inputs=[i for i in (left, right) if i])
    node.operations.append(f"{how} join on {on}")

    # Output columns = left ∪ right (deduped)
    seen: set[str] = set()
    for inp in (left, right):
        upstream = pl.steps.get(inp) if inp else None
        if not upstream:
            continue
        for c in upstream.columns:
            if c in seen:
                # Same column from both sides — merge sources
                node.column_sources[c].append((inp, c))
                continue
            seen.add(c)
            node.columns.append(c)
            node.column_sources[c] = [(inp, c)]
    pl.steps[step_id] = node


def _parse_aggregate(el: ET.Element, pl: Pipeline) -> None:
    step_id = el.attrib.get("id") or f"aggregate_{len(pl.steps)}"
    inp = el.attrib.get("input")
    group_by = [c.strip().upper() for c in (el.attrib.get("group_by") or "").split(",") if c.strip()]
    node = StepNode(id=step_id, kind="aggregate", inputs=[inp] if inp else [])
    upstream = pl.steps.get(inp) if inp else None

    for c in group_by:
        node.columns.append(c)
        node.column_sources[c] = [(inp, c)] if upstream else []

    for op in list(el):
        col = (op.attrib.get("column") or "").upper()
        res = (op.attrib.get("result_column") or "").upper()
        if not res:
            continue
        node.columns.append(res)
        node.column_sources[res] = [(inp, col)] if col else []
        node.operations.append(f"{op.tag}({col}) → {res}")
    pl.steps[step_id] = node


def _parse_load(el: ET.Element, pl: Pipeline) -> None:
    step_id = "load"
    inp = el.attrib.get("input")
    path = el.attrib.get("path")
    node = StepNode(id=step_id, kind="load", inputs=[inp] if inp else [], output_path=path)
    upstream = pl.steps.get(inp) if inp else None
    if upstream:
        node.columns = list(upstream.columns)
        for c in upstream.columns:
            node.column_sources[c] = [(inp, c)]
    pl.steps[step_id] = node
    pl.load_step_id = step_id


# ─── 2. SQL helper ────────────────────────────────────────────────────────


def _parse_select_columns(query: str) -> tuple[list[str], list[str]]:
    """Return (columns_in_select, source_tables) from a SELECT statement."""
    if not query:
        return [], []
    try:
        tree = sqlglot.parse_one(query, dialect="oracle")
    except Exception:
        return [], []
    cols: list[str] = []
    tables: list[str] = []
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if not select:
        return [], []
    for proj in select.expressions:
        name = proj.alias_or_name
        if name:
            cols.append(name.upper())
    for t in select.find_all(exp.Table):
        # We don't have schema here — use just the name; agent will normalize.
        tables.append(t.name.upper())
    return cols, tables


# ─── 3. Expand DAG into edge list ─────────────────────────────────────────


def to_lineage_edges(pl: Pipeline) -> list[ColumnEdge]:
    """Produce column-level edges for one pipeline.

    Three flavors of edge:
      - extract:    (oracle.table, col) → (step, col)
      - intra-pipe: (step, col)         → (step, col)
      - load:       (step, col)         → (csv_path, col)
    """
    edges: list[ColumnEdge] = []
    csv_path = None
    if pl.load_step_id:
        csv_path = pl.steps[pl.load_step_id].output_path

    for step in pl.steps.values():
        if step.kind == "extract":
            for col, sources in step.column_sources.items():
                for tbl, src_col in sources:
                    if tbl is None:
                        continue
                    edges.append(ColumnEdge(
                        source_kind="oracle", source_object=tbl, source_column=src_col or col,
                        target_kind="step", target_object=f"{pl.name}.{step.id}", target_column=col,
                        operation="extract",
                        detail=(step.source_query or "")[:200],
                        pipeline=pl.name,
                    ))
        else:
            for col, sources in step.column_sources.items():
                for src_step, src_col in sources:
                    if not src_step or not src_col:
                        continue
                    target_obj = (csv_path or step.id) if step.kind == "load" else f"{pl.name}.{step.id}"
                    target_kind = "load" if step.kind == "load" else "step"
                    edges.append(ColumnEdge(
                        source_kind="step", source_object=f"{pl.name}.{src_step}", source_column=src_col,
                        target_kind=target_kind, target_object=target_obj, target_column=col,
                        operation=step.kind if step.kind != "load" else "load",
                        detail="; ".join(step.operations) if step.operations else None,
                        pipeline=pl.name,
                    ))
    return edges


def collapse_to_table_level(pl: Pipeline) -> list[ColumnEdge]:
    """Compact view: oracle table → csv path, with op="pipeline" annotation.

    Skip the intra-pipeline plumbing — useful for the high-level overview graph.
    """
    if not pl.load_step_id:
        return []
    csv_path = pl.steps[pl.load_step_id].output_path
    if not csv_path:
        return []
    src_tables: set[str] = set()
    for step in pl.steps.values():
        if step.kind == "extract":
            src_tables.update(step.source_tables)
    edges: list[ColumnEdge] = []
    for t in sorted(src_tables):
        edges.append(ColumnEdge(
            source_kind="oracle", source_object=t, source_column="*",
            target_kind="load", target_object=csv_path, target_column="*",
            operation="pipeline", detail=pl.name, pipeline=pl.name,
        ))
    return edges


# ─── 4. Convenience: parse all XMLs in a list of (filename, content) ──────


def parse_all(files: list[tuple[str, str]]) -> list[Pipeline]:
    out: list[Pipeline] = []
    for name, text in files:
        if not name.lower().endswith(".xml"):
            continue
        pl = parse_pipeline(text, name)
        if pl is not None:
            out.append(pl)
    return out
