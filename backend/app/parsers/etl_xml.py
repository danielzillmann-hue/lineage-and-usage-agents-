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
    kind: str  # extract | extract_csv | transform | join | aggregate | load | execute_sql
    inputs: list[str] = field(default_factory=list)
    raw: ET.Element | None = None
    # Column model: list preserves ordering. Each column has provenance — which
    # upstream (step, column) feeds it. When None it's a freshly created column.
    columns: list[str] = field(default_factory=list)
    column_sources: dict[str, list[tuple[str | None, str | None]]] = field(default_factory=dict)
    # For extract / execute_sql steps:
    source_tables: list[str] = field(default_factory=list)
    source_query: str | None = None
    # For load / execute_sql steps:
    output_path: str | None = None     # CSV file path
    output_table: str | None = None    # Oracle target table (load type="oracle")
    # For extract_csv steps:
    external_path: str | None = None
    # SQL-driven steps (execute_sql) — INSERT/UPDATE/DELETE/TRUNCATE/MERGE
    sql_kind: str | None = None
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
        elif child.tag == "extract_csv":
            _parse_extract_csv(child, pl)
        elif child.tag == "transform":
            _parse_transform(child, pl)
        elif child.tag == "join":
            _parse_join(child, pl)
        elif child.tag == "aggregate":
            _parse_aggregate(child, pl)
        elif child.tag == "load":
            _parse_load(child, pl)
        elif child.tag == "execute_sql":
            _parse_execute_sql(child, pl)
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
    # Make load step ids unique — a single pipeline can load to multiple targets.
    base = el.attrib.get("id") or "load"
    step_id = base if base not in pl.steps else f"{base}_{len(pl.steps)}"
    inp = el.attrib.get("input")
    path = el.attrib.get("path")
    table = el.attrib.get("table")
    load_type = (el.attrib.get("type") or "").lower()
    node = StepNode(
        id=step_id, kind="load", inputs=[inp] if inp else [],
        output_path=path if (path and load_type != "oracle") else None,
        output_table=(table.upper() if table and (load_type == "oracle" or table) else None),
    )
    upstream = pl.steps.get(inp) if inp else None
    if upstream:
        node.columns = list(upstream.columns)
        for c in upstream.columns:
            node.column_sources[c] = [(inp, c)]
    if node.output_table:
        node.operations.append(f"load → {node.output_table}")
    pl.steps[step_id] = node
    # Only set the pipeline's primary load_step_id for the CSV output (the
    # canonical "deliverable" of the pipeline). Oracle table loads are recorded
    # but don't override the CSV target.
    if path and load_type != "oracle":
        pl.load_step_id = step_id
    elif pl.load_step_id is None:
        pl.load_step_id = step_id


def _parse_extract_csv(el: ET.Element, pl: Pipeline) -> None:
    """External CSV input (e.g. tax_brackets.csv). Renders as 'external'."""
    step_id = el.attrib.get("id") or f"extract_csv_{len(pl.steps)}"
    path = el.attrib.get("path") or ""
    node = StepNode(id=step_id, kind="extract", external_path=path)
    # We don't know the columns of an external CSV — leave empty; lineage
    # collapses table-level when columns aren't available.
    node.operations.append(f"read external {path.split('/')[-1]}")
    pl.steps[step_id] = node


def _parse_execute_sql(el: ET.Element, pl: Pipeline) -> None:
    """SQL-driven steps — INSERT INTO / UPDATE / DELETE / TRUNCATE / MERGE.

    Uses sqlglot to extract source tables and the target table. Produces
    column-level edges where possible (column-aligned INSERTs); falls back to
    table-level edges for complex statements.
    """
    step_id = el.attrib.get("id") or f"sql_{len(pl.steps)}"
    query = (el.findtext("query") or "").strip()
    node = StepNode(id=step_id, kind="execute_sql", source_query=query)
    node.sql_kind, target, sources = _classify_sql(query)
    if target:
        node.output_table = target.upper()
    for s in sources:
        if s.upper() not in [t.upper() for t in node.source_tables]:
            node.source_tables.append(s.upper())
    if node.sql_kind:
        node.operations.append(f"{node.sql_kind.lower()} → {target or '(none)'}")
    pl.steps[step_id] = node


def _classify_sql(sql: str) -> tuple[str | None, str | None, list[str]]:
    """Return (kind, target_table, source_tables).

    kind ∈ {INSERT, UPDATE, DELETE, TRUNCATE, MERGE, SELECT, None}
    """
    if not sql:
        return None, None, []
    try:
        tree = sqlglot.parse_one(sql, dialect="oracle")
    except Exception:
        return None, None, []
    if isinstance(tree, exp.Insert):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table)
                             else tree.this.find(exp.Table) if tree.this else None)
        sources: list[str] = []
        sel = tree.find(exp.Select)
        if sel:
            for t in sel.find_all(exp.Table):
                n = _table_name(t)
                if n:
                    sources.append(n)
        return "INSERT", target, sources
    if isinstance(tree, exp.Update):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table) if _table_name(t)]
        # Drop the target from the source list — UPDATE doesn't move from itself
        if target:
            sources = [s for s in sources if s and s.upper() != target.upper()]
        return "UPDATE", target, [s for s in sources if s]
    if isinstance(tree, exp.Delete):
        return "DELETE", _table_name(tree.this if isinstance(tree.this, exp.Table) else None), []
    if isinstance(tree, exp.TruncateTable):
        # sqlglot may parse TRUNCATE as Command; handle below if so.
        tbl = tree.this if isinstance(tree.this, exp.Table) else None
        return "TRUNCATE", _table_name(tbl), []
    if isinstance(tree, exp.Merge):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table)
                   if _table_name(t) and _table_name(t) != target]
        return "MERGE", target, [s for s in sources if s]
    if isinstance(tree, exp.Select):
        sources = [_table_name(t) for t in tree.find_all(exp.Table) if _table_name(t)]
        return "SELECT", None, [s for s in sources if s]
    # Fallback for parser-as-Command (e.g. TRUNCATE in some dialects)
    text = sql.strip().upper()
    if text.startswith("TRUNCATE"):
        # Naive extract: "TRUNCATE TABLE NAME" → NAME
        parts = text.split()
        for i, t in enumerate(parts):
            if t == "TABLE" and i + 1 < len(parts):
                return "TRUNCATE", parts[i + 1].rstrip(";"), []
        return "TRUNCATE", None, []
    return None, None, []


def _table_name(t) -> str | None:
    if t is None:
        return None
    if isinstance(t, exp.Table):
        return t.name.upper()
    return None


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

    Edge flavours:
      - extract:    (oracle.table, col) → (step, col)
      - extract_csv: (external.csv)      → (step, *)
      - intra-pipe: (step, col)          → (step, col)
      - load (csv): (step, col)          → (csv_path, col)
      - load (table): (step, col)        → (oracle.table, col)
      - execute_sql: (oracle.src, *)     → (oracle.tgt, *)   — table-level
    """
    edges: list[ColumnEdge] = []

    for step in pl.steps.values():
        if step.kind == "extract":
            if step.external_path:
                # extract_csv → external source feeding this step
                edges.append(ColumnEdge(
                    source_kind="external", source_object=step.external_path.split("/")[-1],
                    source_column="*",
                    target_kind="step", target_object=f"{pl.name}.{step.id}", target_column="*",
                    operation="extract",
                    detail=f"external CSV: {step.external_path}",
                    pipeline=pl.name,
                ))
                continue
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
        elif step.kind == "execute_sql":
            # Route through the pipeline step so the pipeline appears as a
            # node in the chain (otherwise SQL-only stg pipelines disappear).
            if step.output_table and step.source_tables:
                for src in step.source_tables:
                    edges.append(ColumnEdge(
                        source_kind="oracle", source_object=src, source_column="*",
                        target_kind="step", target_object=f"{pl.name}.{step.id}", target_column="*",
                        operation=(step.sql_kind or "execute_sql").lower(),
                        detail=(step.source_query or "")[:200],
                        pipeline=pl.name,
                    ))
                edges.append(ColumnEdge(
                    source_kind="step", source_object=f"{pl.name}.{step.id}", source_column="*",
                    target_kind="oracle", target_object=step.output_table, target_column="*",
                    operation=(step.sql_kind or "execute_sql").lower(),
                    detail=(step.source_query or "")[:200],
                    pipeline=pl.name,
                ))
            elif step.output_table:
                # No sources detected (e.g. INSERT INTO X VALUES) — still emit
                # the pipeline → table edge so the pipeline owns the table.
                edges.append(ColumnEdge(
                    source_kind="step", source_object=f"{pl.name}.{step.id}", source_column="*",
                    target_kind="oracle", target_object=step.output_table, target_column="*",
                    operation=(step.sql_kind or "execute_sql").lower(),
                    detail=(step.source_query or "")[:200],
                    pipeline=pl.name,
                ))
            elif step.source_tables and step.sql_kind == "SELECT":
                for src in step.source_tables:
                    edges.append(ColumnEdge(
                        source_kind="oracle", source_object=src, source_column="*",
                        target_kind="step", target_object=f"{pl.name}.{step.id}", target_column="*",
                        operation="select",
                        detail=(step.source_query or "")[:200],
                        pipeline=pl.name,
                    ))
        elif step.kind == "load":
            # Determine the target — prefer Oracle table, else CSV path.
            if step.output_table:
                for col, sources in step.column_sources.items():
                    for src_step, src_col in sources:
                        if not src_step or not src_col:
                            continue
                        edges.append(ColumnEdge(
                            source_kind="step", source_object=f"{pl.name}.{src_step}", source_column=src_col,
                            target_kind="oracle", target_object=step.output_table, target_column=col,
                            operation="load",
                            detail=f"load → {step.output_table}",
                            pipeline=pl.name,
                        ))
            elif step.output_path:
                for col, sources in step.column_sources.items():
                    for src_step, src_col in sources:
                        if not src_step or not src_col:
                            continue
                        edges.append(ColumnEdge(
                            source_kind="step", source_object=f"{pl.name}.{src_step}", source_column=src_col,
                            target_kind="load", target_object=step.output_path, target_column=col,
                            operation="load",
                            detail="; ".join(step.operations) if step.operations else None,
                            pipeline=pl.name,
                        ))
        else:
            for col, sources in step.column_sources.items():
                for src_step, src_col in sources:
                    if not src_step or not src_col:
                        continue
                    edges.append(ColumnEdge(
                        source_kind="step", source_object=f"{pl.name}.{src_step}", source_column=src_col,
                        target_kind="step", target_object=f"{pl.name}.{step.id}", target_column=col,
                        operation=step.kind,
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
