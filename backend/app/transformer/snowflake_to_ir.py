"""Snowflake pipeline → TA-IR DataflowGraph builder.

Mapping from Snowflake constructs to IR node types:

    Snowflake construct                       → IR
    ─────────────────────────────────────────────────────────────────
    STREAM ON TABLE <t>                        → SourceNode marking the
                                                  source table (`is_stream`
                                                  recorded in operations)
    TASK <t> AS <body>                         → one DataflowGraph; the body
                                                  is parsed via the same
                                                  routines below
    TASK ... AFTER <other>                     → DAG dependency on a sibling
                                                  graph (carried in `description`)
    PROCEDURE ... LANGUAGE SQL AS $$…$$        → procedure body is split into
                                                  inner DML statements; each
                                                  becomes its own graph
    INSERT INTO t SELECT …                     → SourceNode + TargetMapping
    MERGE INTO tgt USING src ON …              → LookupNode + ExpressionNode
                                                  + TargetMapping (incremental)
    DELETE FROM …                              → emitted as OperationsScript
    COPY INTO …                                → emitted as OperationsScript
                                                  (data ingestion, not modelled)

IR node types NOT used:
  * NormalizerNode — Snowflake has FLATTEN but it's out of scope here
  * UnionNode      — UNION in subqueries handled inline by SQL gen
  * RouterNode     — no native row router in the Snowflake DDL surface
  * SequenceNode   — surrogate key generation is target-side concern
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

from transformation_core import (
    ColumnDef,
    DataflowGraph,
    ExpressionNode,
    JoinType,
    LookupNode,
    SourceNode,
    TargetMapping,
)

from app.parsers.snowflake_pipeline import (
    SnowflakePipeline,
    SnowflakeProcedure,
    SnowflakeStream,
    SnowflakeTask,
    parse_pipeline,
)

log = logging.getLogger(__name__)


@dataclass
class SnowflakeOperationsScript:
    name: str
    sql: str
    sql_kind: str
    target_table: str
    depends_on: str = ""


@dataclass
class SnowflakeTransformResult:
    pipeline_name: str
    primaries: list[DataflowGraph] = field(default_factory=list)
    operations: list[SnowflakeOperationsScript] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    # Task DAG: task_name -> list of upstream task names (preserved verbatim
    # from the AFTER clause so the pixel-office UI can render the DAG).
    task_dag: dict[str, list[str]] = field(default_factory=dict)


def parse(text: str, filename: str) -> SnowflakeTransformResult:
    pipe = parse_pipeline(text, filename)
    return _to_ir(pipe)


def _to_ir(pipe: SnowflakePipeline) -> SnowflakeTransformResult:
    result = SnowflakeTransformResult(pipeline_name=pipe.name)

    # ── Streams ───────────────────────────────────────────────────────────
    # Streams aren't materialised as graphs on their own; we collect them
    # as a "stream registry" the IR bridge can attach as a SourceNode hint.
    stream_sources: dict[str, str] = {s.name.lower(): s.on_table for s in pipe.streams}

    # ── Procedures (resolve before tasks so CALL <proc> can dereference) ──
    proc_graphs: dict[str, list[DataflowGraph]] = {}
    for proc in pipe.procedures:
        proc_graphs[proc.name.lower()] = list(_procedure_to_graphs(pipe.name, proc, stream_sources))

    # ── Tasks ─────────────────────────────────────────────────────────────
    for task in pipe.tasks:
        result.task_dag[task.name] = list(task.after)
        try:
            graphs = _task_to_graphs(pipe.name, task, stream_sources, proc_graphs)
            result.primaries.extend(graphs)
        except Exception as e:  # noqa: BLE001
            result.warnings.append(f"task {task.name}: {e}")
            log.warning("snowflake task %s build failed", task.name, exc_info=True)

    # ── Stand-alone DML at file top level ─────────────────────────────────
    for stmt in pipe.statements:
        if stmt.kind in {"INSERT", "MERGE"}:
            graphs = _stmt_to_graphs(pipe.name, stmt.kind, stmt.raw, stream_sources)
            result.primaries.extend(graphs)
        elif stmt.kind in {"DELETE", "UPDATE", "COPY"}:
            result.operations.append(SnowflakeOperationsScript(
                name=f"{pipe.name}_{stmt.kind.lower()}",
                sql=stmt.raw,
                sql_kind=stmt.kind.lower(),
                target_table=stmt.target_table or "unknown",
            ))

    # Surface stream registry as a warning-style note (visible in demo output).
    for sn in pipe.streams:
        result.warnings.append(
            f"stream {sn.name} on table {sn.on_table} (is_stream=True)"
        )
    return result


# ─── Per-task / per-procedure graph builders ──────────────────────────────


def _task_to_graphs(
    pipe_name: str,
    task: SnowflakeTask,
    stream_sources: dict[str, str],
    proc_graphs: dict[str, list[DataflowGraph]],
) -> list[DataflowGraph]:
    """A task body is one of:

      * A single MERGE / INSERT / UPDATE / DELETE statement.
      * `CALL proc()` — we substitute the procedure's pre-built graphs.
      * Anything else we don't model — log + skip.
    """
    body = task.body.strip()
    if not body:
        return []

    upper = body.lstrip().upper()
    after_desc = f" (AFTER: {', '.join(task.after)})" if task.after else ""

    if upper.startswith("CALL "):
        m = re.match(r"\s*CALL\s+([A-Za-z0-9_.]+)", body, re.IGNORECASE)
        proc_name = (m.group(1) if m else "").lower()
        graphs = list(proc_graphs.get(proc_name, []))
        # Re-tag with task name so the demo can show "this graph belongs
        # to TASK_X via CALL".
        for g in graphs:
            g.description = f"task {task.name} → {g.description}{after_desc}"
        return graphs

    if upper.startswith("MERGE") or upper.startswith("INSERT") or upper.startswith("UPDATE"):
        graphs = _stmt_to_graphs(pipe_name, _shape(upper), body, stream_sources)
        for g in graphs:
            g.description = f"task {task.name}{after_desc}"
            g.tags = [tag for tag in g.tags] + [f"task:{task.name}"]
        return graphs

    log.warning("snowflake task %s: unhandled body shape: %s", task.name, upper[:40])
    return []


def _procedure_to_graphs(
    pipe_name: str,
    proc: SnowflakeProcedure,
    stream_sources: dict[str, str],
) -> list[DataflowGraph]:
    """Each inner statement in a stored procedure becomes one graph."""
    graphs: list[DataflowGraph] = []
    for inner in proc.inner_statements:
        kind = _shape(inner.lstrip().upper())
        if kind in {"INSERT", "MERGE", "UPDATE"}:
            graphs.extend(_stmt_to_graphs(pipe_name, kind, inner, stream_sources))
    return graphs


# ─── Statement → IR graph ─────────────────────────────────────────────────


def _stmt_to_graphs(
    pipe_name: str,
    kind: str,
    sql: str,
    stream_sources: dict[str, str],
) -> list[DataflowGraph]:
    try:
        tree = sqlglot.parse_one(sql.rstrip(";"), dialect="snowflake")
    except Exception as e:
        log.warning("snowflake: sqlglot parse failed (%s): %s", e, sql[:80])
        return []

    if kind == "INSERT" and isinstance(tree, exp.Insert):
        return [_insert_graph(pipe_name, tree, sql, stream_sources)]
    if kind == "MERGE" and isinstance(tree, exp.Merge):
        return [_merge_graph(pipe_name, tree, sql, stream_sources)]
    if kind == "UPDATE" and isinstance(tree, exp.Update):
        # Treat as a no-source update — wrap as a single-node graph.
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        if not target:
            return []
        cte = f"cte_{_slug(target)}_upd"
        node = SourceNode(cte_name=cte, table_ref=target, columns=[])
        return [DataflowGraph(
            mapping_name=f"{pipe_name}__{_slug(target)}_update",
            description=f"Snowflake UPDATE → {target}",
            nodes=[node],
            target=TargetMapping(target_table=target, columns=[], is_incremental=True),
            table_type="incremental",
        )]
    return []


def _insert_graph(
    pipe_name: str,
    tree: exp.Insert,
    raw: str,
    stream_sources: dict[str, str],
) -> DataflowGraph:
    target = _table_name(tree.this if isinstance(tree.this, exp.Table)
                         else (tree.this.find(exp.Table) if tree.this else None)) or "unknown"
    select = tree.find(exp.Select)
    src_tables = [_table_name(t) for t in (select.find_all(exp.Table) if select else [])
                  if _table_name(t)]
    src_tables = [s for s in src_tables if s]

    columns: list[ColumnDef] = []
    if select is not None:
        for proj in select.expressions:
            name = proj.alias_or_name
            if not name:
                continue
            columns.append(ColumnDef(
                name=name,
                expression=proj.sql(dialect="snowflake"),
                is_passthrough=isinstance(proj, exp.Column),
            ))

    cte = f"cte_{_slug(target)}_src"
    is_stream_input = any(t.lower() in stream_sources for t in src_tables)
    src = SourceNode(
        cte_name=cte,
        table_ref=src_tables[0] if src_tables else "",
        custom_sql=select.sql(dialect="snowflake") if select else "",
        columns=columns,
        joined_tables=list(src_tables[1:]),
    )
    target_map = TargetMapping(
        target_table=target,
        columns=[ColumnDef(name=c.name, expression=c.name) for c in columns],
        is_incremental=True,
    )
    desc = f"Snowflake INSERT → {target}"
    if is_stream_input:
        desc += " [stream-driven]"
    return DataflowGraph(
        mapping_name=f"{pipe_name}__{_slug(target)}_insert",
        description=desc,
        nodes=[src],
        target=target_map,
        all_targets=[target_map],
        table_type="incremental",
    )


def _merge_graph(
    pipe_name: str,
    tree: exp.Merge,
    raw: str,
    stream_sources: dict[str, str],
) -> DataflowGraph:
    target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None) or "unknown"

    using = tree.args.get("using")
    src_table = None
    src_select_sql = ""
    if isinstance(using, exp.Table):
        src_table = using.name
    elif isinstance(using, exp.Subquery):
        inner = using.find(exp.Table)
        src_table = inner.name if inner else None
        inner_sel = using.find(exp.Select)
        src_select_sql = inner_sel.sql(dialect="snowflake") if inner_sel else ""

    on_expr = tree.args.get("on")
    on_sql = on_expr.sql(dialect="snowflake") if on_expr is not None else "TRUE"

    cte_src = f"cte_{_slug(target)}_src"
    is_stream_input = bool(src_table and src_table.lower() in stream_sources)
    src = SourceNode(
        cte_name=cte_src,
        table_ref=src_table or "",
        custom_sql=src_select_sql,
        columns=[],
    )

    cte_match = f"cte_{_slug(target)}_match"
    lkp = LookupNode(
        cte_name=cte_match,
        upstream=cte_src,
        lookup_table=target,
        lookup_alias="tgt",
        join_type=JoinType.LEFT,
        join_condition=on_sql,
        return_columns=[ColumnDef(
            name="merge_match_flag",
            expression="CASE WHEN tgt.* IS NOT NULL THEN 1 ELSE 0 END",
        )],
    )

    set_cols: list[ColumnDef] = []
    insert_cols: list[ColumnDef] = []
    whens_arg = tree.args.get("whens")
    if whens_arg is not None:
        whens_list = whens_arg.expressions if hasattr(whens_arg, "expressions") else []
    else:
        whens_list = []
    for when in whens_list:
        if not isinstance(when, exp.When):
            continue
        action = when.args.get("then")
        if isinstance(action, exp.Update):
            for set_eq in action.args.get("expressions") or []:
                if isinstance(set_eq, exp.EQ) and isinstance(set_eq.left, exp.Column):
                    set_cols.append(ColumnDef(
                        name=set_eq.left.name,
                        expression=set_eq.right.sql(dialect="snowflake"),
                    ))
        elif isinstance(action, exp.Insert):
            for col in (action.this.expressions if action.this else []):
                if isinstance(col, exp.Column):
                    insert_cols.append(ColumnDef(name=col.name, expression=col.name))

    cte_payload = f"cte_{_slug(target)}_payload"
    expr_node = ExpressionNode(
        cte_name=cte_payload,
        upstream=cte_match,
        columns=set_cols + insert_cols,
        pass_upstream=True,
    )

    match_keys = []
    if on_expr is not None:
        for eq in on_expr.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column) and eq.left.name:
                match_keys.append(eq.left.name)

    desc = f"Snowflake MERGE → {target}"
    if is_stream_input:
        desc += " [stream-driven]"

    target_map = TargetMapping(
        target_table=target,
        columns=set_cols + insert_cols,
        is_incremental=True,
        unique_key=match_keys,
    )
    return DataflowGraph(
        mapping_name=f"{pipe_name}__{_slug(target)}_merge",
        description=desc,
        nodes=[src, lkp, expr_node],
        target=target_map,
        all_targets=[target_map],
        table_type="incremental",
    )


# ─── helpers ──────────────────────────────────────────────────────────────


def _shape(upper: str) -> str:
    if upper.startswith("INSERT"):
        return "INSERT"
    if upper.startswith("MERGE"):
        return "MERGE"
    if upper.startswith("UPDATE"):
        return "UPDATE"
    if upper.startswith("DELETE"):
        return "DELETE"
    if upper.startswith("CALL"):
        return "CALL"
    if upper.startswith("COPY"):
        return "COPY"
    return "OTHER"


def _slug(s: str) -> str:
    out = []
    for ch in (s or "").lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "_":
            out.append("_")
    return "".join(out).strip("_") or "node"


def _table_name(t) -> str | None:
    if t is None or not isinstance(t, exp.Table):
        return None
    parts = []
    if t.args.get("catalog"):
        parts.append(t.args["catalog"].name)
    if t.args.get("db"):
        parts.append(t.args["db"].name)
    if t.name:
        parts.append(t.name)
    return ".".join(p for p in parts if p) or None
