"""Teradata BTEQ → TA-IR DataflowGraph builder.

Mapping from BTEQ statement kinds to IR node types:

    BTEQ statement                             → IR
    ─────────────────────────────────────────────────────────────────
    CREATE [MULTISET] TABLE x AS (SELECT …)    → SourceNode + (optional)
                                                  ExpressionNode + TargetMapping
    INSERT INTO x SELECT …                     → SourceNode + TargetMapping
    UPDATE … SET …                             → emitted as OperationsScript
    DELETE FROM …                              → emitted as OperationsScript
    DROP TABLE …                               → emitted as OperationsScript
    MERGE INTO tgt USING src ON …              → LookupNode (matched-row detection)
                                                  + ExpressionNode (SET payload)
                                                  + TargetMapping (incremental)
    COLLECT STATISTICS …                       → no IR (recorded as op note)
    Dot-commands (.LOGON / .QUIT / .IF …)      → no IR

IR node types NOT used:
  * NormalizerNode — Teradata has no row-explosion in this script style
  * UnionNode      — UNION ALL exists in SQL but isn't a top-level form
  * RouterNode     — BTEQ has no native row router (use SQL CASE instead)
  * SequenceNode   — surrogate keys via IDENTITY are out of scope here

One BTEQ file produces one or more `DataflowGraph` instances — one per
materialised CTAS / INSERT / MERGE target. UPDATE / DELETE / DROP land in
`SsisOperationsScript`-style sibling operations.
"""

from __future__ import annotations

import logging
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

from app.parsers.teradata_bteq import (
    BteqScript,
    SqlStatement,
    parse_script,
)

log = logging.getLogger(__name__)


@dataclass
class BteqOperationsScript:
    name: str
    sql: str
    sql_kind: str
    target_table: str
    depends_on: str = ""


@dataclass
class BteqTransformResult:
    script_name: str
    primaries: list[DataflowGraph] = field(default_factory=list)
    operations: list[BteqOperationsScript] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    # Echoed for visibility — what BTEQ session controls were declared.
    dot_commands: list[str] = field(default_factory=list)


def parse(text: str, filename: str) -> BteqTransformResult:
    script = parse_script(text, filename)
    return _to_ir(script)


def _to_ir(script: BteqScript) -> BteqTransformResult:
    result = BteqTransformResult(script_name=script.name)
    result.dot_commands = [f".{d.name} {d.args}".strip() for d in script.dot_commands]

    for stmt in script.statements:
        try:
            if stmt.kind == "CTAS":
                graph = _ctas_to_graph(script.name, stmt)
                if graph is not None:
                    result.primaries.append(graph)
            elif stmt.kind == "INSERT":
                graph = _insert_to_graph(script.name, stmt)
                if graph is not None:
                    result.primaries.append(graph)
            elif stmt.kind == "MERGE":
                graph = _merge_to_graph(script.name, stmt)
                if graph is not None:
                    result.primaries.append(graph)
            elif stmt.kind in {"UPDATE", "DELETE", "DROP"}:
                result.operations.append(BteqOperationsScript(
                    name=f"{script.name}_{stmt.kind.lower()}",
                    sql=stmt.raw,
                    sql_kind=stmt.kind.lower(),
                    target_table=stmt.target_table or "unknown",
                ))
            elif stmt.kind == "COLLECT_STATS":
                # Recorded but produces no IR.
                result.warnings.append(
                    f"COLLECT STATISTICS on {stmt.target_table or '?'} (no IR)"
                )
            else:
                result.warnings.append(f"unhandled SQL kind: {stmt.kind}")
        except Exception as e:  # noqa: BLE001
            result.warnings.append(f"{stmt.kind}: {e}")
            log.warning("bteq IR build failed", exc_info=True)
    return result


# ─── CTAS / INSERT (single-source pipelines) ──────────────────────────────


def _ctas_to_graph(script_name: str, stmt: SqlStatement) -> DataflowGraph | None:
    """`CREATE TABLE <tgt> AS (SELECT … FROM <src> [WHERE …])` →
       SourceNode (custom_sql=inner select) + TargetMapping.
    """
    if not stmt.target_table or not stmt.inner_select:
        return None

    columns = _select_output_columns(stmt.inner_select)
    cte_name = f"cte_{_slug(stmt.target_table)}_src"

    src = SourceNode(
        cte_name=cte_name,
        table_ref=stmt.source_tables[0] if stmt.source_tables else "",
        custom_sql=stmt.inner_select,
        columns=columns,
        joined_tables=[t for t in stmt.source_tables[1:]],
    )
    target = TargetMapping(
        target_table=stmt.target_table,
        columns=[ColumnDef(name=c.name, expression=c.name) for c in columns],
    )
    return DataflowGraph(
        mapping_name=f"{script_name}__{_slug(stmt.target_table)}",
        description=f"BTEQ CTAS → {stmt.target_table}",
        nodes=[src],
        target=target,
        all_targets=[target],
        table_type="table",
    )


def _insert_to_graph(script_name: str, stmt: SqlStatement) -> DataflowGraph | None:
    """`INSERT INTO <tgt> SELECT …` — same shape as CTAS, just an
    incremental target. We mark `is_incremental=True`.
    """
    if not stmt.target_table or not stmt.inner_select:
        return None
    columns = _select_output_columns(stmt.inner_select)
    cte_name = f"cte_{_slug(stmt.target_table)}_src"
    src = SourceNode(
        cte_name=cte_name,
        table_ref=stmt.source_tables[0] if stmt.source_tables else "",
        custom_sql=stmt.inner_select,
        columns=columns,
    )
    target = TargetMapping(
        target_table=stmt.target_table,
        columns=[ColumnDef(name=c.name, expression=c.name) for c in columns],
        is_incremental=True,
    )
    return DataflowGraph(
        mapping_name=f"{script_name}__{_slug(stmt.target_table)}_insert",
        description=f"BTEQ INSERT → {stmt.target_table}",
        nodes=[src],
        target=target,
        all_targets=[target],
        table_type="incremental",
    )


# ─── MERGE (SCD-style upsert) ─────────────────────────────────────────────


def _merge_to_graph(script_name: str, stmt: SqlStatement) -> DataflowGraph | None:
    """`MERGE INTO tgt USING src ON cond WHEN MATCHED … WHEN NOT MATCHED …` →

        SourceNode (the USING source)
        LookupNode (LEFT JOIN to tgt on the match key — produces match-status flag)
        ExpressionNode (renders the SET payload + insert columns)
        TargetMapping (incremental, with unique_key = match key)
    """
    if not stmt.target_table:
        return None

    # Re-parse the raw MERGE so we can tease out the USING source, ON
    # condition, and SET assignments — sqlglot keeps these as structured
    # children on `exp.Merge`.
    try:
        tree = sqlglot.parse_one(stmt.raw.rstrip(";"), dialect="teradata")
    except Exception:
        return None
    if not isinstance(tree, exp.Merge):
        return None

    src_expr = tree.args.get("using")
    src_table = None
    if isinstance(src_expr, exp.Table):
        src_table = src_expr.name
    elif isinstance(src_expr, exp.Subquery):
        inner = src_expr.find(exp.Table)
        src_table = inner.name if inner else None

    on_expr = tree.args.get("on")
    on_sql = on_expr.sql(dialect="teradata") if on_expr is not None else "TRUE"

    cte_src = f"cte_{_slug(stmt.target_table)}_src"
    src = SourceNode(
        cte_name=cte_src,
        table_ref=src_table or "",
        columns=[],  # column list resolved at SQL-render time
    )

    cte_lkp = f"cte_{_slug(stmt.target_table)}_match"
    lkp = LookupNode(
        cte_name=cte_lkp,
        upstream=cte_src,
        lookup_table=stmt.target_table,
        lookup_alias="tgt",
        join_type=JoinType.LEFT,
        join_condition=on_sql,
        return_columns=[ColumnDef(
            name="merge_match_flag",
            expression="CASE WHEN tgt.* IS NOT NULL THEN 1 ELSE 0 END",
        )],
    )

    # WHEN-MATCHED SET payload + WHEN-NOT-MATCHED INSERT columns are fused
    # into one ExpressionNode that the SQL generator can later materialise
    # as a real MERGE statement. For the demo we just record the assignment
    # expressions verbatim.
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
                        expression=set_eq.right.sql(dialect="teradata"),
                    ))
        elif isinstance(action, exp.Insert):
            for col in action.this.expressions if action.this else []:
                if isinstance(col, exp.Column):
                    insert_cols.append(ColumnDef(name=col.name, expression=col.name))

    cte_payload = f"cte_{_slug(stmt.target_table)}_payload"
    expr_node = ExpressionNode(
        cte_name=cte_payload,
        upstream=cte_lkp,
        columns=set_cols + insert_cols,
        pass_upstream=True,
    )

    # Pull match keys out of the ON condition (best-effort: equality preds).
    match_keys = _extract_eq_keys(on_expr)
    target = TargetMapping(
        target_table=stmt.target_table,
        columns=set_cols + insert_cols,
        is_incremental=True,
        unique_key=match_keys,
    )
    return DataflowGraph(
        mapping_name=f"{script_name}__{_slug(stmt.target_table)}_merge",
        description=f"BTEQ MERGE → {stmt.target_table}",
        nodes=[src, lkp, expr_node],
        target=target,
        all_targets=[target],
        table_type="incremental",
    )


# ─── helpers ──────────────────────────────────────────────────────────────


def _slug(s: str) -> str:
    out = []
    for ch in (s or "").lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "_":
            out.append("_")
    return "".join(out).strip("_") or "node"


def _select_output_columns(select_sql: str) -> list[ColumnDef]:
    """Pull `alias_or_name` for each top-level projection."""
    try:
        tree = sqlglot.parse_one(select_sql, dialect="teradata")
    except Exception:
        return []
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return []
    cols: list[ColumnDef] = []
    for proj in select.expressions:
        name = proj.alias_or_name
        if not name:
            continue
        cols.append(ColumnDef(
            name=name,
            expression=proj.sql(dialect="teradata"),
            is_passthrough=isinstance(proj, exp.Column),
        ))
    return cols


def _extract_eq_keys(on_expr) -> list[str]:
    """Walk an ON predicate and return the LHS column names of `=` preds."""
    if on_expr is None:
        return []
    keys: list[str] = []
    for eq in on_expr.find_all(exp.EQ):
        if isinstance(eq.left, exp.Column) and eq.left.name:
            keys.append(eq.left.name)
    return keys
