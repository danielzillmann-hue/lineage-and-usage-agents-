"""sqlglot-based helpers for translating raw Oracle SQL into TA-IR fragments.

These are used by both the `<extract>` step processor and the `<execute_sql>`
processor — both contain SELECT-shaped SQL that needs to be deconstructed
into Source / Filter / Aggregator / etc. IR nodes.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from transformation_core import (
    AggregatorNode,
    ColumnDef,
    JoinerNode,
    JoinType,
    SourceNode,
)


@dataclass
class SelectChain:
    """A SELECT statement decomposed into a chain of TA-IR nodes.

    The order is: source(s) → joins → filter (where) → aggregate (group by)
    → final projection. The last node is the chain's output and feeds the
    next step (or the target).
    """
    nodes: list = None        # list of IR nodes in topo order
    final_cte: str = ""       # name of the last node's CTE
    output_columns: list[ColumnDef] = None
    warnings: list[str] = None

    def __post_init__(self) -> None:
        if self.nodes is None:
            self.nodes = []
        if self.output_columns is None:
            self.output_columns = []
        if self.warnings is None:
            self.warnings = []


def parse_select(sql: str, base_name: str) -> SelectChain | None:
    """Translate a SELECT statement into a chain of IR nodes.

    `base_name` is used as a stem for CTE names (e.g. "ext_stg" produces
    cte_ext_stg, cte_ext_stg_agg, etc.).
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="oracle")
    except Exception:
        return None
    if not isinstance(tree, exp.Select):
        return None

    chain = SelectChain()

    # ─── 1. FROM clause: extract base table + any explicit JOINs ────────
    from_clause = tree.find(exp.From)
    if from_clause is None:
        chain.warnings.append("SELECT has no FROM clause")
        return chain
    base_table = from_clause.find(exp.Table)
    if base_table is None:
        chain.warnings.append("FROM clause has no table reference")
        return chain
    base_table_name = base_table.name.lower() if base_table.name else "unknown"

    # WHERE → SourceNode.where (carried directly, BigQuery-rendered)
    where_clause = tree.find(exp.Where)
    where_sql = ""
    if where_clause is not None:
        where_sql = where_clause.this.sql(dialect="bigquery")

    # If the SELECT has explicit JOINs, the original SQL has table aliases
    # referenced by every projection / WHERE / ON. Rebuilding from scratch
    # breaks those refs, so we hand the whole FROM-subtree to the emitter
    # via SourceNode.custom_sql and let it pass through. The aggregate /
    # projection logic below still runs but reads from the custom-SQL CTE.
    join_nodes = list(tree.find_all(exp.Join))
    if join_nodes:
        # SELECT has joins. Aggregation, where, projections all reference
        # table aliases (s, a, m) that only exist inside this CTE — splitting
        # them across separate CTEs would orphan those aliases. So we emit
        # the entire SELECT (including GROUP BY) as one custom_sql source
        # and keep its projection list as the output schema.
        custom = tree.sql(dialect="bigquery")
        custom = _wrap_tables_with_ref(custom, tree)
        proj_columns: list[ColumnDef] = [_projection_to_columndef(p) for p in tree.expressions]
        src_node = SourceNode(
            cte_name=f"cte_{base_name}_src",
            table_ref=base_table_name,
            custom_sql=custom,
            columns=proj_columns,
        )
        chain.nodes.append(src_node)
        chain.final_cte = src_node.cte_name
        chain.output_columns = proj_columns
        return chain

    # Pull the base columns referenced by the SELECT projection or by
    # downstream nodes. We don't try to deduce the full table schema —
    # just the columns the pipeline uses.
    base_cols = _collect_referenced_columns(tree, base_table)
    src_node = SourceNode(
        cte_name=f"cte_{base_name}_src",
        table_ref=base_table_name,
        columns=[
            ColumnDef(name=c, expression=c, is_passthrough=True) for c in base_cols
        ],
        where=where_sql,
    )
    chain.nodes.append(src_node)
    last_cte = src_node.cte_name

    return _finalize_chain_with_aggregations(tree, chain, base_name, last_cte)


def _finalize_chain_with_aggregations(
    tree: exp.Select,
    chain: SelectChain,
    base_name: str,
    last_cte: str,
) -> SelectChain:
    """Append AggregatorNode/projection columns to the chain. Shared
    between the no-JOIN and custom-SQL paths."""
    group_clause = tree.find(exp.Group)
    has_aggregates = any(
        isinstance(p, (exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Count))
        or (isinstance(p, exp.Alias) and isinstance(p.this, (exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Count)))
        for p in tree.expressions
    )

    if group_clause is not None or has_aggregates:
        group_by_cols = []
        if group_clause is not None:
            for g in group_clause.expressions:
                group_by_cols.append(g.sql(dialect="bigquery"))

        agg_columns: list[ColumnDef] = []
        for proj in tree.expressions:
            agg_columns.append(_projection_to_columndef(proj))

        agg = AggregatorNode(
            cte_name=f"cte_{base_name}_agg",
            upstream=last_cte,
            group_by=group_by_cols,
            columns=agg_columns,
        )
        chain.nodes.append(agg)
        chain.final_cte = agg.cte_name
        chain.output_columns = agg_columns
        return chain

    # No aggregation — last node's column list comes from the projection.
    # Skip Star projections (SELECT *) — they pass everything through, and
    # we let TA's sql_generator fall back to `j.*` in the final SELECT.
    proj_columns: list[ColumnDef] = []
    has_star = False
    for proj in tree.expressions:
        if isinstance(proj, exp.Star):
            has_star = True
            continue
        proj_columns.append(_projection_to_columndef(proj))
    if has_star and not proj_columns:
        chain.final_cte = last_cte
        chain.output_columns = []
        return chain
    if proj_columns and chain.nodes:
        last = chain.nodes[-1]
        if hasattr(last, "columns"):
            last.columns = proj_columns
    chain.final_cte = last_cte
    chain.output_columns = proj_columns
    return chain


def _projection_to_columndef(proj: exp.Expression) -> ColumnDef:
    """Convert one SELECT projection element to a TA-IR ColumnDef.

    Pass-through column          → ColumnDef(name=col, expression=col, is_passthrough=True)
    Aliased expression           → ColumnDef(name=alias, expression=<rendered SQL>)
    Function (incl aggregate)    → ColumnDef(name=alias_or_default, expression=<SQL>)
    """
    name = proj.alias_or_name
    if isinstance(proj, exp.Column):
        return ColumnDef(name=name, expression=proj.sql(dialect="bigquery"), is_passthrough=True)
    if isinstance(proj, exp.Alias):
        inner = proj.this.sql(dialect="bigquery")
        return ColumnDef(name=name, expression=inner)
    # Unaliased expression — sqlglot's alias_or_name returns the function
    # name (e.g. "TRUNC", "SUM") which collides on multi-aggregate SELECTs.
    # Caller may rename via INSERT target columns; placeholder for now.
    expr_sql = proj.sql(dialect="bigquery")
    return ColumnDef(name=name or "col", expression=expr_sql)


def _wrap_tables_with_ref(rendered_sql: str, tree: exp.Expression) -> str:
    """Rewrite bare table identifiers in `rendered_sql` to Dataform
    `${ref('table')}` template calls. Walks the tree to collect the table
    names actually referenced and replaces each at every position SQL
    can mention a table — SELECT FROM, JOINs, UPDATE/DELETE/INSERT/MERGE
    targets, and TRUNCATE.
    """
    import re
    table_names: list[str] = []
    for tbl in tree.find_all(exp.Table):
        if tbl.name and tbl.name not in table_names:
            table_names.append(tbl.name)
    # The clauses where a bare table name can appear. UPDATE has the
    # target right after the keyword; the rest take "INTO <name>" /
    # "TABLE <name>" / "FROM <name>" forms.
    clause_keywords = [
        r"FROM",
        r"JOIN",
        r"UPDATE",
        r"DELETE\s+FROM",
        r"INSERT\s+INTO",
        r"MERGE\s+INTO",
        r"TRUNCATE\s+TABLE",
    ]
    out = rendered_sql
    for name in table_names:
        out = re.sub(rf"`{re.escape(name)}`", f"${{ref('{name}')}}", out)
        for kw in clause_keywords:
            pattern = rf"(?i)(\b{kw})\s+{re.escape(name)}\b"
            out = re.sub(
                pattern,
                lambda m, n=name: f"{m.group(1)} ${{ref('{n}')}}",
                out,
            )
    return out


def _collect_referenced_columns(tree: exp.Expression, base_table: exp.Table) -> list[str]:
    """Collect the column names from `base_table` actually referenced anywhere
    in the SELECT — projections, where, group by, joins. Order-preserving,
    de-duplicated.
    """
    seen: set[str] = set()
    out: list[str] = []
    base_alias = base_table.alias_or_name.lower() if base_table.alias_or_name else None
    for col in tree.find_all(exp.Column):
        # If a table prefix is present, restrict to base table.
        if col.table:
            if base_alias and col.table.lower() != base_alias:
                continue
        cname = col.name
        if cname and cname.lower() not in seen:
            seen.add(cname.lower())
            out.append(cname)
    return out


def parse_insert_select(sql: str, base_name: str) -> tuple[str | None, list[str], SelectChain | None]:
    """Decompose an INSERT INTO target (cols) SELECT ... statement.

    Returns: (target_table, target_columns, chain_for_select)
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="oracle")
    except Exception:
        return None, [], None
    if not isinstance(tree, exp.Insert):
        return None, [], None

    # Target — `INSERT INTO X (...)`.
    target_table = None
    target_columns: list[str] = []
    target = tree.this
    if isinstance(target, exp.Schema):
        # Schema(this=Table, expressions=[Identifier columns])
        if isinstance(target.this, exp.Table):
            target_table = target.this.name
        target_columns = [e.name for e in target.expressions]
    elif isinstance(target, exp.Table):
        target_table = target.name

    # SELECT body
    select_node = tree.expression
    if not isinstance(select_node, exp.Select):
        return target_table, target_columns, None
    chain = parse_select(select_node.sql(dialect="oracle"), base_name)

    # Apply target-column names by position. INSERT INTO target (a, b, c)
    # SELECT x, TRUNC(y), SUM(z) — we rename the projections to (a, b, c).
    if chain is not None and target_columns and chain.output_columns:
        renamed: list[ColumnDef] = []
        for i, col in enumerate(chain.output_columns):
            if i < len(target_columns):
                renamed.append(ColumnDef(
                    name=target_columns[i],
                    expression=col.expression,
                    is_passthrough=col.is_passthrough,
                ))
            else:
                renamed.append(col)
        chain.output_columns = renamed
        # Also update the last node's columns
        if chain.nodes:
            last = chain.nodes[-1]
            if hasattr(last, "columns"):
                last.columns = renamed

    return target_table, target_columns, chain


def classify_dml(sql: str) -> str:
    """Return 'insert' / 'update' / 'delete' / 'truncate' / 'merge' / 'select' / 'unknown'.

    Used by the execute_sql step processor to decide whether to roll into
    the primary IR (insert/select) or emit as a separate operations script
    (update/delete/truncate/merge).
    """
    if not sql or not sql.strip():
        return "unknown"
    try:
        tree = sqlglot.parse_one(sql, dialect="oracle")
    except Exception:
        # Fallback to first keyword
        tok = sql.strip().upper().split(None, 1)[0] if sql.strip() else ""
        return tok.lower() if tok in {"INSERT", "UPDATE", "DELETE", "TRUNCATE", "MERGE", "SELECT"} else "unknown"

    if isinstance(tree, exp.Insert):
        return "insert"
    if isinstance(tree, exp.Update):
        return "update"
    if isinstance(tree, exp.Delete):
        return "delete"
    if isinstance(tree, exp.TruncateTable):
        return "truncate"
    if isinstance(tree, exp.Merge):
        return "merge"
    if isinstance(tree, exp.Select):
        return "select"
    text = sql.strip().upper()
    if text.startswith("TRUNCATE"):
        return "truncate"
    return "unknown"


def render_dml_for_bigquery(sql: str) -> str:
    """Translate Oracle DML (UPDATE/DELETE/MERGE) to BigQuery-compatible SQL.

    Two passes:
    1. sqlglot transpile from `oracle` → `bigquery` to convert dialect
       differences (SYSDATE → CURRENT_DATETIME, NVL → IFNULL, etc.)
    2. `_wrap_tables_with_ref` to rewrite every bare table identifier to
       `${ref('table')}` so Dataform resolves them to the project's
       declared sources.

    Falls back to the original SQL if transpile fails.
    """
    if not sql or not sql.strip():
        return sql

    try:
        # parse_one + .sql() is equivalent to transpile but lets us also
        # walk the tree afterward for the ${ref()} rewrite.
        tree = sqlglot.parse_one(sql, dialect="oracle")
    except Exception:
        return sql.rstrip(";")

    transpiled = tree.sql(dialect="bigquery")
    return _wrap_tables_with_ref(transpiled, tree).rstrip(";")
