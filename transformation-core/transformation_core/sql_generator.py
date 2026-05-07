"""
SQL Generator — Converts a DataflowGraph into a SQL string.

Walks the IR nodes in order and emits one CTE per node.
Each node type has a simple, self-contained emit function.

The generator is stateless — it only reads the IR, never mutates it.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from transformation_core.ir import (
    AggregatorNode, ColumnDef, DataflowGraph, ExpressionNode, FilterNode,
    JoinType, JoinerNode, LookupNode, NormalizerNode, Node, NodeType,
    RouterNode, SourceNode, TargetMapping, UnionNode,
)
from transformation_core.naming import to_pascal_name

# BigQuery reserved words that must be backtick-quoted when used as aliases
_BIGQUERY_RESERVED_WORDS = {
    'all', 'and', 'any', 'array', 'as', 'asc', 'assert_rows_modified', 'at',
    'between', 'by', 'case', 'cast', 'collate', 'contains', 'create', 'cross',
    'cube', 'current', 'default', 'define', 'desc', 'distinct', 'else', 'end',
    'enum', 'escape', 'except', 'exclude', 'exists', 'extract', 'false', 'fetch',
    'following', 'for', 'from', 'full', 'group', 'grouping', 'groups', 'hash',
    'having', 'if', 'ignore', 'in', 'inner', 'intersect', 'interval', 'into',
    'is', 'join', 'lateral', 'left', 'like', 'limit', 'lookup', 'merge', 'natural',
    'new', 'no', 'not', 'null', 'nulls', 'of', 'on', 'or', 'order', 'outer',
    'over', 'partition', 'preceding', 'proto', 'range', 'recursive', 'respect',
    'right', 'rollup', 'rows', 'select', 'set', 'some', 'struct', 'tablesample',
    'then', 'to', 'treat', 'true', 'unbounded', 'union', 'unnest', 'using',
    'when', 'where', 'window', 'with', 'within',
}


def _escape_alias(name: str) -> str:
    """Backtick-quote a column alias if it's a BigQuery reserved word.

    Also sanitizes names that aren't valid SQL identifiers (e.g., port
    names that are expressions like ``CAST(NULL AS INT64)``).
    """
    if name.lower() in _BIGQUERY_RESERVED_WORDS:
        return f'`{name}`'
    # Guard: if the name isn't a valid SQL identifier, sanitize it
    if not re.match(r'^[A-Za-z_]\w*$', name):
        sanitized = re.sub(r'[^A-Za-z0-9_]', '', name)
        if not sanitized or not sanitized[0].isalpha() and sanitized[0] != '_':
            sanitized = f"col_{sanitized}" if sanitized else "col_unknown"
        return sanitized
    return name

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates a SQL string from a DataflowGraph.

    Usage:
        gen = SQLGenerator()
        sql = gen.generate(graph)
    """

    def __init__(self):
        # Track accumulated lookups for merging into single CTEs
        self._pending_lookups: list[LookupNode] = []
        self._lookup_merged_into: str = ""  # CTE name of merged lookup target

    def generate(self, graph: DataflowGraph, target_override: TargetMapping = None) -> str:
        """Generate the complete SQL from the IR.

        Args:
            target_override: When set, use this TargetMapping instead of
                            graph.target for the final SELECT and WHERE clause.
                            Used for multi-target mappings to generate one SQL
                            per target from the same CTE chain.
        """
        # Handle skip/empty graphs (e.g., cache-warming mappings)
        if not graph.nodes:
            return "-- No SQL generated (mapping skipped)"

        self._pending_lookups = []
        self._lookup_merged_into = ""
        # Build column sets per CTE for src. qualification in expressions
        self._cte_columns = self._build_cte_column_sets(graph)

        cte_pairs: list[tuple[str, str]] = []  # (cte_name, cte_sql)
        last_cte = ""

        for node in graph.nodes:
            cte_sql = self._emit_node(node, graph)
            if cte_sql:
                cte_pairs.append((node.cte_name, cte_sql))
                last_cte = node.cte_name

        # Prune unreachable CTEs — only keep those reachable from last_cte.
        # This removes dead branches (e.g., reject paths, secondary router
        # groups, unused source streams).
        reachable = self._find_reachable_ctes(graph, last_cte)
        ctes = [sql for name, sql in cte_pairs if name in reachable]

        # Emit final SELECT (using override target if provided)
        final_select = self._emit_final_select(graph, last_cte, target_override)

        # Assemble — strip \r to prevent carriage returns from Informatica
        # XML expressions leaking into SQL output
        if ctes:
            sql = "WITH\n" + ",\n\n".join(ctes) + "\n\n" + final_select
        else:
            sql = final_select
        return sql.replace('\r', '')

    def _find_reachable_ctes(self, graph: DataflowGraph, last_cte: str) -> set[str]:
        """Find all CTEs reachable from last_cte by tracing upstream references."""
        # Build upstream map from nodes
        upstream_map: dict[str, list[str]] = {}
        for node in graph.nodes:
            deps = []
            if isinstance(node, SourceNode):
                pass  # No CTE dependencies
            elif isinstance(node, JoinerNode):
                if node.detail_upstream:
                    deps.append(node.detail_upstream)
                if node.master_upstream:
                    deps.append(node.master_upstream)
            elif isinstance(node, UnionNode):
                deps.extend(node.upstreams)
            else:
                # ExpressionNode, FilterNode, LookupNode, AggregatorNode,
                # NormalizerNode, RouterNode — all have .upstream
                upstream = getattr(node, 'upstream', '')
                if upstream:
                    deps.append(upstream)
            upstream_map[node.cte_name] = deps

        # BFS from last_cte
        reachable: set[str] = set()
        queue = [last_cte]
        while queue:
            cte = queue.pop()
            if cte in reachable:
                continue
            reachable.add(cte)
            for dep in upstream_map.get(cte, []):
                if dep not in reachable:
                    queue.append(dep)

        return reachable

    # ── Node dispatch ────────────────────────────────────────────────

    def _emit_node(self, node: Node, graph: DataflowGraph) -> str:
        """Dispatch to the appropriate emitter."""
        if isinstance(node, SourceNode):
            return self._emit_source(node, graph)
        elif isinstance(node, ExpressionNode):
            return self._emit_expression(node, graph)
        elif isinstance(node, FilterNode):
            return self._emit_filter(node, graph)
        elif isinstance(node, LookupNode):
            return self._emit_lookup(node, graph)
        elif isinstance(node, JoinerNode):
            return self._emit_joiner(node, graph)
        elif isinstance(node, AggregatorNode):
            return self._emit_aggregator(node, graph)
        elif isinstance(node, UnionNode):
            return self._emit_union(node, graph)
        elif isinstance(node, NormalizerNode):
            return self._emit_normalizer(node, graph)
        elif isinstance(node, RouterNode):
            return self._emit_router(node, graph)
        else:
            logger.warning("Unknown node type: %s", type(node).__name__)
            return ""

    # ── Source ────────────────────────────────────────────────────────

    def _emit_source(self, node: SourceNode, graph: DataflowGraph) -> str:
        """Emit a source CTE."""
        lines = [f"{node.cte_name} AS ("]

        if node.custom_sql:
            # Use the custom SQL as-is — table ref wrapping is done by the
            # IR builder. Don't mangle it here.
            #
            # Special case: custom SQL starting with WITH defines local CTEs
            # (e.g., "WITH SPC AS (SELECT ...) SELECT ... FROM SPC").
            # Nesting this inside another CTE would produce invalid BigQuery:
            #   cte_source AS (WITH SPC AS (...) SELECT ...)  -- INVALID
            # Fix: Strip the outer WITH and inline the local CTEs as preceding
            # CTEs in the chain, then wrap only the final SELECT in the source CTE.
            custom = node.custom_sql.strip()
            if custom.upper().startswith('WITH '):
                return self._emit_source_with_inline_ctes(node, custom)

            # Check if port names differ from SQL column names. When the SQ
            # has table-prefixed port names (e.g., W_ATS_Daily_Revenue_Turnover)
            # but the custom SQL produces bare column names (e.g., Turnover),
            # downstream CTEs would reference non-existent columns.
            # Fix: wrap the custom SQL in a sub-SELECT that aliases SQL column
            # names to Informatica port names.
            alias_select = self._build_custom_sql_alias_wrapper(node)
            if alias_select:
                lines.append("  SELECT")
                lines.append(",\n".join(f"    {line}" for line in alias_select))
                lines.append("  FROM (")
                for sql_line in custom.split('\n'):
                    lines.append(f"    {sql_line}")
                lines.append("  ) AS _sq")
            else:
                # No aliasing needed — emit as the CTE body directly.
                # Indent each line for readability inside the CTE wrapper.
                for sql_line in custom.split('\n'):
                    lines.append(f"  {sql_line}")
        else:
            # Alias map — built BEFORE SELECT so column expressions that
            # reference Informatica table names (e.g., VW_S_DACOM_Class_Code_B.CLong)
            # can be rewritten to use short aliases (e.g., clas.CLong).
            alias_map: dict[str, str] = {}  # UPPER table name → alias
            used_aliases: set[str] = set()

            def _gen_alias(tbl: str) -> str:
                """Generate a short unique alias from a table name."""
                name = tbl
                for pfx in ('S_DACOM_', 'S_IGT_', 'S_MTET_', 'VW_S_DACOM_', 'VW_',
                             'S_', 'D_', 'F_', 'W_'):
                    if name.upper().startswith(pfx):
                        name = name[len(pfx):]
                        break
                parts = [p for p in re.split(r'[_\s]+', name) if p]
                a = parts[0][:4].lower() if parts else tbl[:4].lower()
                base = a
                ctr = 2
                while a in used_aliases:
                    a = f"{base}{ctr}"
                    ctr += 1
                used_aliases.add(a)
                return a

            # Pre-build alias_map for multi-source joins
            if node.joined_tables and not node.table_ref.startswith("/*"):
                alias_map[node.table_ref.upper()] = _gen_alias(node.table_ref)
                for jt in node.joined_tables:
                    alias_map[jt.upper()] = _gen_alias(jt)

            # Build SELECT from columns — use alias_map to resolve table qualifiers
            select_keyword = "SELECT DISTINCT" if node.is_distinct else "SELECT"
            col_strs = []
            for col in node.columns:
                expr = col.expression
                # Replace Informatica table qualifiers with short aliases
                if alias_map and '.' in expr:
                    for tbl_upper, tbl_alias in alias_map.items():
                        expr = re.sub(
                            r'\b' + re.escape(tbl_upper) + r'\.',
                            f"{tbl_alias}.",
                            expr, flags=re.IGNORECASE)
                if col.name == expr:
                    col_strs.append(f"    {col.name}")
                else:
                    col_strs.append(f"    {expr} AS {_escape_alias(col.name)}")

            if col_strs:
                lines.append(f"  {select_keyword}")
                lines.append(",\n".join(col_strs))
            else:
                lines.append(f"  {select_keyword} *")

            # FROM clause
            if node.table_ref.startswith("/*"):
                # Opaque source (e.g., unresolved mapplet) — emit as comment
                lines.append(f"  FROM {node.table_ref}")
            else:
                ref_name = self._ref(node.table_ref, graph)
                if node.joined_tables:
                    alias = alias_map.get(node.table_ref.upper(), "")
                    lines.append(f"  FROM {ref_name} AS {alias}")
                else:
                    lines.append(f"  FROM {ref_name}")

            # Additional joined tables (Fix 2: per-table ON clauses)
            if node.joined_tables:
                if node.join_conditions:
                    # Per-table ON clauses — each JOIN gets its own condition
                    for jt in node.joined_tables:
                        ref = self._ref(jt, graph)
                        alias = alias_map.get(jt.upper(), _gen_alias(jt))
                        jt_cond = node.join_conditions.get(jt, "")
                        jt_type = node.join_types.get(jt, "INNER JOIN")
                        # Replace bare table qualifiers in the condition
                        for tbl_upper, tbl_alias in alias_map.items():
                            jt_cond = re.sub(
                                r'\b' + re.escape(tbl_upper) + r'\.',
                                f"{tbl_alias}.",
                                jt_cond, flags=re.IGNORECASE)
                        if jt_cond:
                            lines.append(f"  {jt_type} {ref} AS {alias}")
                            lines.append(f"    ON {jt_cond}")
                        else:
                            # No join condition with earlier tables — use CROSS JOIN.
                            # This happens when a multi-source SQ has a predicate
                            # between two non-primary tables (e.g., mach.X = gami.X)
                            # but no condition linking this table to the primary.
                            lines.append(f"  CROSS JOIN {ref} AS {alias}")
                elif node.join_condition:
                    # Legacy monolithic: all JOINs then single ON (backward compat)
                    for jt in node.joined_tables:
                        ref = self._ref(jt, graph)
                        alias = alias_map.get(jt.upper(), _gen_alias(jt))
                        lines.append(f"  INNER JOIN {ref} AS {alias}")
                    # Replace bare qualifiers in monolithic join condition
                    jc = node.join_condition
                    for tbl_upper, tbl_alias in alias_map.items():
                        jc = re.sub(
                            r'\b' + re.escape(tbl_upper) + r'\.',
                            f"{tbl_alias}.",
                            jc, flags=re.IGNORECASE)
                    lines.append(f"    ON {jc}")

            # WHERE — replace bare table qualifiers with aliases
            if node.where:
                w = node.where
                for tbl_upper, tbl_alias in alias_map.items():
                    w = re.sub(
                        r'\b' + re.escape(tbl_upper) + r'\.',
                        f"{tbl_alias}.",
                        w, flags=re.IGNORECASE)
                lines.append(f"  WHERE {w}")

        lines.append(")")
        return "\n".join(lines)

    def _emit_source_with_inline_ctes(
        self, node: SourceNode, custom: str,
    ) -> str:
        """Handle custom SQL that starts with WITH (local CTE definitions).

        Splits 'WITH cte1 AS (...), cte2 AS (...) SELECT ...' into:
        - Local CTEs emitted as separate CTE definitions
        - The final SELECT wrapped in the source CTE name

        This avoids invalid nested WITH in BigQuery:
          cte_source AS (WITH local AS (...) SELECT ...)  -- INVALID
        Becomes:
          local AS (...),
          cte_source AS (SELECT ... FROM local)           -- VALID
        """
        # Parse: strip leading WITH, then find CTE blocks by tracking parens
        body = custom[4:].lstrip()  # remove 'WITH '

        cte_blocks: list[str] = []  # "name AS (...)"
        final_select = ""

        pos = 0
        while pos < len(body):
            # Extract CTE name
            name_m = re.match(r'(\w+)\s+AS\s*\(', body[pos:], re.IGNORECASE)
            if not name_m:
                # No more CTEs — rest is the final SELECT
                final_select = body[pos:].strip()
                # Remove leading comma if present
                if final_select.startswith(','):
                    final_select = final_select[1:].strip()
                break

            cte_name = name_m.group(1)
            paren_start = pos + name_m.end() - 1  # position of '('
            # Find matching closing paren
            depth = 1
            i = paren_start + 1
            in_string = False
            while i < len(body) and depth > 0:
                ch = body[i]
                if ch == "'" and not in_string:
                    in_string = True
                elif ch == "'" and in_string:
                    in_string = False
                elif not in_string:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                i += 1

            cte_body = body[paren_start + 1: i - 1]
            cte_blocks.append(f"{cte_name} AS (\n  {cte_body.strip()}\n)")

            # Skip past closing paren and optional comma
            pos = i
            rest = body[pos:].lstrip()
            if rest.startswith(','):
                pos += body[pos:].index(',') + 1
            else:
                final_select = rest.strip()
                break

        if not final_select:
            # Fallback: just emit the custom SQL as-is (shouldn't happen)
            return f"{node.cte_name} AS (\n  {custom}\n)"

        # Emit: local CTEs first, then source CTE with final SELECT
        parts = []
        for cte_block in cte_blocks:
            parts.append(cte_block)
        parts.append(f"{node.cte_name} AS (\n  {final_select}\n)")
        return ",\n\n".join(parts)

    # ── Expression ───────────────────────────────────────────────────

    def _emit_expression(self, node: ExpressionNode, graph: DataflowGraph) -> str:
        """Emit an expression CTE."""
        lines = [f"{node.cte_name} AS ("]
        lines.append("  SELECT")

        # Get upstream columns for src. qualification
        upstream_cols = self._cte_columns.get(node.upstream, set())

        # Collect explicit column names/aliases first to build EXCEPT list
        explicit_col_names: list[str] = []
        explicit_col_strs: list[str] = []
        for col in node.columns:
            expr = self._qualify_expression(col.expression, graph)
            # Add src. prefix to bare column refs from upstream
            expr = self._add_src_prefix(expr, upstream_cols)
            if col.name.lower() == expr.lower():
                explicit_col_strs.append(f"    {expr}")
            else:
                explicit_col_strs.append(f"    {expr} AS {_escape_alias(col.name)}")
            explicit_col_names.append(col.name)

        col_strs = []
        if node.pass_upstream:
            # When passing upstream columns AND adding explicit columns,
            # use EXCEPT to exclude names that would be duplicated.
            #
            # Two duplicate patterns:
            # 1. Output alias matches upstream column: src.* includes X,
            #    explicit col selects X → EXCEPT(X)
            # 2. Expression references upstream column but renames it:
            #    src.X AS Y → src.* includes X AND explicit has X AS Y
            #    → EXCEPT(X) to avoid X appearing twice
            #
            # We scan the QUALIFIED expression string (after _add_src_prefix)
            # to find all src.COLNAME references. These are the actual SQL
            # column names from upstream that the expression accesses. If any
            # of them differ from the output alias, they'd create duplicates
            # because src.* also includes them.
            shadowed: set[str] = set()
            for i, name in enumerate(explicit_col_names):
                # Pattern 1: output alias name matches upstream column
                if name.lower() in upstream_cols:
                    shadowed.add(name)
                # Pattern 2: when the expression is a simple column rename
                # (src.X AS Y), the upstream column X would appear twice:
                # once from transformation_core.* and once from the explicit "src.X AS Y".
                # Only shadow X when the ENTIRE expression is just src.X
                # (a simple rename).  Do NOT shadow when src.X appears
                # inside a complex expression (CASE, IF, UPPER, etc.) —
                # the expression consumes X as input but produces a new
                # column Y, so X should remain available via src.*.
                qualified_expr = explicit_col_strs[i].strip()
                # Check if expression is a simple column rename: "src.COL"
                # or "src.COL AS alias" with nothing else
                simple_rename_m = re.match(
                    r'^src\.(\w+)(?:\s+AS\s+\w+)?$',
                    qualified_expr, re.IGNORECASE,
                )
                if simple_rename_m:
                    ref_col = simple_rename_m.group(1)
                    if (ref_col.lower() in upstream_cols
                            and ref_col.lower() != name.lower()):
                        shadowed.add(ref_col)
            if shadowed:
                except_list = ", ".join(sorted(shadowed))
                col_strs.append(f"    src.* EXCEPT({except_list})")
            else:
                col_strs.append("    src.*")

        col_strs.extend(explicit_col_strs)

        lines.append(",\n".join(col_strs))
        lines.append(f"  FROM {node.upstream} AS src")
        lines.append(")")
        return "\n".join(lines)

    # ── Filter ───────────────────────────────────────────────────────

    def _emit_filter(self, node: FilterNode, graph: DataflowGraph) -> str:
        """Emit a filter CTE."""
        condition = self._qualify_expression(node.condition, graph)
        # Add src. prefix to bare column refs in the filter condition
        upstream_cols = self._cte_columns.get(node.upstream, set())
        condition = self._add_src_prefix(condition, upstream_cols)
        lines = [
            f"{node.cte_name} AS (",
            "  SELECT src.*",
            f"  FROM {node.upstream} AS src",
            f"  WHERE {condition}",
            ")",
        ]
        return "\n".join(lines)

    # ── Lookup ───────────────────────────────────────────────────────

    def _emit_lookup(self, node: LookupNode, graph: DataflowGraph) -> str:
        """Emit a lookup CTE (LEFT JOIN)."""
        lines = [f"{node.cte_name} AS ("]
        lines.append("  SELECT")

        # All upstream columns — use EXCEPT to exclude columns that would be
        # shadowed by lookup return columns (avoids duplicate column names).
        if node.shadowed_columns:
            except_list = ", ".join(sorted(node.shadowed_columns))
            col_strs = [f"    src.* EXCEPT({except_list})"]
        else:
            col_strs = ["    src.*"]

        # Return columns from lookup
        for col in node.return_columns:
            col_strs.append(f"    {col.expression} AS {_escape_alias(col.name)}")

        lines.append(",\n".join(col_strs))
        lines.append(f"  FROM {node.upstream} AS src")

        # The JOIN
        ref_name = self._ref(node.lookup_table, graph)
        if node.sql_override:
            lines.append(f"  {node.join_type.value} ({node.sql_override}) AS {node.lookup_alias}")
        else:
            lines.append(f"  {node.join_type.value} {ref_name} AS {node.lookup_alias}")

        condition = self._qualify_expression(node.join_condition, graph)
        lines.append(f"    ON {condition}")

        lines.append(")")
        return "\n".join(lines)

    # ── Joiner ───────────────────────────────────────────────────────

    def _emit_joiner(self, node: JoinerNode, graph: DataflowGraph) -> str:
        """Emit a joiner CTE (JOIN between two streams).

        Fix 5: Uses node.detail_alias/master_alias for unique aliases
        across sequential joiners (detail, detail2, detail3, etc.).
        """
        d_alias = node.detail_alias
        m_alias = node.master_alias
        lines = [f"{node.cte_name} AS ("]
        lines.append("  SELECT")

        col_strs = []
        for col in node.columns:
            col_strs.append(f"    {col.expression} AS {_escape_alias(col.name)}")

        if col_strs:
            lines.append(",\n".join(col_strs))
        else:
            lines.append(f"    {d_alias}.*, {m_alias}.*")

        lines.append(f"  FROM {node.detail_upstream} AS {d_alias}")
        lines.append(f"  {node.join_type.value} {node.master_upstream} AS {m_alias}")

        if node.join_condition:
            condition = self._qualify_expression(node.join_condition, graph)
            lines.append(f"    ON {condition}")

        lines.append(")")
        return "\n".join(lines)

    # ── Aggregator ───────────────────────────────────────────────────

    def _emit_aggregator(self, node: AggregatorNode, graph: DataflowGraph) -> str:
        """Emit an aggregator CTE.

        Passthrough columns that are NOT in the GROUP BY clause must be wrapped
        in ANY_VALUE() to satisfy BigQuery's requirement that all non-aggregated
        SELECT columns appear in GROUP BY or inside an aggregate function.
        """
        lines = [f"{node.cte_name} AS ("]
        lines.append("  SELECT")

        # Build a set of GROUP BY column identifiers (lowercase) for matching.
        # A passthrough column is a GROUP BY key if its name, expression, or
        # source_column matches any group_by entry.
        gb_lower = set()
        for g in node.group_by:
            gb_lower.add(g.lower())
            # Also add the clean version without alias prefix
            if '.' in g:
                gb_lower.add(g.split('.')[-1].lower())

        col_strs = []
        for col in node.columns:
            expr = self._qualify_expression(col.expression, graph)

            # Determine if this column is a GROUP BY key
            is_group_by = (
                col.name.lower() in gb_lower
                or expr.lower() in gb_lower
                or (col.source_column and col.source_column.lower() in gb_lower)
            )

            # Check if expression already contains an aggregate function
            _has_agg = bool(re.search(
                r'\b(?:ANY_VALUE|COUNT|SUM|AVG|MIN|MAX|STRING_AGG|ARRAY_AGG)\s*\(',
                expr, re.IGNORECASE,
            )) if not is_group_by and node.group_by else False

            if not is_group_by and node.group_by and not _has_agg:
                # Non-GROUP-BY column without aggregate function →
                # wrap in ANY_VALUE() to satisfy BigQuery requirement.
                # Applies to both passthroughs (src.col) and computed
                # columns (NULL, literal values) that aren't in GROUP BY.
                col_strs.append(f"    ANY_VALUE({expr}) AS {_escape_alias(col.name)}")
            elif col.name.lower() == expr.lower():
                col_strs.append(f"    {expr}")
            else:
                col_strs.append(f"    {expr} AS {_escape_alias(col.name)}")

        lines.append(",\n".join(col_strs))
        lines.append(f"  FROM {node.upstream} AS src")

        if node.group_by:
            lines.append(f"  GROUP BY {', '.join(node.group_by)}")

        lines.append(")")
        return "\n".join(lines)

    # ── Union ────────────────────────────────────────────────────────

    def _emit_union(self, node: UnionNode, graph: DataflowGraph) -> str:
        """Emit a UNION ALL CTE."""
        col_names = [c.name for c in node.columns]
        selects = []

        for upstream in node.upstreams:
            col_strs = []
            for col_name in col_names:
                # Check if there's a mapping for this upstream
                mapping = node.column_mappings.get(upstream, {})
                source_col = mapping.get(col_name, col_name)
                if source_col.lower() != col_name.lower():
                    col_strs.append(f"    {source_col} AS {col_name}")
                else:
                    col_strs.append(f"    {col_name}")
            select_str = "  SELECT\n" + ",\n".join(col_strs) + f"\n  FROM {upstream}"
            selects.append(select_str)

        body = "\n  UNION ALL\n".join(selects)
        return f"{node.cte_name} AS (\n{body}\n)"

    # ── Normalizer ───────────────────────────────────────────────────

    def _emit_normalizer(self, node: NormalizerNode, graph: DataflowGraph) -> str:
        """Emit a normalizer CTE (row explosion)."""
        lines = [f"{node.cte_name} AS ("]
        lines.append("  SELECT")

        col_strs = []
        for col in node.columns:
            expr = self._qualify_expression(col.expression, graph)
            if col.name.lower() == expr.lower():
                col_strs.append(f"    {expr}")
            else:
                col_strs.append(f"    {expr} AS {_escape_alias(col.name)}")

        lines.append(",\n".join(col_strs))
        lines.append(f"  FROM {node.upstream} AS src")
        lines.append(f"  CROSS JOIN UNNEST(GENERATE_ARRAY(1, {node.occurs})) AS idx")
        lines.append(")")
        return "\n".join(lines)

    # ── Router ────────────────────────────────────────────────────────

    def _emit_router(self, node: RouterNode, graph: DataflowGraph) -> str:
        """Emit a pass-through CTE for the router.

        Routers split a stream into groups via WHERE conditions. The actual
        group filtering is applied in the final SELECT (via target.router_group).
        But we still need a CTE so downstream nodes (normalizer, expression,
        filter) can reference the router's output.
        """
        lines = [
            f"{node.cte_name} AS (",
            "  SELECT src.*",
            f"  FROM {node.upstream} AS src",
            ")",
        ]
        return "\n".join(lines)

    # ── Final SELECT ─────────────────────────────────────────────────

    def _emit_final_select(self, graph: DataflowGraph, last_cte: str,
                           target_override: TargetMapping = None) -> str:
        """Emit the final SELECT that produces the target columns."""
        target = target_override if target_override else graph.target
        if not target.columns:
            return f"SELECT *\nFROM {last_cte} AS j"

        lines = ["SELECT"]
        col_strs = []
        for col in target.columns:
            source_col = col.expression  # What to SELECT from the last CTE
            target_col = col.name        # What to alias it as

            # Detect computed expressions (ROW_NUMBER, ANY_VALUE, etc.)
            # These should NOT be prefixed with j.
            is_expr = (not re.match(r'^[A-Za-z_]\w*$', source_col)
                       and not source_col.startswith('j.'))

            if is_expr:
                col_strs.append(f"  {source_col} AS {_escape_alias(target_col)}")
            elif source_col.lower() == target_col.lower():
                col_strs.append(f"  j.{_escape_alias(source_col)}")
            else:
                col_strs.append(f"  j.{_escape_alias(source_col)} AS {_escape_alias(target_col)}")

        lines.append(",\n".join(col_strs))
        lines.append(f"FROM {last_cte} AS j")

        # Add WHERE from router if present.
        # Use the pre-computed router_group condition from _build_target(),
        # which traces connector flow to find the correct group for the
        # primary target. Fallback: first non-default group's condition.
        if target.router_group:
            condition = self._qualify_expression(target.router_group, graph)
            lines.append(f"WHERE {condition}")
        else:
            router_node = None
            for node in graph.nodes:
                if isinstance(node, RouterNode):
                    router_node = node
                    break
            if router_node:
                # Prefer Accept/Insert/New/Update groups over Reject/Alert.
                # When _build_target() can't trace the connector flow, the
                # fallback should pick the non-reject path for the primary target.
                reject_names = {'reject', 'alert', 'rej', 'alt'}
                accept_group = None
                any_group = None
                for group in router_node.groups:
                    if not group.is_default and group.condition:
                        if any_group is None:
                            any_group = group
                        if group.name.lower() not in reject_names:
                            accept_group = group
                            break
                chosen = accept_group or any_group
                if chosen:
                    condition = self._qualify_expression(chosen.condition, graph)
                    lines.append(f"WHERE {condition}")

        return "\n".join(lines)

    # ── Helpers ──────────────────────────────────────────────────────

    def _build_custom_sql_alias_wrapper(self, node: SourceNode) -> list[str] | None:
        """Build alias SELECT lines when port names differ from SQL column names.

        When a Source Qualifier has custom SQL, the SQL SELECT produces columns
        with bare names (e.g., Turnover, DayId) but the SQ ports have table-
        prefixed names (e.g., W_ATS_Daily_Revenue_Turnover).  Downstream CTEs
        reference the port names, which don't exist in the SQL output.

        Also handles the common case where Informatica ports use snake_case
        (e.g., player_pct) but the custom SQL outputs PascalCase from BigQuery
        (e.g., PlayerPct).

        Fix: wrap the custom SQL in a sub-SELECT that aliases SQL column names
        to their corresponding port names:
            SELECT Turnover AS W_ATS_Daily_Revenue_Turnover, ...
            FROM (<custom SQL>) AS _sq

        Returns a list of "sql_col AS port_name" strings, or None if no
        aliasing is needed (all port names already match SQL column names).
        """
        sql_cols_cased = self._extract_sql_select_columns_cased(node.custom_sql)
        if not sql_cols_cased:
            return None
        sql_cols = set(sql_cols_cased.keys())  # lowercase set for compatibility

        # Build underscore-stripped lookup for snake_case → PascalCase matching.
        # E.g., {playerpct: PlayerPct, meterdigits: MeterDigits, floorloc: FloorLoc}
        no_us_to_original: dict[str, str] = {}
        for low, original in sql_cols_cased.items():
            stripped = low.replace('_', '')
            if stripped not in no_us_to_original:
                no_us_to_original[stripped] = original

        # Check if ANY port name differs from its SQL column name
        has_mismatch = False
        for col in node.columns:
            if col.name.lower() not in sql_cols:
                has_mismatch = True
                break

        if not has_mismatch:
            return None

        # Build mapping: port_name -> sql_column_name
        # Strategy: for each port name, try suffix-stripping, then
        # underscore-stripped matching to find the SQL col
        alias_lines: list[str] = []
        used_sql_cols: set[str] = set()  # track to avoid duplicate mappings

        for col in node.columns:
            port = col.name
            port_lower = port.lower()

            # Already matches — pass through as-is
            if port_lower in sql_cols:
                alias_lines.append(f"_sq.{port}")
                used_sql_cols.add(port_lower)
                continue

            # Try suffix stripping: progressively remove leading segments
            # e.g., W_ATS_Daily_Revenue_Turnover -> ATS_Daily_Revenue_Turnover
            #   -> Daily_Revenue_Turnover -> Revenue_Turnover -> Turnover
            parts = port.split('_')
            matched_sql_col = None
            for i in range(1, len(parts)):
                suffix = '_'.join(parts[i:])
                suffix_lower = suffix.lower()
                if suffix_lower in sql_cols and suffix_lower not in used_sql_cols:
                    # Use the port's own suffix casing (e.g., PtyLocNum from
                    # the port name) — BQ is case-insensitive for column refs
                    matched_sql_col = suffix
                    break

            # Try underscore-stripped matching: snake_case port → PascalCase SQL col.
            # E.g., player_pct → playerpct matches PlayerPct.
            if not matched_sql_col:
                port_no_us = port_lower.replace('_', '')
                if port_no_us in no_us_to_original:
                    original = no_us_to_original[port_no_us]
                    if original.lower() not in used_sql_cols:
                        matched_sql_col = original

            if matched_sql_col:
                if matched_sql_col.lower() == port_lower:
                    # Same name (case-insensitive) — no AS needed
                    alias_lines.append(f"_sq.{matched_sql_col}")
                else:
                    alias_lines.append(f"_sq.{matched_sql_col} AS {port}")
                used_sql_cols.add(matched_sql_col.lower())
            else:
                # Port has no matching SQL column — it's a phantom port from
                # a joined table that isn't in the SELECT.  Skip it; downstream
                # should not reference it.
                logger.debug(
                    "Skipping phantom port %s in %s (not in custom SQL SELECT)",
                    port, node.cte_name,
                )

        return alias_lines if alias_lines else None

    @staticmethod
    def _extract_sql_select_columns(sql: str) -> set[str]:
        """Extract lowercase column names/aliases from a SELECT clause.

        Handles table.column, bare column, and AS alias patterns.
        Tracks parenthesis depth to correctly handle EXTRACT(... FROM ...).
        """
        return set(SQLGenerator._extract_sql_select_columns_cased(sql).keys())

    @staticmethod
    def _extract_sql_select_columns_cased(sql: str) -> dict[str, str]:
        """Extract column names from a SELECT clause preserving original case.

        Returns {lowercase_name: original_case_name}.
        Handles table.column, bare column, and AS alias patterns.
        Tracks parenthesis depth to correctly handle EXTRACT(... FROM ...).
        """
        cols: dict[str, str] = {}
        upper = sql.upper()
        select_pos = upper.find('SELECT')
        if select_pos == -1:
            return cols

        # Find FROM at parenthesis depth 0
        depth = 0
        in_string = False
        from_pos = -1
        i = select_pos + 6
        while i < len(sql) - 3:
            ch = sql[i]
            if ch == "'" and not in_string:
                in_string = True
            elif ch == "'" and in_string:
                in_string = False
            elif not in_string:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                elif (depth == 0 and upper[i:i + 4] == 'FROM'
                      and (i == 0 or not upper[i - 1].isalnum())
                      and (i + 4 >= len(sql) or not upper[i + 4].isalnum())):
                    from_pos = i
                    break
            i += 1

        if from_pos == -1:
            return cols

        select_body = sql[select_pos + 6:from_pos].strip()
        if select_body.upper().startswith('DISTINCT'):
            select_body = select_body[8:].strip()

        # Split by comma at depth 0
        parts: list[str] = []
        depth = 0
        in_str = False
        current: list[str] = []
        for ch in select_body:
            if ch == "'" and not in_str:
                in_str = True
                current.append(ch)
            elif ch == "'" and in_str:
                in_str = False
                current.append(ch)
            elif not in_str:
                if ch == '(':
                    depth += 1
                    current.append(ch)
                elif ch == ')':
                    depth -= 1
                    current.append(ch)
                elif ch == ',' and depth == 0:
                    parts.append(''.join(current))
                    current = []
                else:
                    current.append(ch)
            else:
                current.append(ch)
        if current:
            parts.append(''.join(current))

        def _add(name: str) -> None:
            low = name.lower()
            if low not in cols:
                cols[low] = name

        for part in parts:
            part = part.strip()
            if not part or part == '*':
                continue
            as_match = re.search(r'\bAS\s+(\w+)\s*$', part, re.IGNORECASE)
            if as_match:
                _add(as_match.group(1))
            else:
                dot_match = re.search(r'\.(\w+)\s*$', part)
                if dot_match:
                    _add(dot_match.group(1))
                else:
                    word_match = re.search(r'(\w+)\s*$', part)
                    if word_match:
                        _add(word_match.group(1))
        return cols

    def _build_cte_column_sets(self, graph: DataflowGraph) -> dict[str, set[str]]:
        """Build column output sets per CTE for upstream column qualification.

        Returns a dict mapping cte_name → set of lowercase column names available
        from that CTE. Non-narrowing nodes (Expression, Filter, Lookup, Router)
        inherit upstream columns via src.*.
        """
        cte_cols: dict[str, set[str]] = {}
        for node in graph.nodes:
            cols: set[str] = set()
            if hasattr(node, 'columns'):
                for c in node.columns:
                    cols.add(c.name.lower())
            # Inherit upstream for pass-through nodes
            if isinstance(node, (ExpressionNode, FilterNode, LookupNode, RouterNode)):
                upstream = getattr(node, 'upstream', '')
                if upstream and upstream in cte_cols:
                    cols |= cte_cols[upstream]
            elif isinstance(node, JoinerNode):
                for up in (node.detail_upstream, node.master_upstream):
                    if up and up in cte_cols:
                        cols |= cte_cols[up]
            if isinstance(node, LookupNode) and hasattr(node, 'return_columns'):
                for c in node.return_columns:
                    cols.add(c.name.lower())
            cte_cols[node.cte_name] = cols
        return cte_cols

    _SQL_KEYWORDS = frozenset({
        'and', 'or', 'not', 'is', 'null', 'true', 'false', 'in', 'between',
        'like', 'case', 'when', 'then', 'else', 'end', 'where', 'select',
        'from', 'as', 'on', 'join', 'left', 'right', 'inner', 'full', 'cross',
        'group', 'order', 'by', 'having', 'limit', 'union', 'all', 'distinct',
        'exists', 'asc', 'desc', 'set', 'into', 'values', 'update', 'delete',
        'insert', 'create', 'drop', 'alter', 'table', 'if', 'int64', 'float64',
        'string', 'bool', 'date', 'timestamp', 'datetime', 'numeric', 'bytes',
        'interval',
    })
    _SQL_FUNCTIONS = frozenset({
        'cast', 'safe_cast', 'ifnull', 'nullif', 'coalesce', 'concat',
        'length', 'substr', 'substring', 'upper', 'lower', 'trim', 'ltrim',
        'rtrim', 'replace', 'lpad', 'rpad', 'reverse', 'format',
        'abs', 'round', 'ceil', 'floor', 'trunc', 'mod', 'pow', 'sqrt',
        'sign', 'greatest', 'least', 'any_value', 'count', 'sum', 'avg',
        'min', 'max', 'row_number', 'rank', 'dense_rank', 'ntile',
        'lead', 'lag', 'first_value', 'last_value', 'over', 'partition',
        'date', 'time', 'datetime', 'timestamp', 'date_add', 'date_sub',
        'date_diff', 'datetime_add', 'datetime_sub', 'datetime_diff',
        'timestamp_add', 'timestamp_sub', 'timestamp_diff',
        'extract', 'format_date', 'format_datetime', 'format_timestamp',
        'parse_date', 'parse_datetime', 'parse_timestamp',
        'current_date', 'current_datetime', 'current_timestamp',
        'generate_array', 'unnest', 'array_agg', 'struct', 'decode',
        'string_agg',
    })

    def _add_src_prefix(self, expr: str, upstream_cols: set[str]) -> str:
        """Add 'src.' prefix to bare column refs that come from upstream.

        Scans the expression for bare identifiers (not already prefixed by a
        dot, not SQL keywords/functions, not inside string literals) and adds
        src. prefix if the identifier matches an upstream column name.

        Does NOT qualify identifiers inside scalar subqueries (SELECT...FROM)
        since those refer to the subquery's own table scope.
        """
        if not upstream_cols or not expr:
            return expr

        # ── Detect scalar subquery regions to skip ──
        # Find all (SELECT ... FROM ... WHERE ... LIMIT N) regions
        # and mark them so we don't qualify columns inside them.
        subquery_ranges: list[tuple[int, int]] = []
        upper = expr.upper()
        search_start = 0
        while True:
            # Find '(SELECT' pattern (start of scalar subquery)
            sq_start = upper.find('(SELECT ', search_start)
            if sq_start < 0:
                sq_start = upper.find('(SELECT\n', search_start)
            if sq_start < 0:
                break
            # Find the matching close paren, skipping template expressions
            # and string literals which may contain parentheses
            depth = 0
            j = sq_start
            while j < len(expr):
                ch = expr[j]
                # Skip ${...} template expressions (contain parens)
                if ch == '$' and j + 1 < len(expr) and expr[j + 1] == '{':
                    close = expr.find('}', j + 2)
                    if close >= 0:
                        j = close + 1
                        continue
                # Skip string literals
                if ch == "'":
                    j += 1
                    while j < len(expr) and expr[j] != "'":
                        j += 1
                    j += 1
                    continue
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        subquery_ranges.append((sq_start, j + 1))
                        break
                j += 1
            search_start = sq_start + 1

        def _in_subquery(pos: int) -> bool:
            """Check if position is inside a scalar subquery."""
            for start, end in subquery_ranges:
                if start <= pos < end:
                    return True
            return False

        # Tokenize: split on non-identifier chars while preserving structure
        result = []
        i = 0
        n = len(expr)
        while i < n:
            ch = expr[i]
            # String literal — skip
            if ch == "'":
                j = i + 1
                while j < n and expr[j] != "'":
                    j += 1
                result.append(expr[i:j + 1])
                i = j + 1
                continue
            # Identifier
            if ch.isalpha() or ch == '_':
                j = i
                while j < n and (expr[j].isalnum() or expr[j] == '_'):
                    j += 1
                token = expr[i:j]
                token_lower = token.lower()
                # Check if preceded by '.' (already qualified)
                preceded_by_dot = (i > 0 and expr[i - 1] == '.')
                # Check if followed by '(' (function call)
                followed_by_paren = (j < n and expr[j] == '(')
                # Don't qualify inside scalar subqueries — those columns
                # refer to the subquery's table, not the outer src
                in_subq = _in_subquery(i)
                if (not preceded_by_dot
                        and not followed_by_paren
                        and not in_subq
                        and token_lower not in self._SQL_KEYWORDS
                        and token_lower not in self._SQL_FUNCTIONS
                        and token_lower in upstream_cols):
                    result.append(f"src.{token}")
                else:
                    result.append(token)
                i = j
                continue
            # ${ref(...)} — skip template expressions
            if ch == '$' and i + 1 < n and expr[i + 1] == '{':
                j = expr.find('}', i)
                if j >= 0:
                    result.append(expr[i:j + 1])
                    i = j + 1
                    continue
            # Number
            if ch.isdigit():
                j = i
                while j < n and (expr[j].isdigit() or expr[j] == '.'):
                    j += 1
                result.append(expr[i:j])
                i = j
                continue
            # Other character
            result.append(ch)
            i += 1
        return ''.join(result)

    def _ref(self, table_name: str, graph: DataflowGraph) -> str:
        """Wrap a table name in ${ref()}.

        table_name is already resolved by IRBuilder._resolve_ref() — it may
        be a ref_name_map value (e.g. 'D_Site') or a PascalCase fallback
        (e.g. 'DProductdet'). Use the already-resolved name to avoid double
        PascalCase conversion (lowering strips underscores, re-PascalCasing
        produces wrong casing).
        """
        lower = table_name.lower()
        if lower in graph.ref_name_map:
            display = graph.ref_name_map[lower]
        else:
            # Already resolved by _resolve_ref — use as-is
            display = table_name
        return f"${{ref('{display}')}}"

    def _qualify_expression(self, expr: str, graph: DataflowGraph) -> str:
        """Process an expression — wrap table refs, convert variables."""
        if not expr:
            return expr

        result = expr

        # Convert $$variable references to Dataform project variables
        result = re.sub(
            r'\$\$(\w+)',
            lambda m: f"${{dataform.projectConfig.vars.{m.group(1)}}}",
            result,
        )

        return result

    def _wrap_table_refs(self, sql: str, graph: DataflowGraph) -> str:
        """Wrap bare table names in custom SQL with ${ref()}."""
        # This is a simplified version — handles common patterns
        result = sql

        # Replace schema.table patterns
        result = re.sub(
            r'\b(\w+)\.(\w+)\b(?!\s*\()',  # schema.table but not function calls
            lambda m: self._maybe_wrap_ref(m.group(1), m.group(2), graph),
            result,
        )

        return result

    def _maybe_wrap_ref(self, schema: str, table: str, graph: DataflowGraph) -> str:
        """Wrap a schema.table reference if it looks like a table."""
        # Skip common non-table patterns
        if schema.lower() in ('src', 'lkp', 'detail', 'master', 'j', 'idx'):
            return f"{schema}.{table}"
        # Skip if it's an alias.column reference
        if table[0].islower():
            return f"{schema}.{table}"
        return self._ref(table.lower(), graph)
