"""SSIS package → TA-IR DataflowGraph builder.

Mapping from SSIS Data Flow components to IR node types:

    SSIS component                       → IR node
    ───────────────────────────────────────────────────────────
    OLE DB Source (SqlCommand)           → SourceNode (custom_sql)
    OLE DB Source (OpenRowset table)     → SourceNode (table_ref)
    Lookup                               → LookupNode (LEFT JOIN)
    Derived Column                       → ExpressionNode
    Conditional Split                    → RouterNode
    OLE DB Destination                   → TargetMapping
    Execute SQL Task (TRUNCATE/UPDATE)   → emitted as a sibling operation,
                                            not part of the primary graph

IR node types NOT used here (this source has no equivalent):
  * AggregatorNode    — SSIS aggregations live inside `Microsoft.Aggregate`
                        components which we don't parse in this demo
  * UnionNode         — the SSIS "Union All" component is not modelled
  * NormalizerNode    — SSIS has no row-explosion analogue
  * SequenceNode      — surrogate keys are usually generated downstream
  * JoinerNode        — SSIS "Merge Join" is also out of scope here

This bridge is small because SSIS pushes most of its "logic" into
component properties; the IR mapping is largely 1:1 once the component
kind has been classified.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from transformation_core import (
    ColumnDef,
    DataflowGraph,
    ExpressionNode,
    JoinType,
    LookupNode,
    RouterGroup,
    RouterNode,
    SourceNode,
    TargetMapping,
)

from app.parsers.ssis_xml import (
    SsisComponent,
    SsisDataFlow,
    SsisExecuteSQL,
    SsisPackage,
    parse_package,
)

log = logging.getLogger(__name__)


@dataclass
class SsisOperationsScript:
    """A pre/post-flight Execute SQL task that doesn't belong inside the
    primary graph (TRUNCATE / housekeeping UPDATE / MERGE).
    """
    name: str
    sql: str
    sql_kind: str
    target_table: str
    depends_on: str = ""


@dataclass
class SsisTransformResult:
    package_name: str
    primaries: list[DataflowGraph] = field(default_factory=list)
    operations: list[SsisOperationsScript] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def parse(xml_text: str, filename: str) -> SsisTransformResult | None:
    pkg = parse_package(xml_text, filename)
    if pkg is None:
        return None
    return _to_ir(pkg)


def _to_ir(pkg: SsisPackage) -> SsisTransformResult:
    result = SsisTransformResult(package_name=pkg.name)

    for task in pkg.execute_sql_tasks:
        # Pre/post Execute SQL tasks are emitted as sibling operations.
        # We don't try to merge a TRUNCATE into the data flow's target table
        # — the SQLX assembler can model both as separate operations files.
        if task.sql_kind:
            result.operations.append(SsisOperationsScript(
                name=f"{pkg.name}_{task.name}",
                sql=task.sql,
                sql_kind=task.sql_kind.lower(),
                target_table=task.target_table or "unknown",
            ))

    for flow in pkg.data_flows:
        try:
            graph = _flow_to_graph(pkg.name, flow)
            if graph is not None:
                result.primaries.append(graph)
        except Exception as e:  # noqa: BLE001
            result.warnings.append(f"data flow {flow.name}: {e}")
            log.warning("ssis flow %s build failed", flow.name, exc_info=True)
    return result


def _flow_to_graph(pkg_name: str, flow: SsisDataFlow) -> DataflowGraph | None:
    """Walk a Data Flow Task's components in document order, emit one IR
    node per component, and assemble a DataflowGraph.
    """
    if not flow.components:
        return None

    nodes: list = []
    last_cte: str = ""
    last_cols: list[ColumnDef] = []
    target: TargetMapping | None = None
    router_groups: list[RouterGroup] = []

    for comp in flow.components:
        if comp.kind == "source":
            cte_name = f"cte_{_slug(comp.name)}_src"
            sql_command = comp.properties.get("SqlCommand", "").strip()
            table = comp.properties.get("OpenRowset", "").strip().strip("[]")
            cols = [
                ColumnDef(name=c.name, expression=c.name, is_passthrough=True,
                          bq_data_type=c.data_type)
                for c in comp.columns
            ]
            src = SourceNode(
                cte_name=cte_name,
                table_ref=_normalise_table_ref(table) if table else "",
                custom_sql=sql_command,
                columns=cols,
            )
            nodes.append(src)
            last_cte = cte_name
            last_cols = cols

        elif comp.kind == "lookup":
            cte_name = f"cte_{_slug(comp.name)}"
            ref_table = comp.properties.get("ReferenceTableName", "").strip().strip("[]")
            join_keys = comp.properties.get("JoinKeys", "").strip()
            on_predicate = " AND ".join(
                f"src.{k.strip()} = lkp.{k.strip()}"
                for k in join_keys.split(",")
                if k.strip()
            ) or "TRUE"
            return_cols = [
                ColumnDef(name=c.name, expression=f"lkp.{c.name}",
                          source_node=cte_name, source_column=c.name,
                          bq_data_type=c.data_type)
                for c in comp.columns
            ]
            lkp = LookupNode(
                cte_name=cte_name,
                upstream=last_cte,
                lookup_table=_normalise_table_ref(ref_table),
                lookup_alias="lkp",
                join_type=JoinType.LEFT,
                join_condition=on_predicate,
                return_columns=return_cols,
            )
            nodes.append(lkp)
            last_cte = cte_name
            last_cols = last_cols + return_cols

        elif comp.kind == "derived":
            cte_name = f"cte_{_slug(comp.name)}"
            new_cols = [
                ColumnDef(
                    name=c.name,
                    expression=_translate_ssis_expression(c.expression),
                    bq_data_type=c.data_type,
                )
                for c in comp.columns
            ]
            expr = ExpressionNode(
                cte_name=cte_name,
                upstream=last_cte,
                columns=new_cols,
                pass_upstream=True,
            )
            nodes.append(expr)
            last_cte = cte_name
            last_cols = last_cols + new_cols

        elif comp.kind == "conditional_split":
            for out_name, condition, is_default in comp.split_outputs:
                router_groups.append(RouterGroup(
                    name=out_name,
                    condition=_translate_ssis_expression(condition),
                    is_default=is_default,
                ))
            router = RouterNode(
                cte_name=f"cte_{_slug(comp.name)}",
                upstream=last_cte,
                groups=router_groups,
                columns=list(last_cols),
            )
            nodes.append(router)

        elif comp.kind == "destination":
            tbl = comp.properties.get("OpenRowset", "").strip().strip("[]")
            target = TargetMapping(
                target_table=_normalise_table_ref(tbl),
                columns=[ColumnDef(name=c.name, expression=c.name) for c in last_cols],
            )

        else:
            log.warning("ssis: skipping unknown component kind %s", comp.kind)

    if target is None:
        # No destination found — synthesise one from the flow name so the
        # graph is still valid for inspection.
        target = TargetMapping(
            target_table=flow.name,
            columns=[ColumnDef(name=c.name, expression=c.name) for c in last_cols],
        )

    return DataflowGraph(
        mapping_name=f"{pkg_name}__{flow.name}",
        description=f"SSIS Data Flow: {flow.name}",
        nodes=nodes,
        target=target,
        all_targets=[target],
        table_type="table",
    )


# ─── helpers ──────────────────────────────────────────────────────────────


def _slug(s: str) -> str:
    out = []
    for ch in s.lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "_":
            out.append("_")
    return "".join(out).strip("_") or "node"


def _normalise_table_ref(raw: str) -> str:
    """Strip schema/owner brackets — `[dbo].[Members]` → `dbo.members`."""
    if not raw:
        return ""
    cleaned = raw.replace("[", "").replace("]", "").strip()
    return cleaned.lower()


def _translate_ssis_expression(expr: str) -> str:
    """SSIS expression syntax → SQL-flavoured pseudocode.

    SSIS uses C-ish expressions (`a == b`, `a ? b : c`, `DATEDIFF("yyyy",..)`).
    A production translator would emit a real BigQuery / T-SQL expression;
    for the demo we just normalise the obvious differences so the rendered
    IR reads as SQL.
    """
    if not expr:
        return ""
    out = expr
    # `==` → `=`
    out = out.replace("==", "=")
    # ternary `a ? b : c` → `CASE WHEN a THEN b ELSE c END`
    if "?" in out and ":" in out:
        cond, _, rest = out.partition("?")
        then, _, else_ = rest.partition(":")
        out = f"CASE WHEN {cond.strip()} THEN {then.strip()} ELSE {else_.strip()} END"
    # SSIS string concat with `+` is fine in T-SQL but not BigQuery; the
    # bridge leaves it as-is and lets the SQL generator's dialect render it.
    return out
