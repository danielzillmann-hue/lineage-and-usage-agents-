"""Insignia pipeline XML → TA-IR DataflowGraph builder.

This is the consumer-side adapter for transformation_core. We parse Direnc's
pipeline XML schema (`<extract>`, `<execute_sql>`, `<join>`, `<aggregate>`,
`<transform>`, `<load>`, `<extract_csv>`) and emit a `DataflowGraph` that
the shared `sql_generator` + `wrap_sqlx` can render to a Dataform `.sqlx`.

Three tiers of pipeline:
- **Tier 1 — pure SQL**: only `<execute_sql>` with INSERT INTO ... SELECT.
  We sqlglot-parse the SQL and build IR from the AST.
- **Tier 2 — declarative**: chained `<extract>`/`<join>`/`<aggregate>`/
  `<transform>`/`<load>`. Each step maps to one IR node.
- **Tier 3 — multi-write**: a Tier 2 pipeline plus post-load `<execute_sql>`
  UPDATE/DELETE/TRUNCATE statements that mutate the staging table after
  it's been loaded. The primary IR captures the load; the operations
  scripts are emitted as separate `type: "operations"` SQLX files.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from xml.etree import ElementTree as ET

from transformation_core import (
    AggregatorNode,
    ColumnDef,
    DataflowGraph,
    ExpressionNode,
    JoinerNode,
    JoinType,
    SourceNode,
    TargetMapping,
)

from app.transformer.sql_helpers import (
    classify_dml,
    parse_insert_select,
    parse_select,
)

log = logging.getLogger(__name__)


@dataclass
class OperationsScript:
    """A post-load DML statement (UPDATE/DELETE/TRUNCATE/MERGE) that should
    be emitted as a sibling Dataform `type: "operations"` SQLX, depending on
    the primary load.
    """
    name: str               # e.g. "regulatory_audit_compliance_flag_anomalies"
    sql: str                # the original SQL from the <execute_sql>
    sql_kind: str           # "update" / "delete" / "truncate" / "merge"
    target_table: str       # which table the DML modifies
    depends_on: str = ""    # primary table this operation runs after


@dataclass
class TransformResult:
    """Complete output of transforming one pipeline XML.

    Pipelines with multiple `<load>` steps split into multiple primary
    DataflowGraphs — one per materialised table. Later stages read
    earlier targets via `${ref()}` rather than recomputing.
    """
    pipeline_name: str
    primary: DataflowGraph | None = None
    primaries: list[DataflowGraph] = field(default_factory=list)
    operations: list[OperationsScript] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ─── Internal builder state ──────────────────────────────────────────────


@dataclass
class _Stage:
    """One materialised output — accumulates nodes until a <load> commits it."""
    nodes: list = field(default_factory=list)
    cte_by_step_id: dict[str, str] = field(default_factory=dict)
    cols_by_step_id: dict[str, list[ColumnDef]] = field(default_factory=dict)
    target: TargetMapping | None = None

    def add_node(self, node, step_id: str, columns: list[ColumnDef]) -> None:
        self.nodes.append(node)
        self.cte_by_step_id[step_id] = node.cte_name
        self.cols_by_step_id[step_id] = columns


@dataclass
class _BuilderState:
    pipeline_name: str
    current: _Stage = field(default_factory=_Stage)
    completed_stages: list[_Stage] = field(default_factory=list)
    operations: list[OperationsScript] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    schema: str = ""  # empty -> SQLX omits `schema:` and Dataform uses defaultDataset

    @property
    def nodes(self):
        return self.current.nodes

    def add_node(self, node, step_id: str, columns: list[ColumnDef]) -> None:
        self.current.add_node(node, step_id, columns)

    def upstream_cte(self, step_id: str) -> str | None:
        # Only the current stage is in-scope for upstream lookups; once a
        # stage is committed via <load>, subsequent steps must read its
        # target via ${ref()} rather than referencing its CTE name.
        return self.current.cte_by_step_id.get(step_id)

    def upstream_cols(self, step_id: str) -> list[ColumnDef]:
        return list(self.current.cols_by_step_id.get(step_id, []))

    def commit_stage(self, target: TargetMapping) -> None:
        """Snapshot current stage with the given target and start fresh."""
        self.current.target = target
        self.completed_stages.append(self.current)
        self.current = _Stage()

    @property
    def primary_target(self) -> TargetMapping | None:
        # Back-compat: returns the most recent committed target.
        if self.completed_stages:
            return self.completed_stages[-1].target
        return self.current.target


# ─── Public entry point ──────────────────────────────────────────────────


def parse(xml_text: str, filename: str) -> TransformResult | None:
    """Parse one Insignia pipeline XML to a TransformResult.

    Returns None if the XML doesn't look like a pipeline.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log.warning("XML parse failed for %s: %s", filename, e)
        return None
    if root.tag != "pipeline":
        return None

    name = root.attrib.get("name") or filename.removesuffix(".xml")
    state = _BuilderState(pipeline_name=name)

    steps_el = root.find("steps")
    if steps_el is None:
        return TransformResult(pipeline_name=name, warnings=["pipeline has no <steps>"])

    for child in steps_el:
        try:
            if child.tag == "extract":
                _process_extract(child, state)
            elif child.tag == "extract_csv":
                _process_extract_csv(child, state)
            elif child.tag == "join":
                _process_join(child, state)
            elif child.tag == "aggregate":
                _process_aggregate(child, state)
            elif child.tag == "transform":
                _process_transform(child, state)
            elif child.tag == "load":
                _process_load(child, state)
            elif child.tag == "execute_sql":
                _process_execute_sql(child, state)
            else:
                state.warnings.append(f"unknown step: <{child.tag}>")
        except Exception as e:  # noqa: BLE001
            state.warnings.append(f"step {child.tag}#{child.attrib.get('id','?')} failed: {e}")
            log.warning("step processing failed in %s: %s", name, e, exc_info=True)

    primaries = _build_primary_graphs(state)
    return TransformResult(
        pipeline_name=name,
        primary=primaries[0] if primaries else None,
        primaries=primaries,
        operations=state.operations,
        warnings=state.warnings,
    )


# ─── Step processors ─────────────────────────────────────────────────────


def _process_extract(el: ET.Element, state: _BuilderState) -> None:
    """`<extract id="..."><query>SELECT ... FROM ... [WHERE ...] [GROUP BY ...]</query></extract>`."""
    step_id = el.attrib.get("id") or f"ext_{len(state.nodes)}"
    sql = (el.findtext("query") or "").strip()
    if not sql:
        state.warnings.append(f"extract {step_id}: empty query")
        return
    chain = parse_select(sql, base_name=step_id)
    if chain is None or not chain.nodes:
        state.warnings.append(f"extract {step_id}: SQL parse produced no nodes")
        return
    for n in chain.nodes:
        state.current.nodes.append(n)
    state.current.cte_by_step_id[step_id] = chain.final_cte
    state.current.cols_by_step_id[step_id] = chain.output_columns


def _process_extract_csv(el: ET.Element, state: _BuilderState) -> None:
    """External CSV reader. We treat it as a SourceNode pointing to a logical
    BQ external declaration named after the CSV file (without extension).
    """
    step_id = el.attrib.get("id") or f"extcsv_{len(state.nodes)}"
    path = el.attrib.get("path") or ""
    csv_name = path.split("/")[-1].removesuffix(".csv").lower() or step_id
    src = SourceNode(
        cte_name=f"cte_{step_id}_src",
        table_ref=csv_name,
        columns=[],  # populated downstream when JOIN/SELECT references columns
    )
    state.add_node(src, step_id, src.columns)


def _process_join(el: ET.Element, state: _BuilderState) -> None:
    """`<join id="X" left="A" right="B" on="COL" how="inner|left|right|full"/>`."""
    step_id = el.attrib.get("id") or f"join_{len(state.nodes)}"
    left = el.attrib.get("left") or ""
    right = el.attrib.get("right") or ""
    on_col = el.attrib.get("on") or ""
    how = (el.attrib.get("how") or "left").lower()

    left_cte = state.upstream_cte(left)
    right_cte = state.upstream_cte(right)
    if not left_cte or not right_cte:
        state.warnings.append(f"join {step_id}: missing upstream ({left}={left_cte}, {right}={right_cte})")
        return

    join_type = {
        "inner": JoinType.INNER, "left": JoinType.LEFT,
        "right": JoinType.RIGHT, "full": JoinType.FULL, "cross": JoinType.CROSS,
    }.get(how, JoinType.LEFT)

    # Build merged column list. Always qualify with the join alias
    # (`detail.X` for left, `master.X` for right) so BQ never has to guess
    # when both sides expose the same column name (e.g. a foreign-key
    # column that's also a primary key on the right side). Left wins on
    # collisions — it's the kept side for LEFT JOINs.
    left_cols = state.upstream_cols(left)
    right_cols = state.upstream_cols(right)
    seen: set[str] = set()
    merged: list[ColumnDef] = []
    for c in left_cols:
        key = c.name.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(ColumnDef(
            name=c.name, expression=f"detail.{c.name}", is_passthrough=True,
        ))
    for c in right_cols:
        key = c.name.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(ColumnDef(
            name=c.name, expression=f"master.{c.name}", is_passthrough=True,
        ))

    # TA's sql_generator emits `FROM detail JOIN master`, so the XML's
    # left side (the kept side for LEFT JOIN) maps to detail_upstream.
    join = JoinerNode(
        cte_name=f"cte_{step_id}",
        detail_upstream=left_cte,
        master_upstream=right_cte,
        join_type=join_type,
        join_condition=f"detail.{on_col} = master.{on_col}",
        columns=merged,
        detail_alias="detail",
        master_alias="master",
    )
    state.add_node(join, step_id, merged)


def _process_aggregate(el: ET.Element, state: _BuilderState) -> None:
    """`<aggregate id="X" input="Y" group_by="C1,C2"><sum column="A" result_column="TOT"/></aggregate>`."""
    step_id = el.attrib.get("id") or f"agg_{len(state.nodes)}"
    input_id = el.attrib.get("input") or ""
    group_by = [c.strip() for c in (el.attrib.get("group_by") or "").split(",") if c.strip()]

    upstream_cte = state.upstream_cte(input_id)
    if not upstream_cte:
        state.warnings.append(f"aggregate {step_id}: unknown input '{input_id}'")
        return

    columns: list[ColumnDef] = []
    for g in group_by:
        columns.append(ColumnDef(name=g, expression=g, is_passthrough=True))

    for child in el:
        agg_fn = child.tag.upper()  # sum / avg / min / max / count
        col = child.attrib.get("column") or ""
        result = child.attrib.get("result_column") or f"{agg_fn}_{col}"
        columns.append(ColumnDef(
            name=result,
            expression=f"{agg_fn}({col})",
        ))

    agg = AggregatorNode(
        cte_name=f"cte_{step_id}",
        upstream=upstream_cte,
        group_by=group_by,
        columns=columns,
    )
    state.add_node(agg, step_id, columns)


def _process_transform(el: ET.Element, state: _BuilderState) -> None:
    """`<transform id="X" input="Y">
            <math operation="multiply" col1="A" col2="B" result_column="C"/>
            <calculate_category column="X" result_column="Y" low="1.5" high="2.5"/>
            <drop columns="C1,C2"/>
       </transform>`
    """
    step_id = el.attrib.get("id") or f"xf_{len(state.nodes)}"
    input_id = el.attrib.get("input") or ""
    upstream_cte = state.upstream_cte(input_id)
    if not upstream_cte:
        state.warnings.append(f"transform {step_id}: unknown input '{input_id}'")
        return

    upstream_cols = state.upstream_cols(input_id)
    drop_cols: set[str] = set()
    new_cols: list[ColumnDef] = []

    for child in el:
        tag = child.tag.lower()
        if tag == "drop":
            for c in (child.attrib.get("columns") or "").split(","):
                if c.strip():
                    drop_cols.add(c.strip().lower())
        elif tag == "math":
            op = (child.attrib.get("operation") or "").lower()
            c1 = child.attrib.get("col1") or ""
            c2 = child.attrib.get("col2") or ""
            res = child.attrib.get("result_column") or "calc"
            sym = {"multiply": "*", "add": "+", "subtract": "-", "divide": "/"}.get(op, "+")
            new_cols.append(ColumnDef(name=res, expression=f"src.{c1} {sym} src.{c2}"))
        elif tag == "calculate_category":
            col = child.attrib.get("column") or ""
            res = child.attrib.get("result_column") or "category"
            low = child.attrib.get("low") or "0"
            high = child.attrib.get("high") or "1"
            expr = (
                f"CASE WHEN src.{col} < {low} THEN 'low' "
                f"WHEN src.{col} > {high} THEN 'high' ELSE 'medium' END"
            )
            new_cols.append(ColumnDef(name=res, expression=expr))
        else:
            state.warnings.append(f"transform {step_id}: unknown op <{tag}>")

    # Compose output columns: upstream minus drops + new
    out_cols: list[ColumnDef] = [
        ColumnDef(name=c.name, expression=c.name, is_passthrough=True)
        for c in upstream_cols
        if c.name.lower() not in drop_cols
    ] + new_cols

    # Pure column-list change (drops only, no new computed columns) —
    # emit no CTE. The drop is already realised at the final SELECT
    # projection because TargetMapping.columns reads from cols_by_step_id,
    # which we set to the post-drop out_cols below. Net effect: identical
    # output, one fewer trivial CTE.
    if not new_cols:
        state.current.cte_by_step_id[step_id] = upstream_cte
        state.current.cols_by_step_id[step_id] = out_cols
        return

    expr_node = ExpressionNode(
        cte_name=f"cte_{step_id}",
        upstream=upstream_cte,
        columns=new_cols,
        pass_upstream=True,
    )
    state.add_node(expr_node, step_id, out_cols)


def _process_load(el: ET.Element, state: _BuilderState) -> None:
    """`<load input="X" type="oracle|csv" table="..." path="..."/>`."""
    input_id = el.attrib.get("input") or ""
    load_type = (el.attrib.get("type") or "").lower()
    table = el.attrib.get("table") or ""
    path = el.attrib.get("path") or ""

    upstream_cols = state.upstream_cols(input_id)
    target_table = table or (path.split("/")[-1].removesuffix(".csv") if path else "")
    if not target_table:
        state.warnings.append("load: no target table or path")
        return

    target = TargetMapping(
        target_table=target_table,
        target_schema=state.schema,
        columns=[ColumnDef(name=c.name, expression=c.name) for c in upstream_cols],
        is_incremental=False,
    )
    state.commit_stage(target)


def _process_execute_sql(el: ET.Element, state: _BuilderState) -> None:
    """Two cases:

    * INSERT INTO target (cols) SELECT ... — full pipeline expressed in SQL.
      Translate via parse_insert_select and use as the primary path.
    * UPDATE / DELETE / TRUNCATE / MERGE — modify-only operation. Emit as
      a sibling OperationsScript that the SQLX assembler will wrap with
      `type: "operations"`.
    """
    step_id = el.attrib.get("id") or f"sql_{len(state.nodes)}"
    sql = (el.findtext("query") or "").strip()
    if not sql:
        return
    kind = classify_dml(sql)

    if kind == "insert":
        target_table, target_cols, chain = parse_insert_select(sql, base_name=step_id)
        if not target_table or chain is None or not chain.nodes:
            state.warnings.append(f"execute_sql {step_id}: INSERT parse failed")
            return
        for n in chain.nodes:
            state.current.nodes.append(n)
        state.current.cte_by_step_id[step_id] = chain.final_cte
        state.current.cols_by_step_id[step_id] = chain.output_columns
        # Synthesise a load-equivalent target mapping and commit the stage.
        cols = (
            [ColumnDef(name=c, expression=c) for c in target_cols]
            if target_cols else
            [ColumnDef(name=c.name, expression=c.name) for c in chain.output_columns]
        )
        state.commit_stage(TargetMapping(
            target_table=target_table,
            target_schema=state.schema,
            columns=cols,
        ))
        return

    if kind in {"update", "delete", "truncate", "merge"}:
        # Skip pure TRUNCATE statements that just clean a target table —
        # in Dataform we emit `type: "table"` which auto-replaces, so the
        # truncate is implicit. (For incremental tables it would matter,
        # but those are out of scope for now.)
        if kind == "truncate" and state.primary_target is not None:
            return

        # Find the table this DML modifies (best-effort)
        target_table = _extract_dml_target(sql) or "unknown"
        op_name = f"{state.pipeline_name}_{step_id}"
        state.operations.append(OperationsScript(
            name=op_name,
            sql=sql,
            sql_kind=kind,
            target_table=target_table,
            depends_on=state.primary_target.target_table if state.primary_target else "",
        ))
        return

    if kind == "select":
        # Bare SELECT inside a pipeline is unusual — log and skip.
        state.warnings.append(f"execute_sql {step_id}: bare SELECT, skipping")
        return

    state.warnings.append(f"execute_sql {step_id}: unrecognised DML kind '{kind}'")


def _extract_dml_target(sql: str) -> str | None:
    """Best-effort: pull the target table name out of UPDATE/DELETE/TRUNCATE."""
    try:
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(sql, dialect="oracle")
        if isinstance(tree, (exp.Update, exp.Delete, exp.Merge)):
            t = tree.this if isinstance(tree.this, exp.Table) else None
            if t is not None:
                return t.name
        if isinstance(tree, exp.TruncateTable):
            t = tree.this if isinstance(tree.this, exp.Table) else None
            if t is not None:
                return t.name
    except Exception:
        pass
    # Fallback to text scan
    text = sql.upper()
    for kw in ("UPDATE ", "DELETE FROM ", "TRUNCATE TABLE ", "MERGE INTO "):
        idx = text.find(kw)
        if idx >= 0:
            after = sql[idx + len(kw):].strip()
            return after.split()[0].strip(";").strip("`'\"") if after else None
    return None


# ─── Assemble the primary DataflowGraph ──────────────────────────────────


def _build_primary_graphs(state: _BuilderState) -> list[DataflowGraph]:
    """Build one DataflowGraph per committed stage. If steps were processed
    after the last <load> (e.g. transforms with no terminal load), they're
    flushed as a final auto-named stage targeting the pipeline name.
    """
    stages = list(state.completed_stages)

    # Flush any uncommitted current stage as an implicit terminal target
    # (named after the pipeline). Useful when a pipeline ends without a
    # <load> step.
    if state.current.nodes:
        target = TargetMapping(
            target_table=state.pipeline_name,
            target_schema=state.schema,
            columns=[],
        )
        state.current.target = target
        stages.append(state.current)

    out: list[DataflowGraph] = []
    for i, stage in enumerate(stages):
        if not stage.nodes:
            continue
        target = stage.target or TargetMapping(
            target_table=state.pipeline_name, target_schema=state.schema
        )
        if not target.columns and stage.cte_by_step_id:
            # Default to the last node's column list
            last_id = list(stage.cte_by_step_id.keys())[-1]
            target.columns = [
                ColumnDef(name=c.name, expression=c.name)
                for c in stage.cols_by_step_id.get(last_id, [])
            ]
        # Multi-stage pipelines: name later stages by target table to avoid
        # collisions (each stage emits its own SQLX file).
        mapping_name = (
            state.pipeline_name if len(stages) == 1
            else target.target_table or f"{state.pipeline_name}_{i}"
        )
        out.append(DataflowGraph(
            mapping_name=mapping_name,
            nodes=stage.nodes,
            target=target,
            all_targets=[target],
            table_type="table",
            schema=state.schema,
        ))
    return out
