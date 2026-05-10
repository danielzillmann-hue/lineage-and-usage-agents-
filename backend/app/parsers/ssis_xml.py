"""Deterministic parser for SQL Server Integration Services (SSIS) `.dtsx`
packages.

SSIS packages are XML containers (`<DTS:Executable>`) holding a tree of
*executable* tasks. The interesting ones for lineage are:

- **Execute SQL Task** (`DTS:ExecutableType="...ExecuteSQLTask"`) — runs a
  parameterised SQL statement carried on a `<DTS:Property Name="SqlStatementSource">`.
  Used for TRUNCATE/MERGE/post-load housekeeping.
- **Data Flow Task** (`DTS:ExecutableType="SSIS.Pipeline.X"`) — the only place
  where component-to-component column lineage exists. The body is a
  `<pipeline>` of `<component>` elements typed by `componentClassID`:
    * `Microsoft.OLEDBSource`        → relational source (a `SqlCommand` or `OpenRowset`)
    * `Microsoft.Lookup`             → cached lookup against a reference table
    * `Microsoft.DerivedColumn`      → SSIS-expression column additions
    * `Microsoft.ConditionalSplit`   → row router with one output per condition
    * `Microsoft.OLEDBDestination`   → relational target

What we parse:
  * Connection managers (for source/target wiring)
  * Variables (`<DTS:Variable>`)
  * Execute SQL tasks (sql text + connection)
  * Data Flow components above, with input/output column lists

What we deliberately ignore (a production parser would handle these):
  * `<DTS:PrecedenceConstraint>` between executables — task DAG ordering
  * `<DTS:LogProvider>`, `<DTS:EventHandler>` — operational instrumentation
  * Script Task / Script Component — embedded C# is opaque to us
  * Foreach loops, package configurations, ProtectionLevel-encrypted blobs
  * The component-to-component path map (`<paths>` in real .dtsx) — we infer
    flow as the document order of components within a pipeline

Differences vs the Insignia ETL XML parser (`etl_xml.py`):
  * SSIS components are typed by an attribute (`componentClassID`), not by
    their tag name. We dispatch on that attribute.
  * SSIS uses an XML namespace (`xmlns:DTS="www.microsoft.com/SqlServer/Dts"`)
    so element matching has to be namespace-aware.
  * SSIS packs SQL statements inside `<DTS:Property Name="...">` text, not as
    a child `<query>` element.
  * SSIS embedded SQL is T-SQL, not Oracle PL/SQL — we tell sqlglot to use
    the `tsql` dialect.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from xml.etree import ElementTree as ET

import sqlglot
from sqlglot import exp

log = logging.getLogger(__name__)

DTS_NS = "www.microsoft.com/SqlServer/Dts"
DTS = f"{{{DTS_NS}}}"


@dataclass
class SsisColumn:
    """A column on a component's input or output buffer."""
    name: str
    data_type: str = ""
    length: int | None = None
    expression: str = ""           # only for DerivedColumn outputs
    lineage_id: str = ""           # SSIS upstream pointer (free-form string)


@dataclass
class SsisComponent:
    """One component inside a Data Flow Task pipeline."""
    ref_id: str
    name: str
    kind: str  # source | lookup | derived | conditional_split | destination | unknown
    class_id: str = ""
    properties: dict[str, str] = field(default_factory=dict)
    columns: list[SsisColumn] = field(default_factory=list)
    # ConditionalSplit: list of (output_name, expression, is_default)
    split_outputs: list[tuple[str, str, bool]] = field(default_factory=list)


@dataclass
class SsisExecuteSQL:
    """An Execute SQL Task — a SQL statement run outside any data flow."""
    name: str
    connection: str = ""
    sql: str = ""
    sql_kind: str | None = None
    target_table: str | None = None
    source_tables: list[str] = field(default_factory=list)


@dataclass
class SsisDataFlow:
    """One Data Flow Task. `components` is in document order; we treat
    document order as the linear flow because we drop the `<paths>` map.
    """
    name: str
    components: list[SsisComponent] = field(default_factory=list)


@dataclass
class SsisPackage:
    name: str
    file: str
    connection_managers: dict[str, str] = field(default_factory=dict)
    variables: dict[str, str] = field(default_factory=dict)
    execute_sql_tasks: list[SsisExecuteSQL] = field(default_factory=list)
    data_flows: list[SsisDataFlow] = field(default_factory=list)


# ─── 1. Parse one .dtsx file ──────────────────────────────────────────────


def parse_package(xml_text: str, filename: str) -> SsisPackage | None:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log.warning("DTSX parse failed for %s: %s", filename, e)
        return None
    if not root.tag.endswith("Executable"):
        return None

    name = root.attrib.get(f"{DTS}ObjectName") or filename.removesuffix(".dtsx")
    pkg = SsisPackage(name=name, file=filename)

    cm_root = root.find(f"{DTS}ConnectionManagers")
    if cm_root is not None:
        for cm in cm_root.findall(f"{DTS}ConnectionManager"):
            cm_name = cm.attrib.get(f"{DTS}ObjectName") or ""
            conn_str = cm.attrib.get(f"{DTS}ConnectionString") or ""
            if cm_name:
                pkg.connection_managers[cm_name] = conn_str

    var_root = root.find(f"{DTS}Variables")
    if var_root is not None:
        for v in var_root.findall(f"{DTS}Variable"):
            v_name = v.attrib.get(f"{DTS}ObjectName") or ""
            v_value = (v.findtext(f"{DTS}VariableValue") or "").strip()
            if v_name:
                pkg.variables[v_name] = v_value

    # Walk every nested DTS:Executable. SSIS allows arbitrary nesting; demo
    # packages are typically one level deep but we handle the general case.
    for child in root.iter(f"{DTS}Executable"):
        if child is root:
            continue
        ex_type = child.attrib.get(f"{DTS}ExecutableType") or ""
        if "ExecuteSQLTask" in ex_type:
            pkg.execute_sql_tasks.append(_parse_execute_sql(child))
        elif "SSIS.Pipeline" in ex_type:
            pkg.data_flows.append(_parse_data_flow(child))
        else:
            log.warning("ssis: unhandled executable type %r", ex_type)
    return pkg


def _parse_execute_sql(el: ET.Element) -> SsisExecuteSQL:
    name = el.attrib.get(f"{DTS}ObjectName") or "ExecuteSQL"
    task = SsisExecuteSQL(name=name)
    for prop in el.findall(f"{DTS}Property"):
        prop_name = prop.attrib.get(f"{DTS}Name") or ""
        text = (prop.text or "").strip()
        if prop_name == "SqlStatementSource":
            task.sql = text
        elif prop_name == "Connection":
            task.connection = text
    if task.sql:
        task.sql_kind, task.target_table, task.source_tables = _classify_sql(task.sql)
    return task


def _parse_data_flow(el: ET.Element) -> SsisDataFlow:
    name = el.attrib.get(f"{DTS}ObjectName") or "DataFlow"
    flow = SsisDataFlow(name=name)
    # The pipeline body is wrapped in DTS:ObjectData → <pipeline>. Some
    # demo files embed <pipeline> directly under the Executable; tolerate both.
    pipeline_el = el.find(f"{DTS}ObjectData/pipeline") or el.find("pipeline") \
        or el.find(f".//pipeline")
    if pipeline_el is None:
        return flow
    for comp_el in pipeline_el.findall("component"):
        flow.components.append(_parse_component(comp_el))
    return flow


def _parse_component(el: ET.Element) -> SsisComponent:
    class_id = el.attrib.get("componentClassID") or ""
    kind = _component_kind(class_id)
    comp = SsisComponent(
        ref_id=el.attrib.get("refId") or "",
        name=el.attrib.get("name") or el.attrib.get("refId") or "",
        kind=kind,
        class_id=class_id,
    )

    props_el = el.find("properties")
    if props_el is not None:
        for p in props_el.findall("property"):
            p_name = p.attrib.get("name") or ""
            text = (p.text or "").strip()
            if p_name:
                comp.properties[p_name] = text

    # Output columns — for sources/lookups/derived this is the schema we
    # expose downstream. We pull every `<column>` we see across all outputs;
    # ordering within the document is preserved.
    for out in el.findall("outputs/output"):
        # ConditionalSplit publishes its row groups as outputs; capture the
        # group expression rather than column lists.
        if kind == "conditional_split":
            out_name = out.attrib.get("name") or ""
            is_default = (out.attrib.get("isDefault") or "").lower() == "true"
            expr = ""
            for p in out.findall("properties/property"):
                if p.attrib.get("name") == "EvaluationExpression":
                    expr = (p.text or "").strip()
            comp.split_outputs.append((out_name, expr, is_default))
            continue
        for col in out.findall("columns/column"):
            try:
                length = int(col.attrib.get("length")) if col.attrib.get("length") else None
            except ValueError:
                length = None
            comp.columns.append(SsisColumn(
                name=col.attrib.get("name") or "",
                data_type=col.attrib.get("dataType") or "",
                length=length,
                expression=col.attrib.get("expression") or "",
                lineage_id=col.attrib.get("lineageId") or "",
            ))
    return comp


def _component_kind(class_id: str) -> str:
    cid = class_id.lower()
    if "oledbsource" in cid:
        return "source"
    if "lookup" in cid:
        return "lookup"
    if "derivedcolumn" in cid:
        return "derived"
    if "conditionalsplit" in cid:
        return "conditional_split"
    if "oledbdestination" in cid or "destination" in cid:
        return "destination"
    log.warning("ssis: unknown component class %r", class_id)
    return "unknown"


# ─── 2. SQL helper (T-SQL) ────────────────────────────────────────────────


def _classify_sql(sql: str) -> tuple[str | None, str | None, list[str]]:
    """Return (kind, target_table, source_tables) for an Execute SQL task.

    Mirrors `etl_xml._classify_sql` but uses the `tsql` dialect.
    """
    if not sql:
        return None, None, []
    try:
        tree = sqlglot.parse_one(sql, dialect="tsql")
    except Exception:
        return None, None, []
    if isinstance(tree, exp.Insert):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table)
                             else tree.this.find(exp.Table) if tree.this else None)
        sources = [
            _table_name(t)
            for sel in tree.find_all(exp.Select)
            for t in sel.find_all(exp.Table)
        ]
        return "INSERT", target, [s for s in sources if s]
    if isinstance(tree, exp.Update):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table) if _table_name(t)]
        if target:
            sources = [s for s in sources if s and s.upper() != target.upper()]
        return "UPDATE", target, [s for s in sources if s]
    if isinstance(tree, exp.Delete):
        return "DELETE", _table_name(tree.this if isinstance(tree.this, exp.Table) else None), []
    if isinstance(tree, exp.TruncateTable):
        tbl = tree.this if isinstance(tree.this, exp.Table) else None
        return "TRUNCATE", _table_name(tbl), []
    if isinstance(tree, exp.Merge):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table)
                   if _table_name(t) and _table_name(t) != target]
        return "MERGE", target, [s for s in sources if s]
    # Fallback for parser-as-Command (TRUNCATE often falls here in tsql)
    text = sql.strip().upper()
    if text.startswith("TRUNCATE"):
        parts = text.split()
        for i, t in enumerate(parts):
            if t == "TABLE" and i + 1 < len(parts):
                return "TRUNCATE", parts[i + 1].rstrip(";"), []
    return None, None, []


def _table_name(t: Any) -> str | None:
    if t is None or not isinstance(t, exp.Table):
        return None
    return t.name.upper() if t.name else None


def _extract_source_query_tables(sql_command: str) -> list[str]:
    """For an OLE DB Source's `SqlCommand`, list FROM-clause tables."""
    if not sql_command:
        return []
    # SSIS allows `?` placeholders for parameter binding — these confuse
    # sqlglot. Replace with a literal so the parse succeeds.
    cleaned = sql_command.replace("?", "NULL")
    try:
        tree = sqlglot.parse_one(cleaned, dialect="tsql")
    except Exception:
        return []
    if not isinstance(tree, exp.Select):
        return []
    return [t.name for t in tree.find_all(exp.Table) if t.name]


def parse_all(files: list[tuple[str, str]]) -> list[SsisPackage]:
    out: list[SsisPackage] = []
    for name, text in files:
        if not name.lower().endswith(".dtsx"):
            continue
        pkg = parse_package(text, name)
        if pkg is not None:
            out.append(pkg)
    return out
