"""Deterministic parser for Snowflake pipeline scripts (Tasks + Streams +
Snowflake Scripting procedures).

A Snowflake "pipeline" in this codebase means a single .sql file that
defines:

  * **STREAMs**     — change-data-capture cursors over a base table.
                      `CREATE OR REPLACE STREAM s ON TABLE t [...]`
  * **TASKs**       — schedulable SQL units, optionally chained via AFTER
                      to form a DAG.
                      `CREATE OR REPLACE TASK t [WAREHOUSE = ...]
                                                 [SCHEDULE = ...]
                                                 [AFTER other_task]
                       AS <body>`
  * **PROCEDUREs**  — Snowflake Scripting blocks (`LANGUAGE SQL AS $$ … $$`)
                      whose body contains nested `INSERT/MERGE/DELETE/UPDATE`
                      statements that we want to follow into.

  * Standalone DML  — `INSERT INTO`, `MERGE INTO`, `COPY INTO`. These can
                      appear inside a TASK body, inside a procedure body,
                      or at the file top level.

What we parse:
  * Each STREAM definition (source table + flags).
  * Each TASK definition (warehouse, schedule, AFTER dependency, body SQL).
  * Each PROCEDURE definition (language, dollar-quoted body, parsed inner
    statements).
  * Top-level DML statements outside any task/procedure (rare but legal).

What we deliberately ignore (a production parser would handle these):
  * `CREATE OR REPLACE PIPE … AUTO_INGEST = TRUE` — Snowpipe ingestion
  * `CREATE OR REPLACE DYNAMIC TABLE … TARGET_LAG = '...' AS …`
  * `EXECUTE AS OWNER` / privileges / row-access policies
  * Snowflake Scripting control flow (LET, IF/THEN/ELSE, FOR cursor) —
    we collapse a procedure body into its top-level DML statements
  * `TASK GRAPH` orchestration via FINALIZE / `SYSTEM$STREAM_HAS_DATA`

Differences vs the Insignia ETL XML parser:
  * No XML — input is free-form SQL with a small set of DDL-shaped
    pipeline objects. We tokenise statement boundaries on `;`, splitting
    on dollar-quoted (`$$ … $$`) procedure bodies first.
  * The DAG is encoded *inside* the SQL (`AFTER <other_task>`); the XML
    pipeline embeds the DAG in its element tree.
  * Snowflake Streams are stateful — the SOURCE table relationship is what
    matters for lineage, not the stream's own row history.
  * SQL flavour is Snowflake; sqlglot dialect is `snowflake`.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

log = logging.getLogger(__name__)


@dataclass
class SnowflakeStream:
    name: str
    on_table: str
    options: dict[str, str] = field(default_factory=dict)


@dataclass
class SnowflakeTask:
    """One TASK. `body` may be a single statement (MERGE/INSERT/CALL) or
    a multi-statement block — we record the raw SQL and let the IR bridge
    decide how to map it.
    """
    name: str
    warehouse: str = ""
    schedule: str = ""
    after: list[str] = field(default_factory=list)   # task names this AFTERs
    body: str = ""
    body_kind: str = ""                              # MERGE | INSERT | CALL | OTHER
    body_target_table: str | None = None
    body_source_tables: list[str] = field(default_factory=list)


@dataclass
class SnowflakeProcedure:
    name: str
    language: str = "SQL"
    body: str = ""
    inner_statements: list[str] = field(default_factory=list)


@dataclass
class SnowflakeStatement:
    """A standalone top-level DML statement (not wrapped in TASK/PROCEDURE)."""
    raw: str
    kind: str
    target_table: str | None = None
    source_tables: list[str] = field(default_factory=list)


@dataclass
class SnowflakePipeline:
    name: str
    file: str
    streams: list[SnowflakeStream] = field(default_factory=list)
    tasks: list[SnowflakeTask] = field(default_factory=list)
    procedures: list[SnowflakeProcedure] = field(default_factory=list)
    statements: list[SnowflakeStatement] = field(default_factory=list)


# ─── Top-level parse ──────────────────────────────────────────────────────


def parse_pipeline(text: str, filename: str) -> SnowflakePipeline:
    name = filename.removesuffix(".sql").split("/")[-1].split("\\")[-1]
    pipe = SnowflakePipeline(name=name, file=filename)

    cleaned = _strip_line_comments(text)
    statements = _split_statements(cleaned)

    for stmt in statements:
        kind = _shape_of(stmt)
        if kind == "STREAM":
            s = _parse_stream(stmt)
            if s is not None:
                pipe.streams.append(s)
        elif kind == "TASK":
            t = _parse_task(stmt)
            if t is not None:
                pipe.tasks.append(t)
        elif kind == "PROCEDURE":
            p = _parse_procedure(stmt)
            if p is not None:
                pipe.procedures.append(p)
        elif kind == "ALTER_TASK":
            # ALTER TASK … RESUME / SUSPEND — operationally important but no
            # IR effect. Drop on the floor; don't even warn.
            continue
        elif kind in {"INSERT", "MERGE", "DELETE", "UPDATE", "COPY"}:
            pipe.statements.append(_classify_top_level(stmt, kind))
        elif kind == "EMPTY":
            continue
        else:
            log.warning("snowflake: unrecognised top-level shape: %s", stmt[:80])
    return pipe


# ─── Statement splitter (dollar-quote-aware) ──────────────────────────────


def _split_statements(text: str) -> list[str]:
    """Split on `;` while respecting dollar-quoted (`$$ … $$`) blocks.

    Snowflake Scripting procedures wrap multi-statement bodies inside
    `$$ … $$`; the inner `;`s must NOT terminate the outer CREATE
    PROCEDURE statement. Single-quoted strings ('…') are also respected.
    """
    out: list[str] = []
    buf: list[str] = []
    i = 0
    in_dollar = False
    in_squote = False
    while i < len(text):
        ch = text[i]
        # Detect $$ (only when not inside a single quote)
        if not in_squote and ch == "$" and i + 1 < len(text) and text[i + 1] == "$":
            in_dollar = not in_dollar
            buf.append("$$")
            i += 2
            continue
        if not in_dollar and ch == "'":
            in_squote = not in_squote
            buf.append(ch)
            i += 1
            continue
        if not in_dollar and not in_squote and ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def _strip_line_comments(text: str) -> str:
    """Drop `-- …` line comments. Doesn't track strings — fine for demos."""
    cleaned: list[str] = []
    for line in text.splitlines():
        idx = line.find("--")
        cleaned.append(line if idx < 0 else line[:idx])
    return "\n".join(cleaned)


def _shape_of(stmt: str) -> str:
    """Cheap classification of a top-level statement by its leading tokens."""
    s = stmt.lstrip().upper()
    if not s:
        return "EMPTY"
    if s.startswith("CREATE") and "STREAM" in s.split()[:5]:
        return "STREAM"
    if s.startswith("CREATE") and "TASK" in s.split()[:5]:
        return "TASK"
    if s.startswith("CREATE") and "PROCEDURE" in s.split()[:5]:
        return "PROCEDURE"
    if s.startswith("ALTER TASK"):
        return "ALTER_TASK"
    if s.startswith("INSERT"):
        return "INSERT"
    if s.startswith("MERGE"):
        return "MERGE"
    if s.startswith("DELETE"):
        return "DELETE"
    if s.startswith("UPDATE"):
        return "UPDATE"
    if s.startswith("COPY"):
        return "COPY"
    return "OTHER"


# ─── Per-shape parsers ────────────────────────────────────────────────────


_STREAM_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?STREAM\s+([A-Za-z0-9_.]+)\s+ON\s+TABLE\s+([A-Za-z0-9_.]+)\s*(.*?)$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_stream(stmt: str) -> SnowflakeStream | None:
    m = _STREAM_RE.search(stmt)
    if not m:
        log.warning("snowflake: stream stmt didn't match shape: %s", stmt[:80])
        return None
    name, on_table, tail = m.group(1), m.group(2), m.group(3)
    options: dict[str, str] = {}
    for opt_match in re.finditer(r"([A-Z_]+)\s*=\s*([A-Za-z0-9'_-]+)", tail, re.IGNORECASE):
        options[opt_match.group(1).upper()] = opt_match.group(2).strip("'")
    return SnowflakeStream(name=name, on_table=on_table, options=options)


_TASK_HEAD_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+([A-Za-z0-9_.]+)(.*?)\bAS\b\s+(.*)$",
    re.IGNORECASE | re.DOTALL,
)
_AFTER_RE = re.compile(r"\bAFTER\s+([A-Za-z0-9_.,\s]+?)(?=\bAS\b|\bWAREHOUSE\b|\bSCHEDULE\b|$)",
                       re.IGNORECASE)
_WAREHOUSE_RE = re.compile(r"\bWAREHOUSE\s*=\s*([A-Za-z0-9_]+)", re.IGNORECASE)
_SCHEDULE_RE = re.compile(r"\bSCHEDULE\s*=\s*'([^']+)'", re.IGNORECASE)


def _parse_task(stmt: str) -> SnowflakeTask | None:
    m = _TASK_HEAD_RE.search(stmt)
    if not m:
        log.warning("snowflake: task stmt didn't match shape: %s", stmt[:80])
        return None
    name = m.group(1)
    head = m.group(2) or ""
    body = (m.group(3) or "").strip()

    task = SnowflakeTask(name=name, body=body)
    wh = _WAREHOUSE_RE.search(head)
    sched = _SCHEDULE_RE.search(head)
    after = _AFTER_RE.search(head)
    if wh:
        task.warehouse = wh.group(1)
    if sched:
        task.schedule = sched.group(1)
    if after:
        task.after = [a.strip() for a in after.group(1).split(",") if a.strip()]

    task.body_kind = _shape_of(body)
    task.body_target_table, task.body_source_tables = _classify_body(body, task.body_kind)
    return task


_PROC_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([A-Za-z0-9_.]+)\s*\(.*?\)"
    r"\s+RETURNS\s+\S+\s+LANGUAGE\s+(\w+)\s+AS\s+\$\$(.*?)\$\$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_procedure(stmt: str) -> SnowflakeProcedure | None:
    m = _PROC_RE.search(stmt)
    if not m:
        log.warning("snowflake: procedure stmt didn't match shape: %s", stmt[:80])
        return None
    name = m.group(1)
    lang = m.group(2).upper()
    body = m.group(3).strip()
    # Strip BEGIN/END/RETURN — the inner DML is what matters for lineage.
    body_inner = re.sub(r"^\s*BEGIN\b", "", body, flags=re.IGNORECASE)
    body_inner = re.sub(r"\bRETURN\b\s*[^;]*;?\s*$", "", body_inner, flags=re.IGNORECASE)
    body_inner = re.sub(r"\bEND\b\s*;?\s*$", "", body_inner, flags=re.IGNORECASE)
    inner = [s for s in _split_statements(body_inner) if s.strip()]
    return SnowflakeProcedure(name=name, language=lang, body=body, inner_statements=inner)


def _classify_top_level(stmt: str, kind: str) -> SnowflakeStatement:
    target, sources = _classify_body(stmt, kind)
    return SnowflakeStatement(raw=stmt, kind=kind, target_table=target, source_tables=sources)


# ─── SQL helpers (Snowflake dialect) ──────────────────────────────────────


def _classify_body(sql: str, kind: str) -> tuple[str | None, list[str]]:
    """Best-effort: extract target + source tables for the given body."""
    if not sql:
        return None, []
    if kind == "CALL" or sql.lstrip().upper().startswith("CALL "):
        # CALL <proc>(args) — record the proc name as "target" so lineage
        # can chain into the procedure later.
        m = re.match(r"\s*CALL\s+([A-Za-z0-9_.]+)", sql, re.IGNORECASE)
        return (m.group(1) if m else None), []
    try:
        tree = sqlglot.parse_one(sql, dialect="snowflake")
    except Exception:
        return None, []
    if isinstance(tree, exp.Insert):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table)
                             else tree.this.find(exp.Table) if tree.this else None)
        select = tree.find(exp.Select)
        sources = [_table_name(t) for t in (select.find_all(exp.Table) if select else [])
                   if _table_name(t)]
        return target, [s for s in sources if s]
    if isinstance(tree, exp.Merge):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table)
                   if _table_name(t) and _table_name(t) != target]
        return target, [s for s in sources if s]
    if isinstance(tree, exp.Update):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table) if _table_name(t)]
        if target:
            sources = [s for s in sources if s and s != target]
        return target, [s for s in sources if s]
    if isinstance(tree, exp.Delete):
        return _table_name(tree.this if isinstance(tree.this, exp.Table) else None), []
    return None, []


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


def parse_all(files: list[tuple[str, str]]) -> list[SnowflakePipeline]:
    out: list[SnowflakePipeline] = []
    for name, text in files:
        if not name.lower().endswith(".sql"):
            continue
        out.append(parse_pipeline(text, name))
    return out
