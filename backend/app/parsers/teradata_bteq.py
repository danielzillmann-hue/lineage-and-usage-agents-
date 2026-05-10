"""Deterministic parser for Teradata BTEQ scripts (`.bteq`).

A BTEQ script is a *sequenced* mix of two grammars:

  1. **Dot-commands** — start with `.` in column 1, control the BTEQ session
     itself (login, exit code routing, output formatting, file inclusion).
     They never end with a `;` and are line-oriented.

       .LOGON / .LOGOFF / .QUIT
       .SET SESSIONS / .SET WIDTH / .SET ERRORLEVEL
       .IF ERRORCODE <op> <value> THEN .<inner-cmd>
       .RUN FILE = <path>
       .IMPORT / .EXPORT
       .OS <shell command>

  2. **SQL statements** — semicolon-terminated, multi-line, in the Teradata
     dialect. The interesting forms for lineage are:

       CREATE [MULTISET|SET] TABLE <name> AS ( SELECT ... ) WITH DATA
       INSERT INTO <tgt> SELECT ...
       UPDATE <tgt> SET ... WHERE ...
       DELETE FROM <tgt> WHERE ...
       MERGE INTO <tgt> USING <src> ON ... WHEN [NOT] MATCHED THEN ...
       COLLECT STATISTICS [COLUMN (...)] ON <table>     -- metadata only
       DROP TABLE <name>

What we parse:
  * Every dot-command is recorded as an `Operation` (no IR effect)
  * Every SQL statement is sqlglot-parsed in the `teradata` dialect
  * For CTAS (`CREATE TABLE ... AS (SELECT ...)`) we capture target +
    source-table list + the inner SELECT
  * For MERGE we capture target + source + WHEN clauses (UPDATE/INSERT)

What a production-grade parser would also handle:
  * `.IMPORT DATA FILE = ...` flatfile loads (FastLoad/MultiLoad inputs)
  * `.RUN FILE` recursive include (this would chain into a second .bteq)
  * BTEQ HOST variables and `${VAR}` interpolation across statements
  * Nested BT/ET (begin/end transaction) blocks for multi-statement units
  * `LOCKING` modifiers (`LOCKING TABLE x FOR ACCESS SELECT ...`)

Differences vs the Insignia ETL XML parser:
  * Mixed grammar (control script + SQL) inside one file — XML doesn't
    have this problem. We pre-segment lines into "dot block" vs
    "SQL block" before parsing.
  * Statement boundaries are `;` outside of strings/comments. The XML
    format has explicit element boundaries.
  * SQL flavour is Teradata; we tell sqlglot `dialect="teradata"` for
    every parse (XML uses Oracle).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

log = logging.getLogger(__name__)


@dataclass
class DotCommand:
    """One BTEQ control directive — recorded but produces no IR."""
    name: str            # e.g. "LOGON", "SET", "IF", "QUIT"
    args: str = ""       # raw args text, sans the leading dot+name
    line: int = 0


@dataclass
class SqlStatement:
    """One semicolon-terminated SQL statement."""
    raw: str
    kind: str                                # CTAS | INSERT | UPDATE | DELETE | MERGE | COLLECT_STATS | DROP | OTHER
    target_table: str | None = None          # FQN if known
    source_tables: list[str] = field(default_factory=list)
    inner_select: str = ""                   # only for CTAS / INSERT-SELECT
    merge_when_clauses: list[str] = field(default_factory=list)


@dataclass
class BteqScript:
    name: str
    file: str
    dot_commands: list[DotCommand] = field(default_factory=list)
    statements: list[SqlStatement] = field(default_factory=list)


# Match a leading-dot directive: `.LOGON tdprod/user,pw` etc.
# Capture name in group(1), the rest of the line in group(2).
_DOT_RE = re.compile(r"^\.\s*([A-Za-z]+)(.*)$")


def parse_script(text: str, filename: str) -> BteqScript:
    """Parse a BTEQ file into ordered dot commands + SQL statements.

    The two streams are not interleaved in the result — we keep them in
    parallel ordered lists. For the demo this is enough; downstream code
    only needs to know "what SQL was issued, in what order" plus "what
    control directives were declared".
    """
    name = filename.removesuffix(".bteq").split("/")[-1].split("\\")[-1]
    script = BteqScript(name=name, file=filename)

    # ─── 1. Pre-segment: strip /* … */ comments, then walk lines ──────────
    cleaned = _strip_block_comments(text)

    sql_buf: list[str] = []
    sql_start_line = 0

    for lineno, raw_line in enumerate(cleaned.splitlines(), 1):
        stripped = raw_line.strip()
        if not stripped:
            if sql_buf:
                sql_buf.append("")  # preserve blank line inside a SQL stmt
            continue

        # Strip trailing `--` line comments before deciding what kind of
        # line this is (a `--` comment can sit after a dot-command too).
        no_comment = _strip_line_comment(raw_line)
        if not no_comment.strip():
            continue

        if no_comment.lstrip().startswith("."):
            # A dot-command can NOT appear mid-statement. If we saw one
            # while a SQL buffer was open, that buffer was unterminated —
            # flush it as best-effort and warn.
            if sql_buf:
                pending = "\n".join(sql_buf).strip()
                if pending:
                    log.warning("bteq: SQL not terminated by ';' before line %d", lineno)
                    script.statements.append(_classify_statement(pending))
                sql_buf = []
            m = _DOT_RE.match(no_comment.strip())
            if m:
                script.dot_commands.append(DotCommand(
                    name=m.group(1).upper(),
                    args=m.group(2).strip(),
                    line=lineno,
                ))
            continue

        # Otherwise it's part of a SQL statement. Statements end at `;`
        # but a single line can hold multiple short statements — split on
        # unquoted semicolons.
        if not sql_buf:
            sql_start_line = lineno
        for chunk, terminated in _split_on_semicolons(no_comment):
            sql_buf.append(chunk)
            if terminated:
                stmt_text = "\n".join(sql_buf).strip()
                if stmt_text:
                    script.statements.append(_classify_statement(stmt_text))
                sql_buf = []

    # Trailing un-terminated SQL — uncommon but possible (e.g. file ends
    # mid-statement). Record it so we don't silently lose work.
    if sql_buf:
        tail = "\n".join(sql_buf).strip()
        if tail:
            log.warning("bteq: file ended with un-terminated SQL near line %d", sql_start_line)
            script.statements.append(_classify_statement(tail))
    return script


# ─── classification ───────────────────────────────────────────────────────


def _classify_statement(sql: str) -> SqlStatement:
    """sqlglot-parse a single BTEQ SQL statement and bucket it."""
    text = sql.strip().rstrip(";")
    upper = text.upper().lstrip()

    # COLLECT STATISTICS is Teradata-specific and sqlglot may parse it as a
    # generic Command. Detect it cheaply up front.
    if upper.startswith("COLLECT"):
        target = _extract_table_after_keyword(text, "ON")
        return SqlStatement(raw=sql, kind="COLLECT_STATS", target_table=target)

    if upper.startswith("DROP"):
        target = _extract_table_after_keyword(text, "TABLE")
        return SqlStatement(raw=sql, kind="DROP", target_table=target)

    try:
        tree = sqlglot.parse_one(text, dialect="teradata")
    except Exception as e:
        log.warning("bteq: sqlglot failed on statement: %s", e)
        return SqlStatement(raw=sql, kind="OTHER")

    if isinstance(tree, exp.Create) and (tree.args.get("kind") or "").upper() == "TABLE":
        target = _table_name(tree.this if isinstance(tree.this, exp.Table)
                             else tree.this.find(exp.Table) if tree.this else None)
        # The body of a CTAS lives in tree.expression as a Select.
        select = tree.expression if isinstance(tree.expression, exp.Select) \
            else (tree.find(exp.Select) if tree.expression is not None else None)
        sources = [_table_name(t) for t in (select.find_all(exp.Table) if select else [])
                   if _table_name(t) and _table_name(t) != target]
        return SqlStatement(
            raw=sql,
            kind="CTAS",
            target_table=target,
            source_tables=[s for s in sources if s],
            inner_select=select.sql(dialect="teradata") if select else "",
        )

    if isinstance(tree, exp.Insert):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table)
                             else tree.this.find(exp.Table) if tree.this else None)
        select = tree.find(exp.Select)
        sources = [_table_name(t) for t in (select.find_all(exp.Table) if select else [])
                   if _table_name(t)]
        return SqlStatement(
            raw=sql,
            kind="INSERT",
            target_table=target,
            source_tables=[s for s in sources if s],
            inner_select=select.sql(dialect="teradata") if select else "",
        )

    if isinstance(tree, exp.Update):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table) if _table_name(t)]
        if target:
            sources = [s for s in sources if s and s != target]
        return SqlStatement(raw=sql, kind="UPDATE", target_table=target,
                            source_tables=[s for s in sources if s])

    if isinstance(tree, exp.Delete):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        return SqlStatement(raw=sql, kind="DELETE", target_table=target)

    if isinstance(tree, exp.Merge):
        target = _table_name(tree.this if isinstance(tree.this, exp.Table) else None)
        sources = [_table_name(t) for t in tree.find_all(exp.Table)
                   if _table_name(t) and _table_name(t) != target]
        # WHEN clauses live under tree.args["whens"] in sqlglot
        whens_arg = tree.args.get("whens")
        if whens_arg is not None:
            whens_list = whens_arg.expressions if hasattr(whens_arg, "expressions") else []
        else:
            whens_list = []
        when_strs = [w.sql(dialect="teradata") for w in whens_list]
        return SqlStatement(raw=sql, kind="MERGE", target_table=target,
                            source_tables=[s for s in sources if s],
                            merge_when_clauses=when_strs)

    return SqlStatement(raw=sql, kind="OTHER")


# ─── lexing helpers ───────────────────────────────────────────────────────


def _strip_block_comments(text: str) -> str:
    """Remove `/* … */` blocks. Naive (doesn't track string literals) but
    enough for demo scripts which don't put `/*` inside quoted strings.
    """
    return re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)


def _strip_line_comment(line: str) -> str:
    """Drop trailing `--` comments outside of single-quoted strings."""
    in_quote = False
    out = []
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "'":
            in_quote = not in_quote
            out.append(ch)
        elif not in_quote and ch == "-" and i + 1 < len(line) and line[i + 1] == "-":
            break
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _split_on_semicolons(line: str) -> list[tuple[str, bool]]:
    """Yield (chunk, terminated) for each `;`-delimited piece on one line.

    Ignores semicolons inside single-quoted strings.
    """
    parts: list[tuple[str, bool]] = []
    in_quote = False
    buf: list[str] = []
    for ch in line:
        if ch == "'":
            in_quote = not in_quote
            buf.append(ch)
        elif ch == ";" and not in_quote:
            parts.append(("".join(buf), True))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append(("".join(buf), False))
    return parts


def _table_name(t) -> str | None:
    if t is None or not isinstance(t, exp.Table):
        return None
    db = t.args.get("db")
    db_name = db.name if hasattr(db, "name") else None
    if db_name and t.name:
        return f"{db_name}.{t.name}"
    return t.name or None


def _extract_table_after_keyword(sql: str, keyword: str) -> str | None:
    """Naive: pull the first whitespace-separated token after `keyword`.

    Used for `COLLECT STATISTICS … ON <tbl>` and `DROP TABLE <tbl>` where
    sqlglot doesn't reliably model the form.
    """
    pat = re.compile(rf"\b{re.escape(keyword)}\b\s+([A-Za-z0-9_.\"`\[\]]+)", re.IGNORECASE)
    m = pat.search(sql)
    if not m:
        return None
    raw = m.group(1).rstrip(";").strip("\"`[]")
    return raw or None


def parse_all(files: list[tuple[str, str]]) -> list[BteqScript]:
    out: list[BteqScript] = []
    for name, text in files:
        if not name.lower().endswith(".bteq"):
            continue
        out.append(parse_script(text, name))
    return out
