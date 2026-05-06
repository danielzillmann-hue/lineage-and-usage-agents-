"""Usage agent — parses AWR / V$SQL exports, joins onto lineage, classifies activity.

Outputs the punchline metrics:
  - hot tables (top reads)
  - write-only orphans (ETL targets nobody reads)
  - dead objects (no reads or writes in window)
  - reporting reachability: which raw tables flow to reporting layer; which don't
"""

from __future__ import annotations

import csv
import io
import logging
import re
from collections import defaultdict

from app.agents.base import EmitFn, log_event
from app.models.run import AgentName, RunRequest
from app.models.schema import Layer, ObjectUsage, UsageReport
from app.services import gcs

log = logging.getLogger(__name__)

last_result: UsageReport | None = None


_TABLE_REF = re.compile(r"\b([A-Z][A-Z0-9_]+)\.([A-Z][A-Z0-9_]+)\b")


async def run(req: RunRequest, results, emit: EmitFn) -> None:
    global last_result
    inv = results.inventory
    lineage = results.lineage

    await log_event(emit, AgentName.USAGE, "Reading AWR / V$SQL exports")
    rows = _read_awr(req)
    await log_event(emit, AgentName.USAGE, f"Loaded {len(rows)} query records")

    object_counts = _count_objects(rows, inv)

    await log_event(
        emit,
        AgentName.USAGE,
        f"Resolved query references to {len(object_counts)} objects — computing reachability",
    )

    report = _build_report(object_counts, inv, lineage)
    last_result = report
    await _emit_result(emit, report)


def _read_awr(req: RunRequest) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for f in gcs.iter_classified(req.bucket, req.prefix):
        if f.kind != "awr":
            continue
        text = gcs.read_text(req.bucket, f.name)
        try:
            reader = csv.DictReader(io.StringIO(text))
            for r in reader:
                # normalize keys to upper case for lookup robustness
                rows.append({k.upper(): v for k, v in r.items()})
        except Exception as e:  # noqa: BLE001
            log.warning("AWR parse failed for %s: %s", f.name, e)
    return rows


def _count_objects(rows: list[dict[str, str]], inv) -> dict[str, ObjectUsage]:
    """For each AWR row, extract referenced tables from SQL_TEXT and accrue reads.

    Conservative approach: regex scan for SCHEMA.NAME tokens against known inventory.
    Refined later via sqlglot if AWR captures full text.
    """
    known = {t.fqn for t in (inv.tables if inv else [])}
    counts: dict[str, ObjectUsage] = {}
    user_seen: dict[str, set[str]] = defaultdict(set)

    for r in rows:
        sql = r.get("SQL_TEXT") or r.get("SQL_FULLTEXT") or ""
        execs = int(float(r.get("EXECUTIONS_TOTAL") or r.get("EXECUTIONS") or "1") or 1)
        user = r.get("PARSING_SCHEMA_NAME") or r.get("USERNAME") or ""
        last = r.get("LAST_ACTIVE_TIME") or r.get("LAST_ACTIVE") or None
        is_write = bool(re.search(r"\b(INSERT|UPDATE|MERGE|DELETE)\b", sql, re.I))
        for m in _TABLE_REF.finditer(sql.upper()):
            fqn = f"{m.group(1)}.{m.group(2)}"
            if known and fqn not in known:
                continue
            obj = counts.setdefault(fqn, ObjectUsage(fqn=fqn))
            if is_write:
                obj.write_count += execs
                obj.last_write = last or obj.last_write
            else:
                obj.read_count += execs
                obj.last_read = last or obj.last_read
            if user:
                user_seen[fqn].add(user)

    for fqn, obj in counts.items():
        obj.distinct_users = len(user_seen[fqn])
    return counts


def _build_report(counts: dict[str, ObjectUsage], inv, lineage) -> UsageReport:
    objects = list(counts.values())
    hot = sorted(objects, key=lambda o: o.read_count, reverse=True)[:20]
    write_only = [o.fqn for o in objects if o.write_count > 0 and o.read_count == 0]
    dead = []
    if inv:
        active = {o.fqn for o in objects}
        for t in inv.tables:
            if t.fqn not in active:
                dead.append(t.fqn)

    reach_in, reach_out = _reachability(inv, lineage)

    return UsageReport(
        objects=objects,
        hot_tables=[o.fqn for o in hot],
        write_only_orphans=write_only,
        dead_objects=dead,
        reporting_reachable_sources=sorted(reach_in),
        reporting_unreachable_sources=sorted(reach_out),
    )


def _reachability(inv, lineage) -> tuple[set[str], set[str]]:
    if not inv or not lineage:
        return set(), set()
    fwd: dict[str, set[str]] = defaultdict(set)
    for e in lineage.edges:
        fwd[e.source_fqn].add(e.target_fqn)
    reporting = {t.fqn for t in inv.tables if t.layer == Layer.REPORTING}
    raw = {t.fqn for t in inv.tables if t.layer == Layer.RAW}
    reachable: set[str] = set()
    for src in raw:
        seen, stack = set(), [src]
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            if node in reporting:
                reachable.add(src)
                break
            stack.extend(fwd.get(node, ()))
    return reachable, raw - reachable


async def _emit_result(emit: EmitFn, report: UsageReport) -> None:
    from app.models.run import StreamEvent

    await emit(
        StreamEvent(
            event="result",
            agent=AgentName.USAGE,
            data={
                "objects": len(report.objects),
                "hot_tables": len(report.hot_tables),
                "write_only_orphans": len(report.write_only_orphans),
                "dead_objects": len(report.dead_objects),
                "reporting_reachable": len(report.reporting_reachable_sources),
                "reporting_unreachable": len(report.reporting_unreachable_sources),
            },
        )
    )
