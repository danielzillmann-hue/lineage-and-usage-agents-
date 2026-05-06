"""Usage agent — pipeline run analytics + reporting reachability.

Primary signal: ETL_EXECUTION_LOGS (already on inv.pipelines.runs and
inv.orphan_runs). Secondary signal: count how many pipelines READ each source
table (via lineage). Tertiary: AWR for legacy bucket-only inputs.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from collections import defaultdict

from app.agents.base import EmitFn, log_event
from app.models.run import AgentName, RunRequest, StreamEvent
from app.models.schema import (
    Layer, ObjectUsage, PipelineUsage, UsageReport,
)
from app.services import gcs

log = logging.getLogger(__name__)

last_result: UsageReport | None = None

_TABLE_REF = re.compile(r"\b([A-Z][A-Z0-9_]+)\.([A-Z][A-Z0-9_]+)\b")


async def run(req: RunRequest, results, emit: EmitFn) -> None:
    global last_result
    inv = results.inventory
    lin = results.lineage

    objects: dict[str, ObjectUsage] = {}

    # ─── 1. Pipeline runs from audit log (the strong signal) ────────────
    pipelines: list[PipelineUsage] = []
    never_run: list[str] = []
    if inv:
        for p in inv.pipelines:
            r = p.runs
            usage = PipelineUsage(
                pipeline_name=p.name,
                runs_total=r.runs_total if r else 0,
                runs_success=r.runs_success if r else 0,
                runs_failed=r.runs_failed if r else 0,
                last_run=r.last_run if r else None,
                success_rate=(100.0 * r.runs_success / r.runs_total) if r and r.runs_total else 0.0,
                output_csv=p.output_csv,
                has_definition=True,
                csv_exists=p.csv_exists,
                ran_without_logging=p.csv_exists and (not r or r.runs_total == 0),
            )
            pipelines.append(usage)
            if usage.runs_total == 0 and not p.csv_exists:
                never_run.append(p.name)
        for o in inv.orphan_runs:
            pipelines.append(PipelineUsage(
                pipeline_name=o.pipeline_name,
                runs_total=o.runs.runs_total,
                runs_success=o.runs.runs_success,
                runs_failed=o.runs.runs_failed,
                last_run=o.runs.last_run,
                success_rate=100.0 * o.runs.runs_success / o.runs.runs_total if o.runs.runs_total else 0.0,
                output_csv=o.csv_generated,
                has_definition=False,
            ))

    runs_without_definition = [p.pipeline_name for p in pipelines if not p.has_definition]

    await log_event(
        emit, AgentName.USAGE,
        f"Pipeline analytics: {len(pipelines)} pipelines · {len(never_run)} never run · {len(runs_without_definition)} undocumented",
    )

    # ─── 2. Per-table read pressure: count pipelines using each table ───
    if inv and inv.pipelines:
        for p in inv.pipelines:
            for src in p.source_tables:
                fqn = f"SOURCE.{src.upper()}" if "." not in src else src.upper()
                obj = objects.setdefault(fqn, ObjectUsage(fqn=fqn))
                obj.read_count += (p.runs.runs_total if p.runs else 1)
                obj.distinct_users = max(obj.distinct_users, 1)

    # ─── 3. Legacy AWR fallback (bucket-only mode) ──────────────────────
    if req.bucket and not inv.pipelines if inv else False:
        rows = _read_awr(req)
        if rows:
            await log_event(emit, AgentName.USAGE, f"AWR rows: {len(rows)}")
            _accrue_from_awr(rows, objects, inv)

    # ─── 4. Hot, dead, write-only computed from objects map ─────────────
    inventory_fqns = {t.fqn for t in (inv.tables if inv else [])} | {f"SOURCE.{t.name}" for t in (inv.tables if inv else [])}
    hot = sorted(objects.values(), key=lambda o: o.read_count, reverse=True)[:20]
    write_only_orphans = [o.fqn for o in objects.values() if o.write_count > 0 and o.read_count == 0]
    dead: list[str] = []
    if inv:
        active = {o.fqn for o in objects.values()}
        for t in inv.tables:
            if t.kind == "TABLE" and t.fqn not in active and f"SOURCE.{t.name}" not in active:
                dead.append(t.fqn)

    # ─── 5. Reporting reachability (raw → reporting/output) ─────────────
    reachable: set[str] = set()
    unreachable: set[str] = set()
    if inv and lin:
        fwd: dict[str, set[str]] = defaultdict(set)
        for e in lin.edges:
            fwd[e.source_fqn].add(e.target_fqn)
        targets = {t.fqn for t in inv.tables if t.layer in (Layer.REPORTING, Layer.OUTPUT)}
        sources_to_check: list[str] = []
        for t in inv.tables:
            if t.layer == Layer.RAW:
                sources_to_check.append(t.fqn)
                sources_to_check.append(f"SOURCE.{t.name}")
        for src in sources_to_check:
            seen, stack = {src}, [src]
            hit = False
            while stack:
                n = stack.pop()
                if n in targets:
                    hit = True
                    break
                for nxt in fwd.get(n, ()):
                    if nxt in seen:
                        continue
                    seen.add(nxt)
                    stack.append(nxt)
            if hit:
                reachable.add(src)
            else:
                unreachable.add(src)

    last_result = UsageReport(
        objects=list(objects.values()),
        hot_tables=[o.fqn for o in hot],
        write_only_orphans=write_only_orphans,
        dead_objects=dead,
        reporting_reachable_sources=sorted(reachable),
        reporting_unreachable_sources=sorted(unreachable),
        pipelines=pipelines,
        never_run_pipelines=never_run,
        runs_without_definition=runs_without_definition,
    )

    await emit(StreamEvent(
        event="result", agent=AgentName.USAGE,
        data={
            "pipelines": len(pipelines),
            "never_run": len(never_run),
            "undocumented_runs": len(runs_without_definition),
            "hot_tables": len(hot),
            "dead_objects": len(dead),
            "reporting_reachable": len(reachable),
            "reporting_unreachable": len(unreachable),
        },
    ))


def _read_awr(req: RunRequest) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not req.bucket:
        return rows
    for f in gcs.iter_classified(req.bucket, req.prefix):
        if f.kind != "awr":
            continue
        text = gcs.read_text(req.bucket, f.name)
        try:
            for r in csv.DictReader(io.StringIO(text)):
                rows.append({k.upper(): v for k, v in r.items()})
        except Exception as e:  # noqa: BLE001
            log.warning("AWR parse failed for %s: %s", f.name, e)
    return rows


def _accrue_from_awr(rows, objects, inv) -> None:
    known = {t.fqn for t in (inv.tables if inv else [])}
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
            obj = objects.setdefault(fqn, ObjectUsage(fqn=fqn))
            if is_write:
                obj.write_count += execs
                obj.last_write = last or obj.last_write
            else:
                obj.read_count += execs
                obj.last_read = last or obj.last_read
            if user:
                user_seen[fqn].add(user)
    for fqn, obj in objects.items():
        obj.distinct_users = max(obj.distinct_users, len(user_seen.get(fqn, set())))
