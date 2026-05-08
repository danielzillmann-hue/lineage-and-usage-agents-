"""Summary agent — Opus 4.7 synthesizes the executive summary across the three agents."""

from __future__ import annotations

import json
import logging
from typing import Any

from app.agents.base import EmitFn, log_event, stream_thinking
from app.config import get_settings
from app.models.run import AgentName, RunRequest, StreamEvent
from app.models.schema import ExecutiveSummary, Finding

log = logging.getLogger(__name__)

last_result: ExecutiveSummary | None = None


_SUMMARY_PROMPT = """\
You are presenting findings from a multi-agent analysis of an Oracle data
warehouse + ETL pipelines to a technical audience.

You have:
  - inventory: source tables (with FK relationships), defined ETL pipelines
    each annotated with `csv_exists`, `csv_last_modified`, `ran_without_logging`
    and audit-log run stats; CSV outputs each pipeline produces.
  - lineage: column-level edges from source tables → ETL steps → CSV outputs.
  - usage: per-pipeline run counts, success/failure rates, last-run, plus any
    pipeline runs in the audit log that don't have an XML definition.

Distinguish three "did this pipeline run" outcomes:
  1) Audit log shows runs → confirmed; report run count, success rate, last run.
  2) No audit log entries but `csv_exists: true` → pipeline runs WITHOUT
     logging; this is a CRITICAL governance finding (the pipeline executes
     outside the audit framework — observability and SLA tracking can't see it).
  3) No audit log entries AND `csv_exists: false` → genuinely never run;
     report as warn (decommissioned candidate).

Other angles to lean into: pipelines that fail often, undocumented executions
(audit-log entries with no XML), source tables nothing reads, stale CSVs.
Quote specific names and exact numbers — if you can't quote a number, don't
make one up.

Output ONLY a JSON object with this shape:
{
  "headline": "...",
  "bullets": ["...", "..."],
  "metrics": {"total_tables": 7, "pipelines_total": 10, ...},
  "findings": [
    {"severity": "critical|warn|info", "title": "...", "detail": "...",
     "object_fqns": ["S.T", ...], "recommendation": "..."}
  ]
}
"""


async def run(req: RunRequest, results, emit: EmitFn) -> None:
    global last_result
    if not (results.inventory and results.lineage and results.usage):
        await log_event(
            emit,
            AgentName.SUMMARY,
            "Skipping summary — needs inventory, lineage, and usage agents to have run",
        )
        last_result = ExecutiveSummary(headline="Insufficient data for summary", bullets=[])
        return

    payload = _digest(results)
    s = get_settings()
    text = await stream_thinking(
        emit,
        AgentName.SUMMARY,
        s.summary_model,
        system=_SUMMARY_PROMPT,
        user=json.dumps(payload, indent=2),
        location=s.summary_location,
        json_mode=True,
    )

    try:
        raw = text.strip()
        if raw.startswith("```"):
            raw = raw.strip("`").lstrip("json").strip()
        obj: dict[str, Any] = json.loads(raw)
        findings = [Finding.model_validate(f) for f in obj.get("findings", [])]
        summary = ExecutiveSummary(
            headline=obj.get("headline", ""),
            bullets=obj.get("bullets", []),
            findings=findings,
            metrics=obj.get("metrics", {}),
        )
    except Exception as e:  # noqa: BLE001
        log.warning("summary parse failed: %s; first 200=%r", e, text[:200])
        summary = ExecutiveSummary(headline="Summary parse error", bullets=[text[:600]])

    last_result = summary
    await emit(StreamEvent(event="result", agent=AgentName.SUMMARY, data={"findings": len(summary.findings)}))


def _digest(results) -> dict[str, Any]:
    inv = results.inventory
    lin = results.lineage
    use = results.usage
    return {
        "inventory": {
            "table_count": len(inv.tables),
            "view_count": sum(1 for t in inv.tables if t.kind == "VIEW"),
            "csv_outputs": sum(1 for t in inv.tables if t.kind == "CSV"),
            "pipelines_defined": len(inv.pipelines),
            "orphan_runs_count": len(inv.orphan_runs),
            "by_layer": _count_by(inv.tables, lambda t: t.layer.value),
            "by_domain": _count_by(inv.tables, lambda t: t.domain.value),
            "flags": [{"severity": f.severity, "title": f.title, "object": f.object_fqn} for f in inv.flags[:50]],
            "pipelines": [
                {
                    "name": p.name, "output": p.output_csv,
                    "source_tables": p.source_tables, "column_count": p.column_count,
                    "runs": (p.runs.model_dump() if p.runs else None),
                    "csv_exists": p.csv_exists,
                    "csv_last_modified": p.csv_last_modified,
                    "ran_without_logging": (
                        p.csv_exists and (p.runs is None or p.runs.runs_total == 0)
                    ),
                } for p in inv.pipelines[:50]
            ],
            "orphan_runs": [
                {"name": o.pipeline_name, "csv": o.csv_generated, "runs": o.runs.model_dump()}
                for o in inv.orphan_runs[:20]
            ],
        },
        "lineage": {
            "edges": len(lin.edges),
            "unresolved_objects": lin.unresolved[:30],
            "by_operation": _count_by(lin.edges, lambda e: e.operation),
        },
        "usage": {
            "pipelines": [p.model_dump() for p in use.pipelines[:30]],
            "never_run_pipelines": use.never_run_pipelines,
            "runs_without_definition": use.runs_without_definition,
            "hot_tables": use.hot_tables[:15],
            "dead_objects": use.dead_objects[:30],
            "reporting_reachable_sources": use.reporting_reachable_sources[:30],
            "reporting_unreachable_sources": use.reporting_unreachable_sources[:30],
        },
    }


def _count_by(items, key) -> dict[str, int]:
    out: dict[str, int] = {}
    for it in items:
        k = key(it)
        out[k] = out.get(k, 0) + 1
    return out
