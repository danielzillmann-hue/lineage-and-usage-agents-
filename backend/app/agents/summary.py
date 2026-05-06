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
You are presenting findings from a multi-agent analysis of an Oracle data warehouse to a
technical audience at Insignia Financial. Your job is to write the executive summary.

Voice: confident, evidence-led, specific. Quote real numbers from the inputs. No fluff.
Lead with what's surprising, valuable, or actionable. Findings should each name objects.

Output ONLY a JSON object with this shape:
{
  "headline": "...",
  "bullets": ["...", "..."],
  "metrics": {"total_tables": 312, "reporting_unreachable_pct": 23.5, ...},
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
            "procedure_count": len(inv.procedures),
            "by_layer": _count_by(inv.tables, lambda t: t.layer.value),
            "by_domain": _count_by(inv.tables, lambda t: t.domain.value),
            "flags": [{"severity": f.severity, "title": f.title, "object": f.object_fqn} for f in inv.flags[:50]],
        },
        "lineage": {
            "edges": len(lin.edges),
            "unresolved_objects": lin.unresolved[:30],
            "table_count": len({e.source_fqn for e in lin.edges} | {e.target_fqn for e in lin.edges}),
        },
        "usage": {
            "objects_seen": len(use.objects),
            "hot_tables": use.hot_tables[:15],
            "write_only_orphans": use.write_only_orphans[:30],
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
