"""Business rule extractor — finds value enums, NULL/range constraints,
calculated columns, and filter predicates buried in PL/SQL, view SQL, and
ETL XML transforms.

Migration projects routinely surprise teams when these embedded rules — the
ones encoded in 1990s PL/SQL or buried in Informatica transforms — go missing
on the new platform. This pass surfaces them as structured BusinessRule
records the Transformation Agent (and humans) can preserve.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.agents.base import EmitFn, log_event, stream_thinking
from app.config import get_settings
from app.models.run import AgentName
from app.models.schema import BusinessRule, Inventory

log = logging.getLogger(__name__)


_PROMPT = """\
You are extracting embedded business rules from a wealth-management Oracle
warehouse undergoing migration. The rules will be preserved on the target
platform (SS&C Bluedoor).

Look for:
  - enum   — column constrained to a fixed value set, e.g. "status_cd in ('A', 'C', 'P')"
  - range  — column constrained to numeric/date range, e.g. "amount > 0", "fy_year between 2010 and 2030"
  - not_null — explicit business NOT NULL not just schema-level
  - calculated — derived column, e.g. "age = trunc(months_between(sysdate, dob)/12)"
  - filter   — pipeline/transform-level predicate, e.g. "TRANSACTION_TYPE LIKE '%Tax%'"
  - constraint — anything else worth preserving

Inputs include: view SELECTs, ETL pipeline transforms (calculate_age, math,
filter_text, calculate_category, simulate_performance), and PL/SQL procedures.

Return ONLY a JSON array of objects:
  rule_type, source_object, column (string or null), expression,
  natural_language (8-25 words, plain English), confidence (0-1).

Be specific. If the rule applies to a column, name it. Skip generic ETL
plumbing (load_dt = SYSDATE, etc.) — focus on rules that constrain *business*
values.
"""


async def extract_rules(inv: Inventory, emit: EmitFn) -> None:
    payload = _collect_inputs(inv)
    if not payload:
        return

    await log_event(emit, AgentName.INVENTORY, f"Extracting business rules from {len(payload)} sources")

    text = await stream_thinking(
        emit, AgentName.INVENTORY, get_settings().inventory_model,
        system=_PROMPT,
        user=json.dumps(payload, indent=2),
        json_mode=True,
    )

    rows = _parse_json_array(text)
    extracted: list[BusinessRule] = []
    for r in rows:
        try:
            extracted.append(BusinessRule.model_validate(r))
        except Exception as e:  # noqa: BLE001
            log.debug("rule row dropped: %s — %s", r, e)
    inv.rules.extend(extracted)
    await log_event(emit, AgentName.INVENTORY, f"Business rules extracted: {len(extracted)}")


def _collect_inputs(inv: Inventory) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for t in inv.tables:
        if t.kind == "VIEW" and t.source_text:
            items.append({"kind": "view_sql", "source_object": t.fqn, "text": t.source_text[:6000]})
    for proc in inv.procedures:
        items.append({
            "kind": "procedure",
            "source_object": f"{proc.schema_name}.{proc.name}",
            "text": (proc.source or "")[:8000],
        })
    for p in inv.pipelines:
        # Pull each step's operations + extract query as text
        steps_text: list[str] = []
        for s in p.steps:
            if s.kind == "extract" and s.source_query:
                steps_text.append(f"-- extract {s.id}\n{s.source_query}")
            elif s.operations:
                steps_text.append(f"-- {s.kind} {s.id}: " + " | ".join(s.operations))
        if steps_text:
            items.append({"kind": "etl_pipeline", "source_object": f"PIPELINE.{p.name}", "text": "\n".join(steps_text)[:6000]})
    return items


def _parse_json_array(text: str) -> list[dict[str, Any]]:
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`").lstrip("json").strip()
    try:
        parsed: Any = json.loads(s)
    except Exception:  # noqa: BLE001
        log.warning("rules parse failed; first 200=%r", text[:200])
        return []
    if isinstance(parsed, dict):
        for k in ("items", "results", "rules", "data"):
            if isinstance(parsed.get(k), list):
                return parsed[k]
        return []
    return parsed if isinstance(parsed, list) else []
