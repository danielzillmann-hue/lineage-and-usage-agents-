"""Column annotation pass — Gemini classifies every Inventory column for
sensitivity (PII/financial/tax/internal/public) and nature (data/key/audit/
calculated/reference).

Run after the Inventory agent has built the table list and before the rule
extractor / decommission scoring (since drivers can reference annotations).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.agents.base import EmitFn, log_event, stream_thinking
from app.config import get_settings
from app.models.run import AgentName
from app.models.schema import Column, ColumnNature, Inventory, Sensitivity, Table

log = logging.getLogger(__name__)


_PROMPT = """\
You are classifying columns of an Australian wealth-management Oracle warehouse
for a cloud-migration program.

For each column, return:

  - sensitivity: one of
      pii        — personal identifying (DOB, full name, email, phone, address,
                   member/client/customer number, TFN-like identifier)
      financial  — money amounts, balances, allocations, holdings, fees,
                   transactions, prices
      tax        — tax-related (TFN, tax codes, withheld amounts)
      internal   — business but not externally sensitive (status codes,
                   internal IDs, system flags)
      public     — safe to share (timestamps, generic refs, reference values)

  - nature: one of
      key         — primary key, foreign key, surrogate key, identifier
      audit       — load_dt, created_by, updated_at, source_id, etl-trail
      calculated  — derived (ratios, percentages, _pct suffix, age, score)
      reference   — lookup/dimension code (small enum: status, type, category)
      data        — anything else (the default)

  - notes: 4-12 word rationale.

Output ONLY a JSON array of objects with keys:
  fqn (string, the column's "schema.table.column"),
  sensitivity (string),
  nature (string),
  notes (string).

Be conservative — when in doubt, prefer "internal" + "data".
"""


async def annotate_columns(inv: Inventory, emit: EmitFn) -> None:
    if not inv.tables:
        return

    payload = []
    for t in inv.tables:
        for c in t.columns:
            payload.append({
                "fqn": f"{t.fqn}.{c.name}",
                "table": t.name, "column": c.name,
                "data_type": c.data_type,
                "is_pk": c.is_pk, "is_fk": c.is_fk,
                "fk_target": c.fk_target,
                "comment": c.comment or "",
            })

    if not payload:
        return

    await log_event(emit, AgentName.INVENTORY, f"Classifying {len(payload)} columns for sensitivity + nature")

    text = await stream_thinking(
        emit, AgentName.INVENTORY, get_settings().inventory_model,
        system=_PROMPT,
        user=json.dumps(payload, indent=2),
        json_mode=True,
    )

    rows = _parse_json_array(text)
    if not rows:
        await log_event(emit, AgentName.INVENTORY, "Column annotation: empty response from model")
        return

    # Build multiple lookup keys per row so we can match flexibly:
    #   schema.table.column  (the full fqn we asked for)
    #   table.column         (Gemini sometimes drops the schema)
    #   column               (last-resort, ambiguous but often unique within a table)
    # Each entry is (specificity, row); higher specificity wins on collision.
    flexible: dict[str, tuple[int, dict[str, Any]]] = {}
    for r in rows:
        fqn = str(r.get("fqn", "")).upper()
        if not fqn:
            continue
        parts = fqn.split(".")
        candidates: list[tuple[int, str]] = []
        if len(parts) >= 3:
            candidates.append((3, ".".join(parts[-3:])))
        if len(parts) >= 2:
            candidates.append((2, ".".join(parts[-2:])))
        if parts:
            candidates.append((1, parts[-1]))
        for spec, key in candidates:
            cur = flexible.get(key)
            if not cur or cur[0] < spec:
                flexible[key] = (spec, r)

    applied = 0
    for t in inv.tables:
        for c in t.columns:
            full = f"{t.fqn}.{c.name}".upper()
            short = f"{t.name}.{c.name}".upper()
            bare = c.name.upper()
            row = (
                (flexible.get(full) or (0, None))[1]
                or (flexible.get(short) or (0, None))[1]
                or (flexible.get(bare) or (0, None))[1]
            )
            if not row:
                continue
            try:
                c.sensitivity = Sensitivity(row.get("sensitivity", "internal"))
            except ValueError:
                c.sensitivity = Sensitivity.INTERNAL
            try:
                c.nature = ColumnNature(row.get("nature", "data"))
            except ValueError:
                c.nature = ColumnNature.DATA
            note = row.get("notes")
            if isinstance(note, str) and note.strip():
                c.annotation_notes = note.strip()[:160]
            applied += 1

    await log_event(
        emit, AgentName.INVENTORY,
        f"Column annotations: model returned {len(rows)}, applied to {applied} of {len(payload)} columns",
    )


def _parse_json_array(text: str) -> list[dict[str, Any]]:
    return _parse_array_tolerant(text, log_label="annotations")


def _parse_array_tolerant(text: str, log_label: str) -> list[dict[str, Any]]:
    """Parse a JSON array; if truncated (common when Gemini hits the token cap),
    recover by trimming to the last complete object."""
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`").lstrip("json").strip()
    # Fast path
    try:
        parsed: Any = json.loads(s)
        if isinstance(parsed, dict):
            for k in ("items", "results", "annotations", "rules", "data"):
                if isinstance(parsed.get(k), list):
                    return parsed[k]
            return []
        return parsed if isinstance(parsed, list) else []
    except Exception:  # noqa: BLE001
        pass
    # Recover from truncation: find the last `},` and close the array there.
    if "[" not in s:
        log.warning("%s parse failed and no '[' found; first 200=%r", log_label, text[:200])
        return []
    start = s.index("[")
    body = s[start:]
    # Strip any trailing partial object after the last fully-closed `}`
    last_close = body.rfind("}")
    if last_close == -1:
        log.warning("%s parse failed; no closed object; first 200=%r", log_label, text[:200])
        return []
    repaired = body[: last_close + 1] + "]"
    # Drop a trailing comma before the inserted `]` if any
    repaired = repaired.replace("},\n]", "}\n]").replace("}, ]", "}]")
    try:
        out = json.loads(repaired)
        if isinstance(out, list):
            log.warning("%s parse recovered after truncation: %d objects", log_label, len(out))
            return out
    except Exception as e:  # noqa: BLE001
        log.warning("%s parse recovery failed: %s; first 200=%r last 200=%r", log_label, e, text[:200], text[-200:])
    return []
