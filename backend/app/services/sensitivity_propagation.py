"""Propagate column sensitivity through the lineage graph.

If a source column is classified as PII (or financial / tax), every column
downstream of it in the lineage graph inherits the same sensitivity unless it
already has a stricter classification of its own. The list of propagation
sources is preserved so the UI can show *why* a column is flagged.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Iterable

from app.models.schema import Inventory, LineageGraph, Sensitivity

log = logging.getLogger(__name__)


# Severity ordering — only "uphill" propagation. We never downgrade an
# explicit classification; we only escalate from less- to more-sensitive.
_SEV_ORDER: dict[Sensitivity, int] = {
    Sensitivity.PUBLIC: 0,
    Sensitivity.INTERNAL: 1,
    Sensitivity.FINANCIAL: 2,
    Sensitivity.TAX: 3,
    Sensitivity.PII: 4,
}


def propagate(inv: Inventory, lineage: LineageGraph | None) -> int:
    """Walk the lineage graph forward from sensitive columns; mark inheritors.

    Returns the number of (column, source) pairs added.
    """
    if not lineage or not lineage.edges:
        return 0

    # Index: "<schema.table.column>" → Column ref + parent table for fast lookup
    col_index: dict[str, tuple] = {}
    for t in inv.tables:
        for c in t.columns:
            col_index[f"{t.fqn}.{c.name}".upper()] = (t, c)
            # Cross-schema short refs that the lineage emits as SOURCE.<TABLE>.<COL>
            col_index[f"SOURCE.{t.name}.{c.name}".upper()] = (t, c)

    # Build a downstream adjacency at the (FQN, column) level for full granularity.
    downstream: dict[str, list[str]] = defaultdict(list)
    for e in lineage.edges:
        if not e.source_column or not e.target_column:
            continue
        s_key = f"{e.source_fqn}.{e.source_column}".upper()
        t_key = f"{e.target_fqn}.{e.target_column}".upper()
        downstream[s_key].append(t_key)

    # BFS forward from every column marked sensitive enough to propagate.
    added = 0
    for t in inv.tables:
        for c in t.columns:
            if _SEV_ORDER.get(c.sensitivity, 0) < _SEV_ORDER[Sensitivity.FINANCIAL]:
                continue
            # Skip "inherited"-only entries — only propagate from explicit sources
            origin_key = f"{t.fqn}.{c.name}".upper()
            origin_sev = c.sensitivity
            seen: set[str] = {origin_key}
            queue: list[str] = list(downstream.get(origin_key, []))
            # Also seed with SOURCE.* form for the lineage edges that use it
            queue.extend(downstream.get(f"SOURCE.{t.name}.{c.name}".upper(), []))
            while queue:
                key = queue.pop()
                if key in seen:
                    continue
                seen.add(key)
                target = _resolve(key, col_index)
                if target:
                    _, tgt_col = target
                    if _SEV_ORDER.get(tgt_col.sensitivity, 0) < _SEV_ORDER[origin_sev]:
                        tgt_col.sensitivity = origin_sev
                    if origin_key not in tgt_col.inherited_sensitivity_from:
                        tgt_col.inherited_sensitivity_from.append(origin_key)
                        added += 1
                queue.extend(downstream.get(key, []))
    return added


def _resolve(key: str, idx: dict) -> tuple | None:
    """Match against several FQN spellings the lineage emits."""
    if key in idx:
        return idx[key]
    # SOURCE.<TABLE>.<COL> ↔ <SCHEMA>.<TABLE>.<COL> fallback
    parts = key.split(".")
    if len(parts) >= 2:
        bare = ".".join(parts[-2:])
        for full_key, value in idx.items():
            if full_key.endswith("." + bare) or full_key.endswith(bare):
                return value
    return None


def pii_reach_summary(inv: Inventory) -> dict[str, int]:
    """Summary stats — how many objects/columns were touched by propagation."""
    pii_cols = sum(1 for t in inv.tables for c in t.columns if c.sensitivity == Sensitivity.PII)
    inherited = sum(
        1 for t in inv.tables for c in t.columns
        if c.inherited_sensitivity_from
    )
    inherited_objects = sum(
        1 for t in inv.tables
        if any(c.inherited_sensitivity_from for c in t.columns)
    )
    return {
        "pii_columns_total": pii_cols,
        "columns_with_inherited_sensitivity": inherited,
        "objects_with_inherited_sensitivity": inherited_objects,
    }
