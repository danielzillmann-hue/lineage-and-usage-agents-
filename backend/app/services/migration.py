"""Migration analytics — deterministic computations over Inventory + Lineage.

Produces the program-management signals the Lineage Agent surfaces:
  - DecommissionAssessment per table  (safe / review / blocked + score)
  - MigrationWave list (topological wave plan)
  - MultiWriterTarget list  (mirrors Transformation Agent's registry)
  - MigrationScope manifest (in-scope vs out-of-scope) — emitted on demand
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Iterable

from app.models.schema import (
    DecommissionAssessment, ETLPipeline, Inventory, LineageEdge, LineageGraph,
    MigrationWave, MultiWriterTarget, Table,
)

log = logging.getLogger(__name__)


# ─── 1. Decommission readiness ───────────────────────────────────────────────


def compute_decommission(inv: Inventory, lineage: LineageGraph | None = None) -> list[DecommissionAssessment]:
    """Score every TABLE / VIEW for decommission readiness.

    Higher score = safer to retire. Scoring rubric:
      base 100
        - 60 if any pipeline writes to or reads from it (active dependency)
        - 30 if any view consumes it
        - 30 if read recently (per audit log proxy via pipeline reads)
        - 10 if FK target / referenced
        + 20 if archive eligible per Indigo policy
        + bonus for being in WH_LEGACY-style schema (already pre-tagged for retirement)

    Verdicts: 80+ safe, 50-79 review, <50 blocked.
    """
    pipeline_readers = _pipeline_table_index(inv.pipelines, side="reads")
    pipeline_writers = _pipeline_table_index(inv.pipelines, side="writes")

    fk_referenced = _fk_referenced_set(inv.tables)
    view_consumers = _view_consumer_index(inv.tables, lineage)

    out: list[DecommissionAssessment] = []
    for t in inv.tables:
        if t.kind == "CSV":
            continue  # CSV outputs aren't decommission targets — they're produced
        score = 100
        drivers: list[str] = []

        readers = sorted(pipeline_readers.get(t.name.upper(), set()))
        writers = sorted(pipeline_writers.get(t.name.upper(), set()))
        consumers = sorted(view_consumers.get(t.fqn, set()))

        if writers:
            score -= 30
            drivers.append(f"Written by {len(writers)} pipeline(s): {', '.join(writers[:3])}{'…' if len(writers) > 3 else ''}")
        if readers:
            score -= 60
            drivers.append(f"Read by {len(readers)} pipeline(s): {', '.join(readers[:3])}{'…' if len(readers) > 3 else ''}")
        if consumers:
            score -= 30
            drivers.append(f"Consumed by {len(consumers)} view(s): {', '.join(c.split('.')[-1] for c in consumers[:3])}{'…' if len(consumers) > 3 else ''}")
        if t.fqn in fk_referenced or t.name.upper() in fk_referenced:
            score -= 10
            drivers.append("Target of one or more foreign keys")

        # Archive eligibility — per Indigo: 10y active / 7y exited (we proxy by last_analyzed)
        archive_eligible = False
        days_since: int | None = None
        if t.last_analyzed:
            try:
                last = datetime.fromisoformat(str(t.last_analyzed).split(".")[0])
                days_since = max(0, (datetime.utcnow() - last).days)
                if days_since > 365 * 7:
                    archive_eligible = True
                    score += 20
                    drivers.append(f"Last analysed {days_since} days ago — archive-eligible (>7y)")
            except (ValueError, TypeError):
                pass

        # Pre-classified legacy schema is a strong signal
        if t.schema_name.upper().startswith("WH_LEGACY") or "LEGACY" in t.name.upper():
            score += 15
            drivers.append("Schema or name indicates legacy / archive context")

        if not readers and not writers and not consumers:
            drivers.append("No active producers, consumers, or pipelines — fully orphan")

        score = max(0, min(100, score))
        verdict = "safe" if score >= 80 else "review" if score >= 50 else "blocked"
        out.append(DecommissionAssessment(
            object_fqn=t.fqn, score=score, verdict=verdict,
            last_read=t.last_analyzed,
            days_since_last_read=days_since,
            downstream_pipeline_count=len(readers),
            downstream_view_count=len(consumers),
            archive_eligible=archive_eligible,
            drivers=drivers or ["No additional signal — borderline candidate"],
        ))
    out.sort(key=lambda a: -a.score)
    return out


# ─── 2. Migration sequencing (topological waves) ────────────────────────────


def compute_sequencing(inv: Inventory, lineage: LineageGraph | None = None) -> list[MigrationWave]:
    """Order in-scope objects into waves so dependencies migrate first.

    Wave 1: leaf source tables (read by pipelines, not produced by any).
    Wave 2: pipelines whose source tables are all in Wave ≤1.
    Wave 3: views/CSV outputs whose inputs are all in Wave ≤2.
    Etc.
    """
    # Build dependency graph: target → set(sources)
    deps: dict[str, set[str]] = defaultdict(set)
    nodes: set[str] = set()

    if lineage:
        for e in lineage.edges:
            s = _short(e.source_fqn)
            t = _short(e.target_fqn)
            if s == t:
                continue
            deps[t].add(s)
            nodes.add(s)
            nodes.add(t)

    # Add all inventory tables/pipelines to nodes set
    for t in inv.tables:
        nodes.add(t.fqn)
    for p in inv.pipelines:
        nodes.add(f"PIPELINE.{p.name}")

    # Topo sort by depth (Kahn-ish). depth(node) = 1 + max depth(parents).
    depth: dict[str, int] = {}
    visiting: set[str] = set()

    def get_depth(n: str) -> int:
        if n in depth:
            return depth[n]
        if n in visiting:
            depth[n] = 1  # cycle break
            return 1
        visiting.add(n)
        parents = deps.get(n, set())
        if not parents:
            d = 1
        else:
            d = 1 + max((get_depth(p) for p in parents), default=0)
        depth[n] = d
        visiting.discard(n)
        return d

    for n in nodes:
        get_depth(n)

    by_depth: dict[int, list[str]] = defaultdict(list)
    for n, d in depth.items():
        by_depth[d].append(n)

    waves: list[MigrationWave] = []
    for d in sorted(by_depth.keys()):
        members = sorted(by_depth[d])
        tables = [m for m in members if not m.startswith("PIPELINE.") and m.count(".") <= 2]
        pipelines = [m.replace("PIPELINE.", "") for m in members if m.startswith("PIPELINE.")]
        if not tables and not pipelines:
            continue
        waves.append(MigrationWave(
            wave=d,
            description=_wave_description(d, len(waves) + 1),
            table_fqns=tables,
            pipeline_names=pipelines,
        ))
    return waves


def _wave_description(depth: int, wave_num: int) -> str:
    if wave_num == 1:
        return "Source tables and reference data (no upstream dependencies)"
    if depth == 2:
        return "First-tier pipelines and views consuming source tables"
    if depth == 3:
        return "Second-tier transformations and outputs"
    return f"Tier {depth - 1} — depends on previous waves"


# ─── 3. Multi-writer target detection ───────────────────────────────────────


def compute_multi_writers(inv: Inventory, lineage: LineageGraph | None = None) -> list[MultiWriterTarget]:
    """Find target objects (CSVs, tables) written by multiple pipelines.

    Mirrors the Transformation Agent's multi_writer_registry classification.
    """
    by_target: dict[str, set[str]] = defaultdict(set)
    for p in inv.pipelines:
        if not p.output_csv:
            continue
        by_target[p.output_csv].add(p.name)

    out: list[MultiWriterTarget] = []
    for tgt, writers in by_target.items():
        if len(writers) < 2:
            continue
        out.append(MultiWriterTarget(
            target_fqn=tgt,
            writer_pipelines=sorted(writers),
            pattern=_classify_multi_writer_pattern(sorted(writers)),
            rationale="Multiple ETL pipelines declare this CSV as output_csv",
        ))
    return out


def _classify_multi_writer_pattern(writers: list[str]) -> str:
    """Mirror Crown's three-pattern classification.

    Heuristics (cheap, no LLM):
      - disjoint: writer names look source-partitioned (e.g., contain a source
        prefix like _MEL/_SYD/_PER or are clearly per-region)
      - lifecycle: writers include something with 'expire', 'delete', 'archive'
      - update_back: small number (typically 2-3) without source partitioning
    """
    lower = " ".join(writers).lower()
    if any(t in lower for t in ("expire", "delete", "archive", "purge", "retire")):
        return "lifecycle"
    parts: list[str] = []
    for w in writers:
        # find common suffix bits like _mel, _per, _syd, _src1, _src2
        for tok in w.lower().split("_"):
            if tok in {"mel", "syd", "per", "src1", "src2", "src3", "raw", "stage"}:
                parts.append(tok)
    if len(parts) >= 2 and len(set(parts)) >= 2:
        return "disjoint"
    if len(writers) <= 3:
        return "update_back"
    return "unknown"


# ─── helpers ────────────────────────────────────────────────────────────────


def _pipeline_table_index(pipelines: list[ETLPipeline], side: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = defaultdict(set)
    for p in pipelines:
        if side == "reads":
            for src in p.source_tables:
                out[src.upper()].add(p.name)
        elif side == "writes":
            # heuristic: a pipeline writes to a target only if the source_tables
            # include a shared name with the output. For real lineage, we'd use
            # explicit write-edges. For now, "writes" is empty by default —
            # writes go to CSVs which we treat separately via output_csv.
            pass
    return out


def _fk_referenced_set(tables: list[Table]) -> set[str]:
    refs: set[str] = set()
    for t in tables:
        for c in t.columns:
            if c.is_fk and c.fk_target:
                # fk_target like "TABLE.COLUMN" or "SCHEMA.TABLE.COLUMN"
                parts = c.fk_target.split(".")
                if len(parts) >= 2:
                    refs.add(".".join(parts[:-1]).upper())
                    refs.add(parts[-2].upper())
    return refs


def _view_consumer_index(tables: list[Table], lineage: LineageGraph | None) -> dict[str, set[str]]:
    """Map: source_table_fqn -> set of view fqns that read from it."""
    out: dict[str, set[str]] = defaultdict(set)
    if not lineage:
        return out
    view_fqns = {t.fqn for t in tables if t.kind == "VIEW"}
    for e in lineage.edges:
        if e.target_fqn in view_fqns and e.operation == "VIEW":
            out[e.source_fqn].add(e.target_fqn)
            # Also index by short name for cross-schema fuzzy match
            short = e.source_fqn.split(".")[-1]
            out[short].add(e.target_fqn)
    return out


def _short(fqn: str) -> str:
    """Normalize an FQN — dropping the PIPELINE.<name>.<step> suffix."""
    if fqn.startswith("PIPELINE."):
        parts = fqn.split(".")
        return ".".join(parts[:2])
    return fqn


# ─── 4. Migration scope manifest export ─────────────────────────────────────


def build_scope_manifest(inv: Inventory, lineage: LineageGraph | None = None) -> dict:
    """Produce the manifest the Transformation Agent should ingest.

    Filters out pipelines that never ran (and have no CSV produced) and tables
    flagged decommission-safe.
    """
    decom_by_fqn = {a.object_fqn: a for a in inv.decommission}
    multi = {m.target_fqn: m for m in inv.multi_writers}

    in_scope_pipelines = []
    out_of_scope_pipelines = []
    for p in inv.pipelines:
        runs = p.runs
        ran_recently = (runs and runs.runs_total > 0) or p.csv_exists
        if ran_recently:
            in_scope_pipelines.append(_pipeline_payload(p, multi))
        else:
            out_of_scope_pipelines.append({"name": p.name, "reason": "Pipeline defined but no execution history and no output CSV"})

    in_scope_tables = []
    out_of_scope_tables = []
    for t in inv.tables:
        if t.kind == "CSV":
            continue
        a = decom_by_fqn.get(t.fqn)
        if a and a.verdict == "safe":
            out_of_scope_tables.append({"fqn": t.fqn, "reason": "Decommission verdict: safe", "score": a.score})
            continue
        in_scope_tables.append(_table_payload(t))

    return {
        "generated_at": datetime.utcnow().isoformat(),
        "summary": {
            "in_scope_pipelines": len(in_scope_pipelines),
            "out_of_scope_pipelines": len(out_of_scope_pipelines),
            "in_scope_tables": len(in_scope_tables),
            "out_of_scope_tables": len(out_of_scope_tables),
            "rules_extracted": len(inv.rules),
            "multi_writer_targets": len(inv.multi_writers),
        },
        "pipelines": {"in_scope": in_scope_pipelines, "out_of_scope": out_of_scope_pipelines},
        "tables":    {"in_scope": in_scope_tables,    "out_of_scope": out_of_scope_tables},
        "multi_writers": [m.model_dump() for m in inv.multi_writers],
        "rules":     [r.model_dump() for r in inv.rules],
        "sequencing": [w.model_dump() for w in inv.sequencing],
    }


def _pipeline_payload(p: ETLPipeline, multi_index: dict[str, MultiWriterTarget]) -> dict:
    payload = {
        "name": p.name,
        "file": p.file,
        "output_csv": p.output_csv,
        "source_tables": p.source_tables,
        "column_count": p.column_count,
        "runs": p.runs.model_dump() if p.runs else None,
        "csv_exists": p.csv_exists,
    }
    if p.output_csv and p.output_csv in multi_index:
        m = multi_index[p.output_csv]
        payload["multi_writer"] = {"pattern": m.pattern, "co_writers": [w for w in m.writer_pipelines if w != p.name]}
    return payload


def _table_payload(t: Table) -> dict:
    return {
        "fqn": t.fqn,
        "kind": t.kind,
        "layer": t.layer.value,
        "domain": t.domain.value,
        "row_count": t.row_count,
        "bytes": t.bytes,
        "columns": [
            {
                "name": c.name, "data_type": c.data_type, "nullable": c.nullable,
                "is_pk": c.is_pk, "is_fk": c.is_fk, "fk_target": c.fk_target,
                "sensitivity": c.sensitivity.value, "nature": c.nature.value,
                "annotation_notes": c.annotation_notes,
            }
            for c in t.columns
        ],
        "view_source_text": t.source_text,
    }
