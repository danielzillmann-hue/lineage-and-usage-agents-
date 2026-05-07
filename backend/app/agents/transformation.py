"""Transformation agent — Oracle pipeline XMLs → Dataform SQLX project.

Runs after inventory/lineage/usage/summary as the final step of the run.
Reads pipeline XMLs from the run's source bucket, builds IR via the
Insignia → IR adapter, generates SQLX, assembles a complete Dataform
project, and uploads it to GCS at:

    gs://<results_bucket>/runs/<run_id>/transform/

Skips silently when the run has no source bucket — there's no offline
mode for this (we need the actual pipeline XMLs to translate).
"""

from __future__ import annotations

import logging

from app.agents.base import EmitFn, log_event
from app.models.run import AgentName, RunRequest, StreamEvent
from app.services import gcs, transform_storage
from app.transformer import generate_project
from app.transformer.procedure_converter import convert_all as convert_procedures

log = logging.getLogger(__name__)


# Module-level state for the orchestrator's results-saving pattern.
last_result: dict | None = None


async def run(req: RunRequest, results, emit: EmitFn, run_id: str) -> None:
    """Read pipeline XMLs, generate Dataform project, upload to GCS."""
    global last_result
    last_result = None

    if not req.bucket:
        await log_event(emit, AgentName.TRANSFORM,
                        "skipped: run has no source bucket — nothing to transform")
        return

    await log_event(emit, AgentName.TRANSFORM,
                    f"reading pipeline XMLs from gs://{req.bucket}/{req.prefix}")

    xml_files: list[tuple[str, str]] = []
    for f in gcs.iter_classified(req.bucket, req.prefix):
        if f.kind != "etl":
            continue
        try:
            text = gcs.read_text(req.bucket, f.name)
        except Exception as e:  # noqa: BLE001
            log.warning("failed to read %s: %s", f.name, e)
            continue
        xml_files.append((f.name.split("/")[-1], text))

    if not xml_files:
        await log_event(emit, AgentName.TRANSFORM,
                        "no pipeline XMLs found in source bucket — nothing to transform")
        return

    await log_event(emit, AgentName.TRANSFORM,
                    f"transforming {len(xml_files)} pipeline XMLs to Dataform SQLX")

    # Pull view definitions from the inventory (populated by the upstream
    # inventory agent) so source declarations for views render as proper
    # `type: "view"` blocks with the original SQL body.
    views = _extract_views(results)
    if views:
        await log_event(emit, AgentName.TRANSFORM,
                        f"found {len(views)} Oracle views with source SQL — porting as type:view")

    # Pull PK / non-null info so Dataform assertions can be auto-emitted
    # against source declarations and primary tables.
    table_metadata = _extract_table_metadata(results)
    if table_metadata:
        with_keys = sum(1 for m in table_metadata.values() if m.get("primary_keys"))
        await log_event(emit, AgentName.TRANSFORM,
                        f"inventory metadata for {len(table_metadata)} tables ({with_keys} with PKs) — generating assertions")

    project = generate_project(xml_files, views=views, table_metadata=table_metadata)

    # PL/SQL procedure conversion via Gemini. Procedures come from the
    # inventory agent's introspection; we translate each one in parallel
    # and add the result to the project under definitions/procedures/.
    procedures = _extract_procedures(results)
    if procedures:
        await log_event(emit, AgentName.TRANSFORM,
                        f"converting {len(procedures)} PL/SQL procedures via Gemini 2.5 Pro")
        try:
            converted = await convert_procedures(procedures)
            for c in converted:
                project.files[f"definitions/procedures/{c.name.lower()}.sqlx"] = c.sqlx
                project.operations.append(f"{c.name.lower()} (procedure)")
                if c.warnings:
                    project.warnings.extend([f"{c.name}: {w}" for w in c.warnings])
            success_count = sum(1 for c in converted if not c.warnings)
            await log_event(emit, AgentName.TRANSFORM,
                            f"procedures converted: {success_count}/{len(converted)} successfully")
        except Exception as e:  # noqa: BLE001
            log.warning("procedure conversion batch failed: %s", e)
            await log_event(emit, AgentName.TRANSFORM,
                            f"procedure conversion failed: {e}")

    await log_event(emit, AgentName.TRANSFORM,
                    f"generated {len(project.pipelines)} pipelines, "
                    f"{len(project.sources)} source declarations, "
                    f"{len(project.operations)} operations scripts "
                    f"({len(project.files)} files total)")

    # Surface validation results — usually catches structural issues
    # before users hit them in `dataform compile`.
    if project.validation:
        v = project.validation
        if v.get("ok"):
            await log_event(emit, AgentName.TRANSFORM,
                            f"validation passed: {v['files_total']} files, "
                            f"all refs resolve, all SQL parses, no cycles")
        else:
            await log_event(emit, AgentName.TRANSFORM,
                            f"validation: {v['files_failing']} of {v['files_total']} "
                            f"files have issues — {v['errors']} errors, {v['warnings']} warnings")

    manifest = transform_storage.upload_project(run_id, project)

    last_result = {
        "pipelines": manifest.pipelines,
        "sources": manifest.sources,
        "operations": manifest.operations,
        "files": len(manifest.files),
        "warnings": manifest.warnings,
    }

    await emit(StreamEvent(
        event="result",
        agent=AgentName.TRANSFORM,
        data={
            "pipelines": len(manifest.pipelines),
            "sources": len(manifest.sources),
            "operations": len(manifest.operations),
            "files": len(manifest.files),
            "warnings": len(manifest.warnings),
        },
    ))


def _extract_views(results) -> dict[str, str]:
    """Pull view source SQL from the inventory results, keyed by lowercase
    view name. Returns an empty dict if the inventory hasn't run or no
    views have source_text.
    """
    inv = getattr(results, "inventory", None)
    if inv is None:
        return {}
    out: dict[str, str] = {}
    for t in getattr(inv, "tables", []) or []:
        if getattr(t, "kind", None) != "VIEW":
            continue
        sql = getattr(t, "source_text", None)
        if not sql:
            continue
        out[t.name.lower()] = sql
    return out


def _extract_procedures(results) -> list[tuple[str, str, str]]:
    """Pull (name, schema, source) tuples for every procedure that has
    a non-empty body. Filters PACKAGE/TRIGGER kinds — those need
    different handling.
    """
    inv = getattr(results, "inventory", None)
    if inv is None:
        return []
    out: list[tuple[str, str, str]] = []
    for p in getattr(inv, "procedures", []) or []:
        kind = getattr(p, "kind", "") or ""
        if kind not in ("PROCEDURE", "FUNCTION"):
            continue
        src = getattr(p, "source", None) or ""
        if not src.strip():
            continue
        out.append((p.name, p.schema_name, src))
    return out


def _extract_table_metadata(results) -> dict[str, dict]:
    """Pull PK / non-null info from the inventory, keyed by lowercase table name.

    Each entry: {"primary_keys": ["col1"], "non_null": ["col1", "col2"]}
    """
    inv = getattr(results, "inventory", None)
    if inv is None:
        return {}
    out: dict[str, dict] = {}
    for t in getattr(inv, "tables", []) or []:
        cols = getattr(t, "columns", []) or []
        pks = [c.name for c in cols if getattr(c, "is_pk", False)]
        non_null = [c.name for c in cols if not getattr(c, "nullable", True)]
        if not pks and not non_null:
            continue
        out[t.name.lower()] = {"primary_keys": pks, "non_null": non_null}
    return out
