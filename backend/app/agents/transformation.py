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

    project = generate_project(xml_files, views=views)

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
