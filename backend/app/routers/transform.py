"""Transform tab API — Oracle pipelines → Dataform SQLX project.

Endpoints (all under /api/runs/{run_id}/transform):
    POST   /              Trigger transformation: read pipeline XMLs from
                          the run's bucket, build IR, generate SQLX,
                          assemble Dataform project, upload to GCS.
    GET    /              Manifest (pipelines, sources, ops, file list).
    GET    /files         List all generated file paths.
    GET    /files/{path}  Content of one generated file.
    GET    /download.zip  Whole project as a zip download.
"""

from __future__ import annotations

import io
import logging
import zipfile

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse, Response
from pydantic import BaseModel

from datetime import datetime, timezone
import json

from app.models.run import OracleConnection
from app.services import gcs, github_push, store, transform_storage
from app.transformer import generate_project
from app.config import get_settings

log = logging.getLogger(__name__)

router = APIRouter()


# ─── POST /api/runs/{run_id}/transform — trigger ─────────────────────────


class TransformResponse(BaseModel):
    run_id: str
    pipelines_generated: int
    sources_declared: int
    operations_generated: int
    files: list[str]
    warnings: list[str]
    validation: dict | None = None


@router.post("/{run_id}/transform", response_model=TransformResponse)
async def transform_run(run_id: str) -> TransformResponse:
    """Generate the Dataform project for an already-completed analysis run.

    Reads the pipeline XMLs from the run's source bucket, builds IR, emits
    SQLX, assembles the Dataform project, and uploads everything to GCS at
    `gs://<results_bucket>/runs/{run_id}/transform/`.
    """
    run = await store.get_run(run_id)
    if run is None:
        raise HTTPException(404, f"run {run_id} not found")
    if not run.bucket:
        raise HTTPException(400, f"run {run_id} has no source bucket")

    # Pull every pipeline XML from the run's source bucket/prefix.
    xml_files: list[tuple[str, str]] = []
    for f in gcs.iter_classified(run.bucket, run.prefix):
        if f.kind != "etl":
            continue
        try:
            text = gcs.read_text(run.bucket, f.name)
        except Exception as e:  # noqa: BLE001
            log.warning("failed to read %s: %s", f.name, e)
            continue
        # The filename returned by iter_classified is the full GCS object
        # name including prefix; the parser only needs the leaf.
        xml_files.append((f.name.split("/")[-1], text))

    if not xml_files:
        raise HTTPException(400, "no pipeline XMLs found in the run's source bucket")

    # Pull view source SQL + full column metadata from the saved inventory
    # results so source declarations for views render as `type: "view"`,
    # assertions are emitted on every table with PK info, and the raw-
    # layer bootstrap (DDL + replication README) gets generated.
    views: dict[str, str] = {}
    table_metadata: dict[str, dict] = {}
    try:
        results = await store.get_results(run_id)
        if results and results.inventory:
            for t in results.inventory.tables:
                if t.kind == "VIEW" and t.source_text:
                    views[t.name.lower()] = t.source_text
                if not t.columns:
                    continue
                schema = [
                    {
                        "name": c.name,
                        "oracle_type": c.data_type or "",
                        "nullable": c.nullable,
                        "is_pk": c.is_pk,
                    }
                    for c in t.columns
                ]
                table_metadata[t.name.lower()] = {
                    "primary_keys": [c["name"] for c in schema if c["is_pk"]],
                    "non_null": [c["name"] for c in schema if not c["nullable"]],
                    "schema": schema,
                }
    except Exception as e:  # noqa: BLE001
        log.warning("failed to load inventory metadata for run %s: %s", run_id, e)

    project = generate_project(xml_files, views=views, table_metadata=table_metadata)
    manifest = transform_storage.upload_project(run_id, project)

    return TransformResponse(
        run_id=run_id,
        pipelines_generated=len(set(project.pipelines)),
        sources_declared=len(project.sources),
        operations_generated=len(project.operations),
        files=manifest.files,
        warnings=manifest.warnings,
        validation=manifest.validation,
    )


# ─── GET /api/runs/{run_id}/transform — manifest ────────────────────────


class TransformManifestResponse(BaseModel):
    run_id: str
    pipelines: list[str]
    sources: list[str]
    operations: list[str]
    files: list[str]
    warnings: list[str]
    generated_at: str
    validation: dict | None = None
    file_meta: dict[str, dict] = {}


@router.get("/{run_id}/transform", response_model=TransformManifestResponse)
def get_manifest(run_id: str) -> TransformManifestResponse:
    m = transform_storage.read_manifest(run_id)
    if m is None:
        raise HTTPException(404, f"no transform output for run {run_id} — POST first")
    return TransformManifestResponse(**m.__dict__)


# ─── GET /api/runs/{run_id}/transform/files — list ──────────────────────


@router.get("/{run_id}/transform/files", response_model=list[str])
def list_files(run_id: str) -> list[str]:
    files = transform_storage.list_files(run_id)
    if not files:
        # Don't 404 — empty list is meaningful (hasn't been generated yet).
        return []
    return files


# ─── GET /api/runs/{run_id}/transform/files/{path} — read one ───────────


@router.get("/{run_id}/transform/files/{path:path}", response_class=PlainTextResponse)
def get_file(run_id: str, path: str) -> str:
    content = transform_storage.read_file(run_id, path)
    if content is None:
        raise HTTPException(404, f"file not found: {path}")
    return content


# ─── GET /api/runs/{run_id}/transform/originals/{path} — original source ─


@router.get("/{run_id}/transform/originals/{path:path}", response_class=PlainTextResponse)
def get_original(run_id: str, path: str) -> str:
    """Read the original pipeline XML or execute_sql body that produced
    one of the generated files. The path is the value from
    `manifest.file_meta[<sqlx_path>].original_path`.
    """
    if not path.startswith("_originals/"):
        path = f"_originals/{path}"
    content = transform_storage.read_file(run_id, path)
    if content is None:
        raise HTTPException(404, f"original not found: {path}")
    return content


# ─── POST /api/runs/{run_id}/transform/orchestrate ─────────────────────


_WORKFLOW_YAML = """\
# GitHub Actions workflow for the Dataform project.
# Generated by intelia Lineage & Usage Agents (orchestration).

name: Dataform

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: "0 2 * * *"
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

jobs:
  compile:
    name: Compile
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - name: Install Dataform CLI
        run: npm install -g @dataform/cli @dataform/core
      - name: Compile
        run: dataform compile

  run:
    name: Run
    needs: compile
    runs-on: ubuntu-latest
    if: ${{ github.event_name == 'schedule' || github.event_name == 'workflow_dispatch' }}
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.WIF_SERVICE_ACCOUNT }}
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - name: Install Dataform CLI
        run: npm install -g @dataform/cli @dataform/core
      - name: Run
        run: dataform run
"""


class OrchestrationResponse(BaseModel):
    run_id: str
    files_added: list[str]
    workflow: str


@router.post("/{run_id}/transform/orchestrate", response_model=OrchestrationResponse)
def orchestrate_run(run_id: str) -> OrchestrationResponse:
    """Add orchestration files (.github/workflows/dataform.yaml) to an
    already-generated transform output. Used when the run was created
    before the orchestration agent existed, or to refresh the workflow.
    """
    try:
        return _orchestrate_impl(run_id)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        log.exception("orchestrate failed for run %s", run_id)
        raise HTTPException(
            status_code=500,
            detail=f"orchestrate failed: {type(e).__name__}: {e}",
        ) from e


def _orchestrate_impl(run_id: str) -> OrchestrationResponse:
    manifest = transform_storage.read_manifest(run_id)
    if manifest is None:
        raise HTTPException(404, f"no transform output for run {run_id} — generate it first")

    settings = get_settings()
    bucket = settings.results_bucket
    prefix = f"runs/{run_id}/transform"
    target_path = ".github/workflows/dataform.yaml"

    gcs.write_text(
        bucket,
        f"{prefix}/{target_path}",
        _WORKFLOW_YAML,
        content_type="application/x-yaml",
    )

    # Update the manifest so the file shows up in the tree without
    # waiting for a re-walk of GCS.
    if target_path not in manifest.files:
        manifest.files = sorted(set(manifest.files) | {target_path})
    manifest.file_meta = manifest.file_meta or {}
    manifest.file_meta[target_path] = {
        "kind": "orchestration",
        "pipeline": "(orchestration)",
        "confidence": 100,
        "original_path": "",
    }
    manifest.orchestration = {
        "kind": "github_actions",
        "files": [target_path],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    gcs.write_json(
        bucket,
        f"{prefix}/_manifest.json",
        json.dumps(manifest.__dict__, indent=2),
    )

    return OrchestrationResponse(
        run_id=run_id,
        files_added=[target_path],
        workflow="github_actions",
    )


# ─── POST /api/runs/{run_id}/transform/push — git push to GitHub ────────


class PushRequest(BaseModel):
    repo_url: str          # https://github.com/owner/repo
    branch: str = "main"   # base branch when opening a PR; target branch otherwise
    commit_message: str = "Generated by intelia Lineage & Usage Agents"
    github_token: str      # PAT or fine-grained token (never logged)
    force: bool = True
    as_pull_request: bool = False
    pr_title: str = ""
    pr_body: str = ""


class PushResultResponse(BaseModel):
    repo_url: str
    branch: str
    commit_sha: str
    commit_url: str
    files_pushed: int
    pull_request_url: str | None = None
    pull_request_number: int | None = None


@router.post("/{run_id}/transform/push", response_model=PushResultResponse)
async def push_to_github_endpoint(run_id: str, body: PushRequest) -> PushResultResponse:
    # If opening a PR, default the body to the run's executive summary
    # (when one's available) so reviewers have context.
    pr_body = body.pr_body
    if body.as_pull_request and not pr_body:
        try:
            results = await store.get_results(run_id)
            if results and results.summary:
                bullets = "\n".join(f"- {b}" for b in (results.summary.bullets or []))
                pr_body = (
                    f"_Generated by intelia Lineage & Usage Agents._\n\n"
                    f"## Headline\n\n{results.summary.headline}\n\n"
                    f"## Highlights\n\n{bullets or '_(none)_'}\n"
                )
        except Exception:  # noqa: BLE001
            pass

    try:
        result = github_push.push_to_github(
            run_id=run_id,
            repo_url=body.repo_url,
            branch=body.branch,
            commit_message=body.commit_message,
            github_token=body.github_token,
            force=body.force,
            as_pull_request=body.as_pull_request,
            pr_title=body.pr_title,
            pr_body=pr_body,
        )
    except github_push.PushError as e:
        raise HTTPException(400, str(e)) from e
    return PushResultResponse(**result.__dict__)


# ─── GET /api/runs/{run_id}/transform/download.zip — whole project ──────


@router.get("/{run_id}/transform/download.zip")
def download_zip(run_id: str) -> Response:
    files = transform_storage.list_files(run_id)
    if not files:
        raise HTTPException(404, f"no transform output for run {run_id}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            content = transform_storage.read_file(run_id, path)
            if content is not None:
                zf.writestr(path, content)
    buf.seek(0)

    filename = f"dataform-project-{run_id[:8]}.zip"
    return Response(
        content=buf.read(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ─── Verification endpoints ─────────────────────────────────────────────


class VerifyTriggerRequest(BaseModel):
    """Optional explicit Oracle connection for the verify run.

    When omitted, the verify endpoint reuses the credentials the run was
    started with (cached in orchestrator memory). When the backend has
    restarted since the run was created, the cache is gone and the
    frontend supplies fresh credentials in this body.
    """
    oracle: OracleConnection | None = None


@router.post("/{run_id}/verify")
async def trigger_verify(
    run_id: str,
    body: VerifyTriggerRequest | None = None,
) -> dict:
    """Run the verification agent on demand against the run's existing
    BigQuery tables and Oracle.
    """
    from app.services import store
    from app.services.orchestrator import get_run_request, _emit
    from app.agents import verification_agent
    from app.models.schema import RunResults
    from app.models.run import RunRequest as _RunRequest

    run = await store.get_run(run_id)
    if run is None:
        raise HTTPException(404, f"run {run_id} not found")

    cached = get_run_request(run_id)
    explicit_oracle = body.oracle if body else None

    if explicit_oracle is not None:
        # Caller passed credentials explicitly — use those, but keep the
        # original RunRequest's bucket/prefix etc. if we still have them.
        req = (cached.model_copy(update={"oracle": explicit_oracle})
               if cached is not None
               else _RunRequest(oracle=explicit_oracle))
    elif cached is not None and cached.oracle is not None:
        req = cached
    else:
        raise HTTPException(
            409,
            "Oracle credentials for this run are not cached on the "
            "server (the backend was restarted since this run was "
            "created). Re-enter credentials in the Verify panel to "
            "continue.",
        )

    # Inventory metadata is needed to classify CSV stubs vs Oracle-origin
    # sources; load whatever's in storage for the run.
    results = (await store.get_results(run_id)) or RunResults()

    try:
        await verification_agent.run(req, results, _emit(run_id), run_id)
    except Exception as e:  # noqa: BLE001
        log.exception("verify failed for run %s", run_id)
        raise HTTPException(
            500, f"verify failed: {type(e).__name__}: {e}",
        ) from e

    return verification_agent.last_result or {"summary": {}, "report_path": ""}


@router.get("/{run_id}/verify")
def get_verify_report(run_id: str) -> dict:
    """Return the verification report for a run, if one was produced."""
    settings = get_settings()
    try:
        text = gcs.read_text(
            settings.results_bucket,
            f"runs/{run_id}/verification/_report.json",
        )
    except Exception:
        raise HTTPException(404, f"no verification report for run {run_id}")
    try:
        return json.loads(text)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(500, f"verification report is malformed: {e}")
