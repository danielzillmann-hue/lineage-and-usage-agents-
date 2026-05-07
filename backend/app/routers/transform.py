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

from app.services import gcs, store, transform_storage
from app.transformer import generate_project

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

    # Pull view source SQL from the saved inventory results so source
    # declarations for views render as `type: "view"` with the original
    # body instead of opaque `type: "declaration"` pointers.
    views: dict[str, str] = {}
    try:
        results = await store.get_results(run_id)
        if results and results.inventory:
            for t in results.inventory.tables:
                if t.kind == "VIEW" and t.source_text:
                    views[t.name.lower()] = t.source_text
    except Exception as e:  # noqa: BLE001
        log.warning("failed to load inventory views for run %s: %s", run_id, e)

    project = generate_project(xml_files, views=views)
    manifest = transform_storage.upload_project(run_id, project)

    return TransformResponse(
        run_id=run_id,
        pipelines_generated=len(set(project.pipelines)),
        sources_declared=len(project.sources),
        operations_generated=len(project.operations),
        files=manifest.files,
        warnings=manifest.warnings,
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

    filename = f"insignia-dataform-{run_id[:8]}.zip"
    return Response(
        content=buf.read(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
