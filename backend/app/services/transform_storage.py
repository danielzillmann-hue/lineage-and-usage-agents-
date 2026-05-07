"""Persist generated Dataform projects to GCS, list/read them back.

Layout:
    gs://<results_bucket>/runs/<run_id>/transform/<file_path>
    gs://<results_bucket>/runs/<run_id>/transform/_manifest.json   (metadata)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from app.config import get_settings
from app.services import gcs
from app.transformer import AssembledProject

log = logging.getLogger(__name__)


@dataclass
class TransformManifest:
    run_id: str
    pipelines: list[str]
    sources: list[str]
    operations: list[str]
    files: list[str]                 # all file paths in the project
    warnings: list[str]
    generated_at: str                # ISO timestamp


def _prefix(run_id: str) -> str:
    return f"runs/{run_id}/transform"


def upload_project(run_id: str, project: AssembledProject) -> TransformManifest:
    """Write the project files + a manifest to GCS. Returns the manifest."""
    settings = get_settings()
    bucket = settings.results_bucket
    prefix = _prefix(run_id)

    for path, content in project.files.items():
        ct = "text/markdown" if path.endswith(".md") else (
            "application/x-yaml" if path.endswith(".yaml") else "text/plain"
        )
        gcs.write_text(bucket, f"{prefix}/{path}", content, content_type=ct)
        log.info("uploaded %s/%s (%d bytes)", prefix, path, len(content))

    manifest = TransformManifest(
        run_id=run_id,
        pipelines=sorted(set(project.pipelines)),
        sources=list(project.sources),
        operations=sorted(project.operations),
        files=sorted(project.files.keys()),
        warnings=list(project.warnings),
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
    gcs.write_json(
        bucket,
        f"{prefix}/_manifest.json",
        json.dumps(manifest.__dict__, indent=2),
    )
    return manifest


def read_manifest(run_id: str) -> TransformManifest | None:
    """Load the previously-written manifest, or None if no transform run exists."""
    settings = get_settings()
    try:
        text = gcs.read_text(settings.results_bucket, f"{_prefix(run_id)}/_manifest.json")
    except Exception:
        return None
    try:
        d = json.loads(text)
    except Exception:
        return None
    return TransformManifest(**d)


def read_file(run_id: str, path: str) -> str | None:
    """Read one generated SQLX/YAML/MD file by its path inside the project."""
    settings = get_settings()
    try:
        return gcs.read_text(settings.results_bucket, f"{_prefix(run_id)}/{path}")
    except Exception:
        return None


def list_files(run_id: str) -> list[str]:
    """List all generated file paths (relative to the project root)."""
    settings = get_settings()
    prefix = f"{_prefix(run_id)}/"
    blobs = gcs.list_blobs(settings.results_bucket, prefix)
    out: list[str] = []
    for b in blobs:
        rel = b.removeprefix(prefix)
        if rel and rel != "_manifest.json":
            out.append(rel)
    return sorted(out)
