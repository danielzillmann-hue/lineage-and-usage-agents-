"""GCS reader — list buckets, classify files, stream contents."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from google.cloud import storage

from app.config import get_settings
from app.models.run import BucketPreview

log = logging.getLogger(__name__)


_DDL_EXT = {".sql", ".ddl"}
_DICT_HINTS = {"all_tab", "all_view", "all_dependencies", "dba_source", "dictionary", "data_dictionary", "dba_segments"}
_AWR_HINTS = {"awr", "v$sql", "vsql", "dba_hist_sqlstat", "sqlstat", "sql_history"}
_ETL_EXT = {".xml"}


@dataclass
class ClassifiedFile:
    name: str
    size: int
    kind: str  # ddl | dictionary | awr | etl | output | other
    updated: str | None = None


def _client() -> storage.Client:
    return storage.Client(project=get_settings().gcp_project)


def list_buckets() -> list[str]:
    client = _client()
    return sorted(b.name for b in client.list_buckets())


def _classify(name: str) -> str:
    lower = name.lower()
    if any(lower.endswith(ext) for ext in _DDL_EXT):
        return "ddl"
    if any(lower.endswith(ext) for ext in _ETL_EXT):
        return "etl"
    if any(hint in lower for hint in _DICT_HINTS):
        return "dictionary"
    if any(hint in lower for hint in _AWR_HINTS):
        return "awr"
    if lower.endswith(".csv") and "/output" in lower:
        return "output"
    return "other"


def iter_classified(bucket: str, prefix: str = "") -> Iterable[ClassifiedFile]:
    client = _client()
    for blob in client.list_blobs(bucket, prefix=prefix or None):
        if blob.name.endswith("/"):
            continue
        yield ClassifiedFile(
            name=blob.name,
            size=blob.size or 0,
            kind=_classify(blob.name),
            updated=blob.updated.isoformat() if blob.updated else None,
        )


def list_csv_outputs(bucket: str, prefix: str = "") -> list[ClassifiedFile]:
    """List .csv files at a given prefix as candidate ETL outputs."""
    out: list[ClassifiedFile] = []
    for f in iter_classified(bucket, prefix):
        if f.name.lower().endswith(".csv"):
            out.append(f)
    return out


def preview(bucket: str, prefix: str = "") -> BucketPreview:
    counts = {"ddl": 0, "dictionary": 0, "awr": 0, "etl": 0, "output": 0, "other": 0}
    total = 0
    samples: list[str] = []
    for f in iter_classified(bucket, prefix):
        counts[f.kind] = counts.get(f.kind, 0) + 1
        total += f.size
        if len(samples) < 12:
            samples.append(f.name)
    return BucketPreview(
        bucket=bucket,
        prefix=prefix,
        ddl_files=counts["ddl"],
        dictionary_files=counts["dictionary"],
        awr_files=counts["awr"],
        etl_files=counts["etl"],
        output_files=counts["output"],
        other_files=counts["other"],
        total_bytes=total,
        sample_paths=samples,
    )


def read_text(bucket: str, name: str) -> str:
    client = _client()
    blob = client.bucket(bucket).blob(name)
    return blob.download_as_text()


def read_bytes(bucket: str, name: str) -> bytes:
    client = _client()
    blob = client.bucket(bucket).blob(name)
    return blob.download_as_bytes()


def write_json(bucket: str, name: str, data: str) -> str:
    client = _client()
    blob = client.bucket(bucket).blob(name)
    blob.upload_from_string(data, content_type="application/json")
    return f"gs://{bucket}/{name}"


def write_text(bucket: str, name: str, data: str, content_type: str = "text/plain") -> str:
    client = _client()
    blob = client.bucket(bucket).blob(name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{bucket}/{name}"


def list_blobs(bucket: str, prefix: str) -> list[str]:
    """Return blob names under a prefix (no trailing slash needed)."""
    client = _client()
    return [b.name for b in client.list_blobs(bucket, prefix=prefix)]


def delete_prefix(bucket: str, prefix: str) -> int:
    """Delete every blob under `prefix`. Returns the count deleted.

    Used to clear stale per-run state before re-uploading — without this,
    files emitted by an earlier generation that the current code no
    longer produces stay in the bucket and leak into downstream pushes.
    """
    client = _client()
    bkt = client.bucket(bucket)
    deleted = 0
    for blob in client.list_blobs(bucket, prefix=prefix):
        bkt.blob(blob.name).delete()
        deleted += 1
    return deleted
