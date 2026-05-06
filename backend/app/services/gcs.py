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
_DICT_HINTS = {"all_tab", "all_view", "all_dependencies", "dba_source", "dictionary", "data_dictionary"}
_AWR_HINTS = {"awr", "v$sql", "vsql", "dba_hist_sqlstat", "sqlstat", "sql_history"}


@dataclass
class ClassifiedFile:
    name: str
    size: int
    kind: str  # ddl | dictionary | awr | other


def _client() -> storage.Client:
    return storage.Client(project=get_settings().gcp_project)


def list_buckets() -> list[str]:
    client = _client()
    return sorted(b.name for b in client.list_buckets())


def _classify(name: str) -> str:
    lower = name.lower()
    if any(lower.endswith(ext) for ext in _DDL_EXT):
        return "ddl"
    if any(hint in lower for hint in _DICT_HINTS):
        return "dictionary"
    if any(hint in lower for hint in _AWR_HINTS):
        return "awr"
    return "other"


def iter_classified(bucket: str, prefix: str = "") -> Iterable[ClassifiedFile]:
    client = _client()
    for blob in client.list_blobs(bucket, prefix=prefix or None):
        if blob.name.endswith("/"):
            continue
        yield ClassifiedFile(name=blob.name, size=blob.size or 0, kind=_classify(blob.name))


def preview(bucket: str, prefix: str = "") -> BucketPreview:
    counts = {"ddl": 0, "dictionary": 0, "awr": 0, "other": 0}
    total = 0
    samples: list[str] = []
    for f in iter_classified(bucket, prefix):
        counts[f.kind] += 1
        total += f.size
        if len(samples) < 12:
            samples.append(f.name)
    return BucketPreview(
        bucket=bucket,
        prefix=prefix,
        ddl_files=counts["ddl"],
        dictionary_files=counts["dictionary"],
        awr_files=counts["awr"],
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
