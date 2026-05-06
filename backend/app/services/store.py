"""Firestore-backed run state + result store. Falls back to in-memory for local dev."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from app.config import get_settings
from app.models.run import Run
from app.models.schema import RunResults

log = logging.getLogger(__name__)


_RUNS: dict[str, Run] = {}
_RESULTS: dict[str, RunResults] = {}


def _firestore_client():
    if os.getenv("APP_DISABLE_FIRESTORE", "").lower() in {"1", "true", "yes"}:
        return None
    try:
        from google.cloud import firestore  # type: ignore

        return firestore.AsyncClient(project=get_settings().gcp_project, database=get_settings().firestore_database)
    except Exception as e:  # noqa: BLE001
        log.warning("Firestore unavailable, falling back to memory: %s", e)
        return None


async def upsert_run(run: Run) -> None:
    run.updated_at = datetime.utcnow()
    _RUNS[run.id] = run
    client = _firestore_client()
    if client is None:
        return
    await client.collection(get_settings().firestore_collection_runs).document(run.id).set(json.loads(run.model_dump_json()))


async def get_run(run_id: str) -> Run | None:
    if run_id in _RUNS:
        return _RUNS[run_id]
    client = _firestore_client()
    if client is None:
        return None
    doc = await client.collection(get_settings().firestore_collection_runs).document(run_id).get()
    if not doc.exists:
        return None
    return Run.model_validate(doc.to_dict())


async def list_runs(limit: int = 20) -> list[Run]:
    client = _firestore_client()
    if client is None:
        return sorted(_RUNS.values(), key=lambda r: r.created_at, reverse=True)[:limit]
    coll = client.collection(get_settings().firestore_collection_runs)
    docs = coll.order_by("created_at", direction="DESCENDING").limit(limit).stream()
    return [Run.model_validate(d.to_dict()) async for d in docs]


async def save_results(run_id: str, results: RunResults) -> None:
    _RESULTS[run_id] = results
    client = _firestore_client()
    if client is None:
        return
    payload: dict[str, Any] = json.loads(results.model_dump_json())
    await (
        client.collection(get_settings().firestore_collection_runs)
        .document(run_id)
        .collection("artifacts")
        .document("results")
        .set(payload)
    )


async def get_results(run_id: str) -> RunResults | None:
    if run_id in _RESULTS:
        return _RESULTS[run_id]
    client = _firestore_client()
    if client is None:
        return None
    doc = (
        await client.collection(get_settings().firestore_collection_runs)
        .document(run_id)
        .collection("artifacts")
        .document("results")
        .get()
    )
    if not doc.exists:
        return None
    return RunResults.model_validate(doc.to_dict())
