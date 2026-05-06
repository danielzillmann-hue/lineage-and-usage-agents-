"""Run orchestrator — schedules agents, manages state, fans out SSE events."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import datetime

from app.agents import inventory_agent, lineage_agent, summary_agent, usage_agent
from app.models.run import AgentName, AgentRunState, AgentStatus, Run, RunRequest, StreamEvent
from app.models.schema import RunResults
from app.services import store

log = logging.getLogger(__name__)


_QUEUES: dict[str, asyncio.Queue[StreamEvent]] = {}


def _queue(run_id: str) -> asyncio.Queue[StreamEvent]:
    return _QUEUES.setdefault(run_id, asyncio.Queue())


async def emit(run_id: str, event: StreamEvent) -> None:
    await _queue(run_id).put(event)


async def create_run(req: RunRequest) -> Run:
    now = datetime.utcnow()
    run = Run(
        id=str(uuid.uuid4()),
        bucket=req.bucket,
        prefix=req.prefix,
        oracle_dsn=(f"{req.oracle.host}:{req.oracle.port}/{req.oracle.service}" if req.oracle else None),
        label=req.label,
        status="pending",
        created_at=now,
        updated_at=now,
        agents=[AgentRunState(name=a) for a in req.agents],
    )
    await store.upsert_run(run)
    asyncio.create_task(_execute(run.id, req))
    return run


async def _execute(run_id: str, req: RunRequest) -> None:
    run = await store.get_run(run_id)
    if not run:
        return
    run.status = "running"
    await store.upsert_run(run)
    results = RunResults()
    any_failed = False

    async def _safe(name: AgentName, work) -> None:
        nonlocal any_failed
        try:
            await _run_one(run, name, work)
        except Exception as e:  # noqa: BLE001
            any_failed = True
            log.exception("agent %s failed", name)
            await emit(run_id, StreamEvent(event="error", agent=name, message=str(e)))

    if AgentName.INVENTORY in req.agents:
        await _safe(AgentName.INVENTORY, lambda: inventory_agent.run(req, results, _emit(run_id)))
        results.inventory = inventory_agent.last_result
        await store.save_results(run_id, results)
    if AgentName.LINEAGE in req.agents:
        await _safe(AgentName.LINEAGE, lambda: lineage_agent.run(req, results, _emit(run_id)))
        results.lineage = lineage_agent.last_result
        await store.save_results(run_id, results)
    if AgentName.USAGE in req.agents:
        await _safe(AgentName.USAGE, lambda: usage_agent.run(req, results, _emit(run_id)))
        results.usage = usage_agent.last_result
        await store.save_results(run_id, results)
    if AgentName.SUMMARY in req.agents:
        await _safe(AgentName.SUMMARY, lambda: summary_agent.run(req, results, _emit(run_id)))
        results.summary = summary_agent.last_result
        await store.save_results(run_id, results)

    run.status = "failed" if any_failed else "completed"
    await store.upsert_run(run)
    await emit(run_id, StreamEvent(event="done"))


async def _run_one(run: Run, name: AgentName, work) -> None:
    state = next(a for a in run.agents if a.name == name)
    state.status = AgentStatus.RUNNING
    state.started_at = datetime.utcnow()
    await store.upsert_run(run)
    try:
        await work()
        state.status = AgentStatus.COMPLETED
    except Exception as e:  # noqa: BLE001
        state.status = AgentStatus.FAILED
        state.error = str(e)
        raise
    finally:
        state.completed_at = datetime.utcnow()
        await store.upsert_run(run)


def _emit(run_id: str):
    async def _send(event: StreamEvent) -> None:
        await emit(run_id, event)

    return _send


async def stream(run_id: str) -> AsyncIterator[dict]:
    q = _queue(run_id)
    while True:
        ev = await q.get()
        yield {"event": ev.event, "data": json.dumps(ev.model_dump(mode="json"))}
        if ev.event == "done":
            break
