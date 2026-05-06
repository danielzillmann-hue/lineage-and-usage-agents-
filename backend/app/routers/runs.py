from fastapi import APIRouter, HTTPException
from sse_starlette.sse import EventSourceResponse

from app.models.run import Run, RunRequest
from app.models.schema import RunResults
from app.services import orchestrator, store

router = APIRouter()


@router.post("", response_model=Run)
async def create_run(req: RunRequest) -> Run:
    return await orchestrator.create_run(req)


@router.get("", response_model=list[Run])
async def list_runs(limit: int = 20) -> list[Run]:
    return await store.list_runs(limit=limit)


@router.get("/{run_id}", response_model=Run)
async def get_run(run_id: str) -> Run:
    run = await store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@router.get("/{run_id}/results", response_model=RunResults)
async def get_results(run_id: str) -> RunResults:
    results = await store.get_results(run_id)
    if not results:
        raise HTTPException(status_code=404, detail="results not ready")
    return results


@router.get("/{run_id}/stream")
async def stream_run(run_id: str) -> EventSourceResponse:
    return EventSourceResponse(orchestrator.stream(run_id))
