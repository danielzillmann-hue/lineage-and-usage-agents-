from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from sse_starlette.sse import EventSourceResponse

from app.models.run import Run, RunRequest
from app.models.schema import RunResults
from app.services import orchestrator, store
from app.services import migration as mig

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


@router.get("/{run_id}/scope.json")
async def scope_manifest_json(run_id: str) -> JSONResponse:
    """Migration scope manifest — feeds the Transformation Agent's IR builder."""
    results = await store.get_results(run_id)
    if not results or not results.inventory:
        raise HTTPException(status_code=404, detail="results not ready")
    payload = mig.build_scope_manifest(results.inventory, results.lineage)
    return JSONResponse(content=payload, headers={
        "Content-Disposition": f'attachment; filename="scope-{run_id[:8]}.json"',
    })


@router.get("/{run_id}/scope.csv")
async def scope_manifest_csv(run_id: str) -> PlainTextResponse:
    """Flat CSV of in-scope objects for spreadsheet review."""
    results = await store.get_results(run_id)
    if not results or not results.inventory:
        raise HTTPException(status_code=404, detail="results not ready")
    inv = results.inventory
    lines = ["kind,name,scope,reason,score"]
    decom_by_fqn = {a.object_fqn: a for a in inv.decommission}
    for t in inv.tables:
        if t.kind == "CSV":
            continue
        a = decom_by_fqn.get(t.fqn)
        if a and a.verdict == "safe":
            lines.append(f"table,{t.fqn},out,decommission_safe,{a.score}")
        else:
            score = a.score if a else ""
            lines.append(f"table,{t.fqn},in,migration_target,{score}")
    for p in inv.pipelines:
        ran = (p.runs and p.runs.runs_total > 0) or p.csv_exists
        if ran:
            lines.append(f"pipeline,{p.name},in,active,")
        else:
            lines.append(f"pipeline,{p.name},out,never_executed,")
    csv_text = "\n".join(lines) + "\n"
    return PlainTextResponse(content=csv_text, headers={
        "Content-Type": "text/csv",
        "Content-Disposition": f'attachment; filename="scope-{run_id[:8]}.csv"',
    })
