from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from fastapi.responses import HTMLResponse

from app.models.run import Run, RunRequest
from app.models.schema import ColumnNature, RunResults, Sensitivity
from app.services import handover, orchestrator, store
from app.services import migration as mig

router = APIRouter()


class ColumnAnnotationUpdate(BaseModel):
    table_fqn: str            # e.g. SUPERUSER.MEMBERS
    column_name: str
    sensitivity: Sensitivity | None = None
    nature: ColumnNature | None = None
    annotation_notes: str | None = None


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


@router.patch("/{run_id}/columns")
async def patch_column_annotation(run_id: str, update: ColumnAnnotationUpdate) -> dict:
    """Apply a human override to a single column's classification.

    Re-runs sensitivity propagation so PII inherited downstream stays in sync.
    Returns the updated column payload.
    """
    results = await store.get_results(run_id)
    if not results or not results.inventory:
        raise HTTPException(status_code=404, detail="results not found")
    inv = results.inventory
    target_table = next((t for t in inv.tables if t.fqn.upper() == update.table_fqn.upper()), None)
    if not target_table:
        raise HTTPException(status_code=404, detail=f"table {update.table_fqn} not found")
    target_col = next((c for c in target_table.columns if c.name.upper() == update.column_name.upper()), None)
    if not target_col:
        raise HTTPException(status_code=404, detail=f"column {update.column_name} not found")

    if update.sensitivity is not None:
        target_col.sensitivity = update.sensitivity
    if update.nature is not None:
        target_col.nature = update.nature
    if update.annotation_notes is not None:
        target_col.annotation_notes = update.annotation_notes[:160] if update.annotation_notes else None
    target_col.user_overridden = True

    # Reset and re-propagate so downstream PII reach reflects the override.
    for t in inv.tables:
        for c in t.columns:
            c.inherited_sensitivity_from = []
    from app.services import sensitivity_propagation as _sens
    _sens.propagate(inv, results.lineage)

    await store.save_results(run_id, results)
    return target_col.model_dump()


@router.get("/{run_id}/handover.html", response_class=HTMLResponse)
async def handover_html(run_id: str) -> HTMLResponse:
    run = await store.get_run(run_id)
    results = await store.get_results(run_id)
    if not run or not results:
        raise HTTPException(status_code=404, detail="results not found")
    return HTMLResponse(content=handover.render_html(run, results))


@router.get("/{run_id}/handover.md")
async def handover_md(run_id: str) -> PlainTextResponse:
    run = await store.get_run(run_id)
    results = await store.get_results(run_id)
    if not run or not results:
        raise HTTPException(status_code=404, detail="results not found")
    return PlainTextResponse(
        content=handover.render_markdown(run, results),
        headers={
            "Content-Type": "text/markdown",
            "Content-Disposition": f'attachment; filename="handover-{run_id[:8]}.md"',
        },
    )


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
