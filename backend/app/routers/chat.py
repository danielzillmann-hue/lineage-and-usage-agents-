"""Ask the agents — RAG-style chat over a completed run's results.

POST /api/runs/{run_id}/chat takes a question (and optional prior turns)
and returns a Gemini-generated answer grounded in the run's
inventory + lineage + usage + summary outputs.

Stateless: the frontend keeps the conversation history and replays it
on every request.
"""

from __future__ import annotations

import json
import logging
from typing import Literal

from fastapi import APIRouter, HTTPException
from google.genai import types
from pydantic import BaseModel

from app.agents.base import gemini
from app.config import get_settings
from app.services import store

log = logging.getLogger(__name__)

router = APIRouter()


class ChatTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    question: str
    history: list[ChatTurn] = []


class ChatResponse(BaseModel):
    answer: str
    model: str
    context_size: int


_SYSTEM_PROMPT = """\
You are a senior data engineer answering questions about an Oracle data
warehouse and the migration project that's being planned for it. You have
the full output of a multi-agent analysis run attached as JSON context:

- inventory: every table, view, procedure, ETL pipeline (with steps), FKs,
  decommission verdicts, and migration sequencing waves.
- lineage: column-level edges from sources -> ETL steps -> outputs.
- usage: pipeline run history, hot tables, dead objects, undocumented
  executions, never-run pipelines.
- summary: headline + bullets + findings + key metrics.

Rules:
1. Only answer using the provided JSON. If something isn't in there, say
   so plainly — never invent table names, row counts, or owners.
2. Quote specific names verbatim. If asked "which pipeline writes X",
   list every match by name.
3. Be concise. Bullet lists for enumerations, short paragraphs for
   explanations. If the answer is one number, just say the number.
4. When the user asks for SQL or a recommendation, anchor it in the
   data: which pipeline, which table, which finding from the summary.
5. If the question is ambiguous, ask one clarifying question rather
   than guessing.
"""


def _trim_inventory(inv: dict) -> dict:
    """Strip the heaviest fields from the inventory so the context fits
    comfortably. We keep names, layers, and metadata but drop column
    annotation_notes and source_text bodies (those can be GBs).
    """
    if not inv:
        return {}
    out = json.loads(json.dumps(inv, default=str))  # deep copy
    for t in out.get("tables", []):
        t.pop("source_text", None)
        for c in t.get("columns", []):
            c.pop("annotation_notes", None)
            c.pop("inherited_sensitivity_from", None)
    for p in out.get("procedures", []):
        # Keep procedure metadata but cap the source body.
        src = p.get("source") or ""
        if len(src) > 4000:
            p["source"] = src[:4000] + "\n-- (truncated)"
    return out


@router.post("/{run_id}/chat", response_model=ChatResponse)
async def chat(run_id: str, body: ChatRequest) -> ChatResponse:
    run = await store.get_run(run_id)
    if run is None:
        raise HTTPException(404, f"run {run_id} not found")

    results = await store.get_results(run_id)
    if results is None:
        raise HTTPException(400, f"run {run_id} has no results yet")

    # Assemble compact JSON context
    context = {
        "inventory": _trim_inventory(
            results.inventory.model_dump(mode="json") if results.inventory else None
        ),
        "lineage": (
            results.lineage.model_dump(mode="json") if results.lineage else None
        ),
        "usage": (
            results.usage.model_dump(mode="json") if results.usage else None
        ),
        "summary": (
            results.summary.model_dump(mode="json") if results.summary else None
        ),
    }
    context_json = json.dumps(context, default=str, separators=(",", ":"))

    # Build the chat content from prior history + new question.
    convo_lines: list[str] = []
    for turn in body.history[-12:]:  # cap on history to avoid blowing tokens
        prefix = "User" if turn.role == "user" else "Assistant"
        convo_lines.append(f"{prefix}: {turn.content}")
    convo_lines.append(f"User: {body.question}")
    convo_lines.append("Assistant:")
    user_message = (
        f"=== Run results JSON ===\n{context_json}\n\n"
        f"=== Conversation ===\n" + "\n\n".join(convo_lines)
    )

    settings = get_settings()
    # Gemini 2.5 Pro for synthesis — same model + region the summary
    # agent uses (us-central1; Pro isn't available in australia-southeast1).
    model = settings.summary_model
    location = settings.summary_location

    client = gemini(location=location)
    cfg = types.GenerateContentConfig(
        system_instruction=_SYSTEM_PROMPT,
        max_output_tokens=4096,
        temperature=0.3,
    )
    try:
        resp = await client.aio.models.generate_content(
            model=model, contents=user_message, config=cfg,
        )
    except Exception as e:  # noqa: BLE001
        log.exception("chat: Gemini call failed for run %s", run_id)
        raise HTTPException(
            status_code=502,
            detail=f"Gemini call failed: {type(e).__name__}: {e}",
        ) from e

    answer = (getattr(resp, "text", None) or "").strip()
    if not answer:
        answer = "(no response — try rephrasing)"

    return ChatResponse(
        answer=answer,
        model=model,
        context_size=len(context_json),
    )
