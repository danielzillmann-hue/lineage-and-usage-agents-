"""Shared agent helpers — Vertex Gemini client, streaming hooks, prompt utilities."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from google import genai
from google.genai import types

from app.config import get_settings
from app.models.run import AgentName, StreamEvent

log = logging.getLogger(__name__)


EmitFn = Callable[[StreamEvent], Awaitable[None]]


def gemini(location: str | None = None) -> genai.Client:
    """Vertex AI client. Auth via Application Default Credentials.

    Pass a non-default location for models that aren't available in the primary
    region (e.g. Gemini 2.5 Pro often needs us-central1).
    """
    s = get_settings()
    return genai.Client(vertexai=True, project=s.gcp_project, location=location or s.vertex_location)


async def log_event(emit: EmitFn, agent: AgentName, message: str, *, kind: str = "log") -> None:
    await emit(StreamEvent(event=kind, agent=agent, message=message))


async def stream_thinking(
    emit: EmitFn,
    agent: AgentName,
    model: str,
    system: str,
    user: str,
    *,
    location: str | None = None,
    json_mode: bool = False,
) -> str:
    """Run a Gemini completion with streaming; mirror text deltas to the UI as 'thinking'.

    When json_mode is True, asks Gemini to emit application/json so downstream
    JSON parsing stops fighting prose preambles and code fences.
    """
    client = gemini(location=location)
    parts: list[str] = []
    cfg = types.GenerateContentConfig(
        system_instruction=system,
        max_output_tokens=32768,
        temperature=0.2 if json_mode else 0.3,
        response_mime_type="application/json" if json_mode else None,
    )
    stream = await client.aio.models.generate_content_stream(model=model, contents=user, config=cfg)
    async for chunk in stream:
        text = getattr(chunk, "text", None)
        if not text:
            continue
        parts.append(text)
        await emit(StreamEvent(event="thinking", agent=agent, message=text))
    return "".join(parts)
