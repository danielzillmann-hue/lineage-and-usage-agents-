"""Shared agent helpers — Claude client, streaming hooks, prompt utilities."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from anthropic import AsyncAnthropic

from app.config import get_settings
from app.models.run import AgentName, StreamEvent

log = logging.getLogger(__name__)


EmitFn = Callable[[StreamEvent], Awaitable[None]]


def claude() -> AsyncAnthropic:
    return AsyncAnthropic(api_key=get_settings().anthropic_api_key)


async def log_event(emit: EmitFn, agent: AgentName, message: str, *, kind: str = "log") -> None:
    await emit(StreamEvent(event=kind, agent=agent, message=message))


async def stream_thinking(emit: EmitFn, agent: AgentName, model: str, system: str, user: str) -> str:
    """Run a Claude completion with streaming, mirror text deltas to the UI as 'thinking' events."""
    client = claude()
    parts: list[str] = []
    async with client.messages.stream(
        model=model,
        max_tokens=8192,
        system=system,
        messages=[{"role": "user", "content": user}],
    ) as s:
        async for text in s.text_stream:
            parts.append(text)
            await emit(StreamEvent(event="thinking", agent=agent, message=text))
    return "".join(parts)
