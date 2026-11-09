"""Preserve exact SciFetch tool replies through the Hermes model turn."""

from __future__ import annotations

import asyncio
import threading
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Final

_PENDING_TTL_SECONDS: Final[float] = 300.0
_MAX_PENDING_SESSIONS: Final[int] = 256
_TOOL_ACK_GATE_TIMEOUT_SECONDS: Final[float] = 60.0
_TOOL_ACK_POST_TRANSFORM_GRACE_SECONDS: Final[float] = 0.5
_TOOL_ACK_GATE_POLL_SECONDS: Final[float] = 0.05
_TOOL_ACK_GATE_KWARG: Final[str] = "_scifetch_tool_ack_gate"


class ToolAckDeliveryGate:
    """Release tool-started watchers after exact ACK output reaches transform."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        post_transform_grace_seconds: float,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[object]] | None = None,
    ) -> None:
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._state_lock = threading.Lock()
        self._fallback_deadline = self._monotonic() + max(0.0, timeout_seconds)
        self._post_transform_grace_seconds = max(0.0, post_transform_grace_seconds)
        self._released_at: float | None = None

    def extend_fallback(self, *, timeout_seconds: float) -> None:
        """Keep one multi-tool turn gated until its most recent tool result."""
        deadline = self._monotonic() + max(0.0, timeout_seconds)
        with self._state_lock:
            self._fallback_deadline = max(self._fallback_deadline, deadline)

    def release(self) -> None:
        with self._state_lock:
            if self._released_at is None:
                self._released_at = self._monotonic()

    @property
    def is_released(self) -> bool:
        with self._state_lock:
            return self._released_at is not None

    @property
    def fallback_deadline(self) -> float:
        with self._state_lock:
            return self._fallback_deadline

    async def wait(self) -> bool:
        """Return after transform plus grace, or False after bounded fallback."""
        while True:
            now = self._monotonic()
            with self._state_lock:
                released_at = self._released_at
                fallback_deadline = self._fallback_deadline
            if released_at is None:
                remaining = fallback_deadline - now
                if remaining <= 0:
                    return False
            else:
                remaining = released_at + self._post_transform_grace_seconds - now
                if remaining <= 0:
                    return True
            await self._sleep(min(_TOOL_ACK_GATE_POLL_SECONDS, remaining))


@dataclass(frozen=True)
class _PendingResponse:
    created_at: float
    messages: tuple[str, ...]
    ack_gate: ToolAckDeliveryGate | None = None


_PENDING_RESPONSES: OrderedDict[str, _PendingResponse] = OrderedDict()
_PENDING_LOCK = threading.Lock()


def with_tool_ack_gate(kwargs: Mapping[str, object]) -> dict[str, object]:
    """Attach one session-scoped watcher gate to an official tool invocation."""
    runtime_kwargs = dict(kwargs)
    session_id = _session_id(kwargs)
    if not session_id:
        return runtime_kwargs

    now = time.monotonic()
    with _PENDING_LOCK:
        _prune(now)
        pending = _PENDING_RESPONSES.get(session_id)
        gate = pending.ack_gate if pending is not None else None
        if gate is None:
            gate = _new_tool_ack_gate()
        else:
            gate.extend_fallback(timeout_seconds=_TOOL_ACK_GATE_TIMEOUT_SECONDS)
        _PENDING_RESPONSES[session_id] = _PendingResponse(
            created_at=now,
            messages=pending.messages if pending is not None else (),
            ack_gate=gate,
        )
        _PENDING_RESPONSES.move_to_end(session_id)
        _trim_pending_sessions()
    runtime_kwargs[_TOOL_ACK_GATE_KWARG] = gate
    return runtime_kwargs


def tool_ack_gate_from_kwargs(
    kwargs: Mapping[str, object],
) -> ToolAckDeliveryGate | None:
    value = kwargs.get(_TOOL_ACK_GATE_KWARG)
    return value if isinstance(value, ToolAckDeliveryGate) else None


def with_command_ack_gate(kwargs: Mapping[str, object]) -> dict[str, object]:
    """Attach a command-local gate when no Hermes session context is exposed."""
    runtime_kwargs = dict(kwargs)
    runtime_kwargs[_TOOL_ACK_GATE_KWARG] = _new_tool_ack_gate()
    return runtime_kwargs


def release_ack_gate(kwargs: Mapping[str, object]) -> None:
    """Release a command gate only after the complete response has been selected."""
    gate = tool_ack_gate_from_kwargs(kwargs)
    if gate is not None:
        gate.release()


def remember_tool_response(kwargs: Mapping[str, object], message: str) -> None:
    """Remember exact tool results until Hermes transforms the final reply."""
    session_id = _session_id(kwargs)
    if not session_id or not message:
        return
    now = time.monotonic()
    with _PENDING_LOCK:
        _prune(now)
        pending = _PENDING_RESPONSES.get(session_id)
        messages = (*pending.messages, message) if pending is not None else (message,)
        _PENDING_RESPONSES[session_id] = _PendingResponse(
            created_at=now,
            messages=messages,
            ack_gate=pending.ack_gate if pending is not None else None,
        )
        _PENDING_RESPONSES.move_to_end(session_id)
        _trim_pending_sessions()


def discard_pending_tool_response(kwargs: Mapping[str, object]) -> None:
    """Discard a stale result when a new turn starts in the same session."""
    session_id = _session_id(kwargs)
    if not session_id:
        return
    with _PENDING_LOCK:
        pending = _PENDING_RESPONSES.pop(session_id, None)
    _release_gate(pending)


def handle_transform_llm_output(**kwargs: object) -> str | None:
    """Replace model paraphrase with the exact result from this session's tool."""
    session_id = _session_id(kwargs)
    if not session_id:
        return None
    now = time.monotonic()
    with _PENDING_LOCK:
        _prune(now)
        pending = _PENDING_RESPONSES.pop(session_id, None)
    if pending is None:
        return None
    try:
        return _render_pending_messages(pending.messages)
    finally:
        _release_gate(pending)


def handle_session_response_cleanup(**kwargs: object) -> None:
    """Drop turn-scoped outputs after a turn or before session destruction."""
    discard_pending_tool_response(kwargs)


def _session_id(kwargs: Mapping[str, object]) -> str:
    value = kwargs.get("session_id")
    return value.strip() if isinstance(value, str) else ""


def _prune(now: float) -> None:
    expired = [
        session_id
        for session_id, pending in _PENDING_RESPONSES.items()
        if now - pending.created_at > _PENDING_TTL_SECONDS
    ]
    for session_id in expired:
        _release_gate(_PENDING_RESPONSES.pop(session_id, None))


def _trim_pending_sessions() -> None:
    while len(_PENDING_RESPONSES) > _MAX_PENDING_SESSIONS:
        _session_id_value, pending = _PENDING_RESPONSES.popitem(last=False)
        _release_gate(pending)


def _render_pending_messages(messages: tuple[str, ...]) -> str | None:
    return "\n\n".join(messages) if messages else None


def _new_tool_ack_gate() -> ToolAckDeliveryGate:
    return ToolAckDeliveryGate(
        timeout_seconds=_TOOL_ACK_GATE_TIMEOUT_SECONDS,
        post_transform_grace_seconds=_TOOL_ACK_POST_TRANSFORM_GRACE_SECONDS,
    )


def _release_gate(pending: _PendingResponse | None) -> None:
    if pending is not None and pending.ack_gate is not None:
        pending.ack_gate.release()
