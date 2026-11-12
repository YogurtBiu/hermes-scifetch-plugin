"""Official Hermes gateway-adapter delivery bridge for personal instances."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Final, cast

from .delivery import (
    QQBOT_NATIVE_UPLOAD_MAX_BYTES,
    QQBOT_PASSIVE_REPLY_BUDGET_EXHAUSTED_ERROR_KIND,
    QQBOT_PASSIVE_REPLY_EXPIRED_ERROR_KIND,
    QQBOT_PASSIVE_REPLY_UNAVAILABLE_ERROR_KIND,
    TESTED_HERMES_VERSIONS,
    WEIXIN_NATIVE_UPLOAD_MAX_BYTES,
)
from .environment import runtime_version

_CONTEXT_TTL_SECONDS: Final[float] = 300.0
_MAX_CONTEXTS: Final[int] = 256
_DETERMINISTIC_ROUTE_VERSIONS: Final[frozenset[str]] = frozenset(
    {"0.18.2", "0.20.0", "0.21.2", "0.21.3", "0.22.5", "0.23.0", "0.23.1"}
)
_QQBOT_PASSIVE_REPLY_WINDOWS_SECONDS: Final[dict[str, float]] = {
    "c2c": 60.0 * 60.0,
    "group": 5.0 * 60.0,
}
_QQBOT_WATCHER_PASSIVE_REPLY_LIMITS: Final[dict[str, int]] = {
    # QQ permits four C2C and five group passive replies per inbound message.
    # Reserve one for Hermes' ordinary agent response to the triggering turn.
    "c2c": 3,
    "group": 4,
}

GatewaySender = Callable[..., object]


class _PassiveReplyBudget:
    """Share a bounded QQ passive-reply allowance across watcher callbacks."""

    def __init__(self, remaining: int) -> None:
        self._remaining = max(0, remaining)
        self._lock = threading.Lock()

    def claim(self) -> bool:
        """Reserve one passive attempt before the adapter call."""
        with self._lock:
            if self._remaining <= 0:
                return False
            self._remaining -= 1
            return True


@dataclass(frozen=True)
class _DeferredGatewaySendResult:
    """SendResult-shaped rejection that keeps QQ delivery locally resumable."""

    error: str
    error_kind: str
    success: bool = False
    message_id: str = ""
    retryable: bool = False
    retry_after: float | None = None


@dataclass(frozen=True)
class _GatewayDeliveryContext:
    platform: str
    user_id: str
    chat_id: str
    chat_type: str
    message_key: str
    send_document: GatewaySender
    send_text: GatewaySender
    event_loop: asyncio.AbstractEventLoop
    created_at: float
    message_id: str = ""
    passive_reply_budget: _PassiveReplyBudget | None = None


@dataclass(frozen=True)
class _PersonalTurnContext:
    platform: str
    created_at: float
    gateway_delivery: _GatewayDeliveryContext | None = None


@dataclass(frozen=True)
class _DeterministicRoute:
    raw_payload: str
    gateway_delivery: _GatewayDeliveryContext
    created_at: float
    runtime_values: Mapping[str, object]
    source: object


_TURN_CONTEXTS: OrderedDict[str, _PersonalTurnContext] = OrderedDict()
_GATEWAY_CONTEXTS: OrderedDict[str, _GatewayDeliveryContext] = OrderedDict()
_DETERMINISTIC_ROUTES: OrderedDict[asyncio.Task[object], _DeterministicRoute] = OrderedDict()
# Current Hermes copies context into its synchronous command executor. This is
# transport ownership only; it carries no authenticated user/principal claim.
_DETERMINISTIC_ROUTE_OWNER: ContextVar[asyncio.Task[object] | None] = ContextVar(
    "scifetch_deterministic_route_owner", default=None
)
_TURN_CONTEXTS_LOCK = threading.Lock()


def remember_personal_gateway_context(kwargs: Mapping[str, object]) -> None:
    """Stage an official gateway adapter route without making any HTTP request."""
    context = _gateway_delivery_context(kwargs)
    if context is None:
        return
    key = f"{context.platform}:{context.chat_id}:{context.message_id or time.monotonic_ns()}"
    with _TURN_CONTEXTS_LOCK:
        _prune(context.created_at)
        _GATEWAY_CONTEXTS[key] = context
        _GATEWAY_CONTEXTS.move_to_end(key)
        while len(_GATEWAY_CONTEXTS) > _MAX_CONTEXTS:
            _GATEWAY_CONTEXTS.popitem(last=False)


def stage_deterministic_route(kwargs: Mapping[str, object], raw_payload: str) -> bool:
    """Stage a transport-only route for the current stock gateway task."""
    task = asyncio.current_task()
    if task is None:
        return False
    version = _normalized_version(_installed_hermes_version())
    if version not in _DETERMINISTIC_ROUTE_VERSIONS:
        return False
    context = _gateway_delivery_context(kwargs)
    if context is None:
        return False
    now = _monotonic()
    with _TURN_CONTEXTS_LOCK:
        _prune(now)
        runtime_values = {
            key: kwargs[key]
            for key in ("state_path", "transport", "background_task_scheduler")
            if key in kwargs
        }
        _DETERMINISTIC_ROUTES[task] = _DeterministicRoute(
            raw_payload,
            context,
            now,
            runtime_values,
            getattr(kwargs["event"], "source", None),
        )
        _DETERMINISTIC_ROUTES.move_to_end(task)
        while len(_DETERMINISTIC_ROUTES) > _MAX_CONTEXTS:
            _DETERMINISTIC_ROUTES.popitem(last=False)
    _DETERMINISTIC_ROUTE_OWNER.set(task)
    return True


def claim_deterministic_route(
    raw_payload: str, *, source: object | None = None
) -> dict[str, object] | None:
    """Atomically claim the current task's same-event route after stock auth/ACL."""
    off_loop = False
    try:
        task = asyncio.current_task()
    except RuntimeError:
        off_loop = True
        task = _DETERMINISTIC_ROUTE_OWNER.get()
    if task is None:
        return None
    now = _monotonic()
    with _TURN_CONTEXTS_LOCK:
        _prune(now)
        route = _DETERMINISTIC_ROUTES.get(task)
        # The official command invoker forwards the same source object after
        # gateway auth/ACL. A copied child/thread context alone is insufficient.
        if route is None or (off_loop and source is not route.source):
            return None
        _DETERMINISTIC_ROUTES.pop(task)
    if route is None or route.raw_payload.strip() != raw_payload.strip():
        return None
    runtime = _runtime_kwargs_for_delivery(route.gateway_delivery)
    runtime.update(route.runtime_values)
    runtime["client_request_id"] = _opaque_client_request_id(route)
    return runtime


def _gateway_delivery_context(
    kwargs: Mapping[str, object],
) -> _GatewayDeliveryContext | None:
    event = kwargs.get("event")
    gateway = kwargs.get("gateway")
    source = getattr(event, "source", None) if event is not None else None
    if source is None or gateway is None:
        return None
    # The audited Weixin private / QQ C2C+group transports have no threaded
    # document contract. Never silently collapse a topic into its parent chat.
    if any(
        getattr(value, field, None)
        for value in (source, event)
        for field in ("thread_id", "topic_id", "message_thread_id")
    ):
        return None

    platform_value = getattr(source, "platform", "")
    platform = _platform_key(platform_value)
    user_id = _string_field(source, "user_id")
    chat_id = _string_field(source, "chat_id")
    chat_type = _platform_key(getattr(source, "chat_type", ""))
    message_key = _message_key(getattr(event, "text", ""))
    message_id = _string_field(event, "message_id")
    if not user_id or not chat_id or not message_key:
        return None
    if platform == "weixin":
        if chat_type not in {"dm", "private"}:
            return None
    elif platform == "qqbot":
        chat_type = _qqbot_chat_type(
            event=event,
            source_chat_type=chat_type,
            user_id=user_id,
            chat_id=chat_id,
        )
        if not chat_type or not message_id:
            return None
    else:
        return None

    adapter = _gateway_adapter(gateway, platform_value, platform)
    send_document = getattr(adapter, "send_document", None)
    send_text = getattr(adapter, "send", None)
    if (
        not callable(send_document)
        or not callable(send_text)
        or _is_base_document_fallback(send_document)
    ):
        return None
    try:
        event_loop = asyncio.get_running_loop()
    except RuntimeError:
        return None

    now = _monotonic()
    return _GatewayDeliveryContext(
        platform=platform,
        user_id=user_id,
        chat_id=chat_id,
        chat_type=chat_type or "private",
        message_key=message_key,
        send_document=send_document,
        send_text=send_text,
        event_loop=event_loop,
        created_at=now,
        message_id=message_id,
        passive_reply_budget=(
            _PassiveReplyBudget(_QQBOT_WATCHER_PASSIVE_REPLY_LIMITS[chat_type])
            if platform == "qqbot"
            else None
        ),
    )


def remember_personal_turn_context(kwargs: Mapping[str, object]) -> None:
    """Remember documented pre-LLM platform context for this one tool turn."""
    session_id = _session_id(kwargs)
    platform = _platform_key(kwargs.get("platform"))
    if not session_id or not platform:
        return
    now = _monotonic()
    with _TURN_CONTEXTS_LOCK:
        _prune(now)
        gateway_delivery = _claim_gateway_context(
            platform=platform,
            sender_id=_string_field(kwargs, "sender_id"),
            message_key=_message_key(kwargs.get("user_message")),
        )
        _TURN_CONTEXTS[session_id] = _PersonalTurnContext(
            platform=platform,
            created_at=now,
            gateway_delivery=gateway_delivery,
        )
        _TURN_CONTEXTS.move_to_end(session_id)
        while len(_TURN_CONTEXTS) > _MAX_CONTEXTS:
            _TURN_CONTEXTS.popitem(last=False)


def discard_personal_turn_context(kwargs: Mapping[str, object]) -> None:
    """Discard the reusable delivery context when its authenticated turn ends."""
    session_id = _session_id(kwargs)
    if not session_id:
        return
    with _TURN_CONTEXTS_LOCK:
        _TURN_CONTEXTS.pop(session_id, None)


def with_home_delivery_bridge(kwargs: Mapping[str, object]) -> dict[str, object]:
    """Attach a tested gateway adapter route to an authenticated personal turn."""
    runtime_kwargs = dict(kwargs)
    context = _personal_turn_context(kwargs)
    if context is None or context.platform not in {"qqbot", "weixin"}:
        return runtime_kwargs

    hermes_version = _normalized_version(_installed_hermes_version())
    if hermes_version not in TESTED_HERMES_VERSIONS:
        return runtime_kwargs

    gateway_delivery = context.gateway_delivery
    if gateway_delivery is None:
        return runtime_kwargs

    runtime_kwargs.update(_runtime_kwargs_for_delivery(gateway_delivery, platform=context.platform))
    return runtime_kwargs


def _runtime_kwargs_for_delivery(
    gateway_delivery: _GatewayDeliveryContext,
    *,
    platform: str = "",
) -> dict[str, object]:
    context_platform = platform or gateway_delivery.platform
    hermes_version = _normalized_version(_installed_hermes_version())
    if hermes_version not in TESTED_HERMES_VERSIONS:
        return {}

    async def send_document(path: str) -> object:
        return await _invoke_gateway_sender(
            gateway_delivery.event_loop,
            gateway_delivery.send_document,
            gateway_delivery.chat_id,
            path,
            delivery_platform=context_platform,
            delivery_context=gateway_delivery,
        )

    async def native_document(path: str, before_native: Callable[[], bool]) -> object:
        # The durable start token is consumed on the owning gateway loop, after
        # passive-reply preflight and immediately before the concrete native call.
        return await _invoke_gateway_sender(
            gateway_delivery.event_loop,
            gateway_delivery.send_document,
            gateway_delivery.chat_id,
            path,
            delivery_platform=context_platform,
            delivery_context=gateway_delivery,
            before_native=before_native,
        )

    async def send_text(message: str) -> object:
        return await _invoke_gateway_sender(
            gateway_delivery.event_loop,
            gateway_delivery.send_text,
            gateway_delivery.chat_id,
            message,
            delivery_platform=context_platform,
            delivery_context=gateway_delivery,
        )

    max_document_bytes = (
        QQBOT_NATIVE_UPLOAD_MAX_BYTES
        if context_platform == "qqbot"
        else WEIXIN_NATIVE_UPLOAD_MAX_BYTES
    )
    return {
        "delivery_platform": context_platform,
        "delivery_user_id": gateway_delivery.user_id,
        "delivery_chat_id": gateway_delivery.chat_id,
        "delivery_chat_type": gateway_delivery.chat_type,
        "delivery_hermes_version": hermes_version,
        "delivery_capabilities": {
            "document_upload": True,
            "max_document_bytes": max_document_bytes,
            "supports_local_path": True,
            "supports_remote_url": False,
        },
        "send_document": send_document,
        "native_document_sender": native_document,
        "send_text": send_text,
        # The tool result is already returned in the active chat. This
        # callback is reserved for watcher output and must not duplicate ACKs.
        "suppress_immediate_text": True,
    }


async def _invoke_gateway_sender(
    event_loop: asyncio.AbstractEventLoop,
    sender: GatewaySender,
    *args: object,
    delivery_platform: str,
    delivery_context: _GatewayDeliveryContext,
    before_native: Callable[[], bool] | None = None,
) -> object:
    if event_loop.is_closed():
        raise RuntimeError("Hermes gateway delivery loop is no longer available.")

    async def invoke_sender() -> object:
        reply_kwargs, rejection = _gateway_reply_decision(
            delivery_platform,
            delivery_context,
        )
        if rejection is not None:
            return rejection
        if before_native is not None and not before_native():
            return _DeferredGatewaySendResult(
                error="Native dispatch was fenced before invocation.",
                error_kind="dispatch_fenced",
            )
        result = sender(*args, **reply_kwargs)
        return await result if inspect.isawaitable(result) else result

    running_loop = asyncio.get_running_loop()
    if running_loop is event_loop:
        return await invoke_sender()

    future = asyncio.run_coroutine_threadsafe(invoke_sender(), event_loop)
    return await asyncio.wrap_future(future)


def _personal_turn_context(
    kwargs: Mapping[str, object],
) -> _PersonalTurnContext | None:
    session_id = _session_id(kwargs)
    if not session_id:
        return None
    now = _monotonic()
    with _TURN_CONTEXTS_LOCK:
        _prune(now)
        return _TURN_CONTEXTS.get(session_id)


def _installed_hermes_version() -> str:
    return runtime_version()


def _normalized_version(value: str) -> str:
    normalized = value.strip().lower()
    return normalized[1:] if normalized.startswith("v") else normalized


def _session_id(kwargs: Mapping[str, object]) -> str:
    value = kwargs.get("session_id")
    return value.strip() if isinstance(value, str) else ""


def _platform_key(value: object) -> str:
    raw = getattr(value, "value", value)
    return raw.strip().lower() if isinstance(raw, str) else ""


def _string_field(value: object, name: str) -> str:
    raw = value.get(name, "") if isinstance(value, Mapping) else getattr(value, name, "")
    enum_value = getattr(raw, "value", raw)
    return enum_value.strip() if isinstance(enum_value, str) else ""


def _message_key(value: object) -> str:
    return " ".join(value.split()) if isinstance(value, str) else ""


def _gateway_adapter(gateway: object, platform_value: object, platform: str) -> object | None:
    adapters = getattr(gateway, "adapters", None)
    if not isinstance(adapters, Mapping):
        return None
    adapter = cast(object | None, adapters.get(platform_value))
    if adapter is not None:
        return adapter
    for key, candidate in adapters.items():
        if _platform_key(key) == platform:
            return cast(object, candidate)
    return None


def _qqbot_chat_type(
    *,
    event: object,
    source_chat_type: str,
    user_id: str,
    chat_id: str,
) -> str:
    raw_message = getattr(event, "raw_message", None)
    if not isinstance(raw_message, Mapping):
        return ""
    author = raw_message.get("author")
    if not isinstance(author, Mapping):
        return ""

    if source_chat_type == "dm":
        if user_id != chat_id:
            return ""
        if any(
            _has_nonempty_field(raw_message, field)
            for field in ("guild_id", "group_openid", "channel_id")
        ):
            return ""
        return "c2c" if _string_field(author, "user_openid") == user_id else ""

    if source_chat_type == "group":
        if any(_has_nonempty_field(raw_message, field) for field in ("guild_id", "channel_id")):
            return ""
        if _string_field(raw_message, "group_openid") != chat_id:
            return ""
        return "group" if _string_field(author, "member_openid") == user_id else ""

    return ""


def _has_nonempty_field(value: Mapping[str, object], field: str) -> bool:
    raw = value.get(field)
    if raw is None:
        return False
    return bool(raw.strip()) if isinstance(raw, str) else True


def _gateway_reply_decision(
    platform: str,
    context: _GatewayDeliveryContext,
) -> tuple[dict[str, object], _DeferredGatewaySendResult | None]:
    if platform != "qqbot":
        return {}, None
    if not context.message_id:
        return {}, _qqbot_deferred_result(
            QQBOT_PASSIVE_REPLY_UNAVAILABLE_ERROR_KIND,
            "QQ passive reply context is unavailable; wait for a fresh inbound message.",
        )
    window_seconds = _QQBOT_PASSIVE_REPLY_WINDOWS_SECONDS.get(context.chat_type)
    if window_seconds is None:
        return {}, _qqbot_deferred_result(
            QQBOT_PASSIVE_REPLY_UNAVAILABLE_ERROR_KIND,
            "QQ passive reply context is unavailable for this chat type.",
        )
    if _monotonic() - context.created_at >= window_seconds:
        return {}, _qqbot_deferred_result(
            QQBOT_PASSIVE_REPLY_EXPIRED_ERROR_KIND,
            "QQ passive reply window expired; wait for a fresh inbound message.",
        )
    budget = context.passive_reply_budget
    if budget is None or not budget.claim():
        return {}, _qqbot_deferred_result(
            QQBOT_PASSIVE_REPLY_BUDGET_EXHAUSTED_ERROR_KIND,
            "QQ passive reply budget is exhausted; wait for a fresh inbound message.",
        )
    return {"reply_to": context.message_id}, None


def _qqbot_deferred_result(error_kind: str, error: str) -> _DeferredGatewaySendResult:
    return _DeferredGatewaySendResult(error=error, error_kind=error_kind)


def _monotonic() -> float:
    return time.monotonic()


def _opaque_client_request_id(route: _DeterministicRoute) -> str:
    context = route.gateway_delivery
    event_key = context.message_id or f"{route.created_at:.9f}"
    material = "\x1f".join((context.platform, context.user_id, context.chat_id, event_key))
    return f"gateway_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]}"


def _is_base_document_fallback(sender: GatewaySender) -> bool:
    func = getattr(sender, "__func__", sender)
    qualname = getattr(func, "__qualname__", "")
    return isinstance(qualname, str) and qualname.endswith("BasePlatformAdapter.send_document")


def _claim_gateway_context(
    *,
    platform: str,
    sender_id: str,
    message_key: str,
) -> _GatewayDeliveryContext | None:
    if not sender_id or not message_key:
        return None
    matches = [
        (key, context)
        for key, context in _GATEWAY_CONTEXTS.items()
        if context.platform == platform
        and context.user_id == sender_id
        and _auth_message_matches(context.message_key, message_key)
    ]
    if not matches:
        return None

    _, newest = max(matches, key=lambda item: item[1].created_at)
    ambiguous = len({context.chat_id for _, context in matches}) > 1
    if ambiguous:
        for key, _ in matches:
            _GATEWAY_CONTEXTS.pop(key, None)
        return None

    for key, context in list(_GATEWAY_CONTEXTS.items()):
        if (
            context.platform == newest.platform
            and context.user_id == newest.user_id
            and context.chat_id == newest.chat_id
            and context.created_at <= newest.created_at
        ):
            _GATEWAY_CONTEXTS.pop(key, None)
    return newest


def _auth_message_matches(candidate: str, authenticated: str) -> bool:
    if not candidate or not authenticated:
        return False
    return authenticated == candidate or authenticated.endswith(f" {candidate}")


def _prune(now: float) -> None:
    expired = [
        session_id
        for session_id, context in _TURN_CONTEXTS.items()
        if now - context.created_at > _CONTEXT_TTL_SECONDS
    ]
    for session_id in expired:
        _TURN_CONTEXTS.pop(session_id, None)
    expired_gateway = [
        key
        for key, context in _GATEWAY_CONTEXTS.items()
        if now - context.created_at > _CONTEXT_TTL_SECONDS
    ]
    for key in expired_gateway:
        _GATEWAY_CONTEXTS.pop(key, None)
    expired_tasks = [
        task
        for task, route in _DETERMINISTIC_ROUTES.items()
        if task.done() or now - route.created_at > _CONTEXT_TTL_SECONDS
    ]
    for task in expired_tasks:
        _DETERMINISTIC_ROUTES.pop(task, None)
