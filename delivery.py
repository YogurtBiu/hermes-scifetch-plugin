"""Signed-URL watcher and Hermes file delivery for scifetch-paper."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import logging
import os
import random
import re
import tempfile
import threading
import time
import zipfile
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final, Literal
from urllib.parse import urljoin, urlparse

import httpx

from .auth import ensure_fresh_state, record_sse_reconnect
from .client import SciFetchClientConfig, SciFetchPluginClient, SciFetchPluginError
from .pending import PendingDelivery, PendingDeliveryStore, resolve_pending_path
from .public_errors import browser_stop_explanation, canonical_retained_fact, public_error_message
from .response_output import ToolAckDeliveryGate, tool_ack_gate_from_kwargs
from .state import PluginState, load_state, resolve_state_path
from .trust import state_principal_fields

logger = logging.getLogger(__name__)


def _float_from_env(name: str, *, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("invalid %s value; using default %.1f", name, default)
        return default


def _int_from_env(name: str, *, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("invalid %s value; using default %d", name, default)
        return default


_PUBLIC_FILE_METADATA_KEYS: Final[frozenset[str]] = frozenset(
    {
        "title",
        "authors",
        "doi",
        "journal",
        "publisher",
        "year",
        "publication_year",
        "published_year",
        "published_at",
        "published_date",
        "publication_date",
        "date",
        "abstract",
        "abstract_snippet",
        "summary",
        "first_author",
        "first_affiliation",
        "raw_affiliation",
        "corresponding_authors",
        "si_status",
        "si_count",
        "si_count_known",
        "acquisition_source",
        "delivery_source",
        "from_cache",
        "item_id",
        "file_id",
    }
)
_TEXT_SEND_RETRY_DELAYS: Final[tuple[float, ...]] = (2.0, 8.0, 25.0)
_TEXT_SEND_RATE_LIMIT_RETRY_DELAYS: Final[tuple[float, ...]] = (35.0, 65.0, 125.0)
_DOCUMENT_SEND_RATE_LIMIT_RETRY_DELAYS: Final[tuple[float, ...]] = (35.0, 65.0, 125.0)
_RATE_LIMIT_RETRY_BUFFER_SECONDS: Final[float] = 2.0
_DOCUMENT_SEND_RETRY_MAX_RETRIES: Final[int] = max(
    0,
    _int_from_env("SCIFETCH_DOCUMENT_SEND_RETRY_MAX_RETRIES", default=3),
)
_DOCUMENT_SEND_RETRY_BASE_SECONDS: Final[float] = max(
    0.0,
    _float_from_env("SCIFETCH_DOCUMENT_SEND_RETRY_BASE_SECONDS", default=2.0),
)
_DOCUMENT_SEND_RETRY_MAX_DELAY_SECONDS: Final[float] = max(
    0.0,
    _float_from_env("SCIFETCH_DOCUMENT_SEND_RETRY_MAX_DELAY_SECONDS", default=60.0),
)
_DOCUMENT_SEND_RETRY_JITTER_SECONDS: Final[float] = max(
    0.0,
    _float_from_env("SCIFETCH_DOCUMENT_SEND_RETRY_JITTER_SECONDS", default=1.0),
)
_SSE_READ_TIMEOUT_SECONDS: Final[float] = max(
    0.1,
    _float_from_env("SCIFETCH_SSE_READ_TIMEOUT_SECONDS", default=30.0),
)
_TERMINAL_TEXT_CALLBACK_TIMEOUT_SECONDS: Final[float] = 30.0
_HUMAN_VERIFICATION_GUIDANCE_ZH: Final[str] = (
    "出版商验证尚未稳定完成；请查看可见的服务 Chrome：若页面提示人工操作，请完成后重试；"
    "若页面仍在自动跳转，请等待文章页稳定后再试。"
)
_WILEY_VERIFICATION_GUIDANCE_ZH: Final[str] = "本次 Wiley 官网自动验证未完成，获取已结束。"
_WILEY_CLEANUP_UNCONFIRMED_ZH: Final[str] = (
    "Wiley 官网自动验证的结束状态未能确认，本次获取失败；勿自动重试或重发。"
)
_BROWSER_CLEANUP_UNCONFIRMED_ZH: Final[str] = "浏览器结束状态未能确认，本次获取失败。"
_BROWSER_CDP_COMMAND_TIMEOUT: Final[str] = "browser_cdp_command_timeout"
_BROWSER_RESOURCE_POLICY_UNCERTAIN: Final[str] = "browser_resource_policy_uncertain"
_RESOURCE_POLICY_GUIDANCE_ZH: Final[str] = (
    "浏览器资源安全处理未能确认，已停止获取；请管理员检查会话归属和资源处理记录，"
    "确认安全后再恢复。反复提交不会自动解除。"
)
_BROWSER_CDP_TIMEOUT_LEGACY_MARKER: Final[str] = (
    "browser cdp command timed out; uncertain tab quarantined without navigation"
)

_OBSOLETE_HUMAN_VERIFICATION_ASSERTIONS: Final[tuple[str, ...]] = (
    "requires manual verification",
    "complete verification in visible chrome",
    "complete the visible challenge",
)
_OUTBOUND_SEND_MIN_INTERVAL_SECONDS: Final[float] = max(
    0.0,
    _float_from_env("SCIFETCH_OUTBOUND_SEND_INTERVAL_SECONDS", default=2.0),
)
_COOLDOWN_RE: Final[re.Pattern[str]] = re.compile(
    r"cooldown\s+active\s+for\s+(?P<seconds>\d+(?:\.\d+)?)s",
    re.IGNORECASE,
)
DeliveryOutcome = Literal[
    "sent",
    "retry",
    "deferred",
    "failed",
    "delivered_via_link",
    "unsupported",
]
WECOM_DOCUMENT_MAX_BYTES: Final[int] = 20 * 1024 * 1024
QQBOT_FILE_HARD_LIMIT_BYTES: Final[int] = 200_000_000
QQBOT_PASSIVE_REPLY_EXPIRED_ERROR_KIND: Final[str] = "qq_passive_reply_expired"
QQBOT_PASSIVE_REPLY_BUDGET_EXHAUSTED_ERROR_KIND: Final[str] = "qq_passive_reply_budget_exhausted"
QQBOT_PASSIVE_REPLY_UNAVAILABLE_ERROR_KIND: Final[str] = "qq_passive_reply_unavailable"
QQBOT_NATIVE_UPLOAD_MAX_BYTES: Final[int] = min(
    QQBOT_FILE_HARD_LIMIT_BYTES,
    max(
        0,
        _int_from_env(
            "SCIFETCH_QQBOT_NATIVE_UPLOAD_MAX_BYTES",
            default=QQBOT_FILE_HARD_LIMIT_BYTES,
        ),
    ),
)
WEIXIN_NATIVE_UPLOAD_MAX_BYTES: Final[int] = max(
    0,
    _int_from_env(
        "SCIFETCH_WEIXIN_NATIVE_UPLOAD_MAX_BYTES",
        default=100 * 1024 * 1024,
    ),
)
TESTED_HERMES_VERSIONS: Final[frozenset[str]] = frozenset(
    {"0.18.0", "0.18.2", "0.19.0", "0.20.0", "0.21.2", "0.21.3", "0.22.5", "0.23.0"}
)
_PLATFORM_ALIASES: Final[dict[str, str]] = {
    "wechat": "weixin",
    "wecom": "wecom_ws",
    "qq": "qqbot",
}
_SUPPORTED_DOCUMENT_SUFFIXES: Final[frozenset[str]] = frozenset(
    {
        ".csv",
        ".doc",
        ".docx",
        ".gif",
        ".jpeg",
        ".jpg",
        ".json",
        ".pdf",
        ".png",
        ".ppt",
        ".pptx",
        ".tif",
        ".tiff",
        ".txt",
        ".xls",
        ".xlsx",
        ".xml",
        ".zip",
    }
)
_SUPPORTED_DOCUMENT_CONTENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        "application/json",
        "application/msword",
        "application/pdf",
        "application/vnd.ms-excel",
        "application/vnd.ms-powerpoint",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/xml",
        "application/zip",
        "image/gif",
        "image/jpeg",
        "image/png",
        "image/tiff",
        "text/csv",
        "text/plain",
        "text/xml",
    }
)
_WEIXIN_NATIVE_VIDEO_SUFFIXES: Final[frozenset[str]] = frozenset({".mp4"})
_WEIXIN_NATIVE_VIDEO_CONTENT_TYPES: Final[frozenset[str]] = frozenset(
    {"application/octet-stream", "video/mp4"}
)
_WEIXIN_NATIVE_ZIP_SUFFIX: Final[str] = ".zip"
_WEIXIN_HISTORICAL_ZIP_CONTENT_TYPE: Final[str] = "application/octet-stream"

_DOCUMENT_TERMINAL_ERROR_KINDS: Final[frozenset[str]] = frozenset(
    {
        "unsupported",
        "document_unsupported",
        "payload_too_large",
        "too_large",
        "fatal",
        "auth",
        "forbidden",
        "not_found",
    }
)
_DOCUMENT_RETRYABLE_ERROR_KINDS: Final[frozenset[str]] = frozenset(
    {
        "rate_limited",
        "transient",
        "server",
        "server_error",
        "timeout",
        "overloaded",
        "unknown_retryable",
    }
)
_WEIXIN_CDN_TLS_HOST: Final[str] = "novac2c.cdn.weixin.qq.com"
_WEIXIN_CDN_TLS_HANDSHAKE_MARKERS: Final[tuple[str, ...]] = (
    "handshake failure",
    "sslv3_alert_handshake_failure",
)
_QQBOT_DEFERRED_ERROR_KINDS: Final[frozenset[str]] = frozenset(
    {
        QQBOT_PASSIVE_REPLY_EXPIRED_ERROR_KIND,
        QQBOT_PASSIVE_REPLY_BUDGET_EXHAUSTED_ERROR_KIND,
        QQBOT_PASSIVE_REPLY_UNAVAILABLE_ERROR_KIND,
    }
)


@dataclass(frozen=True)
class _StockDeliveryRule:
    document_upload: bool
    chat_types: frozenset[str]
    max_document_bytes: int | None = None


_STOCK_DELIVERY_MATRIX: Final[dict[str, _StockDeliveryRule]] = {
    "qqbot": _StockDeliveryRule(
        document_upload=True,
        chat_types=frozenset({"c2c", "group"}),
        max_document_bytes=QQBOT_NATIVE_UPLOAD_MAX_BYTES,
    ),
    "weixin": _StockDeliveryRule(
        document_upload=True,
        chat_types=frozenset({"c2c", "direct", "dm", "group", "home", "private"}),
    ),
    "wecom_ws": _StockDeliveryRule(
        document_upload=True,
        chat_types=frozenset({"direct", "dm", "group", "private"}),
        max_document_bytes=WECOM_DOCUMENT_MAX_BYTES,
    ),
    "wecom_callback": _StockDeliveryRule(
        document_upload=False,
        chat_types=frozenset({"direct", "dm", "group", "private"}),
    ),
}


@dataclass(frozen=True)
class DeliveryTarget:
    """Opaque messaging destination carried by delivery attempts."""

    platform: str
    chat_id: str
    chat_type: str = ""
    hermes_version: str = ""
    capabilities: DeliveryCapabilities | None = None
    user_id: str = ""
    thread_id: str = ""


@dataclass(frozen=True)
class DeliveryCapabilities:
    """Subset of Hermes PlatformCapabilities needed by SciFetch delivery."""

    document_upload: bool | None = None
    max_document_bytes: int | None = None
    supports_local_path: bool | None = None
    supports_remote_url: bool | None = None


@dataclass(frozen=True)
class DeliveryRuntime:
    """Runtime dependencies for one request watcher."""

    state: PluginState
    request_id: str
    target: DeliveryTarget
    send_document: Callable[[str], object] | None
    send_text: Callable[[str], object] | None = None
    transport: httpx.AsyncBaseTransport | None = None
    state_path: Path | None = None
    pending_path: Path | None = None
    native_document_sender: Callable[[str, Callable[[], bool]], object] | None = None


@dataclass(frozen=True)
class SendResultReceipt:
    """Normalized Hermes SendResult fields used by the plugin contract."""

    success: bool
    message_id: str
    error: str
    retryable: bool
    retry_after: float | None
    error_kind: str
    result: object | None = None


class SignedUrlExpiredError(Exception):
    """Raised when a download URL expired and must be re-fetched."""


class SendResultError(Exception):
    """Raised when Hermes returns an explicit unsuccessful SendResult."""

    def __init__(self, receipt: SendResultReceipt):
        super().__init__(receipt.error or receipt.error_kind or "platform send failed")
        self.receipt = receipt


class DocumentSendError(SendResultError):
    """Raised when Hermes did not explicitly confirm native document delivery."""


class TextSendError(SendResultError):
    """Raised when Hermes explicitly rejects a text delivery."""


class DeliveryAttemptRecordError(Exception):
    """Raised when the server rejects or misses a delivery-attempt write."""


class WatcherDeadlineError(Exception):
    """Raised when the SSE watcher reaches its wall-clock delivery deadline."""


class WatcherAuthenticationError(RuntimeError):
    """Raised when a watcher cannot recover authenticated server access."""


def build_request_status_message(
    *,
    request_id: str,
    server_url: str,
    events: Sequence[Mapping[str, object]],
    files: Sequence[Mapping[str, object]],
) -> str:
    """Build a path-free status summary for a completed or in-flight request."""
    events = [
        {**event, "data": {**data, "job_id": job_id}}
        if isinstance(data := event.get("data"), Mapping)
        and (job_id := _string_value(event, "job_id"))
        else event
        for event in events
    ]
    terminals = _event_payloads(events, "done")
    events = [
        {**event, "data": _bind_item_from_job(data, terminals)}
        if isinstance(data := event.get("data"), Mapping)
        else event
        for event in events
    ]
    ready = _event_payloads(events, "file_ready")
    enriched = []
    for file in files:
        matches = [
            record
            for record in ready
            if _string_value(record, "filename") == _string_value(file, "filename")
            and _string_value(record, "role") == _string_value(file, "role")
            and not (
                _string_value(record, "item_id")
                and _string_value(file, "item_id")
                and record["item_id"] != file["item_id"]
            )
        ]
        enriched.append({**(matches[0] if len(matches) == 1 else {}), **file})
    anchors = _request_item_anchors(enriched or ready, _event_payloads(events, "done"))
    if len(anchors) <= 1:
        return _single_request_status_message(
            request_id=request_id, server_url=server_url, events=events, files=files
        )
    return "\n\n".join(
        _single_request_status_message(
            request_id=request_id,
            server_url=server_url,
            events=[
                event
                for event in events
                if isinstance(data := event.get("data"), Mapping)
                and _same_request_item(anchor, data)
            ],
            files=[file for file in enriched if _same_request_item(anchor, file)],
        )
        for anchor in anchors
    )


def _single_request_status_message(
    *,
    request_id: str,
    server_url: str,
    events: Sequence[Mapping[str, object]],
    files: Sequence[Mapping[str, object]],
) -> str:
    file_ready_payloads = _event_payloads(events, "file_ready")
    error_payloads = _event_payloads(events, "error")
    done_payloads = _event_payloads(events, "done")
    main_payload = _first_role_payload(file_ready_payloads, "main")
    main_file = _first_role_payload(files, "main")
    si_file_count = _role_count(files, "si") or _role_count(file_ready_payloads, "si")
    partial_payload = _last_partial_payload(error_payloads)
    done_payload = done_payloads[-1] if done_payloads else {}
    if done_payload.get("ok") is True and any(_event_failed(p) for p in done_payloads[:-1]):
        partial_payload = None
    partial_payload = _with_terminal_cleanup(partial_payload, done_payload)
    cleanup_unconfirmed = _cleanup_unconfirmed(partial_payload)
    main_payload = _with_terminal_si_metadata(main_payload, done_payloads)
    web_url = _absolute_url(server_url, f"/requests/{request_id}")
    force_pause_cache_fallback = _is_force_pause_cache_fallback(
        partial_payload,
        main_payload,
    )

    if _event_failed(done_payload) and not files:
        return _failure_status_message(
            request_id=request_id,
            web_url=web_url,
            payload=done_payload,
            fallback_payload=error_payloads[-1] if error_payloads else None,
        )

    if partial_payload is not None:
        lines = [
            (
                "SciFetch 已生成可用文件，本次获取失败。"
                if cleanup_unconfirmed
                else "SciFetch 已生成缓存回退正文，但强制刷新未执行。"
                if force_pause_cache_fallback
                else "SciFetch 已生成可用全文，但附件未完整获取。"
            )
        ]
    elif _event_failed(done_payload):
        lines = ["SciFetch 已生成可用文件，但本次获取未完成。"]
    elif done_payload:
        lines = ["SciFetch 请求已完成。"]
    elif _event_payloads(events, "attention_required"):
        lines = ["SciFetch 正在等待 Science 页面验证；请查看可见的服务 Chrome。"]
    else:
        lines = ["SciFetch 请求正在处理中。"]

    metadata_payload = main_payload or done_payload
    if metadata_payload:
        lines.append("📚 文献信息：")
        lines.extend(_paper_metadata_lines(metadata_payload))

    main_filename = _filename_from_payloads(main_payload, main_file)
    if main_filename:
        lines.append(f"📄 全文：已可用 {main_filename}")
    elif not done_payload:
        lines.append("📄 全文：仍在获取中")

    if partial_payload is not None:
        if cleanup_unconfirmed:
            lines.append(_cleanup_si_status_line(partial_payload, si_file_count, sent=False))
        else:
            lines.append(_partial_si_status_line(partial_payload, observed_count=si_file_count))
        reason = _partial_delivery_reason(partial_payload, {**main_payload, "files": list(files)})
        if reason:
            lines.append(f"⚠️ 原因：{reason}")
    elif si_file_count > 0:
        lines.append(f"📎 SI：已生成 {si_file_count} 个附件")
    else:
        si_line = _si_status_line(main_payload)
        if si_line:
            lines.append(si_line)

    if partial_payload is None and _event_failed(done_payload):
        reason = _failure_reason(
            {**done_payload, "files": list(files)},
            error_payloads[-1] if error_payloads else None,
        )
        if reason:
            lines.append(f"⚠️ 原因：{reason}")

    source_line = (
        _partial_delivery_source_line(main_payload, partial_payload)
        if partial_payload is not None
        else _delivery_source_line(main_payload)
    )
    if source_line:
        lines.append(source_line)
    mode_line = _fresh_result_line(
        {
            **main_payload,
            **(partial_payload or {}),
            "_fresh_result_context": done_payload or partial_payload or {},
        }
    )
    if mode_line:
        lines.append(mode_line)
    lines.append(_request_detail_line(request_id=request_id, web_url=web_url))
    return "\n".join(lines)


class DeliveryQueue:
    """Per-chat queue to serialize outbound Hermes sends."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._last_send_at: dict[str, float] = {}

    async def send_document(
        self,
        key: str,
        path: Path,
        send_document: Callable[[str], object],
    ) -> object:
        return await self._send_serialized(
            key,
            lambda: _send_document_with_retry(path, send_document),
        )

    async def send_text(
        self,
        key: str,
        message: str,
        send_text: Callable[[str], object],
    ) -> object:
        return await self._send_serialized(key, lambda: _send_text_with_retry(send_text, message))

    async def send_terminal_text(
        self,
        key: str,
        message: str,
        send_text: Callable[[str], object],
    ) -> object:
        return await self._send_serialized(
            key,
            lambda: _send_terminal_text_with_retry(send_text, message),
        )

    async def _send_serialized(self, key: str, sender: Callable[[], object]) -> object:
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            await self._wait_for_slot(key)
            try:
                return await _maybe_await(sender())
            finally:
                self._last_send_at[key] = time.monotonic()

    async def _wait_for_slot(self, key: str) -> None:
        if _OUTBOUND_SEND_MIN_INTERVAL_SECONDS <= 0:
            return
        last_sent_at = self._last_send_at.get(key)
        if last_sent_at is None:
            return
        elapsed = time.monotonic() - last_sent_at
        delay = _OUTBOUND_SEND_MIN_INTERVAL_SECONDS - elapsed
        if delay > 0:
            await asyncio.sleep(delay)


_OUTBOUND_QUEUE = DeliveryQueue()
_ACTIVE_WATCHERS: set[str] = set()
_ACTIVE_WATCHERS_LOCK = threading.Lock()


def schedule_request_watcher(
    *,
    request_id: str,
    state: PluginState,
    kwargs: Mapping[str, object],
) -> bool:
    """Start a background watcher for native or explicit link delivery."""
    if not request_id:
        return False

    state_path = _state_path_from_kwargs(kwargs)
    target = _target_from_kwargs(kwargs)
    pending_path = _pending_path_from_state_path(state_path)

    sender = _document_sender_from_kwargs(kwargs, target)
    text_sender = _text_sender_from_kwargs(kwargs, target)
    if sender is None and text_sender is None:
        return False

    transport = kwargs.get("transport")
    runtime = DeliveryRuntime(
        state=state,
        request_id=request_id,
        target=target,
        send_document=sender,
        send_text=text_sender,
        transport=transport if isinstance(transport, httpx.AsyncBaseTransport) else None,
        state_path=state_path,
        pending_path=pending_path,
        native_document_sender=_native_sender_from_kwargs(kwargs),
    )
    from .durable_delivery import persist_request_intent

    if not persist_request_intent(runtime):
        return False
    return _schedule_runtime_watcher(runtime, kwargs)


def schedule_text_send(
    *,
    kwargs: Mapping[str, object],
    message: str,
) -> bool:
    """Schedule a user-facing text send through the shared per-chat outbox."""
    target = _target_from_kwargs(kwargs)
    sender = _text_sender_from_kwargs(kwargs, target)
    if sender is None:
        return False

    from .durable_delivery import _valid_runtime, controlled_text

    state_path = _state_path_from_kwargs(kwargs)
    state = load_state(state_path) if state_path is not None else None
    runtime = (
        DeliveryRuntime(
            state=state,
            request_id="current_notice",
            target=target,
            send_document=None,
            send_text=sender,
            state_path=state_path,
        )
        if state is not None
        else None
    )
    coroutine = (
        controlled_text(runtime, message)
        if runtime is not None and _valid_runtime(runtime)
        else _OUTBOUND_QUEUE.send_text(_queue_key_for_target(target), message, sender)
    )
    scheduler = _background_scheduler(kwargs)
    try:
        if scheduler is not None:
            result = scheduler(coroutine)
            if inspect.iscoroutine(result):
                _schedule_coroutine(result)
            return True
        return _schedule_coroutine(coroutine)
    except Exception:
        coroutine.close()
        logger.warning("failed to schedule scifetch-paper text send", exc_info=True)
        return False


def resume_pending_deliveries(**kwargs: object) -> int:
    """Recover only exact-target durable work with fresh authenticated context."""
    from .durable_delivery import resume_durable_deliveries

    return resume_durable_deliveries(kwargs)


def _schedule_runtime_watcher(
    runtime: DeliveryRuntime,
    kwargs: Mapping[str, object],
) -> bool:
    key = _watcher_key(runtime)
    with _ACTIVE_WATCHERS_LOCK:
        if key in _ACTIVE_WATCHERS:
            return False
        _ACTIVE_WATCHERS.add(key)

    coroutine = _watch_request_guarded(
        runtime,
        key,
        ack_gate=tool_ack_gate_from_kwargs(kwargs),
    )
    scheduler = _background_scheduler(kwargs)
    try:
        if scheduler is not None:
            result = scheduler(coroutine)
            if inspect.iscoroutine(result):
                _schedule_coroutine(result)
            return True
        return _schedule_coroutine(coroutine)
    except Exception:
        with _ACTIVE_WATCHERS_LOCK:
            _ACTIVE_WATCHERS.discard(key)
        coroutine.close()
        logger.warning("failed to schedule scifetch-paper watcher", exc_info=True)
        return False


async def _watch_request_guarded(
    runtime: DeliveryRuntime,
    key: str,
    *,
    ack_gate: ToolAckDeliveryGate | None = None,
) -> None:
    try:
        if ack_gate is not None and not await ack_gate.wait():
            logger.warning(
                "starting scifetch-paper watcher after bounded tool ACK gate fallback "
                "request_id=%s",
                runtime.request_id,
            )
        await watch_request(runtime)
    finally:
        with _ACTIVE_WATCHERS_LOCK:
            _ACTIVE_WATCHERS.discard(key)


async def watch_request(
    runtime: DeliveryRuntime,
    *,
    max_reconnects: int | None = None,
    retry_delay_seconds: float = 1.0,
    terminal_timeout_seconds: float = 1800.0,
) -> None:
    """Ingest immutable readiness; the installation outbox owns every native send."""
    from .durable_delivery import watch_durable_request

    await watch_durable_request(
        runtime,
        max_reconnects=max_reconnects,
        retry_delay_seconds=retry_delay_seconds,
        terminal_timeout_seconds=terminal_timeout_seconds,
    )


async def _consume_event_stream(
    runtime: DeliveryRuntime,
    delivered_file_ids: set[str],
    dead_letter_file_ids: set[str],
    deferred_file_ids: set[str],
    *,
    delivered_file_records: list[dict[str, object]] | None = None,
    pending_store: PendingDeliveryStore | None,
    event_cursor: int,
    deadline: float,
) -> tuple[bool, int, bool]:
    if delivered_file_records is None:
        delivered_file_records = []
    pending_ready_files: list[dict[str, object]] = []
    handled_file_ids: set[str] = set(delivered_file_ids) | set(dead_letter_file_ids)
    last_error_payload: dict[str, object] | None = None
    partial_error_payload: dict[str, object] | None = None
    terminal_seen = False
    failed_payload: dict[str, object] | None = None
    successful_done_payloads: list[dict[str, object]] = []
    seen_job_ids: set[str] = set()
    terminal_contexts: list[dict[str, object]] = []
    error_contexts: list[dict[str, object]] = []
    file_jobs: dict[str, set[str]] = {}
    attention_seen = False
    async with (
        _async_client(runtime) as client,
        client.stream(
            "GET",
            f"/v1/paper-requests/{runtime.request_id}/events",
            headers=_auth_headers(runtime),
        ) as response,
    ):
        if response.status_code >= 400:
            body = await response.aread()
            message = _api_error_message_from_body(response, body)
            if response.status_code in {401, 403}:
                raise WatcherAuthenticationError(message)
            raise RuntimeError(message)
        async for event_name, data in _iter_sse(response.aiter_lines(), deadline=deadline):
            event_type, payload, event_job_id = _parse_event_payload(event_name, data)
            if event_job_id:
                seen_job_ids.add(event_job_id)
            event_cursor += 1
            if pending_store is not None:
                pending_store.mark_event_seen(runtime.request_id, event_cursor)
            context = {**payload, **({"job_id": event_job_id} if event_job_id else {})}
            if event_type == "done":
                terminal_contexts.append(context)
            elif event_type == "error":
                error_contexts.append(context)
            if event_type == "file_ready":
                file_id = _string_value(payload, "file_id")
                if file_id and event_job_id:
                    file_jobs.setdefault(file_id, set()).add(event_job_id)
                pending_ready_files.append(payload)
            elif event_type == "attention_required" and not attention_seen:
                attention_seen = True
                await _send_text(
                    runtime,
                    "Science 页面仍在验证，请查看可见的服务 Chrome；如出现勾选框请手动完成。本次请求仍在等待，验证页不会因提示而重新打开。",
                )
            elif event_type == "error":
                last_error_payload = payload
                if _is_partial_delivery_error(payload):
                    partial_error_payload = payload
            if pending_ready_files and not deferred_file_ids:
                await _deliver_pending_ready_files(
                    runtime,
                    pending_ready_files,
                    delivered_file_ids,
                    dead_letter_file_ids,
                    deferred_file_ids,
                    delivered_file_records,
                    handled_file_ids,
                    pending_store=pending_store,
                )
                if deferred_file_ids:
                    logger.info("request watcher deferred until a fresh matching delivery context")
                    return terminal_seen, event_cursor, True
            if event_type == "done":
                terminal_seen = True
                if _event_failed(payload):
                    failed_payload = payload
                elif payload.get("ok") is True:
                    successful_done_payloads.append(payload)
    should_sweep_files = (
        terminal_seen
        and failed_payload is None
        and not deferred_file_ids
        and (bool(pending_ready_files) or len(seen_job_ids) > 1)
    )
    if should_sweep_files:
        await _deliver_remaining_request_files(
            runtime,
            pending_ready_files,
            delivered_file_ids,
            dead_letter_file_ids,
            deferred_file_ids,
            delivered_file_records,
            handled_file_ids,
            pending_store=pending_store,
        )
    if deferred_file_ids:
        logger.info("request watcher deferred until a fresh matching delivery context")
        return terminal_seen, event_cursor, True
    if pending_ready_files:
        logger.info("request completed with unresolved file_ready payloads")
        return False, event_cursor, False
    terminal_notice_sent = True
    rendered_records = []
    for record in delivered_file_records:
        jobs = file_jobs.get(_string_value(record, "file_id"), set())
        rendered = dict(record)
        item_id = _string_value(record, "item_id")
        if item_id and any(_string_value(t, "item_id") == item_id for t in terminal_contexts):
            # A shared item identity outranks optional job-envelope observations.
            pass
        elif len(jobs) == 1:
            rendered["job_id"] = next(iter(jobs))
        else:
            # A reused canonical ID or an old event without exact identity
            # cannot borrow one occurrence's force result from its DOI.
            rendered["_fresh_context_unbound"] = True
        rendered_records.append(rendered)
    if terminal_seen and (
        len({t.get("job_id") for t in terminal_contexts if t.get("job_id")}) > 1
        or len(_request_item_anchors(rendered_records, terminal_contexts)) > 1
    ):
        terminal_notice_sent = await _send_item_delivery_outcomes(
            runtime, rendered_records, terminals=terminal_contexts, errors=error_contexts
        )
    elif terminal_seen and failed_payload is not None and not delivered_file_ids:
        terminal_notice_sent = await _send_failure_notification(
            runtime,
            failed_payload,
            fallback_payload=last_error_payload,
        )
    elif terminal_seen and failed_payload is not None and delivered_file_ids:
        terminal_notice_sent = await _send_partial_delivery_notification(
            runtime,
            {
                **(
                    _with_terminal_cleanup(partial_error_payload, failed_payload)
                    or _partial_payload_from_terminal_failure(failed_payload)
                ),
                "_fresh_result_context": failed_payload,
            },
            delivered_file_records=delivered_file_records,
        )
    elif terminal_seen and partial_error_payload is not None and delivered_file_ids:
        terminal_notice_sent = await _send_partial_delivery_notification(
            runtime,
            _with_terminal_si_metadata(partial_error_payload, successful_done_payloads),
            delivered_file_records=delivered_file_records,
        )
    elif terminal_seen and delivered_file_records:
        terminal_notice_sent = await _send_delivery_summary(
            runtime, delivered_file_records, terminal_payloads=successful_done_payloads
        )
    if terminal_seen and not terminal_notice_sent:
        logger.warning("terminal notification was not confirmed; recovery state retained")
    return terminal_seen, event_cursor, False


async def _deliver_pending_ready_files(
    runtime: DeliveryRuntime,
    pending_ready_files: list[dict[str, object]],
    delivered_file_ids: set[str],
    dead_letter_file_ids: set[str],
    deferred_file_ids: set[str],
    delivered_file_records: list[dict[str, object]],
    handled_file_ids: set[str],
    *,
    pending_store: PendingDeliveryStore | None,
) -> None:
    still_pending: list[dict[str, object]] = []
    for event_payload in pending_ready_files:
        handled = await _deliver_ready_file(
            runtime,
            event_payload,
            delivered_file_ids,
            dead_letter_file_ids,
            deferred_file_ids,
            delivered_file_records,
            handled_file_ids,
            pending_store=pending_store,
        )
        if not handled:
            still_pending.append(event_payload)
    pending_ready_files[:] = still_pending


async def _deliver_ready_file(
    runtime: DeliveryRuntime,
    event_payload: dict[str, object],
    delivered_file_ids: set[str],
    dead_letter_file_ids: set[str],
    deferred_file_ids: set[str],
    delivered_file_records: list[dict[str, object]],
    handled_file_ids: set[str],
    *,
    pending_store: PendingDeliveryStore | None,
) -> bool:
    files = await _list_request_files(
        runtime,
        item_id=_string_value(event_payload, "item_id") or None,
        file_id=_string_value(event_payload, "file_id") or None,
    )
    if _matching_handled_file_exists(files, event_payload, handled_file_ids):
        return True

    file_record = _select_ready_file_for_event(files, event_payload, handled_file_ids)
    if file_record is None:
        logger.info("file_ready had no matching signed file yet")
        return False

    file_id = _string_value(file_record, "file_id")
    if file_id in handled_file_ids:
        return True

    file_record = _with_public_file_metadata(file_record, event_payload)
    outcome = await _download_and_send(runtime, file_record, attempt=1)
    if outcome in {"sent", "delivered_via_link"}:
        delivered_file_ids.add(file_id)
        handled_file_ids.add(file_id)
        if outcome == "sent":
            delivered_file_records.append(file_record)
        if pending_store is not None:
            pending_store.mark_delivered(runtime.request_id, file_id)
        return True
    if outcome in {"failed", "unsupported"}:
        dead_letter_file_ids.add(file_id)
        handled_file_ids.add(file_id)
        if pending_store is not None:
            pending_store.mark_dead_letter(runtime.request_id, file_id)
        return True
    if outcome == "deferred":
        deferred_file_ids.add(file_id)
        if pending_store is not None:
            pending_store.mark_deferred(runtime.request_id)
        return True
    return False


async def _deliver_remaining_request_files(
    runtime: DeliveryRuntime,
    pending_ready_files: list[dict[str, object]],
    delivered_file_ids: set[str],
    dead_letter_file_ids: set[str],
    deferred_file_ids: set[str],
    delivered_file_records: list[dict[str, object]],
    handled_file_ids: set[str],
    *,
    pending_store: PendingDeliveryStore | None,
) -> None:
    files = await _list_request_files(runtime)
    ready_payloads = [dict(payload) for payload in pending_ready_files]
    for file_record in files:
        if deferred_file_ids:
            break
        file_id = _string_value(file_record, "file_id")
        if not file_id or file_id in handled_file_ids:
            continue
        metadata = _matching_ready_payload(file_record, ready_payloads)
        enriched = _with_public_file_metadata(file_record, metadata or {})
        outcome = await _download_and_send(runtime, enriched, attempt=1)
        if outcome in {"sent", "delivered_via_link"}:
            delivered_file_ids.add(file_id)
            handled_file_ids.add(file_id)
            if outcome == "sent":
                delivered_file_records.append(enriched)
            if pending_store is not None:
                pending_store.mark_delivered(runtime.request_id, file_id)
        elif outcome in {"failed", "unsupported"}:
            dead_letter_file_ids.add(file_id)
            handled_file_ids.add(file_id)
            if pending_store is not None:
                pending_store.mark_dead_letter(runtime.request_id, file_id)
        elif outcome == "deferred":
            deferred_file_ids.add(file_id)
            if pending_store is not None:
                pending_store.mark_deferred(runtime.request_id)

    pending_ready_files[:] = [
        payload
        for payload in pending_ready_files
        if not _matching_handled_file_exists(files, payload, handled_file_ids)
    ]


def _matching_ready_payload(
    file_record: Mapping[str, object],
    payloads: Sequence[Mapping[str, object]],
) -> Mapping[str, object] | None:
    filename = Path(_string_value(file_record, "filename")).name
    role = _string_value(file_record, "role")
    for payload in payloads:
        payload_filename = Path(_string_value(payload, "filename")).name
        if filename and payload_filename == filename:
            return payload
    for payload in payloads:
        if role and _string_value(payload, "role") == role:
            return payload
    return None


def _select_ready_file_for_event(
    files: list[dict[str, object]],
    event_payload: dict[str, object],
    handled_file_ids: set[str],
) -> dict[str, object] | None:
    event_file_id = _string_value(event_payload, "file_id")
    if event_file_id:
        for file_record in files:
            if (
                _string_value(file_record, "file_id") == event_file_id
                and event_file_id not in handled_file_ids
            ):
                return file_record
    filename = Path(_string_value(event_payload, "filename")).name
    role = _string_value(event_payload, "role")
    return _select_ready_file(files, filename, role, handled_file_ids)


def _matching_handled_file_exists(
    files: list[dict[str, object]],
    event_payload: dict[str, object],
    handled_file_ids: set[str],
) -> bool:
    event_item_id = _string_value(event_payload, "item_id")
    if event_item_id:
        item_files = [
            file_record
            for file_record in files
            if _string_value(file_record, "item_id") == event_item_id
        ]
        if item_files or any(_string_value(file_record, "item_id") for file_record in files):
            files = item_files
    event_file_id = _string_value(event_payload, "file_id")
    if event_file_id:
        if event_file_id in handled_file_ids:
            return True
        if any(_string_value(file_record, "file_id") == event_file_id for file_record in files):
            return False
    filename = Path(_string_value(event_payload, "filename")).name
    role = _string_value(event_payload, "role")
    role_matches: list[dict[str, object]] = []
    for file_record in files:
        file_id = _string_value(file_record, "file_id")
        if filename and Path(_string_value(file_record, "filename")).name == filename:
            return file_id in handled_file_ids
        if role and _string_value(file_record, "role") == role:
            role_matches.append(file_record)
    if role_matches:
        return all(
            _string_value(file_record, "file_id") in handled_file_ids
            for file_record in role_matches
        )
    return False


def _select_ready_file(
    files: list[dict[str, object]],
    filename: str,
    role: str,
    handled_file_ids: set[str],
) -> dict[str, object] | None:
    candidates = [
        file_record
        for file_record in files
        if _string_value(file_record, "file_id") not in handled_file_ids
    ]
    if filename:
        exact = [
            file_record
            for file_record in candidates
            if Path(_string_value(file_record, "filename")).name == filename
        ]
        if exact:
            return exact[0]
    if role:
        role_matches = [
            file_record for file_record in candidates if _string_value(file_record, "role") == role
        ]
        if role_matches:
            return role_matches[0]
    return candidates[0] if candidates else None


async def _download_and_send(
    runtime: DeliveryRuntime,
    file_record: dict[str, object],
    *,
    attempt: int,
) -> DeliveryOutcome:
    try:
        return await _download_and_send_once(runtime, file_record, attempt=attempt)
    except SignedUrlExpiredError:
        refreshed = await _refetch_file(runtime, _string_value(file_record, "file_id"))
        if refreshed is None:
            await _record_delivery_attempt(
                runtime,
                file_record,
                status="failed",
                attempt=attempt,
                error_code="signed_url_expired",
                error_message="Signed URL expired and refresh did not return the file.",
            )
            return "failed"
        refreshed = _with_public_file_metadata(refreshed, file_record)
        return await _download_and_send_once(runtime, refreshed, attempt=attempt + 1)


def _with_public_file_metadata(
    file_record: dict[str, object],
    metadata: Mapping[str, object],
) -> dict[str, object]:
    enriched = dict(file_record)
    for key in _PUBLIC_FILE_METADATA_KEYS:
        if _has_public_metadata(enriched.get(key)):
            continue
        value = metadata.get(key)
        if _has_public_metadata(value):
            enriched[key] = value
    return enriched


def _has_public_metadata(value: object) -> bool:
    if isinstance(value, str):
        return bool(value)
    if isinstance(value, bool):
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return True
    if isinstance(value, list):
        return any(isinstance(item, str) and bool(item) for item in value)
    return False


async def _download_and_send_once(
    runtime: DeliveryRuntime,
    file_record: dict[str, object],
    *,
    attempt: int,
) -> DeliveryOutcome:
    filename = Path(_string_value(file_record, "filename") or "scifetch-file").name
    preflight = _preflight_delivery(runtime, file_record)
    if preflight is not None:
        status, error_code, message = preflight
        return await _deliver_file_link(
            runtime,
            file_record,
            filename=filename,
            attempt=attempt,
            status=status,
            error_code=error_code,
            error_message=message,
        )
    zip_validation_failed = False
    result: object | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="scifetch-paper-") as temp_dir:
            destination = Path(temp_dir) / filename
            await _download_file(runtime, file_record, destination)
            if _requires_weixin_zip_validation(
                runtime, file_record
            ) and not await asyncio.to_thread(
                zipfile.is_zipfile,
                destination,
            ):
                zip_validation_failed = True
            else:
                sender = runtime.send_document
                if sender is None:
                    raise RuntimeError("native document sender is unavailable after preflight")
                result = await _OUTBOUND_QUEUE.send_document(
                    _queue_key(runtime),
                    destination,
                    sender,
                )
    except SignedUrlExpiredError:
        raise
    except Exception as exc:
        receipt = _receipt_from_exception(exc)
        try:
            await _record_delivery_attempt(
                runtime,
                file_record,
                status="failed",
                attempt=attempt,
                error_code=_send_error_code(exc),
                error_message=_send_error_message(exc),
                delivery_result=receipt,
            )
        except DeliveryAttemptRecordError:
            if not _is_qqbot_deferred_send_error(exc):
                raise
            logger.warning("failed to audit deferred QQ send; retaining local pending state")
        if _is_qqbot_deferred_send_error(exc):
            logger.info("QQ passive delivery context is no longer usable; deferring file")
            return "deferred"
        if _is_weixin_cdn_tls_handshake_send_error(exc):
            logger.warning(
                "scifetch-paper Weixin CDN TLS send retries exhausted; keeping file deferred"
            )
            await _send_delivery_deferred(runtime, filename=filename)
            return "deferred"
        if _should_keep_pending_for_send_error(exc):
            logger.warning(
                "scifetch-paper document send exhausted retryable failures; keeping file pending"
            )
            return "retry"
        await _send_delivery_failure(runtime, filename=filename, reason=_send_error_message(exc))
        return "failed"

    if zip_validation_failed:
        return await _deliver_file_link(
            runtime,
            file_record,
            filename=filename,
            attempt=attempt,
            status="delivered_via_link",
            error_code="unsupported_media_type",
            error_message="The downloaded file is not a valid ZIP archive.",
        )

    await _record_delivery_attempt(
        runtime,
        file_record,
        status="sent",
        attempt=attempt,
        delivery_result=result,
    )
    return "sent"


def _preflight_delivery(
    runtime: DeliveryRuntime,
    file_record: Mapping[str, object],
) -> tuple[str, str, str] | None:
    platform = _canonical_delivery_platform(runtime.target.platform)
    chat_type = runtime.target.chat_type.strip().lower()
    if platform == "wecom_callback" or (
        platform == "qqbot" and chat_type in {"channel", "dm", "guild", "guild_dm"}
    ):
        return (
            "delivered_via_link",
            "unsupported",
            "This Hermes channel does not support native document upload.",
        )

    version = _normalized_hermes_version(runtime.target.hermes_version)
    rule = _STOCK_DELIVERY_MATRIX.get(platform)
    if version not in TESTED_HERMES_VERSIONS or rule is None:
        return (
            "delivered_via_link",
            "capability_unknown",
            "This Hermes platform/version has no tested native document capability.",
        )
    if not rule.document_upload:
        return (
            "delivered_via_link",
            "unsupported",
            "This Hermes platform does not support native document upload.",
        )
    if chat_type not in rule.chat_types:
        return (
            "delivered_via_link",
            "capability_unknown",
            "This Hermes chat type has no tested native document capability.",
        )

    capabilities = runtime.target.capabilities
    document_upload = capabilities.document_upload if capabilities is not None else None
    if document_upload is False:
        return (
            "delivered_via_link",
            "unsupported",
            "This Hermes platform does not support native document upload.",
        )
    if document_upload is not True:
        return (
            "delivered_via_link",
            "capability_unknown",
            "Hermes did not explicitly confirm native document upload capability.",
        )
    if capabilities is not None and capabilities.supports_local_path is False:
        return (
            "delivered_via_link",
            "unsupported",
            "This Hermes platform cannot upload SciFetch local document paths.",
        )
    if capabilities is None or capabilities.supports_local_path is not True:
        return (
            "delivered_via_link",
            "capability_unknown",
            "Hermes did not explicitly confirm local document path upload capability.",
        )
    if runtime.send_document is None:
        return (
            "delivered_via_link",
            "unsupported",
            "Hermes did not provide a native document sender for this channel.",
        )
    if not _document_type_supported(file_record, platform=platform):
        return (
            "delivered_via_link",
            "unsupported_media_type",
            "This file type is not approved for native document upload.",
        )

    max_document_bytes = rule.max_document_bytes
    explicit_max = capabilities.max_document_bytes if capabilities is not None else None
    if explicit_max is not None:
        max_document_bytes = (
            min(max_document_bytes, explicit_max)
            if max_document_bytes is not None
            else explicit_max
        )
    if max_document_bytes is None:
        return (
            "delivered_via_link",
            "capability_unknown",
            "Hermes did not provide a tested document size limit for this channel.",
        )

    size_bytes = _file_size_bytes(file_record)
    if size_bytes is None:
        return (
            "delivered_via_link",
            "payload_size_unknown",
            "SciFetch could not verify the file size before native document upload.",
        )
    if max_document_bytes is not None and size_bytes > max_document_bytes:
        size_mb = size_bytes / (1024 * 1024)
        limit_mb = max_document_bytes / (1024 * 1024)
        return (
            "delivered_via_link",
            "payload_too_large",
            f"File size {size_mb:.2f} MiB exceeds the configured native upload "
            f"limit {limit_mb:.2f} MiB.",
        )

    return None


def _canonical_delivery_platform(platform: str) -> str:
    normalized = _platform_key(platform)
    return _PLATFORM_ALIASES.get(normalized, normalized)


def _normalized_hermes_version(version: str) -> str:
    normalized = version.strip().lower()
    if normalized.startswith("v"):
        normalized = normalized[1:]
    return normalized


def _document_type_supported(
    file_record: Mapping[str, object],
    *,
    platform: str,
) -> bool:
    filename = _string_value(file_record, "filename")
    suffix = Path(filename).suffix.lower()
    content_type = _string_value(file_record, "content_type").split(";", 1)[0].lower()
    if platform == "weixin" and suffix in _WEIXIN_NATIVE_VIDEO_SUFFIXES:
        return content_type in _WEIXIN_NATIVE_VIDEO_CONTENT_TYPES
    if (
        platform == "weixin"
        and suffix == _WEIXIN_NATIVE_ZIP_SUFFIX
        and content_type == _WEIXIN_HISTORICAL_ZIP_CONTENT_TYPE
    ):
        return True
    if suffix and suffix not in _SUPPORTED_DOCUMENT_SUFFIXES:
        return False
    if content_type and content_type not in _SUPPORTED_DOCUMENT_CONTENT_TYPES:
        return False
    return bool(suffix or content_type)


def _requires_weixin_zip_validation(
    runtime: DeliveryRuntime,
    file_record: Mapping[str, object],
) -> bool:
    filename = _string_value(file_record, "filename")
    return (
        _canonical_delivery_platform(runtime.target.platform) == "weixin"
        and Path(filename).suffix.lower() == _WEIXIN_NATIVE_ZIP_SUFFIX
    )


async def _deliver_file_link(
    runtime: DeliveryRuntime,
    file_record: dict[str, object],
    *,
    filename: str,
    attempt: int,
    status: str,
    error_code: str,
    error_message: str,
) -> DeliveryOutcome:
    download_url = _string_value(file_record, "download_url")
    if not download_url:
        await _record_delivery_attempt(
            runtime,
            file_record,
            status="unsupported",
            attempt=attempt,
            error_code=error_code,
            error_message=f"{error_message} SciFetch did not return a signed download URL.",
        )
        await _send_delivery_failure(runtime, filename=filename, reason=error_message)
        return "unsupported"
    if runtime.send_text is None:
        await _record_delivery_attempt(
            runtime,
            file_record,
            status="unsupported",
            attempt=attempt,
            error_code=error_code,
            error_message=f"{error_message} Hermes did not provide a text sender for the link.",
        )
        return "unsupported"

    link_url = _absolute_url(runtime.state.server_url, download_url)
    text_sent = await _send_text(
        runtime,
        _file_link_message(
            runtime,
            file_record,
            filename=filename,
            link_url=link_url,
            reason=error_message,
        ),
    )
    if not text_sent:
        await _record_delivery_attempt(
            runtime,
            file_record,
            status="unsupported",
            attempt=attempt,
            error_code="link_delivery_failed",
            error_message=f"{error_message} Hermes failed to send the temporary link.",
        )
        return "unsupported"
    await _record_delivery_attempt(
        runtime,
        file_record,
        status=status,
        attempt=attempt,
        error_code=error_code,
        error_message=error_message,
    )
    return "delivered_via_link"


def _file_link_message(
    runtime: DeliveryRuntime,
    file_record: Mapping[str, object],
    *,
    filename: str,
    link_url: str,
    reason: str,
) -> str:
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    lines = ["SciFetch 已生成文件，但当前平台无法直接发送附件。"]
    title = _public_field(file_record, "title", default="")
    if title:
        lines.extend(["📚 文献信息：", f"📝 题目：{title}"])
    lines.append(f"📄 文件：{filename}")
    lines.append(f"🔗 临时链接：{link_url}")
    lines.append("⚠️ 原因：当前通道未能直接发送该文件，可使用上方临时链接。")
    lines.append(_request_detail_line(request_id=runtime.request_id, web_url=web_url))
    return "\n".join(lines)


def _file_size_bytes(file_record: Mapping[str, object]) -> int | None:
    for key in ("size_bytes", "size", "bytes"):
        value = file_record.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int) and value >= 0:
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


async def _list_request_files(
    runtime: DeliveryRuntime,
    *,
    item_id: str | None = None,
    file_id: str | None = None,
) -> list[dict[str, object]]:
    params: dict[str, str] = {}
    if item_id:
        params["item_id"] = item_id
    if file_id:
        params["file_id"] = file_id
    async with _async_client(runtime) as client:
        response = await client.get(
            f"/v1/paper-requests/{runtime.request_id}/files",
            headers=_auth_headers(runtime),
            params=params or None,
        )
    if response.status_code >= 400:
        raise RuntimeError(_api_error_message(response))
    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError("SciFetch server returned invalid file list.")
    return [item for item in data if isinstance(item, dict)]


async def _refetch_file(
    runtime: DeliveryRuntime,
    file_id: str,
) -> dict[str, object] | None:
    files = await _list_request_files(runtime)
    for file_record in files:
        if _string_value(file_record, "file_id") == file_id:
            return file_record
    return None


async def _download_file(
    runtime: DeliveryRuntime,
    file_record: dict[str, object],
    destination: Path,
) -> None:
    download_url = _string_value(file_record, "download_url")
    if not download_url:
        raise RuntimeError("SciFetch did not return a signed download URL.")
    try:
        absolute_url = _signed_download_url(runtime.state.server_url, download_url)
    except ValueError as exc:
        raise RuntimeError(f"SciFetch signed download URL was rejected: {exc}") from exc
    tmp = destination.with_name("." + destination.name + ".tmp")
    expected_size = _file_size_bytes(file_record)
    expected_sha = _string_value(file_record, "sha256")
    actual_size = 0
    digest = hashlib.sha256()
    try:
        async with (
            _async_client(runtime) as client,
            client.stream("GET", absolute_url) as response,
        ):
            if 300 <= response.status_code < 400:
                location = response.headers.get("location", "")
                redirect_url = urljoin(absolute_url, location)
                try:
                    _signed_download_url(runtime.state.server_url, redirect_url)
                except ValueError as exc:
                    raise RuntimeError(
                        f"SciFetch signed download redirect was rejected: {exc}"
                    ) from exc
                raise RuntimeError("SciFetch signed download returned an unexpected redirect.")
            if response.status_code >= 400:
                body = await response.aread()
                if (
                    response.status_code == 409
                    and _response_reason_from_body(body) == "signed_url_expired"
                ):
                    raise SignedUrlExpiredError
                raise RuntimeError(_api_error_message_from_body(response, body))
            with tmp.open("wb") as writer:
                async for chunk in response.aiter_bytes():
                    if not chunk:
                        continue
                    actual_size += len(chunk)
                    digest.update(chunk)
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
        if expected_size is not None and actual_size != expected_size:
            raise RuntimeError("SciFetch signed download size verification failed.")
        actual_sha = digest.hexdigest()
        if expected_sha and actual_sha.lower() != expected_sha.lower():
            raise RuntimeError("SciFetch signed download hash verification failed.")
        await asyncio.to_thread(tmp.replace, destination)
        await asyncio.to_thread(_fsync_directory, destination.parent)
    except Exception:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


async def _record_delivery_attempt(
    runtime: DeliveryRuntime,
    file_record: dict[str, object],
    *,
    status: str,
    attempt: int,
    error_code: str | None = None,
    error_message: str | None = None,
    delivery_result: object | None = None,
) -> None:
    file_id = _string_value(file_record, "file_id")
    item_id = _string_value(file_record, "item_id")
    metadata: dict[str, object] = {
        "filename": _string_value(file_record, "filename"),
        "role": _string_value(file_record, "role"),
        "chat_id": runtime.target.chat_id,
    }
    if item_id:
        metadata["item_id"] = item_id
    metadata.update(_delivery_result_metadata(delivery_result))
    payload: dict[str, object] = {
        "notification_id": f"ntf_{runtime.request_id}_{file_id}" if file_id else None,
        "request_id": runtime.request_id,
        "item_id": item_id or None,
        "file_id": file_id or None,
        "channel": f"hermes_{_platform_key(runtime.target.platform) or 'unknown'}",
        "status": status,
        "attempt": attempt,
        "error_code": error_code,
        "error_message": error_message,
        "metadata": metadata,
    }
    try:
        async with _async_client(runtime) as client:
            response = await client.post(
                "/v1/delivery-attempts",
                headers=_auth_headers(runtime),
                json=payload,
            )
        if response.status_code >= 400:
            raise RuntimeError(_api_error_message(response))
    except Exception as exc:
        logger.warning("failed to record SciFetch delivery attempt", exc_info=True)
        raise DeliveryAttemptRecordError("failed to record SciFetch delivery attempt") from exc


async def _ensure_fresh_runtime(runtime: DeliveryRuntime) -> DeliveryRuntime:
    state_path = runtime.state_path
    if state_path is None:
        return runtime

    runtime = _runtime_with_latest_state(runtime)
    if not _needs_refresh(runtime.state.access_expires_at):
        return runtime

    transport = runtime.transport
    if transport is not None and not isinstance(transport, httpx.BaseTransport):
        raise RuntimeError("SciFetch watcher refresh transport is not sync-capable.")
    client = SciFetchPluginClient(
        SciFetchClientConfig(server_url=runtime.state.server_url),
        transport=transport,
    )
    try:
        # Commands and watchers must consume a rotating token through one profile lock.
        refreshed = await asyncio.to_thread(
            ensure_fresh_state,
            client=client,
            state=runtime.state,
            state_path=state_path,
        )
    except SciFetchPluginError as exc:
        if exc.status_code in {400, 401, 403}:
            raise WatcherAuthenticationError(str(exc)) from exc
        raise RuntimeError(str(exc)) from exc
    return replace(runtime, state=refreshed)


def _runtime_with_latest_state(runtime: DeliveryRuntime) -> DeliveryRuntime:
    if runtime.state_path is None:
        return runtime
    latest = load_state(runtime.state_path)
    if latest is None or not latest.is_authenticated:
        raise WatcherAuthenticationError("SciFetch credentials were revoked")
    if (
        latest.server_url.rstrip("/"),
        latest.device_id,
        latest.installation_id,
        latest.trust_mode,
    ) != (
        runtime.state.server_url.rstrip("/"),
        runtime.state.device_id,
        runtime.state.installation_id,
        runtime.state.trust_mode,
    ):
        raise WatcherAuthenticationError("SciFetch credential ownership changed")
    return replace(runtime, state=latest)


async def _send_text(runtime: DeliveryRuntime, message: str) -> bool:
    return await _send_text_confirmation(runtime, message) is True


async def _send_terminal_batch(
    runtime: DeliveryRuntime,
    messages: Sequence[str],
    *,
    actionable: bool = False,
) -> bool:
    """Persist every segment before the first callback, without a replay queue."""
    from .durable_delivery import _binding, _valid_runtime, open_store

    messages = [text for message in messages if (text := _scrub_terminal_text(message))]
    batch_id = ""
    if messages and _platform_key(runtime.target.platform) == "weixin" and _valid_runtime(runtime):
        durable = open_store(runtime)
        batch_id = durable.save_terminal_batch(
            runtime.request_id,
            _binding(runtime),
            runtime.state.server_url.rstrip("/"),
            runtime.state.device_id,
            list(messages),
        )
    for ordinal, message in enumerate(messages):
        if not await _send_terminal_text(
            runtime,
            message,
            actionable=actionable,
            _batch_id=batch_id,
            _ordinal=ordinal,
        ):
            return False
    return True


async def _send_terminal_text(
    runtime: DeliveryRuntime,
    message: str,
    *,
    actionable: bool = False,
    _batch_id: str = "",
    _ordinal: int = 0,
) -> bool:
    """Send terminal text; Weixin observations remain dormant for status lookup."""
    message = _scrub_terminal_text(message)
    logger.info(
        "stage=terminal_text_prepared process_id=%s module=%s source=%s request_id=%s text_sha256=%s",
        os.getpid(),
        __name__,
        __file__,
        runtime.request_id,
        hashlib.sha256(message.encode()).hexdigest(),
    )
    if not message:
        return False
    from .durable_delivery import _binding, _valid_runtime, open_store

    durable_store = open_store(runtime) if _valid_runtime(runtime) else None
    store = _pending_store(runtime) if durable_store is None else None
    retain = _platform_key(runtime.target.platform) == "weixin"
    if retain and durable_store is not None:
        if not _batch_id:
            _batch_id = durable_store.save_terminal_batch(
                runtime.request_id,
                _binding(runtime),
                runtime.state.server_url.rstrip("/"),
                runtime.state.device_id,
                [message],
            )
        durable_store.update_terminal_segment(_batch_id, _ordinal, "unavailable")
    if retain and actionable and store is not None:
        store.persist_terminal_text(
            runtime.request_id,
            (message,),
            outcome="unavailable",
            user_id=runtime.target.user_id,
        )

    outcome = "unavailable"
    message_id_present = False
    retryable = False
    error_kind = ""
    if runtime.send_text is not None:
        try:
            from .durable_delivery import _valid_runtime, controlled_text

            operation = (
                controlled_text(runtime, message)
                if _valid_runtime(runtime)
                else _OUTBOUND_QUEUE.send_terminal_text(
                    _queue_key(runtime), message, runtime.send_text
                )
            )
            result = await asyncio.wait_for(
                operation,
                timeout=_TERMINAL_TEXT_CALLBACK_TIMEOUT_SECONDS,
            )
            if result is None or _result_field(result, "success") is None:
                outcome = "legacy_unconfirmed"
            else:
                receipt = _normalize_send_result(result, operation="terminal_text")
                message_id_present = bool(receipt.message_id)
                retryable = receipt.retryable
                error_kind = receipt.error_kind
                if receipt.error_kind in {"text_unavailable", "text_outcome_unknown"}:
                    outcome = "unavailable"
                else:
                    outcome = "adapter_accepted_unverified" if receipt.success else "rejected"
        except TimeoutError:
            outcome = "timeout"
        except Exception as exc:
            failure_receipt = _receipt_from_exception(exc)
            if failure_receipt is not None:
                message_id_present = bool(failure_receipt.message_id)
                retryable = failure_receipt.retryable
                error_kind = failure_receipt.error_kind
                outcome = "rejected"
            else:
                outcome = "unavailable"
    if retain and durable_store is not None:
        durable_store.update_terminal_segment(_batch_id, _ordinal, outcome)
    if retain and actionable and store is not None:
        store.mark_terminal_outcome(runtime.request_id, outcome)
    logger.info(
        "terminal text callback request_id=%s platform=%s operation=terminal_text "
        "outcome=%s attempt=1 replay_decision=%s message_id_present=%s "
        "error_kind=%s retryable=%s",
        runtime.request_id,
        _platform_key(runtime.target.platform),
        outcome,
        "retain" if retain else "none",
        message_id_present,
        _safe_log_value(error_kind),
        retryable,
    )
    return outcome in {"adapter_accepted_unverified", "legacy_unconfirmed"}


async def _send_text_confirmation(runtime: DeliveryRuntime, message: str) -> bool | None:
    if runtime.send_text is None:
        return False
    from .durable_delivery import _valid_runtime, controlled_text

    if _valid_runtime(runtime):
        result = await controlled_text(runtime, message)
        return _normalize_send_result(result, operation="send_text").success
    result = await _OUTBOUND_QUEUE.send_text(_queue_key(runtime), message, runtime.send_text)
    if result is True:
        return True
    if result is None:
        return None
    return False


async def _send_document_with_retry(
    path: Path,
    send_document: Callable[[str], object],
) -> object:
    rate_limit_retry_index = 0
    retryable_retry_index = 0
    last_error: Exception | None = None
    while True:
        try:
            result = await _maybe_await(send_document(str(path)))
            receipt = _normalize_send_result(result)
            if not receipt.success:
                raise DocumentSendError(receipt)
            return result
        except Exception as exc:
            last_error = exc
            failure_receipt = _receipt_from_exception(exc)
            is_rate_limited = _is_rate_limited_send_error(exc)
            is_retryable = _is_retryable_send_error(exc)
            if not is_retryable:
                raise
            if is_rate_limited:
                if rate_limit_retry_index >= len(_DOCUMENT_SEND_RATE_LIMIT_RETRY_DELAYS):
                    raise
                delay = _structured_retry_delay(
                    failure_receipt,
                    fallback=_DOCUMENT_SEND_RATE_LIMIT_RETRY_DELAYS[rate_limit_retry_index],
                    error_message=str(exc),
                )
                rate_limit_retry_index += 1
            else:
                if retryable_retry_index >= _DOCUMENT_SEND_RETRY_MAX_RETRIES:
                    raise
                delay = _structured_retry_delay(
                    failure_receipt,
                    fallback=_document_retry_backoff_delay(retryable_retry_index),
                    error_message=str(exc),
                )
                retryable_retry_index += 1
            logger.info(
                "retrying scifetch-paper document send after %s in %.1fs",
                type(exc).__name__,
                delay,
            )
            if delay > 0:
                await asyncio.sleep(delay)
    assert last_error is not None
    raise last_error


async def _send_text_with_retry(
    send_text: Callable[[str], object],
    message: str,
) -> bool | None:
    normal_retry_index = 0
    rate_limit_retry_index = 0
    last_error: Exception | None = None
    while True:
        try:
            result = await _maybe_await(send_text(message))
            if result is None or _result_field(result, "success") is None:
                logger.debug("scifetch-paper legacy send_text callback returned no receipt")
                return None
            receipt = _normalize_send_result(result, operation="send_text")
            if not receipt.success:
                raise TextSendError(receipt)
            return True
        except Exception as exc:
            last_error = exc
            failure_receipt = _receipt_from_exception(exc)
            is_rate_limited = _is_rate_limited_send_error(exc)
            if failure_receipt is not None and not _is_retryable_send_error(exc):
                break
            if is_rate_limited:
                if rate_limit_retry_index >= len(_TEXT_SEND_RATE_LIMIT_RETRY_DELAYS):
                    break
                delay = _structured_retry_delay(
                    failure_receipt,
                    fallback=_TEXT_SEND_RATE_LIMIT_RETRY_DELAYS[rate_limit_retry_index],
                    error_message=str(exc),
                )
                rate_limit_retry_index += 1
            else:
                if normal_retry_index >= len(_TEXT_SEND_RETRY_DELAYS):
                    break
                delay = _structured_retry_delay(
                    failure_receipt,
                    fallback=_TEXT_SEND_RETRY_DELAYS[normal_retry_index],
                    error_message=str(exc),
                )
                normal_retry_index += 1
            logger.info(
                "retrying scifetch-paper text send after %s in %.1fs",
                type(exc).__name__,
                delay,
            )
            if delay > 0:
                await asyncio.sleep(delay)
    logger.warning(
        "scifetch-paper send_text callback failed after retries: %s",
        last_error,
    )
    return False


async def _send_terminal_text_with_retry(
    send_text: Callable[[str], object],
    message: str,
) -> object:
    normal_retry_index = 0
    rate_limit_retry_index = 0
    while True:
        try:
            result = await _maybe_await(send_text(message))
            if result is None or _result_field(result, "success") is None:
                return result
            receipt = _normalize_send_result(result, operation="terminal_text")
            if not receipt.success:
                raise TextSendError(receipt)
            return result
        except Exception as exc:
            failure_receipt = _receipt_from_exception(exc)
            is_rate_limited = _is_rate_limited_send_error(exc)
            if failure_receipt is not None and not _is_retryable_send_error(exc):
                raise
            if is_rate_limited:
                if rate_limit_retry_index >= len(_TEXT_SEND_RATE_LIMIT_RETRY_DELAYS):
                    raise
                delay = _structured_retry_delay(
                    failure_receipt,
                    fallback=_TEXT_SEND_RATE_LIMIT_RETRY_DELAYS[rate_limit_retry_index],
                    error_message=str(exc),
                )
                rate_limit_retry_index += 1
            else:
                if normal_retry_index >= len(_TEXT_SEND_RETRY_DELAYS):
                    raise
                delay = _structured_retry_delay(
                    failure_receipt,
                    fallback=_TEXT_SEND_RETRY_DELAYS[normal_retry_index],
                    error_message=str(exc),
                )
                normal_retry_index += 1
            if delay > 0:
                await asyncio.sleep(delay)


async def _send_delivery_summary(
    runtime: DeliveryRuntime,
    delivered_file_records: list[dict[str, object]],
    *,
    terminal_payloads: Sequence[Mapping[str, object]] = (),
) -> bool:
    """Give users one path-free summary after native file delivery."""
    if runtime.send_text is None and _platform_key(runtime.target.platform) != "weixin":
        return True
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    messages = _delivery_summary_messages(
        delivered_file_records,
        request_id=runtime.request_id,
        web_url=web_url,
        terminal_payloads=terminal_payloads,
    )
    return await _send_terminal_batch(runtime, messages)


def _delivery_summary_messages(
    delivered_file_records: Sequence[Mapping[str, object]],
    *,
    request_id: str,
    web_url: str,
    terminal_payloads: Sequence[Mapping[str, object]] = (),
) -> list[str]:
    grouped_records = _group_delivered_records_by_paper(
        [_with_terminal_si_metadata(record, terminal_payloads) for record in delivered_file_records]
    )
    return [
        _delivery_summary_message(group, request_id=request_id, web_url=web_url)
        for group in grouped_records
    ]


def _with_terminal_si_metadata(
    record: Mapping[str, object],
    terminal_payloads: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Use confirmed final SI discovery only in a paper-matched rendering copy."""
    rendered = dict(record)
    for terminal in reversed(terminal_payloads):
        if not record.get("_fresh_context_unbound") and _same_request_item(record, terminal):
            rendered["_fresh_result_context"] = dict(terminal)
            break
    doi = _string_value(record, "doi").strip().lower()
    if not doi or _string_value(record, "role") != "main":
        return rendered
    for terminal in reversed(terminal_payloads):
        if not _same_request_item(record, terminal):
            continue
        if (
            terminal.get("ok") is not True
            or terminal.get("artifact_set_status") != "complete"
            or terminal.get("artifact_errors")
        ):
            return rendered
        stats = terminal.get("si_stats")
        stats = stats if isinstance(stats, Mapping) else {}
        known = terminal.get("si_expected_known", stats.get("expected_known"))
        expected = terminal.get("si_expected", stats.get("expected"))
        if known is not True or type(expected) is not int or expected < 0:
            return rendered
        if ("expected_known" in stats and stats["expected_known"] is not True) or (
            "expected" in stats and stats["expected"] != expected
        ):
            return rendered
        rendered["si_count_known"] = True
        rendered["si_count"] = expected
        rendered["si_status"] = "si_checked_none" if expected == 0 else "si_found"
        return rendered
    return rendered


def _group_delivered_records_by_paper(
    records: Sequence[Mapping[str, object]],
) -> list[list[Mapping[str, object]]]:
    main_records = [record for record in records if _string_value(record, "role") == "main"]
    if len(main_records) <= 1:
        return [list(records)] if records else []

    groups: list[list[Mapping[str, object]]] = [[record] for record in main_records]
    unmatched: list[Mapping[str, object]] = []

    for record in records:
        if _string_value(record, "role") == "main":
            continue
        matched_index = next(
            (index for index, main in enumerate(main_records) if _same_request_item(main, record)),
            -1,
        )
        if matched_index >= 0:
            groups[matched_index].append(record)
        else:
            unmatched.append(record)

    if unmatched:
        groups.append(unmatched)
    return groups


def _paper_group_key(record: Mapping[str, object]) -> str:
    item = _string_value(record, "item_id")
    if item:
        return f"item:{item}"
    doi = _public_field(record, "doi", default="", max_length=160).lower()
    if doi:
        return f"doi:{doi}"
    title = _public_field(record, "title", default="", max_length=360).lower()
    if title:
        return f"title:{title}"
    return ""


def _same_request_item(left: Mapping[str, object], right: Mapping[str, object]) -> bool:
    for key in ("item_id", "doi", "job_id", "title"):
        a, b = _string_value(left, key).strip(), _string_value(right, key).strip()
        if a and b:
            if (
                key == "doi"
                and a.casefold() == b.casefold()
                and (_string_value(left, "job_id") and _string_value(right, "job_id"))
            ):
                continue
            return a.casefold() == b.casefold() if key != "item_id" else a == b
    return not _paper_group_key(left) and not _paper_group_key(right)


def _bind_item_from_job(
    payload: Mapping[str, object], terminals: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    result = dict(payload)
    job_id = _string_value(payload, "job_id")
    for terminal in reversed(terminals):
        if job_id and _string_value(terminal, "job_id") == job_id:
            for field in ("item_id", "doi"):
                if not _string_value(result, field) and _string_value(terminal, field):
                    result[field] = terminal[field]
            break
    return result


def _request_item_anchors(
    records: Sequence[Mapping[str, object]], terminals: Sequence[Mapping[str, object]]
) -> list[Mapping[str, object]]:
    anchors: list[Mapping[str, object]] = []
    for record in [
        *(r for r in records if _string_value(r, "role") == "main"),
        *terminals,
    ]:
        if anchors and not _paper_group_key(record):
            continue
        if not any(_same_request_item(record, anchor) for anchor in anchors):
            anchors.append(record)
    return anchors


def _latest_item_terminals(
    payloads: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    latest: list[dict[str, object]] = []
    for payload in payloads:
        latest = [old for old in latest if not _same_request_item(old, payload)]
        latest.append(payload)
    return latest


def _delivery_summary_message(
    delivered_file_records: Sequence[Mapping[str, object]],
    *,
    request_id: str,
    web_url: str,
) -> str:
    main_record = _main_delivered_record(delivered_file_records)
    si_records = [
        record for record in delivered_file_records if _string_value(record, "role") == "si"
    ]
    if main_record:
        filename = Path(_string_value(main_record, "filename") or "SciFetch file").name
        if not si_records:
            return _main_delivery_confirmation(
                main_record,
                filename=filename,
                request_id=request_id,
                web_url=web_url,
            )
        lines = [
            "SciFetch 已获取并发送全文和附件。",
            "📚 文献信息：",
        ]
        lines.extend(_paper_metadata_lines(main_record))
        lines.append(f"📄 全文：已发送 {filename}")
        lines.append(f"📎 SI：已发送 {len(si_records)} 个附件")
        source_line = _delivery_summary_source_line(main_record, si_records)
        if source_line:
            lines.append(source_line)
        mode_line = _fresh_result_line(main_record)
        if mode_line:
            lines.append(mode_line)
        lines.append(_request_detail_line(request_id=request_id, web_url=web_url))
        return "\n".join(lines)

    lines = ["SciFetch 已发送附件。"]
    if si_records:
        lines.append(f"📎 SI：已发送 {len(si_records)} 个附件")
    else:
        filenames = [
            Path(_string_value(record, "filename") or "SciFetch file").name
            for record in delivered_file_records
        ]
        if filenames:
            lines.append(f"📎 附件：已发送 {'；'.join(filenames)}")
    mode_line = _fresh_result_line(delivered_file_records[0]) if delivered_file_records else ""
    if mode_line:
        lines.append(mode_line)
    lines.append(_request_detail_line(request_id=request_id, web_url=web_url))
    return "\n".join(lines)


def _main_delivery_confirmation(
    file_record: Mapping[str, object],
    *,
    filename: str,
    request_id: str,
    web_url: str,
) -> str:
    lines = [
        "SciFetch 已获取并发送全文。",
        "📚 文献信息：",
    ]
    lines.extend(_paper_metadata_lines(file_record))
    lines.append(f"📄 全文：已发送 {filename}")
    source_line = _delivery_source_line(file_record)
    if source_line:
        lines.append(source_line)
    si_line = _si_status_line(file_record)
    if si_line:
        lines.append(si_line)
    mode_line = _fresh_result_line(file_record)
    if mode_line:
        lines.append(mode_line)
    lines.append(_request_detail_line(request_id=request_id, web_url=web_url))
    return "\n".join(lines)


def _fresh_result_line(payload: Mapping[str, object]) -> str:
    """Describe confirmed request execution, independently of file provenance."""
    if payload.get("_fresh_context_unbound") is True:
        return ""
    context = payload.get("_fresh_result_context")
    facts = context if isinstance(context, Mapping) else payload
    requested = facts.get("fresh_requested")
    if requested is not True:
        if requested is None and facts.get("force_refresh_requested") is True:
            result = "未执行" if facts.get("fresh_retrieval_attempted") is False else "结果未确认"
            return f"🔄 强制重新下载：{result}"
        return ""
    attempted, outcome = facts.get("fresh_attempted"), facts.get("fresh_outcome")
    result = "结果未确认"
    if attempted is False and outcome == "not_attempted":
        result = "未执行"
    elif attempted is True:
        if (
            outcome == "succeeded"
            and facts.get("ok") is True
            and facts.get("artifact_set_status") == "complete"
            and not facts.get("artifact_errors")
            and facts.get("from_cache") is not True
            and payload.get("from_cache") is not True
            and _string_value(payload, "delivery_source").strip().lower()
            not in {"cache", "partial_cache"}
            and not _cleanup_unconfirmed(facts)
            and not _cleanup_unconfirmed(payload)
            and not _is_partial_delivery_error(payload)
        ):
            result = "已完成"
        elif outcome == "partial" and facts.get("ok") is False:
            result = "未完成" if _cleanup_unconfirmed(facts) else "部分完成"
        elif outcome == "failed" and facts.get("ok") is False:
            result = "未完成"
    return f"🔄 强制重新下载：{result}"


def _delivery_source_line(file_record: Mapping[str, object]) -> str:
    source = _string_value(file_record, "delivery_source").strip().lower()
    if source == "cache":
        return "🗂️ 来源：缓存文件（未重新下载）"
    if source == "partial_cache":
        return "🗂️ 来源：缓存正文；附件继续下载中"
    if source in {"download", "fresh"}:
        return "⬇️ 已下载"
    from_cache = file_record.get("from_cache")
    if from_cache is True:
        return "🗂️ 来源：缓存文件（未重新下载）"
    if from_cache is False:
        return "⬇️ 已下载"
    return ""


def _delivery_summary_source_line(
    main_record: Mapping[str, object],
    si_records: Sequence[Mapping[str, object]],
) -> str:
    if (
        si_records
        and _string_value(main_record, "delivery_source").strip().lower() == "partial_cache"
    ):
        return "🗂️ 来源：缓存正文；附件本次补全"
    return _delivery_source_line(main_record)


async def _send_delivery_failure(
    runtime: DeliveryRuntime,
    *,
    filename: str,
    reason: str,
) -> None:
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    lines = [
        "SciFetch 文件已获取，但发送未完成或结果尚未确认。",
        f"📄 文件：{filename}",
        f"⚠️ 原因：{public_error_message('', reason, origin='delivery')}",
        _request_detail_line(request_id=runtime.request_id, web_url=web_url),
    ]
    await _send_text(runtime, "\n".join(lines))


async def _send_delivery_deferred(
    runtime: DeliveryRuntime,
    *,
    filename: str,
) -> None:
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    lines = [
        "SciFetch 文件已获取，附件投递已暂缓。",
        f"📄 文件：{filename}",
        (
            "⚠️ 原因：微信文件通道 TLS 握手暂时失败；文件仍保留在待投递队列，"
            "将在下一次同一会话文献消息后重试。"
        ),
        _request_detail_line(request_id=runtime.request_id, web_url=web_url),
    ]
    await _send_text(runtime, "\n".join(lines))


async def _send_partial_delivery_notification(
    runtime: DeliveryRuntime,
    payload: dict[str, object],
    *,
    delivered_file_records: list[dict[str, object]],
) -> bool:
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    message = _partial_delivery_message(
        payload, delivered_file_records, request_id=runtime.request_id, web_url=web_url
    )
    return await _send_terminal_text(runtime, message, actionable=True)


def _partial_delivery_message(
    payload: dict[str, object],
    delivered_file_records: Sequence[Mapping[str, object]],
    *,
    request_id: str,
    web_url: str,
) -> str:
    main_record = _main_delivered_record(delivered_file_records)
    cleanup_unconfirmed = _cleanup_unconfirmed(payload)
    force_pause_cache_fallback = _is_force_pause_cache_fallback(payload, main_record)
    lines = [
        (
            "SciFetch 已发送可用文件，本次获取失败。"
            if cleanup_unconfirmed
            else "SciFetch 已发送缓存回退正文，但强制刷新未执行。"
            if force_pause_cache_fallback
            else "SciFetch 已发送可用文件，但本次获取未完成。"
            if _string_value(payload, "error_type") in {"job_timeout", "timeout"}
            else "SciFetch 已发送可用全文，但附件未完整获取。"
        )
    ]
    if main_record:
        lines.append("📚 文献信息：")
        lines.extend(_paper_metadata_lines(main_record))
        filename = Path(_string_value(main_record, "filename") or "SciFetch file").name
        lines.append(f"📄 全文：已发送 {filename}")
    if cleanup_unconfirmed:
        lines.append(
            _cleanup_si_status_line(payload, _role_count(delivered_file_records, "si"), sent=True)
        )
    else:
        lines.append(
            _partial_si_status_line(
                payload, observed_count=_role_count(delivered_file_records, "si"), sent=True
            )
        )
    reason = _partial_delivery_reason(
        payload, {**(main_record or {}), "files": list(delivered_file_records)}
    )
    if reason:
        lines.append(f"⚠️ 原因：{reason}")
    if main_record and _is_publisher_paused_payload(payload):
        source_line = _partial_delivery_source_line(main_record, payload)
        if source_line:
            lines.append(source_line)
    mode_line = (
        _fresh_result_line(payload) if not (main_record or {}).get("_fresh_context_unbound") else ""
    )
    if mode_line:
        lines.append(mode_line)
    lines.append(_request_detail_line(request_id=request_id, web_url=web_url))
    return "\n".join(lines)


async def _send_item_delivery_outcomes(
    runtime: DeliveryRuntime,
    records: list[dict[str, object]],
    *,
    terminals: Sequence[dict[str, object]],
    errors: Sequence[dict[str, object]],
) -> bool:
    anchors = _request_item_anchors(records, terminals)
    single = len(anchors) <= 1
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    messages = []
    for anchor in anchors:
        group = [record for record in records if _same_request_item(anchor, record)]
        scoped_terminals = [t for t in terminals if single or _same_request_item(anchor, t)]
        terminal = scoped_terminals[-1] if scoped_terminals else {}
        scoped_errors = [e for e in errors if single or _same_request_item(anchor, e)]
        error = scoped_errors[-1] if scoped_errors else None
        if terminal.get("ok") is True and any(_event_failed(t) for t in scoped_terminals[:-1]):
            error = None
        partial = _with_terminal_cleanup(error, terminal)
        if _event_failed(terminal) or partial is not None:
            if group:
                messages.append(
                    _partial_delivery_message(
                        {
                            **(partial or terminal),
                            "_fresh_result_context": terminal or partial or {},
                        },
                        group,
                        request_id=runtime.request_id,
                        web_url=web_url,
                    )
                )
            else:
                messages.append(
                    _failure_status_message(
                        request_id=runtime.request_id,
                        web_url=web_url,
                        payload=terminal,
                        fallback_payload=error,
                    )
                )
        elif group:
            messages.append(
                _delivery_summary_message(
                    [_with_terminal_si_metadata(record, scoped_terminals) for record in group],
                    request_id=runtime.request_id,
                    web_url=web_url,
                )
            )
    return await _send_terminal_text(runtime, "\n\n".join(messages), actionable=True)


def _main_delivered_record(records: Sequence[Mapping[str, object]]) -> Mapping[str, object] | None:
    for record in records:
        if _string_value(record, "role") == "main":
            return record
    return records[0] if records else None


def _si_discovery_incomplete(payload: Mapping[str, object]) -> bool:
    stats = payload.get("si_stats")
    return (
        _string_value(payload, "error_type") == "si_discovery_incomplete"
        or payload.get("si_expected_known") is False
        or (isinstance(stats, dict) and stats.get("expected_known") is False)
    )


def _si_discovery_failure_reason(payload: Mapping[str, object]) -> str:
    reasons = {
        "browser_unavailable": "附件检查所需的浏览器不可用，需恢复浏览器服务",
        "article_identity_unproven": "未能核对官方页面的文章身份",
        "document_bound_exceeded": "页面内容超出检查范围",
        "primary_article_unproven": "未能定位唯一的文章正文",
        "article_access_or_challenge_text": "文章页面存在访问限制或验证提示",
        "supplementary_surface_incomplete": "附件区域尚未完整加载或结构不完整",
        "full_article_container_unproven": "未能核对完整文章结构",
        "full_article_metadata_unproven": "文章标识、标题或访问状态尚未核对完整",
        "full_article_body_unproven": "未能核对完整正文",
        "substantive_article_body_unproven": "页面缺少可核对的正文内容",
        "article_back_matter_unproven": "未能核对文章末尾信息",
        "article_back_matter_incomplete": "文章末尾信息不完整",
        "article_references_boundary_unproven": "未能核对正文与参考文献的边界",
        "supplementary_signal_unresolved": "页面提及附件，但尚未解析出完整清单",
        "article_dynamic_surface_unresolved": "页面仍有未完成的动态加载内容",
        "static_rendered_manifest_conflict": "静态页面与渲染页面的附件清单不一致",
    }
    reason = _string_value(payload, "si_completeness_reason")
    if reason not in reasons:
        raw = _string_value(payload, "error") or _string_value(payload, "message")
        match = re.search(r"\[si_completeness_reason=([a-z_]{1,80})\]", raw)
        reason = match.group(1) if match else ""
    detail = reasons.get(reason, "官方附件清单尚未核对完整")
    return (
        f"附件检查未完成：{detail}；不能据此判断本文无附件。"
        "本次检查已结束，需解决页面访问或解析问题后补查附件。"
    )


def _partial_si_status_line(
    payload: Mapping[str, object], *, observed_count: int = 0, sent: bool = False
) -> str:
    expected = _int_value(payload, "si_expected")
    emitted = _int_value(payload, "si_emitted")
    fetched_count = max(observed_count, emitted or 0, 0)
    # Counts from this item's actual ready/accepted records survive a coarse
    # timeout's default zero counters. Emitted alone never proves native send.
    progress = (
        f"已{'发送' if sent else '获取'} {observed_count} 个附件；" if observed_count > 0 else ""
    )
    if _si_discovery_incomplete(payload):
        fetched = f"（已获取 {fetched_count} 个）" if fetched_count > 0 else ""
        return f"📎 SI：{progress}附件检查未完成，附件总数尚未确认{fetched}"
    if expected is not None and expected > fetched_count:
        return f"📎 SI：{progress}附件未完整获取（已获取 {fetched_count}/{expected}）"
    if fetched_count > 0:
        progress = progress or f"已获取 {fetched_count} 个附件；"
        return f"📎 SI：{progress}附件总数及获取完整性尚未确认"
    stats = payload.get("si_stats")
    known = payload.get(
        "si_expected_known", stats.get("expected_known") if isinstance(stats, Mapping) else None
    )
    if expected == 0 and known is True:
        return "📎 SI：已核对本文无附件"
    return "📎 SI：附件数量及获取完整性尚未确认"


def _partial_payload_from_terminal_failure(payload: Mapping[str, object]) -> dict[str, object]:
    partial = {str(key): value for key, value in payload.items()}
    partial.setdefault("stage", "download")
    return partial


def _cleanup_si_status_line(payload: Mapping[str, object], count: int, *, sent: bool) -> str:
    progress = f"已{'发送' if sent else '生成'} {count} 个附件；" if count > 0 else ""
    stats = payload.get("si_stats")
    stats = stats if isinstance(stats, Mapping) else {}
    known = payload.get("si_expected_known", stats.get("expected_known"))
    expected = payload.get("si_expected", stats.get("expected"))
    if known is True and type(expected) is int and expected >= count:
        return f"📎 SI：{progress}附件清单共 {expected} 个，本次获取未正常结束"
    return f"📎 SI：{progress}附件总数及获取完整性尚未确认"


def _with_terminal_cleanup(
    partial: dict[str, object] | None, terminal: Mapping[str, object]
) -> dict[str, object] | None:
    """A later cleanup result supersedes earlier partial-delivery guidance."""
    if _string_value(terminal, "error_type") == "browser_stop_unproven":
        return _partial_payload_from_terminal_failure(terminal)
    if not _wiley_cleanup_unconfirmed(terminal):
        return partial
    result = (
        dict(partial) if partial is not None else _partial_payload_from_terminal_failure(terminal)
    )
    result["artifact_errors"] = terminal["artifact_errors"]
    return result


def _cleanup_unconfirmed(payload: Mapping[str, object] | None) -> bool:
    return payload is not None and (
        _wiley_cleanup_unconfirmed(payload)
        or _string_value(payload, "error_type") == "browser_stop_unproven"
    )


def _is_wiley_result(*payloads: Mapping[str, object] | None) -> bool:
    for payload in payloads:
        if payload is None:
            continue
        publisher = _string_value(payload, "publisher").strip().lower()
        publisher_key = _string_value(payload, "publisher_key").strip().lower()
        doi = _string_value(payload, "doi").strip().lower()
        raw = _string_value(payload, "error") or _string_value(payload, "message")
        if (
            publisher == "wiley"
            or publisher_key == "wiley"
            or publisher_key == "onlinelibrary.wiley.com"
            or publisher_key.endswith(".onlinelibrary.wiley.com")
            or doi.startswith("10.1002/")
            or raw.lower().startswith("wiley ")
        ):
            return True
    return False


def _wiley_cleanup_unconfirmed(*payloads: Mapping[str, object] | None) -> bool:
    for payload in payloads:
        if payload is None:
            continue
        errors = payload.get("artifact_errors")
        if not isinstance(errors, list):
            continue
        if any(
            isinstance(error, Mapping)
            and error.get("error_type") == "acquisition_finish_stop_unproven"
            and error.get("publisher") == "wiley"
            and error.get("retryable") is False
            for error in errors
        ):
            return True
    return False


def _wiley_verification_reason(raw: str) -> str:
    reasons = {
        "pending_timeout": "验证页面在预算内未转为文章页。",
        "unresolved_control": "未能确认可安全操作的唯一验证控件。",
        "unsupported_control": "验证控件结构尚不受当前自动化支持。",
        "explicit_denial": "官网明确拒绝了本次访问。",
        "frame_reported_error": "官网验证组件显示了可见错误，本次获取已停止。",
        "article_identity_mismatch": "验证后页面的文章身份未通过核对。",
        "resource_policy_denied": "继续验证所需请求未通过资源安全检查。",
        "input_uncertain": "验证输入的完成状态未能安全确认。",
        "input_unavailable": "服务的验证输入能力未能通过检查。",
        "input_refused": "验证目标在输入前发生变化，已安全停止操作。",
        "input_deadline": "验证输入未能在原预算内完成。",
        "first_document_timeout": "原预算内未取得可用的验证或文章页面。",
        "cancelled": "本次自动验证已取消。",
    }
    matched = re.search(r"automatic verification ended: ([a-z_]+);", raw)
    detail = reasons.get(matched.group(1), "") if matched else ""
    return detail + _WILEY_VERIFICATION_GUIDANCE_ZH


def _wiley_episode_failure_reason(
    raw: str, context: Mapping[str, object] | None = None
) -> str | None:
    # Only exact historical/canonical presentation shapes carry this old cause.
    legacy = re.fullmatch(
        r"(?:BrowserContractError: )?worker_uncertain:wiley_episode_(runtime|cleanup)_failure",
        raw.strip(),
    )
    canonical = (
        "Wiley 官网自动验证运行失败，本次获取已结束。",
        _WILEY_CLEANUP_UNCONFIRMED_ZH,
    )
    if legacy is None and raw.strip() not in {
        *canonical,
        *(message + "已获取文件仍保留。" for message in canonical),
    }:
        return None
    return public_error_message("download_exception", raw, context=context)


def _partial_delivery_reason(
    payload: Mapping[str, object], context: Mapping[str, object] | None = None
) -> str:
    return _preserve_canonical_retained_fact(
        _partial_delivery_reason_text(payload, context),
        _string_value(payload, "error_type"),
        _string_value(payload, "error") or _string_value(payload, "message"),
        {**payload, **(context or {})},
    )


def _partial_delivery_reason_text(
    payload: Mapping[str, object], context: Mapping[str, object] | None = None
) -> str:
    facts = {**payload, **(context or {})}
    raw = _string_value(payload, "error") or _string_value(payload, "message")
    if _wiley_cleanup_unconfirmed(payload, context):
        return _cleanup_failure_reason(raw, wiley=True, context=facts)
    if _cleanup_unconfirmed(payload):
        return _cleanup_failure_reason(raw, wiley=False, context=facts)
    error_type = _string_value(payload, "error_type")
    normalized = f"{error_type} {raw}".lower()
    if error_type.strip().lower() == _BROWSER_RESOURCE_POLICY_UNCERTAIN:
        proof = _string_value(payload, "message")
        if proof != "Supporting information delivery incomplete after main file was ready.":
            proof = raw
        return public_error_message(error_type, proof, context=facts) + "反复提交不会自动解除。"
    if error_type.strip().lower() == "download_exception":
        episode_reason = _wiley_episode_failure_reason(raw, facts)
        if episode_reason is not None:
            return episode_reason
    if error_type.strip().lower() in {
        "publisher_paused",
        "publisher_queue_paused",
    } or raw.startswith("Publisher queue paused:"):
        return _publisher_paused_partial_reason(payload)
    if error_type.strip().lower() == "publisher_human_verification_required":
        if _is_wiley_result(payload, context):
            return _wiley_verification_reason(raw)
        return _actionable_failure_reason("", error_type, facts)
    if error_type.strip().lower() in {_BROWSER_CDP_COMMAND_TIMEOUT, "job_timeout", "timeout"}:
        return public_error_message(error_type, raw, context=facts)
    if error_type.strip().lower() == "si_unsupported":
        return public_error_message(
            "", "该出版商的附件检查尚未支持；不能据此判断本文无附件。", context=facts
        )
    if _si_discovery_incomplete(payload):
        return public_error_message("", _si_discovery_failure_reason(payload), context=facts)
    if "supporting information delivery incomplete" in normalized:
        return "正文已保留，附件获取未完成，请查看请求详情。"
    return _actionable_failure_reason(raw or error_type, error_type, facts)


def _cleanup_failure_reason(
    raw: str, *, wiley: bool, context: Mapping[str, object] | None = None
) -> str:
    """Preserve finite public copies only after existing cleanup classification."""
    kind = "download_exception" if wiley else "browser_stop_unproven"
    if not wiley and browser_stop_explanation(raw) is not None:
        return public_error_message(kind, raw, context=context)
    legacy = _WILEY_CLEANUP_UNCONFIRMED_ZH if wiley else _BROWSER_CLEANUP_UNCONFIRMED_ZH
    canonical = public_error_message(kind, legacy if wiley else "")
    copies = {canonical, canonical + "已获取文件仍保留。"}
    if not wiley:
        copies.add("正文已保留，附件获取未完成。" + canonical)
    if raw.strip() in copies:
        return public_error_message(kind, raw, context=context)
    return public_error_message("download_exception" if wiley else "", legacy, context=context)


def _publisher_paused_partial_reason(payload: Mapping[str, object]) -> str:
    force_requested = payload.get("force_refresh_requested") is True
    fresh_not_attempted = payload.get("fresh_retrieval_attempted") is False
    cache_fallback = payload.get("cache_fallback") is True
    raw = _string_value(payload, "error") or _string_value(payload, "message")
    publisher_reason = _scrub_public_text(
        _string_value(payload, "publisher_reason"),
        max_length=160,
    )
    human_verification = (
        "publisher_human_verification_required" in f"{publisher_reason} {raw}".lower()
    )
    if force_requested and fresh_not_attempted:
        action = "!! 强制刷新已识别，但出版商队列已暂停，本次未尝试新的下载"
        if cache_fallback:
            action += "；已发送的是缓存回退正文"
        action += "。"
    else:
        action = "出版商队列已暂停；"

    if human_verification:
        action += (
            _WILEY_VERIFICATION_GUIDANCE_ZH
            if _is_wiley_result(payload)
            else _HUMAN_VERIFICATION_GUIDANCE_ZH
        )
    else:
        action += "请管理员检查暂停原因，显式恢复该出版商队列后再重试。"

    publisher_key = _scrub_public_text(
        _string_value(payload, "publisher_key"),
        max_length=120,
    )
    return action + (f"出版商：{publisher_key}。" if publisher_key else "")


def _is_publisher_paused_payload(payload: Mapping[str, object]) -> bool:
    raw = _string_value(payload, "error") or _string_value(payload, "message")
    return _is_publisher_queue_paused(raw, _string_value(payload, "error_type"))


def _is_force_pause_cache_fallback(
    payload: Mapping[str, object] | None,
    main_record: Mapping[str, object] | None,
) -> bool:
    if payload is None or main_record is None:
        return False
    return (
        _is_publisher_paused_payload(payload)
        and payload.get("force_refresh_requested") is True
        and payload.get("fresh_retrieval_attempted") is False
        and _string_value(main_record, "delivery_source").strip().lower() == "partial_cache"
    )


def _partial_delivery_source_line(
    main_record: Mapping[str, object],
    payload: Mapping[str, object],
) -> str:
    if (
        _is_publisher_paused_payload(payload)
        and _string_value(main_record, "delivery_source").strip().lower() == "partial_cache"
    ):
        return "🗂️ 来源：缓存回退正文（本次未重新下载）"
    if (
        _si_discovery_incomplete(payload)
        or _string_value(payload, "error_type") == "si_unsupported"
    ) and _string_value(main_record, "delivery_source").strip().lower() == "partial_cache":
        return "🗂️ 来源：缓存正文（附件检查未完成，本次任务已结束）"
    return _delivery_source_line(main_record)


def _paper_metadata_lines(file_record: Mapping[str, object]) -> list[str]:
    lines: list[str] = []
    title = _public_field(file_record, "title", default="", max_length=360)
    if title:
        lines.append(f"📝 题目：{title}")
    authors = _public_authors(file_record)
    if authors:
        lines.append(f"👥 作者：{authors}")
    else:
        first_author = _public_field(file_record, "first_author", default="")
        if first_author:
            lines.append(f"👤 第一作者：{first_author}")
    journal = _public_field(file_record, "journal", default="")
    if journal:
        lines.append(f"📰 期刊：{journal}")
    year = _public_publication_year(file_record)
    if year:
        lines.append(f"📅 年份：{year}")
    publisher = _public_field(file_record, "publisher", default="")
    if publisher:
        lines.append(f"🏢 出版方：{publisher}")
    doi = _public_field(file_record, "doi", default="")
    if doi:
        lines.append(f"🔗 DOI：{doi}")
    abstract = _public_abstract(file_record)
    if abstract:
        lines.append(f"🧾 摘要：{abstract}")
    corresponding = _public_corresponding_authors(file_record, default="")
    if corresponding:
        lines.append(f"✉️ 通讯作者：{corresponding}")
    affiliation = _public_affiliation(file_record, default="")
    if affiliation:
        lines.append(f"🏛️ 第一单位：{affiliation}")
    if not lines:
        lines.append("📄 服务器暂未返回公开书目信息")
    return lines


def _public_field(
    file_record: Mapping[str, object],
    key: str,
    *,
    default: str = "未知",
    max_length: int = 240,
) -> str:
    return _public_text(file_record.get(key), default=default, max_length=max_length)


def _public_affiliation(file_record: Mapping[str, object], *, default: str = "未知") -> str:
    first_affiliation = _public_text(file_record.get("first_affiliation"), default="")
    if first_affiliation:
        return first_affiliation
    return _public_text(file_record.get("raw_affiliation"), default=default)


def _public_corresponding_authors(
    file_record: Mapping[str, object],
    *,
    default: str = "未知",
) -> str:
    value = file_record.get("corresponding_authors")
    if isinstance(value, list):
        authors = [
            _public_text(item, default="", max_length=120)
            for item in value
            if isinstance(item, str) and item
        ]
        authors = [author for author in authors if author]
        return "；".join(authors) if authors else default
    return _public_text(value, default=default)


def _public_authors(file_record: Mapping[str, object]) -> str:
    value = file_record.get("authors")
    if isinstance(value, list):
        authors = [
            _public_text(item, default="", max_length=120)
            for item in value
            if isinstance(item, str) and item
        ]
        authors = [author for author in authors if author]
        return "；".join(authors)
    return _public_text(value, default="")


def _public_publication_year(file_record: Mapping[str, object]) -> str:
    for key in ("year", "publication_year", "published_year"):
        value = file_record.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        year = _public_text(value, default="", max_length=24)
        if year:
            return year[:4] if re.match(r"^\d{4}", year) else year
    for key in ("published_at", "published_date", "publication_date", "date"):
        value = _public_text(file_record.get(key), default="", max_length=32)
        if value:
            return value[:4] if re.match(r"^\d{4}", value) else value
    return ""


def _public_abstract(file_record: Mapping[str, object]) -> str:
    for key in ("abstract", "abstract_snippet", "summary"):
        value = _public_text(file_record.get(key), default="", max_length=220)
        if value:
            return value
    return ""


def _public_text(value: object, *, default: str = "未知", max_length: int = 240) -> str:
    if not isinstance(value, str) or not value.strip():
        return default
    return _scrub_public_text(value, max_length=max_length) or default


def _si_status_line(file_record: Mapping[str, object]) -> str:
    si_status = _string_value(file_record, "si_status")
    si_count = _int_value(file_record, "si_count")
    if si_status in {"none", "si_checked_none"}:
        return "📎 SI：本文无附件"
    if si_status in {"expected", "si_found"}:
        if si_count is not None and si_count > 0:
            return f"📎 SI：检测到 {si_count} 个附件，正在发送"
        return "📎 SI：检测到附件，正在发送"
    if si_status == "si_unsupported":
        return "📎 SI：该出版商暂未支持附件检测"
    if si_status == "si_unknown":
        return "📎 SI：附件检查尚未完成"
    return ""


async def _send_failure_notification(
    runtime: DeliveryRuntime,
    payload: dict[str, object],
    *,
    fallback_payload: dict[str, object] | None = None,
) -> bool:
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    message = _failure_status_message(
        request_id=runtime.request_id,
        web_url=web_url,
        payload=payload,
        fallback_payload=fallback_payload,
    )
    return await _send_terminal_text(runtime, message, actionable=True)


def _failure_status_message(
    *,
    request_id: str,
    web_url: str,
    payload: dict[str, object],
    fallback_payload: dict[str, object] | None,
) -> str:
    fallback = fallback_payload or {}
    reason = _failure_reason(payload, fallback)
    payload_files, fallback_files = payload.get("files"), fallback.get("files")
    doi = _string_value(payload, "doi") or _string_value(fallback, "doi")
    lines = ["SciFetch 暂时无法获取这篇文献。"]
    if _strict_fresh_failure(payload):
        lines = ["SciFetch 强制重新下载未完成；本次未发送旧缓存，旧文件仍保留。"]
    metadata_lines = [
        line for line in _paper_metadata_lines(payload) if not line.startswith("🔗 DOI：")
    ]
    if metadata_lines and metadata_lines != ["📄 服务器暂未返回公开书目信息"]:
        lines.append("📚 文献信息：")
        lines.extend(metadata_lines)
    if doi:
        lines.append(f"🔗 DOI：{_public_text(doi)}")
    if reason:
        lines.append(f"⚠️ 原因：{reason}")
    mode_line = _fresh_result_line(payload)
    if mode_line:
        lines.append(mode_line)
    lines.append(_request_detail_line(request_id=request_id, web_url=web_url))
    message = "\n".join(lines)
    logger.info(
        "stage=terminal_failure_format process_id=%s module=%s source=%s request_id=%s "
        "error_type=%s payload_file_count=%s fallback_file_count=%s payload_files=%s fallback_files=%s reason_sha256=%s text_sha256=%s",
        os.getpid(),
        __name__,
        __file__,
        request_id,
        _string_value(payload, "error_type")
        if _string_value(payload, "error_type")
        in {"browser_stop_unproven", "browser_resource_policy_uncertain"}
        else "other",
        payload.get("file_count") if type(payload.get("file_count")) is int else None,
        fallback.get("file_count") if type(fallback.get("file_count")) is int else None,
        len(payload_files) if isinstance(payload_files, list) else None,
        len(fallback_files) if isinstance(fallback_files, list) else None,
        hashlib.sha256(reason.encode()).hexdigest(),
        hashlib.sha256(message.encode()).hexdigest(),
    )
    return message


def _strict_fresh_failure(payload: Mapping[str, object]) -> bool:
    return (
        payload.get("fresh_requested") is True
        and _string_value(payload, "doi").casefold().startswith("10.1126/")
        and payload.get("from_cache") is not True
        and payload.get("fresh_outcome") in {"unknown", "not_attempted", "failed"}
    )


def _delivery_result_metadata(delivery_result: object | None) -> dict[str, object]:
    metadata: dict[str, object] = {}
    if delivery_result is None:
        return metadata

    success = _result_field(delivery_result, "success")
    if isinstance(success, bool):
        metadata["platform_success"] = success

    message_id = _result_field(delivery_result, "message_id")
    if isinstance(message_id, str) and message_id:
        metadata["platform_message_id"] = message_id

    retryable = _result_field(delivery_result, "retryable")
    if isinstance(retryable, bool):
        metadata["platform_retryable"] = retryable

    retry_after = _result_field(delivery_result, "retry_after")
    if isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool):
        metadata["platform_retry_after"] = float(retry_after)

    error_kind = _result_field(delivery_result, "error_kind")
    if isinstance(error_kind, str) and error_kind:
        metadata["platform_error_kind"] = error_kind

    return metadata


def _normalize_send_result(
    result: object | None,
    *,
    operation: str = "send_document",
) -> SendResultReceipt:
    if result is None:
        return SendResultReceipt(
            success=False,
            message_id="",
            error=f"{operation} returned None",
            retryable=False,
            retry_after=None,
            error_kind="contract",
            result=result,
        )

    success_value = _result_field(result, "success")
    message_id = _optional_string(_result_field(result, "message_id"))
    error = _optional_string(_result_field(result, "error"))
    retryable = _result_field(result, "retryable") is True
    retry_after = _optional_float(_result_field(result, "retry_after"))
    error_kind = _optional_string(_result_field(result, "error_kind")).lower()
    if (
        operation == "send_document"
        and not error_kind
        and _is_weixin_cdn_tls_handshake_error(error)
    ):
        retryable = True
        error_kind = "transient"
    has_explicit_success = success_value is True
    success = has_explicit_success and error_kind not in _DOCUMENT_TERMINAL_ERROR_KINDS
    if not has_explicit_success and not error:
        error = f"{operation} did not return success=True"
    if has_explicit_success and not success and not error:
        error = error_kind or f"{operation} returned a terminal success kind"
    return SendResultReceipt(
        success=success,
        message_id=message_id,
        error=error,
        retryable=retryable,
        retry_after=retry_after,
        error_kind=error_kind,
        result=result,
    )


def _result_field(result: object, field: str) -> object:
    if isinstance(result, Mapping):
        return result.get(field)
    return getattr(result, field, None)


def _optional_string(value: object) -> str:
    return value if isinstance(value, str) else ""


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return max(0.0, float(value))
    if isinstance(value, str) and value:
        try:
            return max(0.0, float(value))
        except ValueError:
            return None
    return None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, str) and value:
        try:
            return max(0, int(value))
        except ValueError:
            return None
    return None


def _receipt_from_exception(exc: BaseException) -> SendResultReceipt | None:
    if isinstance(exc, SendResultError):
        return exc.receipt
    retry_after = _optional_float(getattr(exc, "retry_after", None))
    retryable = getattr(exc, "retryable", None) is True
    error_kind = _optional_string(getattr(exc, "error_kind", "")).lower()
    if retry_after is None and not retryable and not error_kind:
        return None
    return SendResultReceipt(
        success=False,
        message_id="",
        error=str(exc),
        retryable=retryable,
        retry_after=retry_after,
        error_kind=error_kind,
        result=None,
    )


def _send_error_code(exc: BaseException) -> str:
    receipt = _receipt_from_exception(exc)
    if receipt is not None and receipt.error_kind:
        return receipt.error_kind
    return type(exc).__name__


def _send_error_message(exc: BaseException) -> str:
    receipt = _receipt_from_exception(exc)
    if receipt is not None and receipt.error:
        return receipt.error
    return str(exc)


def _is_retryable_send_error(exc: BaseException) -> bool:
    receipt = _receipt_from_exception(exc)
    if receipt is not None:
        if receipt.retryable:
            return True
        if receipt.error_kind in _DOCUMENT_RETRYABLE_ERROR_KINDS:
            return True
        if receipt.error_kind in _DOCUMENT_TERMINAL_ERROR_KINDS:
            return False
    return _is_rate_limit_error(str(exc))


def _is_rate_limited_send_error(exc: BaseException) -> bool:
    receipt = _receipt_from_exception(exc)
    if receipt is not None and receipt.error_kind == "rate_limited":
        return True
    return _is_rate_limit_error(str(exc))


def _is_qqbot_deferred_send_error(exc: BaseException) -> bool:
    receipt = _receipt_from_exception(exc)
    return receipt is not None and receipt.error_kind in _QQBOT_DEFERRED_ERROR_KINDS


def _is_weixin_cdn_tls_handshake_error(message: str) -> bool:
    normalized = message.lower()
    return _WEIXIN_CDN_TLS_HOST in normalized and any(
        marker in normalized for marker in _WEIXIN_CDN_TLS_HANDSHAKE_MARKERS
    )


def _is_weixin_cdn_tls_handshake_send_error(exc: BaseException) -> bool:
    receipt = _receipt_from_exception(exc)
    return (
        receipt is not None
        and receipt.retryable
        and receipt.error_kind == "transient"
        and _is_weixin_cdn_tls_handshake_error(receipt.error)
    )


def _should_keep_pending_for_send_error(exc: BaseException) -> bool:
    receipt = _receipt_from_exception(exc)
    if receipt is not None:
        if receipt.error_kind in _DOCUMENT_TERMINAL_ERROR_KINDS:
            return False
        return (
            receipt.retryable
            or receipt.error_kind in _DOCUMENT_RETRYABLE_ERROR_KINDS
            or _is_rate_limit_error(str(exc))
        )
    return _is_rate_limit_error(str(exc))


def _document_retry_backoff_delay(retry_index: int) -> float:
    base = _DOCUMENT_SEND_RETRY_BASE_SECONDS * (2**retry_index)
    capped = min(base, _DOCUMENT_SEND_RETRY_MAX_DELAY_SECONDS)
    if capped <= 0:
        return 0.0
    jitter: float = random.uniform(0.0, min(_DOCUMENT_SEND_RETRY_JITTER_SECONDS, capped))
    return float(min(_DOCUMENT_SEND_RETRY_MAX_DELAY_SECONDS, capped + jitter))


def _structured_retry_delay(
    receipt: SendResultReceipt | None,
    *,
    fallback: float,
    error_message: str,
) -> float:
    if receipt is not None and receipt.retry_after is not None:
        return receipt.retry_after
    if _is_rate_limit_error(error_message):
        return _rate_limit_retry_delay(error_message, fallback=fallback)
    return fallback


def _is_rate_limit_error(message: str) -> bool:
    normalized = message.lower()
    return any(
        marker in normalized
        for marker in (
            "rate limited",
            "rate-limit",
            "cooldown active",
            "ret=-2",
            "errcode=-2",
        )
    )


def _rate_limit_retry_delay(message: str, *, fallback: float) -> float:
    match = _COOLDOWN_RE.search(message)
    if match is None:
        return fallback
    try:
        seconds = float(match.group("seconds"))
    except ValueError:
        return fallback
    return max(fallback, seconds + _RATE_LIMIT_RETRY_BUFFER_SECONDS)


async def _iter_sse(
    lines: AsyncIterator[str],
    *,
    deadline: float,
) -> AsyncIterator[tuple[str, str]]:
    event_name = "message"
    data_lines: list[str] = []
    iterator = lines.__aiter__()
    while True:
        try:
            raw_line = await asyncio.wait_for(
                anext(iterator),
                timeout=_next_sse_read_timeout(deadline),
            )
        except StopAsyncIteration:
            break
        except TimeoutError as exc:
            if time.monotonic() >= deadline:
                raise WatcherDeadlineError("SciFetch watcher deadline reached") from exc
            raise httpx.ReadTimeout("SciFetch SSE stream read timed out") from exc
        line = raw_line.strip("\r")
        if not line:
            if data_lines:
                yield event_name, "\n".join(data_lines)
            event_name = "message"
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if data_lines:
        yield event_name, "\n".join(data_lines)


def _next_sse_read_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise WatcherDeadlineError("SciFetch watcher deadline reached")
    return max(0.001, min(_SSE_READ_TIMEOUT_SECONDS, remaining))


def _parse_event_payload(event_name: str, data: str) -> tuple[str, dict[str, object], str]:
    decoded = json.loads(data)
    if not isinstance(decoded, dict):
        return event_name, {}, ""
    event_type = _string_value(decoded, "event_type") or event_name
    payload = decoded.get("data")
    job_id = _string_value(decoded, "job_id")
    return event_type, payload if isinstance(payload, dict) else {}, job_id


def _event_payloads(
    events: Sequence[Mapping[str, object]],
    event_type: str,
) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for event in events:
        if _string_value(event, "event_type") != event_type:
            continue
        data = event.get("data")
        if isinstance(data, Mapping):
            payloads.append({str(key): value for key, value in data.items()})
    return payloads


def _first_role_payload(
    payloads: Sequence[Mapping[str, object]],
    role: str,
) -> dict[str, object]:
    for payload in payloads:
        if _string_value(payload, "role") == role:
            return {str(key): value for key, value in payload.items()}
    return {}


def _role_count(payloads: Sequence[Mapping[str, object]], role: str) -> int:
    return sum(1 for payload in payloads if _string_value(payload, "role") == role)


def _last_partial_payload(payloads: Sequence[Mapping[str, object]]) -> dict[str, object] | None:
    for payload in reversed(payloads):
        if _is_partial_delivery_error(payload):
            return {str(key): value for key, value in payload.items()}
    return None


def _filename_from_payloads(
    primary: Mapping[str, object],
    fallback: Mapping[str, object],
) -> str:
    filename = _string_value(primary, "filename") or _string_value(fallback, "filename")
    return Path(filename).name if filename else ""


def _event_failed(payload: dict[str, object]) -> bool:
    return payload.get("ok") is False


def _is_partial_delivery_error(payload: Mapping[str, object]) -> bool:
    message = _string_value(payload, "message").lower()
    stage = _string_value(payload, "stage").lower()
    has_si_progress = "si_expected" in payload or "si_emitted" in payload
    return "supporting information delivery incomplete" in message or (
        stage == "download" and has_si_progress
    )


def _failure_reason(
    payload: Mapping[str, object],
    fallback_payload: Mapping[str, object] | None = None,
) -> str:
    fallback = fallback_payload or {}
    return _preserve_canonical_retained_fact(
        _failure_reason_text(payload, fallback),
        _string_value(payload, "error_type") or _string_value(fallback, "error_type"),
        _string_value(payload, "error")
        or _string_value(fallback, "error")
        or _string_value(payload, "message")
        or _string_value(fallback, "message"),
        {**fallback, **payload},
    )


def _failure_reason_text(
    payload: Mapping[str, object],
    fallback_payload: Mapping[str, object] | None = None,
) -> str:
    fallback = fallback_payload or {}
    facts = {**fallback, **payload}
    if payload.get("ok") is False:
        facts["terminal"] = True
    raw = (
        _string_value(payload, "error")
        or _string_value(fallback, "error")
        or _string_value(payload, "message")
        or _string_value(fallback, "message")
    )
    if _wiley_cleanup_unconfirmed(payload, fallback):
        return _cleanup_failure_reason(raw, wiley=True, context=facts)
    if _cleanup_unconfirmed(payload):
        return _cleanup_failure_reason(raw, wiley=False, context=facts)
    for candidate in (payload,) if _string_value(payload, "error_type") else (fallback,):
        if (
            _string_value(candidate, "error_type").strip().lower()
            == _BROWSER_RESOURCE_POLICY_UNCERTAIN
        ):
            raw = _string_value(candidate, "error") or _string_value(candidate, "message")
            return (
                public_error_message(_BROWSER_RESOURCE_POLICY_UNCERTAIN, raw, context=facts)
                + "反复提交不会自动解除。"
            )
    for candidate in (payload, fallback):
        raw = _string_value(candidate, "error")
        error_type = _string_value(candidate, "error_type")
        if error_type.strip().lower() == "publisher_human_verification_required":
            if _is_wiley_result(payload, fallback):
                return _wiley_verification_reason(raw)
            return _actionable_failure_reason(raw or error_type, error_type)
    for candidate in (payload, fallback):
        raw = _string_value(candidate, "error")
        error_type = _string_value(candidate, "error_type")
        if error_type.strip().lower() == _BROWSER_CDP_COMMAND_TIMEOUT or (
            raw.strip().lower() == _BROWSER_CDP_TIMEOUT_LEGACY_MARKER
        ):
            return public_error_message(_BROWSER_CDP_COMMAND_TIMEOUT, raw, context=facts)
    for candidate in (fallback, payload):
        raw = _string_value(candidate, "error")
        error_type = _string_value(candidate, "error_type")
        if error_type.strip().lower() in {
            "publisher_paused",
            "publisher_queue_paused",
        } or raw.startswith("Publisher queue paused:"):
            return _actionable_failure_reason(raw or error_type, error_type)
    raw = _string_value(payload, "error") or _string_value(fallback, "error")
    error_type = _string_value(payload, "error_type") or _string_value(fallback, "error_type")
    if not raw and not error_type:
        return ""
    return _actionable_failure_reason(raw or error_type, error_type, facts)


def _actionable_failure_reason(
    raw_reason: str, error_type: str, context: Mapping[str, object] | None = None
) -> str:
    return _preserve_canonical_retained_fact(
        _actionable_failure_reason_text(raw_reason, error_type, context),
        error_type,
        raw_reason,
        context,
    )


def _preserve_canonical_retained_fact(
    rendered: str,
    error_type: str,
    raw: str,
    context: Mapping[str, object] | None = None,
) -> str:
    """Append only known file facts after specialized public guidance is selected."""
    facts = context or {}
    count = facts.get("file_count")
    files = facts.get("files")
    retained = (
        type(count) is int
        and count > 0
        or isinstance(files, (list, tuple))
        and bool(files)
        or canonical_retained_fact(error_type, raw)
    )
    if retained and "已获取文件" not in rendered and "正文已保留" not in rendered:
        return rendered + "已获取文件仍保留。"
    return rendered


def _actionable_failure_reason_text(
    raw_reason: str, error_type: str, context: Mapping[str, object] | None = None
) -> str:
    if error_type.strip().lower() == "weixin_resolve_failed":
        # Historical server events used DownloadFailed's English envelope.
        # Match only the complete reason code, never a substring in another error.
        legacy = raw_reason.strip()
        code = legacy.partition(": ")[2] if legacy.startswith("[WeChat] ") else legacy
        if code in {
            "no_doi_or_publisher_url_in_read_more",
            "no_doi_or_publisher_url",
            "weixin_article_incomplete",
            "weixin_article_unavailable",
        }:
            return public_error_message("weixin_resolve_failed", code, context=context)
    if error_type.strip().lower() == "browser_stop_unproven":
        return _cleanup_failure_reason(raw_reason, wiley=False, context=context)
    if error_type.strip().lower() == _BROWSER_RESOURCE_POLICY_UNCERTAIN:
        canonical = public_error_message(_BROWSER_RESOURCE_POLICY_UNCERTAIN)
        retained = public_error_message(
            _BROWSER_RESOURCE_POLICY_UNCERTAIN, context={"file_count": 1}
        )
        if raw_reason.strip() in {canonical, retained}:
            return public_error_message(error_type, raw_reason, context=context)
        return _RESOURCE_POLICY_GUIDANCE_ZH
    reason = _scrub_public_text(raw_reason) if raw_reason else ""
    episode_reason = (
        _wiley_episode_failure_reason(raw_reason, context)
        if error_type.strip().lower() == "download_exception"
        else None
    )
    if episode_reason is not None:
        return episode_reason
    if error_type.strip().lower() in {
        "science_session_ownership_unavailable",
        "science_acquisition_disabled",
        "science_lifecycle_guard_unavailable",
    }:
        return public_error_message(error_type, raw_reason, context=context)
    if error_type.strip().lower() == "publisher_human_verification_required":
        if _is_wiley_result({"error": raw_reason}):
            return _wiley_verification_reason(raw_reason)
        if any(marker in reason.lower() for marker in _OBSOLETE_HUMAN_VERIFICATION_ASSERTIONS):
            reason = ""
        return _join_reason(_HUMAN_VERIFICATION_GUIDANCE_ZH, reason)
    if error_type.strip().lower() in {
        "publisher_paused",
        "publisher_queue_paused",
    } or raw_reason.startswith("Publisher queue paused:"):
        return _join_reason(
            "出版商队列已暂停；请管理员检查暂停原因，显式恢复该出版商队列后再重试。",
            reason,
        )
    if error_type.strip().lower() == "publisher_pdf_unavailable":
        return _join_reason(
            "本次未能取得经过验证的原始 PDF；不能据此判断出版商尚未发布全文。",
            reason,
        )
    if error_type.strip().lower() in {
        "aip_access_page_or_challenge",
        "science_access_page_or_challenge",
        "rsc_access_or_entitlement",
        "wiley_access_or_entitlement",
    }:
        return public_error_message(error_type, raw_reason, context=context)
    return public_error_message(error_type, reason, context=context)


def _is_publisher_queue_paused(raw_reason: str, error_type: str) -> bool:
    normalized_error_type = error_type.strip().lower()
    normalized = f"{normalized_error_type} {raw_reason}".lower()
    return normalized_error_type in {"publisher_paused", "publisher_queue_paused"} or (
        "publisher_queue_paused" in normalized or "queue paused" in normalized
    )


def _join_reason(action: str, raw_reason: str) -> str:
    # Raw causes remain diagnostic; never append them to the public explanation.
    return action


def _request_detail_line(*, request_id: str, web_url: str) -> str:
    return f"🧭 详情：{request_id} · {web_url}"


def _watcher_issue_message(runtime: DeliveryRuntime, *, reason: str) -> str:
    if reason != "等待服务器结果超时":
        reason = public_error_message("", reason)
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    return "\n".join(
        [
            "SciFetch 正在等待文件结果。",
            f"⚠️ 原因：{reason}",
            _request_detail_line(request_id=runtime.request_id, web_url=web_url),
        ]
    )


def _watcher_timeout_message(runtime: DeliveryRuntime, delivered_file_ids: set[str]) -> str:
    if not delivered_file_ids:
        return _watcher_issue_message(runtime, reason="等待服务器结果超时")
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    return "\n".join(
        [
            "SciFetch 已发送可用文件，但终态确认超时。",
            "⚠️ 已发送的文件不会重复发送；稍后会继续核对剩余结果。",
            _request_detail_line(request_id=runtime.request_id, web_url=web_url),
        ]
    )


def _watcher_auth_required_message(
    runtime: DeliveryRuntime,
    delivered_file_ids: set[str],
) -> str:
    web_url = _absolute_url(runtime.state.server_url, f"/requests/{runtime.request_id}")
    headline = (
        "SciFetch 已发送可用文件，但需要重新登录后确认剩余结果。"
        if delivered_file_ids
        else "SciFetch 需要重新登录后继续获取文件。"
    )
    return "\n".join(
        [
            headline,
            "⚠️ 原因：登录凭据已失效，请使用 /scifetch login 重新配对。",
            _request_detail_line(request_id=runtime.request_id, web_url=web_url),
        ]
    )


def _scrub_public_text(value: str, *, max_length: int = 240) -> str:
    text = " ".join(value.split())
    replacements = (
        (r"Bearer\s+[^\s]+", "Bearer <redacted>"),
        (r"token=[^&\s]+", "token=<redacted>"),
        (r"(?:(?:/Users|/private|/tmp|/var|/Volumes)/[^\s]+)", "<server-path>"),
        # Quoted paths have an explicit end; bare paths may contain spaces.
        # Redact a bare path through the next field delimiter to hide its tail.
        (
            r"""(?:"[A-Za-z]:\\[^"\r\n]*"|'[A-Za-z]:\\[^\r\n"<>|]*'|[A-Za-z]:\\[^\r\n"<>|]*)""",
            "<server-path>",
        ),
    )
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    if len(text) > max_length:
        return f"{text[: max_length - 1].rstrip()}..."
    return text


def _scrub_terminal_text(value: str) -> str:
    lines = [_scrub_public_text(line, max_length=2000) for line in value.splitlines()]
    return "\n".join(lines)[:8000].strip()


def _safe_log_value(value: str) -> str:
    normalized = value.strip().lower()
    known = (
        _DOCUMENT_TERMINAL_ERROR_KINDS
        | _DOCUMENT_RETRYABLE_ERROR_KINDS
        | _QQBOT_DEFERRED_ERROR_KINDS
        | {"contract", "transport"}
    )
    if not normalized:
        return ""
    return normalized if normalized in known else "other"


def _async_client(runtime: DeliveryRuntime) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=runtime.state.server_url.rstrip("/"),
        timeout=httpx.Timeout(10.0, read=None),
        transport=runtime.transport,
        trust_env=False,
    )


def _auth_headers(runtime: DeliveryRuntime) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {runtime.state.access_token}",
        "X-Scifetch-Device-ID": runtime.state.device_id,
        "X-Client-ID": "hermes-scifetch-paper",
    }
    headers.update(_principal_headers(runtime.state))
    return headers


def _principal_headers(state: PluginState) -> dict[str, str]:
    headers: dict[str, str] = {}
    principal = state_principal_fields(state)
    if platform := principal.get("platform"):
        headers["X-Scifetch-Platform"] = platform
    if platform_user_id := principal.get("platform_user_id"):
        headers["X-Scifetch-Platform-User-ID"] = platform_user_id
    if chat_id := principal.get("chat_id"):
        headers["X-Scifetch-Chat-ID"] = chat_id
    if state.trust_mode:
        headers["X-Scifetch-Trust-Mode"] = state.trust_mode
    if state.installation_id:
        headers["X-Scifetch-Installation-ID"] = state.installation_id
    return headers


def _background_scheduler(
    kwargs: Mapping[str, object],
) -> Callable[[Coroutine[Any, Any, object]], object] | None:
    for name in ("create_task", "start_background_task", "schedule_task"):
        candidate = kwargs.get(name)
        if callable(candidate):
            return candidate
    return None


def _schedule_coroutine(coroutine: Coroutine[Any, Any, object]) -> bool:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        thread = threading.Thread(target=_run_in_thread, args=(coroutine,), daemon=True)
        thread.start()
        return True
    loop.create_task(coroutine)
    return True


def _run_in_thread(coroutine: Coroutine[Any, Any, object]) -> None:
    try:
        asyncio.run(coroutine)
    except Exception:
        logger.warning("scifetch-paper watcher stopped", exc_info=True)


def _state_path_from_kwargs(kwargs: Mapping[str, object]) -> Path | None:
    value = kwargs.get("state_path")
    if isinstance(value, Path):
        return value
    try:
        return resolve_state_path()
    except Exception:
        logger.debug("failed to resolve scifetch-paper state path", exc_info=True)
        return None


def _pending_path_from_state_path(state_path: Path | None) -> Path | None:
    return resolve_pending_path(state_path) if state_path is not None else None


def _persist_pending_delivery(
    *,
    pending_path: Path | None,
    request_id: str,
    state: PluginState,
    target: DeliveryTarget,
) -> None:
    if pending_path is None or not target.chat_id:
        return
    try:
        PendingDeliveryStore(pending_path).upsert(
            request_id=request_id,
            device_id=state.device_id,
            platform=_platform_key(target.platform) or "hermes",
            chat_id=target.chat_id,
            channel=f"hermes_{_platform_key(target.platform) or 'unknown'}",
            user_id=target.user_id,
        )
    except Exception:
        logger.warning("failed to persist SciFetch pending delivery", exc_info=True)


def _pending_store(runtime: DeliveryRuntime) -> PendingDeliveryStore | None:
    pending_path = runtime.pending_path or _pending_path_from_state_path(runtime.state_path)
    if pending_path is None:
        return None
    return PendingDeliveryStore(pending_path)


def _ensure_pending_record(
    runtime: DeliveryRuntime,
    store: PendingDeliveryStore | None,
) -> PendingDelivery | None:
    if store is None:
        return None
    try:
        record = store.get(runtime.request_id)
        if record is not None:
            return record
        if not runtime.target.chat_id:
            return None
        return store.upsert(
            request_id=runtime.request_id,
            device_id=runtime.state.device_id,
            platform=_platform_key(runtime.target.platform) or "hermes",
            chat_id=runtime.target.chat_id,
            channel=f"hermes_{_platform_key(runtime.target.platform) or 'unknown'}",
            user_id=runtime.target.user_id,
        )
    except Exception:
        logger.warning("failed to read SciFetch pending delivery", exc_info=True)
        return None


def _target_from_kwargs(kwargs: Mapping[str, object]) -> DeliveryTarget:
    event = kwargs.get("event")
    source = getattr(event, "source", None) if event is not None else None
    platform = _platform_key(
        _first_value(
            kwargs.get("delivery_platform"),
            kwargs.get("platform"),
            getattr(source, "platform", ""),
        )
    )
    chat_id = _first_string(
        kwargs.get("delivery_chat_id"),
        kwargs.get("chat_id"),
        getattr(source, "chat_id", ""),
    )
    user_id = _first_string(
        kwargs.get("delivery_user_id"),
        kwargs.get("platform_user_id"),
        kwargs.get("user_id"),
        getattr(source, "user_id", ""),
    )
    chat_type = _first_string(
        kwargs.get("delivery_chat_type"),
        kwargs.get("chat_type"),
        kwargs.get("channel_type"),
        getattr(source, "chat_type", ""),
        getattr(source, "channel_type", ""),
    )
    hermes = kwargs.get("hermes")
    gateway = kwargs.get("gateway")
    hermes_version = _first_string(
        kwargs.get("delivery_hermes_version"),
        kwargs.get("hermes_version"),
        getattr(hermes, "version", ""),
        getattr(gateway, "version", ""),
    )
    return DeliveryTarget(
        platform=platform or "hermes",
        chat_id=chat_id,
        user_id=user_id,
        chat_type=chat_type.lower(),
        hermes_version=hermes_version,
        capabilities=_capabilities_from_kwargs(kwargs),
    )


def _capabilities_from_kwargs(kwargs: Mapping[str, object]) -> DeliveryCapabilities | None:
    value = _first_value(kwargs.get("delivery_capabilities"), kwargs.get("capabilities"))
    if value is None:
        return None
    document_upload = _optional_bool(_capability_field(value, "document_upload"))
    max_document_bytes = _optional_int(_capability_field(value, "max_document_bytes"))
    supports_local_path = _optional_bool(_capability_field(value, "supports_local_path"))
    supports_remote_url = _optional_bool(_capability_field(value, "supports_remote_url"))
    if (
        document_upload is None
        and max_document_bytes is None
        and supports_local_path is None
        and supports_remote_url is None
    ):
        return None
    return DeliveryCapabilities(
        document_upload=document_upload,
        max_document_bytes=max_document_bytes,
        supports_local_path=supports_local_path,
        supports_remote_url=supports_remote_url,
    )


def _capability_field(value: object, field: str) -> object:
    if isinstance(value, Mapping):
        return value.get(field)
    return getattr(value, field, None)


def _queue_key(runtime: DeliveryRuntime) -> str:
    return _queue_key_for_target(runtime.target, fallback=runtime.state.device_id)


def _queue_key_for_target(target: DeliveryTarget, *, fallback: str = "hermes") -> str:
    return f"{_platform_key(target.platform) or 'hermes'}:{target.chat_id or fallback}"


def _watcher_key(runtime: DeliveryRuntime) -> str:
    identity = (
        runtime.state.server_url.rstrip("/"),
        runtime.state.device_id,
        runtime.state.installation_id,
        runtime.request_id,
        _canonical_delivery_platform(runtime.target.platform),
        runtime.target.chat_id,
        runtime.target.user_id,
        runtime.target.chat_type,
        runtime.target.thread_id,
    )
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def _document_sender_from_kwargs(
    kwargs: Mapping[str, object],
    target: DeliveryTarget,
) -> Callable[[str], object] | None:
    del target
    sender = kwargs.get("send_document")
    if not callable(sender) or _is_base_document_fallback(sender):
        return None
    return sender


def _document_sender_for_pending(
    kwargs: Mapping[str, object],
    target: DeliveryTarget,
    invocation_target: DeliveryTarget,
) -> Callable[[str], object] | None:
    if not _same_delivery_target(target, invocation_target):
        return None
    sender = kwargs.get("send_document")
    if not callable(sender) or _is_base_document_fallback(sender):
        return None
    return sender


def _text_sender_from_kwargs(
    kwargs: Mapping[str, object],
    target: DeliveryTarget,
) -> Callable[[str], object] | None:
    del target
    sender = kwargs.get("send_text")
    return sender if callable(sender) else None


def _text_sender_for_pending(
    kwargs: Mapping[str, object],
    target: DeliveryTarget,
    invocation_target: DeliveryTarget,
) -> Callable[[str], object] | None:
    if not _same_delivery_target(target, invocation_target):
        return None
    sender = kwargs.get("send_text")
    return sender if callable(sender) else None


def _same_delivery_target(left: DeliveryTarget, right: DeliveryTarget) -> bool:
    return bool(
        left.chat_id
        and left.chat_id == right.chat_id
        and _platform_key(left.platform) == _platform_key(right.platform)
        and (not left.user_id or bool(right.user_id and left.user_id == right.user_id))
    )


async def _record_last_reconnect(runtime: DeliveryRuntime) -> None:
    if runtime.state_path is None:
        return
    try:
        await asyncio.to_thread(
            record_sse_reconnect,
            state_path=runtime.state_path,
            fallback_state=runtime.state,
            reconnected_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception:
        logger.debug("failed to persist last_sse_reconnect", exc_info=True)


def _api_error_message(response: httpx.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return f"SciFetch server returned HTTP {response.status_code}."
    if isinstance(data, dict):
        message = data.get("message")
        if isinstance(message, str) and message:
            return message
        details = data.get("details")
        if isinstance(details, dict):
            reason = details.get("reason")
            if isinstance(reason, str) and reason:
                return reason
    return f"SciFetch server returned HTTP {response.status_code}."


def _api_error_message_from_body(response: httpx.Response, body: bytes) -> str:
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return f"SciFetch server returned HTTP {response.status_code}."
    if isinstance(data, dict):
        message = data.get("message")
        if isinstance(message, str) and message:
            return message
        details = data.get("details")
        if isinstance(details, dict):
            reason = details.get("reason")
            if isinstance(reason, str) and reason:
                return reason
    return f"SciFetch server returned HTTP {response.status_code}."


def _response_reason_from_body(body: bytes) -> str:
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return ""
    if not isinstance(data, dict):
        return ""
    details = data.get("details")
    if not isinstance(details, dict):
        return ""
    reason = details.get("reason")
    return reason if isinstance(reason, str) else ""


def _response_reason(response: httpx.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return ""
    if not isinstance(data, dict):
        return ""
    details = data.get("details")
    if not isinstance(details, dict):
        return ""
    reason = details.get("reason")
    return reason if isinstance(reason, str) else ""


def _absolute_url(server_url: str, path_or_url: str) -> str:
    if path_or_url.startswith(("http://", "https://")):
        return path_or_url
    return urljoin(f"{server_url.rstrip('/')}/", path_or_url.lstrip("/"))


def _signed_download_url(server_url: str, path_or_url: str) -> str:
    candidate = urljoin(f"{server_url.rstrip('/')}/", path_or_url)
    if _http_origin(candidate) != _http_origin(server_url):
        raise ValueError("download URL must use the configured SciFetch server origin")
    return candidate


def _http_origin(url: str) -> tuple[str, str, int]:
    if any(ord(character) <= 0x20 for character in url):
        raise ValueError("URL contains whitespace or control characters")
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower().rstrip(".")
    if scheme not in {"http", "https"} or not host:
        raise ValueError("URL must use HTTP(S) with a host")
    if parsed.username or parsed.password:
        raise ValueError("URL userinfo is not allowed")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("URL port is invalid") from exc
    return scheme, host, port or (443 if scheme == "https" else 80)


def _callable_or_none(value: object) -> Callable[[str], object] | None:
    return value if callable(value) else None


async def _maybe_await(value: object) -> object:
    if inspect.isawaitable(value):
        return await value
    return value


def _first_string(*values: object) -> str:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return ""


def _first_value(*values: object) -> object:
    for value in values:
        if value is not None and value != "":
            return value
    return ""


def _platform_key(value: object) -> str:
    raw = getattr(value, "value", value)
    if isinstance(raw, str) and raw:
        return raw.lower()
    name = getattr(value, "name", "")
    if isinstance(name, str) and name:
        return name.lower()
    return str(raw).lower() if raw else ""


def _is_base_document_fallback(send_document: Callable[..., object]) -> bool:
    func = getattr(send_document, "__func__", send_document)
    qualname = getattr(func, "__qualname__", "")
    return isinstance(qualname, str) and qualname.endswith("BasePlatformAdapter.send_document")


def _string_value(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else ""


def _int_value(data: Mapping[str, object], key: str) -> int | None:
    value = data.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _needs_refresh(expires_at: str) -> bool:
    parsed = _parse_datetime(expires_at)
    if parsed is None:
        return True
    return parsed <= datetime.now(timezone.utc) + timedelta(seconds=60)


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _native_sender_from_kwargs(
    kwargs: Mapping[str, object],
) -> Callable[[str, Callable[[], bool]], object] | None:
    sender = kwargs.get("native_document_sender")
    if not callable(sender):
        return None

    def invoke(path: str, before_native: Callable[[], bool]) -> object:
        return sender(path, before_native)

    return invoke
