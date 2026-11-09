"""Gateway hook for paper request submission in the scifetch-paper plugin."""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import threading
from collections.abc import Awaitable, Coroutine, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from urllib.parse import urljoin, urlparse

import httpx

from .auth import ensure_fresh_state
from .client import SciFetchClientConfig, SciFetchPluginClient, SciFetchPluginError
from .delivery import resume_pending_deliveries, schedule_request_watcher, schedule_text_send
from .home_delivery import (
    remember_personal_gateway_context,
    remember_personal_turn_context,
    stage_deterministic_route,
)
from .pending import PendingDeliveryStore, PendingDeliveryStoreError, resolve_pending_path
from .response_output import discard_pending_tool_response
from .state import PluginState, StateFileError, load_state, resolve_state_path
from .trust import (
    INVALID_TRUST_MODE,
    MULTI_USER_MODE,
    PERSONAL_INSTANCE_MODE,
    TrustedPrincipal,
    configured_trust_mode,
    effective_state_trust_mode,
    state_matches_personal_instance,
    state_matches_principal,
    trusted_principal_from_context,
)

ALLOW_ACTION: Final = "allow"
SKIP_ACTION: Final = "skip"
_FORCE_PREFIXES: Final = ("!!", "！！", "!！", "！!")
_LOGIN_HINT: Final = "请先创建 SciFetch 配对码，再运行 /scifetch login <pairing_code>。"
_AUTH_REQUIRED_MESSAGE: Final = (
    "SciFetch 暂时无法提交文献请求。\n"
    "🔐 原因：登录凭据或账号授权已失效。\n"
    "🧭 操作：请先通过 SciFetch Web/API 创建新的配对码，再运行 "
    "/scifetch login <pairing_code>。\n"
    "📄 状态：本次没有创建新的文献请求。"
)
_URL_RE: Final = re.compile(r"https?://[^\s,;，；、\n\t\x00-\x1f]+", re.IGNORECASE)
_DOI_RE: Final = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)
_ARXIV_ID_RE: Final = re.compile(r"\b\d{4}\.\d{4,5}(?:v\d+)?\b")
_PII_RE: Final = re.compile(r"\bS\d[A-Z0-9]{15,}\b", re.IGNORECASE)
_STRICT_DOI_RE: Final = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)
_STRICT_ARXIV_RE: Final = re.compile(
    r"(?:arxiv:)?(?:\d{4}\.\d{4,5}|[a-z-]+(?:\.[A-Z]{2})?/\d{7})(?:v\d+)?",
    re.IGNORECASE,
)
_STRICT_PII_RE: Final = re.compile(r"S\d[A-Z0-9]{15,}", re.IGNORECASE)
_CJK_RE: Final = re.compile(r"[一-鿿]")
_WORD_RE: Final = re.compile(r"[A-Za-z][A-Za-z0-9\-]+")
_YEAR_RE: Final = re.compile(r"\b(?:19|20)\d{2}\b")
_CITATION_CUE_RE: Final = re.compile(
    r"\b(?:et\s+al\.?|vol\.?|volume|issue|journal|nature|science|cell|phys\.?|"
    r"materials?|letters?|communications?|proceedings?|angew|jacs|acs|wiley|"
    r"springer)\b",
    re.IGNORECASE,
)
_PUBLISHER_HOST_PARTS: Final = (
    "nature.com",
    "science.org",
    "sciencedirect.com",
    "springer.com",
    "pubs.acs.org",
    "aps.org",
    "journals.aps.org",
    "aip.scitation.org",
    "onlinelibrary.wiley.com",
    "rsc.org",
    "arxiv.org",
    "biorxiv.org",
    "chemrxiv.org",
)
_STRICT_PUBLISHER_HOSTS: Final = frozenset(
    {
        "nature.com",
        "www.nature.com",
        "science.org",
        "www.science.org",
        "sciencedirect.com",
        "www.sciencedirect.com",
        "linkinghub.elsevier.com",
        "link.springer.com",
        "pubs.acs.org",
        "journals.aps.org",
        "pubs.aip.org",
        "onlinelibrary.wiley.com",
        "pubs.rsc.org",
        "iopscience.iop.org",
        "ieeexplore.ieee.org",
        "www.tandfonline.com",
        "pubmed.ncbi.nlm.nih.gov",
        "pnas.org",
        "www.pnas.org",
        "cell.com",
        "www.cell.com",
        "biorxiv.org",
        "www.biorxiv.org",
        "chemrxiv.org",
        "www.chemrxiv.org",
    }
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExplicitPaperOccurrence:
    text: str
    kind: str
    force: bool


__all__ = [
    "TrustedPrincipal",
    "handle_paper_message",
    "handle_post_auth_paper_context",
    "handle_pre_auth_paper_message",
    "is_paper_request",
    "parse_explicit_paper_payload",
    "submit_paper_text",
    "trusted_principal_from_context",
]


def handle_paper_message(**kwargs: object) -> dict[str, str]:
    """Submit paper-like messages to SciFetch and skip normal Hermes dispatch."""
    return submit_paper_text(_event_text(kwargs), kwargs, require_paper_like=True)


def handle_pre_auth_paper_message(**kwargs: object) -> dict[str, str] | None:
    """Legacy pre-auth hook compatibility shim; never submits to SciFetch."""
    if configured_trust_mode() == PERSONAL_INSTANCE_MODE:
        text = _event_text(kwargs)
        occurrences = parse_explicit_paper_payload(text)
        if occurrences is not None and stage_deterministic_route(kwargs, text):
            # Preserve the existing model/tool bridge if another earlier hook
            # wins action selection and this rewrite is not applied.
            remember_personal_gateway_context(kwargs)
            return {"action": "rewrite", "text": f"/paper {text.strip()}"}
        if text and is_paper_request(text):
            remember_personal_gateway_context(kwargs)
    return None


def parse_explicit_paper_payload(text: str) -> tuple[ExplicitPaperOccurrence, ...] | None:
    """Parse only a complete ordered payload of explicit paper identifiers."""
    stripped = text.strip()
    if not stripped or stripped.startswith("/"):
        return None
    occurrences: list[ExplicitPaperOccurrence] = []
    for raw_line in stripped.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        value, force = _strip_force_prefix(line)
        kind = _strict_occurrence_kind(value)
        if not kind:
            return None
        occurrences.append(ExplicitPaperOccurrence(text=value, kind=kind, force=force))
    return tuple(occurrences) or None


def _strict_occurrence_kind(value: str) -> str:
    if not value or value.startswith(("!", "/")) or any(char.isspace() for char in value):
        return ""
    if _STRICT_DOI_RE.fullmatch(value):
        return "doi"
    if _STRICT_ARXIV_RE.fullmatch(value):
        return "arxiv_id"
    if _STRICT_PII_RE.fullmatch(value):
        return "pii"
    parsed = urlparse(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return ""
    host = parsed.hostname.lower().rstrip(".")
    if host in {"doi.org", "dx.doi.org"} and _STRICT_DOI_RE.fullmatch(parsed.path.lstrip("/")):
        return "doi_url"
    if host == "mp.weixin.qq.com":
        return "weixin_url"
    if host in {"arxiv.org", "www.arxiv.org"} and (
        parsed.path.startswith("/abs/") or parsed.path.startswith("/pdf/")
    ):
        arxiv_value = parsed.path.split("/", 2)[-1].removesuffix(".pdf")
        return "arxiv_url" if _STRICT_ARXIV_RE.fullmatch(arxiv_value) else ""
    if host in _STRICT_PUBLISHER_HOSTS:
        return "publisher_url"
    return ""


def handle_post_auth_paper_context(**kwargs: object) -> dict[str, str] | None:
    """Steer authenticated personal-instance turns through the official tool."""
    discard_pending_tool_response(kwargs)
    if configured_trust_mode() != PERSONAL_INSTANCE_MODE:
        return None
    user_message = kwargs.get("user_message")
    if not isinstance(user_message, str) or not is_paper_request(user_message):
        return None
    remember_personal_turn_context(kwargs)
    return {
        "context": (
            "This authenticated Hermes profile is configured as one private SciFetch "
            "instance. Call scifetch_paper_request once for every paper-request occurrence "
            "in the authenticated user message, in order, including repeated identical "
            "occurrences; do not combine or deduplicate them. Set force=true for each "
            "occurrence prefixed by !!, ！！, !！ or ！!. Then copy all tool results verbatim in call order, "
            "preserving every emoji and line break. Do not paraphrase them or claim that a "
            "document was sent."
        )
    }


def submit_paper_text(
    raw_text: str,
    kwargs: Mapping[str, object],
    *,
    require_paper_like: bool,
) -> dict[str, str]:
    """Submit a text payload when it looks like a paper request."""
    runtime_kwargs = kwargs
    text, force = _strip_force_prefix(raw_text)
    if not text:
        return {"action": ALLOW_ACTION}
    if require_paper_like and not is_paper_request(text):
        return {"action": ALLOW_ACTION}
    if not require_paper_like and not is_paper_request(text):
        return _skip(
            "scifetch_not_paper_request",
            "未识别到可用的文献请求。",
            runtime_kwargs,
        )

    trust_mode = configured_trust_mode()
    if trust_mode == INVALID_TRUST_MODE:
        return _skip(
            "scifetch_trust_mode_invalid",
            "SciFetch 信任模式配置无效，请管理员检查当前实例配置。",
            runtime_kwargs,
        )
    principal = (
        trusted_principal_from_context(runtime_kwargs) if trust_mode == MULTI_USER_MODE else None
    )
    if trust_mode == MULTI_USER_MODE and principal is None:
        return _skip(
            "scifetch_trusted_principal_required",
            "多用户模式缺少官方可信发送者身份，本次未提交文献请求。",
            runtime_kwargs,
        )
    if principal is not None:
        runtime_kwargs = {
            **runtime_kwargs,
            "platform": principal.platform,
            "platform_user_id": principal.platform_user_id,
            "chat_id": principal.chat_id,
        }

    state_path = _state_path_from_kwargs(runtime_kwargs)
    state_result = _load_state_for_hook(runtime_kwargs)
    if isinstance(state_result, StateFileError):
        return _skip(
            "scifetch_state_corrupted",
            f"SciFetch 登录状态无法读取。 {_LOGIN_HINT}",
            runtime_kwargs,
        )
    if state_result is None:
        return _skip(
            "scifetch_login_required",
            f"SciFetch 尚未登录。 {_LOGIN_HINT}",
            runtime_kwargs,
        )

    state = state_result
    state_trust_mode = effective_state_trust_mode(state)
    if state_trust_mode != trust_mode:
        return _skip(
            "scifetch_trust_mode_mismatch",
            "当前登录与实例信任模式不匹配，请重新登录。",
            runtime_kwargs,
        )
    if trust_mode == PERSONAL_INSTANCE_MODE and not state_matches_personal_instance(state):
        return _skip(
            "scifetch_installation_binding_missing",
            "当前私人实例的登录绑定缺失或不一致。请重新登录。",
            runtime_kwargs,
        )
    if trust_mode == MULTI_USER_MODE and (
        principal is None or not state_matches_principal(state, principal)
    ):
        return _skip(
            "scifetch_principal_mismatch",
            "当前登录绑定到其他发送者，请从当前聊天重新登录。",
            runtime_kwargs,
        )

    client = _client(state.server_url, runtime_kwargs)
    try:
        state = ensure_fresh_state(client=client, state=state, state_path=state_path)
    except SciFetchPluginError as exc:
        if exc.status_code in {400, 401, 403}:
            _mark_pending_auth_required(state_path)
            return _skip(
                "scifetch_login_required",
                _AUTH_REQUIRED_MESSAGE,
                runtime_kwargs,
            )
        return _skip(
            "scifetch_submit_failed",
            f"SciFetch 请求未完成： {exc}",
            runtime_kwargs,
        )

    try:
        response = client.submit_paper_request(
            raw_text=text,
            access_token=state.access_token,
            device_id=state.device_id,
            client_request_id=_client_request_id(runtime_kwargs),
            platform=principal.platform if principal is not None else "",
            platform_user_id=principal.platform_user_id if principal is not None else "",
            chat_id=principal.chat_id if principal is not None else "",
            trust_mode=trust_mode,
            installation_id=state.installation_id,
            force=force,
        )
    except SciFetchPluginError as exc:
        if exc.status_code == 401:
            _mark_pending_auth_required(state_path)
            return _skip(
                "scifetch_login_required",
                _AUTH_REQUIRED_MESSAGE,
                runtime_kwargs,
            )
        return _skip(
            "scifetch_submit_failed",
            f"SciFetch 请求未完成： {exc}",
            runtime_kwargs,
        )

    resume_pending_deliveries(**runtime_kwargs)
    request_id = _string_value(response, "request_id")
    delivery_scheduled = schedule_request_watcher(
        request_id=request_id,
        state=state,
        kwargs=runtime_kwargs,
    )
    message = _ack_message(
        response,
        state.server_url,
        delivery_scheduled=delivery_scheduled,
    )
    return _skip("scifetch_handled", message, runtime_kwargs, request_id=request_id)


def _mark_pending_auth_required(state_path: Path) -> None:
    try:
        PendingDeliveryStore(resolve_pending_path(state_path)).mark_active_auth_required()
    except (OSError, PendingDeliveryStoreError):
        logger.warning("failed to mark SciFetch pending deliveries as auth-required", exc_info=True)


def _skip(
    reason: str,
    message: str,
    kwargs: Mapping[str, object],
    *,
    request_id: str = "",
) -> dict[str, str]:
    _maybe_send_text(kwargs, message)
    result = {
        "action": SKIP_ACTION,
        "reason": reason,
        "message": message,
    }
    if request_id:
        result["request_id"] = request_id
    return result


def _ack_message(
    response: dict[str, object],
    server_url: str,
    *,
    delivery_scheduled: bool = False,
) -> str:
    force = _response_force(response)
    lines = [
        (
            "SciFetch 已收到文献请求，将跳过缓存重新获取全文。"
            if force
            else "SciFetch 已收到文献请求，正在检查缓存并获取全文。"
        )
    ]
    query_summary = _query_summary(response)
    if query_summary:
        lines.append(f"🔎 识别：{query_summary}")
    resolution_summary = _resolution_summary(response)
    if resolution_summary:
        lines.append(f"🧠 解析：{resolution_summary}")
    lines.append(
        "♻️ 请求方式：强制重新下载（已识别标记，结果待确认）"
        if force
        else "♻️ 缓存策略：优先发送缓存，未命中再下载"
    )
    status = _request_status_label(_string_value(response, "status"))
    if delivery_scheduled:
        lines.append(f"📄 状态：{status}；完成后会发送文件或明确的下载降级结果。")
    else:
        lines.append(f"📄 状态：{status}；当前入口无法后台推送文件，请稍后打开详情页查看。")
    detail = _request_detail_line(response, server_url)
    if detail:
        lines.append(detail)
    return "\n".join(lines)


def _response_force(response: dict[str, object]) -> bool:
    query = _dict_value(response, "query")
    value = query.get("force")
    if isinstance(value, bool):
        return value
    value = response.get("force")
    return value if isinstance(value, bool) else False


def _request_status_label(status: str) -> str:
    normalized = status.strip().lower()
    if normalized in {"received", "queued", "pending"}:
        return "已排队"
    if normalized in {"running", "processing", "in_progress"}:
        return "正在处理"
    if normalized in {"done", "completed", "succeeded", "success"}:
        return "已完成"
    if normalized in {"failed", "error"}:
        return "获取失败"
    if normalized in {"cancelled", "canceled"}:
        return "已取消"
    return "已提交"


def _request_detail_line(response: dict[str, object], server_url: str) -> str:
    request_id = _string_value(response, "request_id")
    if not request_id:
        return ""
    web_url = urljoin(f"{server_url.rstrip('/')}/", f"requests/{request_id}")
    return f"🧭 详情：{request_id} · {web_url}"


def _query_summary(response: dict[str, object]) -> str:
    query = _dict_value(response, "query")
    detected_queries = query.get("detected_queries")
    if isinstance(detected_queries, list):
        labels = [
            label
            for candidate in detected_queries
            if isinstance(candidate, dict)
            for label in (_query_label(candidate),)
            if label
        ]
        if len(labels) > 1:
            return f"{labels[0]} 等 {len(labels)} 条"
        if len(labels) == 1:
            return labels[0]

    resolved = _dict_value(response, "resolved_query")
    return _query_label(resolved)


def _resolution_summary(response: dict[str, object]) -> str:
    resolution = _dict_value(response, "resolution")
    if resolution.get("method") != "llm":
        return ""
    resolved = _dict_value(resolution, "resolved_query") or _dict_value(response, "resolved_query")
    resolved_label = _query_label(resolved)
    if not resolved_label:
        return ""
    input_kind = resolution.get("input_kind")
    input_label = _resolution_input_label(input_kind if isinstance(input_kind, str) else "")
    provider = _string_value(resolution, "provider")
    suffix = f"（{provider}）" if provider else ""
    return f"{input_label}已解析为{resolved_label}{suffix}"


def _resolution_input_label(kind: str) -> str:
    labels = {
        "title": "标题",
        "citation_snippet": "引文片段",
        "unknown": "模糊文本",
    }
    return labels.get(kind, "模糊文本")


def _query_label(candidate: Mapping[str, object]) -> str:
    value = candidate.get("value")
    if not isinstance(value, str) or not value:
        return ""
    kind = candidate.get("kind")
    if not isinstance(kind, str) or not kind:
        return value
    labels = {
        "doi": "DOI",
        "url": "URL",
        "arxiv": "arXiv",
        "pii": "PII",
        "title": "标题",
    }
    return f"{labels.get(kind.lower(), kind)}：{value}"


def _load_state_for_hook(kwargs: Mapping[str, object]) -> PluginState | StateFileError | None:
    try:
        return load_state(_state_path_from_kwargs(kwargs))
    except StateFileError as exc:
        return exc


def _client(server_url: str, kwargs: Mapping[str, object]) -> SciFetchPluginClient:
    transport = kwargs.get("transport")
    return SciFetchPluginClient(
        SciFetchClientConfig(server_url=server_url),
        transport=transport if isinstance(transport, httpx.BaseTransport) else None,
    )


def _state_path_from_kwargs(kwargs: Mapping[str, object]) -> Path:
    value = kwargs.get("state_path")
    return resolve_state_path(explicit_path=value if isinstance(value, Path) else None)


def _strip_force_prefix(raw_text: str) -> tuple[str, bool]:
    text = raw_text.strip()
    if text.startswith(_FORCE_PREFIXES):
        return text[2:].lstrip(), True
    return text, False


def is_paper_request(text: str) -> bool:
    """Lightweight plugin-only pre-check; the server remains authoritative."""
    stripped = text.strip()
    if not stripped:
        return False
    if _DOI_RE.search(stripped) or _ARXIV_ID_RE.search(stripped) or _PII_RE.search(stripped):
        return True
    for match in _URL_RE.finditer(stripped):
        url = match.group(0).lower()
        if any(host_part in url for host_part in _PUBLISHER_HOST_PARTS):
            return True
        if "mp.weixin.qq.com/" in url:
            return True
    return _looks_like_paper_title(stripped) or _looks_like_citation_fragment(stripped)


def _looks_like_paper_title(text: str) -> bool:
    normalized = " ".join(text.strip().split())
    if not (25 <= len(normalized) <= 300):
        return False
    if normalized.startswith(("/", "!")) or normalized.endswith(("?", "？")):
        return False
    if _CJK_RE.search(normalized):
        return False
    lower = normalized.lower()
    if any(
        lower.startswith(prefix)
        for prefix in (
            "what ",
            "how ",
            "when ",
            "where ",
            "who ",
            "why ",
            "can you",
            "could you",
            "would you",
            "please ",
            "is it",
            "are you",
            "do you",
            "does ",
            "did you",
            "will you",
            "shall ",
        )
    ):
        return False
    words = _WORD_RE.findall(normalized)
    long_words = [word for word in words if len(word) >= 5]
    return len(words) >= 3 and len(long_words) >= 2


def _looks_like_citation_fragment(text: str) -> bool:
    normalized = " ".join(text.strip().split())
    if not (30 <= len(normalized) <= 900):
        return False
    if normalized.startswith(("/", "!")) or normalized.endswith(("?", "？")):
        return False
    if _CJK_RE.search(normalized):
        return False
    return bool(_YEAR_RE.search(normalized) and _CITATION_CUE_RE.search(normalized))


def _event_text(kwargs: Mapping[str, object]) -> str:
    event = kwargs.get("event")
    return _string_attr(event, "text") if event is not None else ""


def _client_request_id(kwargs: Mapping[str, object]) -> str:
    override = kwargs.get("client_request_id")
    if isinstance(override, str) and override:
        return override
    event = kwargs.get("event")
    source = getattr(event, "source", None) if event is not None else None
    principal = trusted_principal_from_context(kwargs)
    platform = (
        (_platform_value(getattr(source, "platform", "")) if source is not None else "")
        or _platform_value(kwargs.get("platform"))
        or (principal.platform if principal else "")
    )
    chat_id = (
        (_string_attr(source, "chat_id") if source is not None else "")
        or _mapping_string_value(kwargs, "chat_id")
        or (principal.chat_id if principal else "")
    )
    message_id = _first_string_attr(
        event,
        source,
        kwargs,
        names=("message_id", "msg_id", "id"),
    )
    parts = [part for part in (platform, chat_id, message_id) if part]
    return ":".join(parts)


def _maybe_send_text(kwargs: Mapping[str, object], message: str) -> None:
    if kwargs.get("suppress_immediate_text") is True:
        return
    if _should_queue_text_send(kwargs) and schedule_text_send(kwargs=kwargs, message=message):
        return
    sender = kwargs.get("send_text")
    if not callable(sender):
        return
    try:
        result = sender(message)
        if inspect.isawaitable(result):
            _schedule_awaitable(result)
    except Exception:
        logger.debug("scifetch-paper send_text callback failed", exc_info=True)


def _should_queue_text_send(kwargs: Mapping[str, object]) -> bool:
    platform, chat_id = _message_target(kwargs)
    return bool(chat_id and platform.lower() in {"wechat", "weixin"})


def _message_target(kwargs: Mapping[str, object]) -> tuple[str, str]:
    event = kwargs.get("event")
    source = getattr(event, "source", None) if event is not None else None
    platform = _platform_value(kwargs.get("platform")) or _platform_value(
        getattr(source, "platform", "")
    )
    chat_id = _mapping_string_value(kwargs, "chat_id") or _string_attr(source, "chat_id")
    return platform, chat_id


def _schedule_awaitable(awaitable: Awaitable[object]) -> None:
    async def _consume() -> None:
        await awaitable

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        thread = threading.Thread(target=_run_awaitable, args=(_consume(),), daemon=True)
        thread.start()
        return

    task = loop.create_task(_consume())
    task.add_done_callback(_log_async_send_failure)


def _run_awaitable(coroutine: Coroutine[object, object, None]) -> None:
    try:
        asyncio.run(coroutine)
    except Exception:
        logger.debug("scifetch-paper async send callback failed", exc_info=True)


def _log_async_send_failure(task: asyncio.Task[None]) -> None:
    try:
        task.result()
    except Exception:
        logger.debug("scifetch-paper async send callback failed", exc_info=True)


def _first_string_attr(*objects: object, names: tuple[str, ...]) -> str:
    for obj in objects:
        if obj is None:
            continue
        for name in names:
            value = _string_attr(obj, name)
            if value:
                return value
    return ""


def _string_attr(obj: object, name: str) -> str:
    value = obj.get(name, "") if isinstance(obj, Mapping) else getattr(obj, name, "")
    return value if isinstance(value, str) else ""


def _mapping_string_value(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else ""


def _platform_value(value: object) -> str:
    if isinstance(value, str):
        return value
    enum_value = getattr(value, "value", "")
    return enum_value if isinstance(enum_value, str) else ""


def _dict_value(data: dict[str, object], key: str) -> dict[str, object]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _string_value(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else ""
