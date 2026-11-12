"""Slash-command handlers for the scifetch-paper Hermes plugin."""

from __future__ import annotations

import logging
import shlex
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from .auth import ensure_fresh_state
from .client import SciFetchClientConfig, SciFetchPluginClient, SciFetchPluginError
from .delivery import build_request_status_message, resume_pending_deliveries
from .detector import parse_explicit_paper_payload, submit_paper_text
from .environment import profile_value, runtime_version
from .home_delivery import claim_deterministic_route
from .pending import PendingDeliveryStore, resolve_pending_path
from .response_output import release_ack_gate, with_command_ack_gate
from .state import (
    PluginState,
    StateFileError,
    clear_state,
    load_state,
    resolve_state_path,
    save_state,
)
from .trust import (
    INVALID_TRUST_MODE,
    MULTI_USER_MODE,
    PERSONAL_INSTANCE_MODE,
    configured_trust_mode,
    effective_state_trust_mode,
    new_installation_id,
    state_matches_personal_instance,
    state_matches_principal,
    state_principal_fields,
    trusted_principal_from_context,
)

PLUGIN_VERSION = "0.2.32"
logger = logging.getLogger(__name__)
LOGIN_HINT = "请先创建 SciFetch 配对码，再运行 /scifetch login <pairing_code>。"
TRUSTED_CONTEXT_UNAVAILABLE = (
    "trusted_command_context_unavailable: 多用户模式缺少官方可信的命令身份上下文，本次未提交请求。"
)
INVALID_TRUST_MODE_MESSAGE = "invalid_trust_mode: 信任模式配置无效，请管理员检查当前实例配置。"
TRUST_BINDING_MISMATCH = (
    "trust_binding_mismatch: 当前登录与实例信任模式或发送者身份不匹配，请从当前实例重新登录。"
)


def handle_scifetch_command(raw_args: str = "", **kwargs: object) -> str:
    """Handle `/scifetch login|logout|status` without local permission decisions."""
    args = _split_args(raw_args)
    command = args[0].lower() if args else "status"
    rest = args[1:]

    if command in {"login", "logout", "status"} and (trust_error := _command_trust_error(kwargs)):
        return trust_error
    if command == "login":
        return _handle_login(rest, kwargs)
    if command == "logout":
        return _handle_logout(kwargs)
    if command == "status":
        return _handle_status(kwargs)
    return "Usage: /scifetch login <pairing_code> [server_url] | status | logout"


def handle_paper_command(raw_args: str = "", **kwargs: object) -> str:
    """Submit an explicit `/paper` request through the Step 5 server endpoint."""
    if trust_error := _command_trust_error(kwargs):
        return trust_error
    args = _split_args(raw_args)
    command = args[0].lower() if args else ""
    if command in {"status", "replay", "summary"}:
        return _handle_paper_status(args[1:], kwargs)
    if command == "retry":
        return _handle_paper_retry(args[1:], kwargs)
    if raw_args.strip():
        if configured_trust_mode() != PERSONAL_INSTANCE_MODE:
            return TRUSTED_CONTEXT_UNAVAILABLE
        occurrences = parse_explicit_paper_payload(raw_args)
        route_kwargs = claim_deterministic_route(raw_args, source=kwargs.get("source"))
        if occurrences is None or route_kwargs is None:
            return TRUSTED_CONTEXT_UNAVAILABLE
        runtime_kwargs = with_command_ack_gate(route_kwargs)
        try:
            resume_pending_deliveries(**runtime_kwargs)
            base_request_id = str(runtime_kwargs.get("client_request_id", ""))
            messages: list[str] = []
            for index, occurrence in enumerate(occurrences):
                occurrence_kwargs = {
                    **runtime_kwargs,
                    "client_request_id": f"{base_request_id}:{index}"
                    if base_request_id
                    else str(index),
                }
                query = f"!! {occurrence.text}" if occurrence.force else occurrence.text
                response = submit_paper_text(query, occurrence_kwargs, require_paper_like=False)
                messages.append(response.get("message", "本次未提交文献请求。"))
            return "\n\n".join(messages)
        finally:
            release_ack_gate(runtime_kwargs)

    state_result = _load_state_for_command(kwargs)
    if isinstance(state_result, StateFileError):
        return f"SciFetch 登录状态无法读取。 {LOGIN_HINT}"
    if state_result is None:
        return f"SciFetch 尚未登录。 {LOGIN_HINT}"
    return (
        "Usage: /paper <doi|url|title> | /paper status <request_id>. "
        "Completed files are delivered through Hermes when available."
    )


def handle_personal_digest_command(raw_args: str = "", **kwargs: object) -> str:
    """Pull personal paper digest notifications from the server-authoritative API."""
    args = _split_args(raw_args)
    command = args[0].lower() if args else "due"
    rest = args[1:]

    if command in {"help", "-h", "--help"}:
        return _personal_digest_usage()
    if trust_error := _command_trust_error(kwargs):
        return trust_error
    if command in {"due", "pull", "list"}:
        return _handle_personal_digest_due(rest, kwargs)
    if command == "status":
        return _handle_personal_digest_status(kwargs)
    return _personal_digest_usage()


def _handle_login(args: list[str], kwargs: Mapping[str, object]) -> str:
    trust_mode = configured_trust_mode()
    if trust_mode == INVALID_TRUST_MODE:
        return INVALID_TRUST_MODE_MESSAGE
    principal = trusted_principal_from_context(kwargs) if trust_mode == MULTI_USER_MODE else None
    if trust_mode == MULTI_USER_MODE and principal is None:
        return TRUSTED_CONTEXT_UNAVAILABLE
    if not args:
        return f"Missing pairing code. {LOGIN_HINT}"

    pairing_code = args[0].strip()
    server_url = args[1].strip() if len(args) > 1 else _server_url_from_env()
    if not server_url:
        return "Missing SciFetch server URL. Set SCIFETCH_SERVER_URL or pass it after the pairing code."

    state_path = _state_path_from_kwargs(kwargs)
    client = _client(server_url, kwargs)
    installation_id = new_installation_id() if trust_mode == PERSONAL_INSTANCE_MODE else ""
    platform = principal.platform if principal is not None else "hermes"
    platform_user_id = principal.platform_user_id if principal is not None else ""
    chat_id = principal.chat_id if principal is not None else ""
    try:
        result = client.pair_device(
            pairing_code=pairing_code,
            platform=platform,
            platform_user_id=platform_user_id,
            chat_id=chat_id,
            trust_mode=trust_mode,
            installation_id=installation_id,
            plugin_version=PLUGIN_VERSION,
            display_name=_principal_display_name(
                platform,
                platform_user_id or installation_id[-8:],
                chat_id,
            ),
        )
    except SciFetchPluginError as exc:
        return f"SciFetch 登录未完成： {exc}"

    device = _dict_value(result, "device")
    user = _dict_value(result, "user")
    state = PluginState(
        server_url=server_url.rstrip("/"),
        device_id=_string_value(device, "device_id"),
        access_token=_string_value(result, "access_token"),
        refresh_token=_string_value(result, "refresh_token"),
        access_expires_at=_string_value(result, "access_expires_at"),
        refresh_expires_at=_string_value(result, "refresh_expires_at"),
        trust_mode=trust_mode,
        installation_id=installation_id,
        platform=platform,
        platform_user_id=platform_user_id,
        chat_id=chat_id,
        last_seen=_now_iso(),
    )
    if not state.is_authenticated:
        return "SciFetch 登录未完成：服务未返回完整的设备凭据。"
    save_state(state_path, state)
    try:
        PendingDeliveryStore(resolve_pending_path(state_path)).reactivate_auth_required(
            device_id=state.device_id
        )
    except Exception:
        logger.warning("failed to reactivate SciFetch deliveries after login", exc_info=True)
    display_name = _string_value(user, "display_name") or _string_value(user, "user_id")
    role = _string_value(user, "role") or "user"
    valid_until = _string_value(user, "valid_until") or "unknown"
    return (
        f"SciFetch login successful for {display_name} ({role}). "
        f"Device {state.device_id} uses trust_mode={trust_mode} until {valid_until}."
    )


def _handle_status(kwargs: Mapping[str, object]) -> str:
    state_result = _load_state_for_command(kwargs)
    if isinstance(state_result, StateFileError):
        return f"SciFetch 登录状态无法读取。 {LOGIN_HINT}"
    if state_result is None:
        return f"SciFetch 尚未登录。 {LOGIN_HINT}"

    state = state_result
    if trust_error := _state_trust_error(state, kwargs):
        return trust_error
    client = _client(state.server_url, kwargs)
    try:
        state = ensure_fresh_state(
            client=client, state=state, state_path=_state_path_from_kwargs(kwargs)
        )
        me = client.get_me(
            access_token=state.access_token,
            device_id=state.device_id,
            trust_mode=state.trust_mode,
            installation_id=state.installation_id,
            **state_principal_fields(state),
        )
        health = client.health()
    except SciFetchPluginError as exc:
        return f"SciFetch 状态暂时无法读取： {exc}"

    user = _dict_value(me, "user")
    device = _dict_value(me, "device")
    display_name = _string_value(user, "display_name") or _string_value(user, "user_id")
    role = _string_value(user, "role") or "user"
    status = _string_value(user, "status") or "unknown"
    valid_until = _string_value(user, "valid_until") or "unknown"
    server_ok = "ok" if health.get("ok") is True else "unhealthy"
    api_version = _string_value(me, "api_version") or "unknown"
    hermes_version = _hermes_version(kwargs)
    reconnect = state.last_sse_reconnect or "never"
    trust_mode = configured_trust_mode()
    if trust_mode == PERSONAL_INSTANCE_MODE:
        gateway_submission = "available_personal_instance"
    elif trusted_principal_from_context(kwargs) is not None:
        gateway_submission = "available_multi_user_context"
    else:
        gateway_submission = "awaiting_official_multi_user_context"
    return (
        f"SciFetch connected to {state.server_url}. "
        f"User {display_name} ({role}, {status}) valid until {valid_until}. "
        f"Device {_string_value(device, 'device_id') or state.device_id}. "
        f"Server health: {server_ok}. "
        f"API: {api_version}. Plugin: {PLUGIN_VERSION}. Hermes: {hermes_version}. "
        f"trust_mode={effective_state_trust_mode(state)}. "
        f"gateway_submission={gateway_submission}. "
        f"Access token expires: {state.access_expires_at or 'unknown'}. "
        f"Last SSE reconnect: {reconnect}."
    )


def _handle_logout(kwargs: Mapping[str, object]) -> str:
    state_path = _state_path_from_kwargs(kwargs)
    state_result = _load_state_for_command(kwargs)
    if isinstance(state_result, StateFileError):
        clear_state(state_path)
        return "SciFetch local login state was corrupted and has been cleared."
    if state_result is None:
        return "SciFetch is already logged out."

    state = state_result
    if trust_error := _state_trust_error(state, kwargs):
        return trust_error
    client = _client(state.server_url, kwargs)
    revoke_error = ""
    try:
        state = ensure_fresh_state(client=client, state=state, state_path=state_path)
        client.revoke_device(
            access_token=state.access_token,
            device_id=state.device_id,
            trust_mode=state.trust_mode,
            installation_id=state.installation_id,
            **state_principal_fields(state),
        )
    except SciFetchPluginError as exc:
        revoke_error = f" 服务端设备撤销结果尚未确认：{exc}"
    clear_state(state_path)
    return f"SciFetch logged out; local device token cleared.{revoke_error}"


def _handle_personal_digest_status(kwargs: Mapping[str, object]) -> str:
    state_result = _load_state_for_command(kwargs)
    if isinstance(state_result, StateFileError):
        return f"SciFetch 登录状态无法读取。 {LOGIN_HINT}"
    if state_result is None:
        return f"SciFetch 尚未登录。 {LOGIN_HINT}"

    state = state_result
    if trust_error := _state_trust_error(state, kwargs):
        return trust_error
    client = _client(state.server_url, kwargs)
    try:
        state = ensure_fresh_state(
            client=client, state=state, state_path=_state_path_from_kwargs(kwargs)
        )
        me = client.get_me(
            access_token=state.access_token,
            device_id=state.device_id,
            trust_mode=state.trust_mode,
            installation_id=state.installation_id,
            **state_principal_fields(state),
        )
    except SciFetchPluginError as exc:
        return f"Personal Paper Digest status unavailable: {exc}"
    user = _dict_value(me, "user")
    display_name = _string_value(user, "display_name") or _string_value(user, "user_id")
    return (
        f"Personal Paper Digest is connected for {display_name or 'this user'} "
        f"via {state.server_url}. "
        "Run /paper-digest due to pull pending personal digests."
    )


def _handle_paper_status(args: list[str], kwargs: Mapping[str, object]) -> str:
    if len(args) != 1 or not args[0].startswith("req_"):
        return "Usage: /paper status <request_id>"

    state_result = _load_state_for_command(kwargs)
    if isinstance(state_result, StateFileError):
        return f"SciFetch 登录状态无法读取。 {LOGIN_HINT}"
    if state_result is None:
        return f"SciFetch 尚未登录。 {LOGIN_HINT}"

    request_id = args[0]
    state = state_result
    if trust_error := _state_trust_error(state, kwargs):
        return trust_error
    client = _client(state.server_url, kwargs)
    try:
        state = ensure_fresh_state(
            client=client,
            state=state,
            state_path=_state_path_from_kwargs(kwargs),
        )
        events = client.list_paper_request_events(
            request_id=request_id,
            access_token=state.access_token,
            device_id=state.device_id,
            trust_mode=state.trust_mode,
            installation_id=state.installation_id,
            **state_principal_fields(state),
        )
        files = client.list_paper_request_files(
            request_id=request_id,
            access_token=state.access_token,
            device_id=state.device_id,
            trust_mode=state.trust_mode,
            installation_id=state.installation_id,
            **state_principal_fields(state),
        )
    except SciFetchPluginError as exc:
        return f"SciFetch 请求状态暂时无法读取： {exc}"

    from .durable_delivery import local_delivery_status

    message = build_request_status_message(
        request_id=request_id,
        server_url=state.server_url,
        events=events,
        files=files,
    )
    local = local_delivery_status(_state_path_from_kwargs(kwargs), state, request_id)
    return message + ("\n\n" + local if local else "")


def _handle_paper_retry(args: list[str], kwargs: Mapping[str, object]) -> str:
    """Explicit profile-authorized recovery; model tool arguments never enter here."""
    usage = (
        "Usage: /paper retry <request_id> <artifact_id> <target_binding_id> confirm-duplicate-risk. "
        "重发可能重复已经被平台接受的文件；仅原调用已确证停止时可登记。"
    )
    if len(args) != 4 or args[3] != "confirm-duplicate-risk":
        return usage
    state_result = _load_state_for_command(kwargs)
    if not isinstance(state_result, PluginState):
        return f"SciFetch 尚未登录。 {LOGIN_HINT}"
    state = state_result
    if state.trust_mode != PERSONAL_INSTANCE_MODE or _state_trust_error(state, kwargs):
        return TRUSTED_CONTEXT_UNAVAILABLE
    request_id, artifact_id, target_id, _ = args
    from .outbox import OutboxError, OutboxStore

    path = _state_path_from_kwargs(kwargs).with_name("delivery_outbox.sqlite3")
    if not path.exists():
        return "没有该文件的本地原生投递证据；不会发送。"
    client = _client(state.server_url, kwargs)
    try:
        state = ensure_fresh_state(
            client=client, state=state, state_path=_state_path_from_kwargs(kwargs)
        )
        # Current authenticated server ownership is checked before authorizing
        # an old local attempt. No publisher or native operation is performed.
        client.list_paper_request_files(
            request_id=request_id,
            access_token=state.access_token,
            device_id=state.device_id,
            trust_mode=state.trust_mode,
            installation_id=state.installation_id,
            **state_principal_fields(state),
        )
        store = OutboxStore(path, state.installation_id)
        matches = [
            record
            for record in store.list_deliveries(request_id=request_id)
            if record.artifact.artifact_id == artifact_id
            and record.target.binding_id == target_id
            and record.scope.server_identity == state.server_url.rstrip("/")
            and record.scope.account_scope == state.device_id
        ]
        if len(matches) != 1:
            return "请求、文件或目标不匹配；不会发送。"
        store.explicit_retry_unknown(
            matches[0].key,
            expected_target_binding_id=target_id,
            user_requested=True,
            duplicate_risk_ack=True,
        )
    except SciFetchPluginError:
        return "当前 SciFetch 认证或请求权限未获确认；不会发送。"
    except OutboxError:
        return "原调用尚未确证停止，或记录不允许重发；继续保留 unknown，不会发送。"
    return "已记录此次文件重发授权（可能重复）。等待同一精确目标的新已认证消息与 ACK 后执行；原 unknown 记录保留。"


def _handle_personal_digest_due(args: list[str], kwargs: Mapping[str, object]) -> str:
    state_result = _load_state_for_command(kwargs)
    if isinstance(state_result, StateFileError):
        return f"SciFetch 登录状态无法读取。 {LOGIN_HINT}"
    if state_result is None:
        return f"SciFetch 尚未登录。 {LOGIN_HINT}"

    limit = _parse_limit(args)
    if limit is None:
        return "Usage: /paper-digest due [limit]"

    state = state_result
    if trust_error := _state_trust_error(state, kwargs):
        return trust_error
    client = _client(state.server_url, kwargs)
    try:
        state = ensure_fresh_state(
            client=client, state=state, state_path=_state_path_from_kwargs(kwargs)
        )
        due = client.list_my_scipaper_notifications(
            access_token=state.access_token,
            device_id=state.device_id,
            limit=limit,
            trust_mode=state.trust_mode,
            installation_id=state.installation_id,
            **state_principal_fields(state),
        )
        if not due:
            return "No pending Personal Paper Digest notifications."
        messages: list[str] = []
        for entry in due:
            notification_id = _string_value(entry, "notification_id")
            ack_token = _string_value(entry, "ack_token")
            final_message = _string_value(entry, "final_message")
            if not notification_id or not ack_token:
                return (
                    "Personal Paper Digest server returned an incomplete notification. "
                    "Please retry later."
                )
            client.acknowledge_my_scipaper_notification(
                notification_id=notification_id,
                ack_token=ack_token,
                access_token=state.access_token,
                device_id=state.device_id,
                channel="hermes_plugin",
                status="sent",
                attempt=1,
                metadata={"command": "/paper-digest due"},
                trust_mode=state.trust_mode,
                installation_id=state.installation_id,
                **state_principal_fields(state),
            )
            if final_message:
                messages.append(final_message)
        if not messages:
            return (
                f"Personal Paper Digest delivered {len(due)} digest(s), "
                "but no message text was available."
            )
        footer = f"Personal Paper Digest delivery acknowledged for {len(due)} digest(s)."
        return "\n\n".join([*messages, footer])
    except SciFetchPluginError as exc:
        return f"Personal Paper Digest request failed: {exc}"


def _load_state_for_command(kwargs: Mapping[str, object]) -> PluginState | StateFileError | None:
    try:
        return load_state(_state_path_from_kwargs(kwargs))
    except StateFileError as exc:
        return exc


def _command_trust_error(kwargs: Mapping[str, object]) -> str | None:
    trust_mode = configured_trust_mode()
    if trust_mode == INVALID_TRUST_MODE:
        return INVALID_TRUST_MODE_MESSAGE
    if trust_mode == MULTI_USER_MODE and trusted_principal_from_context(kwargs) is None:
        return TRUSTED_CONTEXT_UNAVAILABLE
    return None


def _state_trust_error(state: PluginState, kwargs: Mapping[str, object]) -> str | None:
    trust_mode = configured_trust_mode()
    if effective_state_trust_mode(state) != trust_mode:
        return TRUST_BINDING_MISMATCH
    if trust_mode == PERSONAL_INSTANCE_MODE:
        if not state_matches_personal_instance(state):
            return TRUST_BINDING_MISMATCH
        return None

    principal = trusted_principal_from_context(kwargs)
    if principal is None or not state_matches_principal(state, principal):
        return TRUST_BINDING_MISMATCH
    return None


def _client(server_url: str, kwargs: Mapping[str, object]) -> SciFetchPluginClient:
    transport = kwargs.get("transport")
    return SciFetchPluginClient(
        SciFetchClientConfig(server_url=server_url),
        transport=transport if isinstance(transport, httpx.BaseTransport) else None,
    )


def _state_path_from_kwargs(kwargs: Mapping[str, object]) -> Path:
    value = kwargs.get("state_path")
    return resolve_state_path(explicit_path=value if isinstance(value, Path) else None)


def _split_args(raw_args: str) -> list[str]:
    try:
        return shlex.split(raw_args)
    except ValueError:
        return raw_args.split()


def _parse_limit(args: list[str]) -> int | None:
    if not args:
        return 5
    try:
        limit = int(args[0])
    except ValueError:
        return None
    return max(1, min(limit, 20))


def _personal_digest_usage() -> str:
    return "Usage: /paper-digest [due [limit] | status]."


def _server_url_from_env() -> str:
    return profile_value("SCIFETCH_SERVER_URL")


def _principal_display_name(platform: str, platform_user_id: str, chat_id: str) -> str:
    identifier = platform_user_id or chat_id
    return f"{platform}:{identifier}" if identifier else f"{platform} device"


def _hermes_version(kwargs: Mapping[str, object]) -> str:
    direct = kwargs.get("hermes_version")
    if isinstance(direct, str) and direct:
        return direct
    hermes = kwargs.get("hermes")
    version = getattr(hermes, "version", "")
    if isinstance(version, str) and version:
        return version
    gateway = kwargs.get("gateway")
    gateway_version = getattr(gateway, "version", "")
    if isinstance(gateway_version, str) and gateway_version:
        return gateway_version
    return runtime_version() or "unknown"


def _dict_value(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _string_value(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
