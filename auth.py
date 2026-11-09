"""Token refresh helpers for the scifetch-paper Hermes plugin."""

from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .client import SciFetchPluginClient, SciFetchPluginError
from .state import PluginState, load_state, save_state

REFRESH_SKEW_SECONDS = 60
_STATE_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_STATE_REFRESH_LOCKS_GUARD = threading.Lock()


def ensure_fresh_state(
    *,
    client: SciFetchPluginClient,
    state: PluginState,
    state_path: Path,
    now: datetime | None = None,
) -> PluginState:
    """Refresh an expiring access token and persist the rotated credentials."""
    if not _needs_refresh(state.access_expires_at, now=now):
        return state

    with _state_refresh_lock(state_path):
        latest = load_state(state_path) or state
        if not _needs_refresh(latest.access_expires_at, now=now):
            return latest
        response = client.refresh_device_token(
            refresh_token=latest.refresh_token,
            device_id=latest.device_id,
            trust_mode=latest.trust_mode,
            installation_id=latest.installation_id,
        )
        device = _dict_value(response, "device")
        refreshed = PluginState(
            server_url=latest.server_url,
            device_id=_string_value(device, "device_id") or latest.device_id,
            access_token=_string_value(response, "access_token"),
            refresh_token=_string_value(response, "refresh_token"),
            access_expires_at=_string_value(response, "access_expires_at"),
            refresh_expires_at=_string_value(response, "refresh_expires_at"),
            trust_mode=latest.trust_mode,
            installation_id=latest.installation_id,
            platform=latest.platform,
            platform_user_id=latest.platform_user_id,
            chat_id=latest.chat_id,
            last_seen=_now_iso(now),
            last_sse_reconnect=latest.last_sse_reconnect,
        )
        if not refreshed.is_authenticated:
            raise SciFetchPluginError("SciFetch 服务未返回完整的登录刷新结果，请检查登录状态。")
        save_state(state_path, refreshed)
        return refreshed


def record_sse_reconnect(
    *,
    state_path: Path,
    fallback_state: PluginState,
    reconnected_at: str,
) -> PluginState:
    """Merge watcher metadata without overwriting a rotated token pair."""
    with _state_refresh_lock(state_path):
        latest = load_state(state_path) or fallback_state
        updated = replace(latest, last_sse_reconnect=reconnected_at)
        save_state(state_path, updated)
        return updated


def _state_refresh_lock(state_path: Path) -> threading.Lock:
    key = str(state_path.expanduser().resolve())
    with _STATE_REFRESH_LOCKS_GUARD:
        return _STATE_REFRESH_LOCKS.setdefault(key, threading.Lock())


def _needs_refresh(expires_at: str, *, now: datetime | None = None) -> bool:
    parsed = _parse_datetime(expires_at)
    if parsed is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return parsed <= current + timedelta(seconds=REFRESH_SKEW_SECONDS)


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


def _now_iso(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat()


def _dict_value(data: dict[str, object], key: str) -> dict[str, object]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _string_value(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else ""
