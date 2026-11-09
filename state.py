"""Profile-scoped local state for the scifetch-paper Hermes plugin."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path

from .environment import profile_home, profile_value

STATE_FILENAME = "state.json"


class StateFileError(Exception):
    """Raised when the local plugin state file cannot be decoded."""


@dataclass(frozen=True)
class PluginState:
    """Device-bound credentials stored only inside one Hermes profile."""

    server_url: str
    device_id: str
    access_token: str
    refresh_token: str
    access_expires_at: str
    refresh_expires_at: str
    trust_mode: str = ""
    installation_id: str = ""
    platform: str = ""
    platform_user_id: str = ""
    chat_id: str = ""
    last_seen: str = ""
    last_sse_reconnect: str = ""

    @property
    def is_authenticated(self) -> bool:
        return bool(self.server_url and self.device_id and self.access_token and self.refresh_token)


def resolve_state_path(
    *,
    environ: Mapping[str, str] | None = None,
    explicit_path: Path | None = None,
) -> Path:
    """Resolve a state path under the current Hermes profile.

    Tests may pass ``explicit_path``. Runtime prefers ``SCIFETCH_PLUGIN_STATE_DIR``
    for controlled deployments, then Hermes profile variables, then a conservative
    ``~/.hermes/plugins/scifetch-paper`` fallback.
    """
    if explicit_path is not None:
        return explicit_path

    if environ is None:
        if scoped_state_dir := profile_value("SCIFETCH_PLUGIN_STATE_DIR"):
            return Path(scoped_state_dir).expanduser() / STATE_FILENAME
        if home := profile_home():
            return home / "plugins" / "scifetch-paper" / STATE_FILENAME
    env = os.environ if environ is None else environ
    if state_dir := env.get("SCIFETCH_PLUGIN_STATE_DIR"):
        return Path(state_dir).expanduser() / STATE_FILENAME

    if profile_dir := env.get("HERMES_PROFILE_DIR"):
        return Path(profile_dir).expanduser() / "plugins" / "scifetch-paper" / STATE_FILENAME

    hermes_home = Path(env.get("HERMES_HOME", "~/.hermes")).expanduser()
    if profile_name := env.get("HERMES_PROFILE"):
        return (
            hermes_home / "profiles" / profile_name / "plugins" / "scifetch-paper" / STATE_FILENAME
        )
    return hermes_home / "plugins" / "scifetch-paper" / STATE_FILENAME


def load_state(path: Path) -> PluginState | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StateFileError("SciFetch token state is corrupted; please log in again.") from exc
    if not isinstance(data, dict):
        raise StateFileError("SciFetch token state is corrupted; please log in again.")
    try:
        return PluginState(
            server_url=_string_value(data, "server_url"),
            device_id=_string_value(data, "device_id"),
            access_token=_string_value(data, "access_token"),
            refresh_token=_string_value(data, "refresh_token"),
            access_expires_at=_string_value(data, "access_expires_at"),
            refresh_expires_at=_string_value(data, "refresh_expires_at"),
            trust_mode=_string_value(data, "trust_mode"),
            installation_id=_string_value(data, "installation_id"),
            platform=_string_value(data, "platform"),
            platform_user_id=_string_value(data, "platform_user_id"),
            chat_id=_string_value(data, "chat_id"),
            last_seen=_string_value(data, "last_seen"),
            last_sse_reconnect=_string_value(data, "last_sse_reconnect"),
        )
    except TypeError as exc:
        raise StateFileError("SciFetch token state is corrupted; please log in again.") from exc


def save_state(path: Path, state: PluginState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(asdict(state), ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(tmp_path, 0o600)
    tmp_path.replace(path)


def clear_state(path: Path) -> None:
    with suppress(FileNotFoundError):
        path.unlink()


def _string_value(data: dict[object, object], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else ""
