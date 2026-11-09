"""Trust-mode contracts shared by the SciFetch Hermes plugin."""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from .environment import profile_value

TRUST_MODE_ENV = "SCIFETCH_TRUST_MODE"
PERSONAL_INSTANCE_MODE = "personal_instance"
MULTI_USER_MODE = "multi_user"
INVALID_TRUST_MODE = "invalid"
SUPPORTED_TRUST_MODES = frozenset({PERSONAL_INSTANCE_MODE, MULTI_USER_MODE})


class TrustBoundState(Protocol):
    """State fields needed to validate one plugin credential binding."""

    @property
    def trust_mode(self) -> str: ...

    @property
    def installation_id(self) -> str: ...

    @property
    def platform(self) -> str: ...

    @property
    def platform_user_id(self) -> str: ...

    @property
    def chat_id(self) -> str: ...


@dataclass(frozen=True)
class TrustedPrincipal:
    platform: str
    platform_user_id: str
    chat_id: str


def configured_trust_mode(environ: Mapping[str, str] | None = None) -> str:
    """Return the explicit plugin trust mode, defaulting to fail-closed multi-user."""
    value = (
        profile_value(TRUST_MODE_ENV, MULTI_USER_MODE)
        if environ is None
        else environ.get(TRUST_MODE_ENV, MULTI_USER_MODE)
    )
    value = value.strip().lower()
    return value if value in SUPPORTED_TRUST_MODES else INVALID_TRUST_MODE


def new_installation_id() -> str:
    """Create an opaque, profile-local identifier for a personal Hermes instance."""
    return f"inst_{secrets.token_urlsafe(24)}"


def effective_state_trust_mode(state: TrustBoundState) -> str:
    """Infer old principal-bound states while refusing unmarked unbound states."""
    if state.trust_mode in SUPPORTED_TRUST_MODES:
        return state.trust_mode
    if state.platform or state.platform_user_id or state.chat_id:
        return MULTI_USER_MODE
    return INVALID_TRUST_MODE


def trusted_principal_from_context(
    kwargs: Mapping[str, object],
) -> TrustedPrincipal | None:
    """Read only an explicitly trusted principal supplied by an official context."""
    principal = kwargs.get("trusted_principal")
    if principal is None:
        return None
    platform = _platform_value(_object_value(principal, "platform"))
    platform_user_id = _object_string(principal, "platform_user_id") or _object_string(
        principal, "user_id"
    )
    chat_id = _object_string(principal, "chat_id")
    if not platform or not platform_user_id or not chat_id:
        return None
    return TrustedPrincipal(
        platform=platform,
        platform_user_id=platform_user_id,
        chat_id=chat_id,
    )


def state_matches_principal(state: TrustBoundState, principal: TrustedPrincipal) -> bool:
    if not state.platform or not state.platform_user_id or not state.chat_id:
        return False
    return bool(
        principal.platform == state.platform
        and principal.platform_user_id == state.platform_user_id
        and principal.chat_id == state.chat_id
    )


def state_matches_personal_instance(state: TrustBoundState) -> bool:
    """Reject legacy or mixed-principal state in personal-instance mode."""
    return bool(
        effective_state_trust_mode(state) == PERSONAL_INSTANCE_MODE
        and state.installation_id
        and state.platform in {"", "hermes"}
        and not state.platform_user_id
        and not state.chat_id
    )


def state_principal_fields(state: TrustBoundState) -> dict[str, str]:
    """Return chat-principal fields only for a principal-bound device."""
    if effective_state_trust_mode(state) != MULTI_USER_MODE:
        return {}
    return {
        "platform": state.platform,
        "platform_user_id": state.platform_user_id,
        "chat_id": state.chat_id,
    }


def _object_string(obj: object, name: str) -> str:
    value = _object_value(obj, name)
    return value if isinstance(value, str) else ""


def _object_value(obj: object, name: str) -> object:
    if isinstance(obj, Mapping):
        return obj.get(name)
    return getattr(obj, name, None)


def _platform_value(value: object) -> str:
    if isinstance(value, str):
        return value
    enum_value = getattr(value, "value", "")
    return enum_value if isinstance(enum_value, str) else ""
