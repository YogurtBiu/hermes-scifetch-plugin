"""Read this Hermes profile's settings through its public runtime APIs."""

from __future__ import annotations

import importlib
import os
from importlib import metadata
from pathlib import Path
from types import ModuleType


def _host_module(name: str) -> ModuleType | None:
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        if exc.name not in {name, name.partition(".")[0]}:
            raise
        # Older supported Hermes releases expose only process-scoped settings.
        return None


def profile_value(name: str, default: str = "") -> str:
    scope = _host_module("agent.secret_scope")
    if scope is None:
        return os.environ.get(name, default)
    value: str | None = scope.get_secret(name, default)
    return default if value is None else value


def profile_home() -> Path | None:
    constants = _host_module("hermes_constants")
    if constants is None:
        return None
    home: Path = constants.get_hermes_home()
    return home


def runtime_version() -> str:
    version_info = _host_module("hermes_cli.version_info")
    if version_info is not None:
        probe = getattr(version_info, "get_version_info", None)
        if probe is not None:
            # Preserve development/private suffixes: only exact reviewed releases
            # may pass the existing transport/version admission checks.
            version: str = probe().derived_version
            return version
    try:
        return metadata.version("hermes-agent")
    except metadata.PackageNotFoundError:
        return ""
