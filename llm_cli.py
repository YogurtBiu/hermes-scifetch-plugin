"""Bounded local IPC for server extraction through Hermes-owned LLM auth."""

from __future__ import annotations

import argparse
import inspect
import json
import math
import sys
from collections.abc import Mapping
from typing import Protocol, cast

COMMAND_NAME = "scifetch-resolve"
AUXILIARY_TASK = "scifetch_resolve"
MAX_INPUT_BYTES = 128 * 1024
MAX_OUTPUT_BYTES = 1024 * 1024


class CompletionHost(Protocol):
    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        task: str,
        timeout: float,
        max_tokens: int,
        purpose: str,
    ) -> object: ...


def register_llm_cli(ctx: object) -> None:
    """Feature-detect the official APIs without initializing a model or auth."""
    register_cli = getattr(ctx, "register_cli_command", None)
    register_auxiliary = getattr(ctx, "register_auxiliary_task", None)
    if not callable(register_cli) or not callable(register_auxiliary):
        return
    # Older hosts expose both registration APIs but cannot route a completion
    # through a plugin-owned task. Inspect only the public, lazy LLM facade.
    try:
        complete = getattr(getattr(ctx, "llm", None), "complete", None)
        if not callable(complete) or "task" not in inspect.signature(complete).parameters:
            return
    except (TypeError, ValueError, AttributeError):
        return

    register_auxiliary(
        AUXILIARY_TASK,
        display_name="SciFetch paper extraction",
        description="Resolve paper queries and extract Weixin paper mentions.",
        defaults={"timeout": 60},
    )

    def setup(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--check", action="store_true", help="Check IPC without calling a model."
        )

    def handle(args: argparse.Namespace) -> int:
        return handle_completion(ctx, check=bool(args.check))

    register_cli(
        COMMAND_NAME,
        help="Read a SciFetch extraction request as JSON from stdin.",
        setup_fn=setup,
        handler_fn=handle,
    )


def handle_completion(ctx: object, *, check: bool = False) -> int:
    """Return machine JSON only; never expose credentials or raw provider errors."""
    if check:
        print(json.dumps({"ready": True, "task": AUXILIARY_TASK}))
        return 0
    try:
        raw = sys.stdin.read(MAX_INPUT_BYTES + 1)
        messages, timeout = _parse_request(raw)
    except (ValueError, TypeError, OSError, OverflowError, RecursionError):
        print(json.dumps({"error": "invalid_completion_request"}))
        return 2

    try:
        host = cast(CompletionHost, getattr(ctx, "llm", None))
        result = host.complete(
            messages,
            task=AUXILIARY_TASK,
            timeout=timeout,
            max_tokens=4096,
            purpose="scifetch.paper-extraction",
        )
        text = getattr(result, "text", None)
        if not isinstance(text, str):
            raise ValueError("missing completion text")
        payload = json.dumps(
            {
                "choices": [{"message": {"content": text}}],
                "model": _result_label(result, "model"),
                "provider": _result_label(result, "provider"),
            },
            ensure_ascii=False,
        )
        if len(payload.encode("utf-8")) > MAX_OUTPUT_BYTES:
            raise ValueError("completion output limit")
    except Exception:
        print(json.dumps({"error": "hermes_completion_failed"}))
        return 1
    print(payload)
    return 0


def _parse_request(raw: str) -> tuple[list[dict[str, str]], float]:
    if len(raw.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ValueError("completion input limit")
    data = json.loads(raw)
    if not isinstance(data, Mapping) or set(data) != {"messages", "timeout_seconds"}:
        raise ValueError("invalid request fields")
    timeout = data["timeout_seconds"]
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or not 0 < timeout <= 120
    ):
        raise ValueError("invalid timeout")
    raw_messages = data["messages"]
    if not isinstance(raw_messages, list) or not 1 <= len(raw_messages) <= 8:
        raise ValueError("invalid messages")
    messages: list[dict[str, str]] = []
    for message in raw_messages:
        if not isinstance(message, Mapping) or set(message) != {"role", "content"}:
            raise ValueError("invalid message fields")
        role, content = message["role"], message["content"]
        if role not in ("system", "user") or not isinstance(content, str):
            raise ValueError("invalid message content")
        messages.append({"role": role, "content": content})
    return messages, float(timeout)


def _result_label(result: object, field: str) -> str:
    value = getattr(result, field, "")
    return value if isinstance(value, str) else ""
