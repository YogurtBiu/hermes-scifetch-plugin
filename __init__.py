"""SciFetch ordinary-user Hermes plugin.

This plugin registers Hermes extension points, account-binding commands, and
Step 11 paper request submission, request event watching, and signed file
delivery.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Protocol

from .commands import (
    handle_paper_command,
    handle_personal_digest_command,
    handle_scifetch_command,
)
from .detector import handle_post_auth_paper_context, handle_pre_auth_paper_message
from .environment import profile_value
from .home_delivery import discard_personal_turn_context
from .llm_cli import register_llm_cli
from .response_output import handle_session_response_cleanup, handle_transform_llm_output
from .tool import PAPER_REQUEST_TOOL_NAME, PAPER_REQUEST_TOOL_SCHEMA, handle_paper_tool

PLUGIN_NAME = "scifetch-paper"
PERSONAL_DIGEST_ENABLE_ENV = "SCIFETCH_PERSONAL_DIGEST_ENABLED"
_TRUE_ENV_VALUES = {"1", "true", "yes", "on", "enabled"}

logger = logging.getLogger(__name__)


class HermesPluginContext(Protocol):
    """Subset of Hermes PluginContext used by this plugin."""

    def register_hook(self, hook_name: str, callback: Callable[..., object]) -> None:
        """Register a Hermes lifecycle hook."""

    def register_command(
        self,
        name: str,
        handler: Callable[..., object],
        description: str = "",
        args_hint: str = "",
    ) -> None:
        """Register a Hermes slash command."""

    def register_tool(
        self,
        name: str,
        toolset: str,
        schema: dict[str, object],
        handler: Callable[..., object],
        **kwargs: object,
    ) -> None:
        """Register an official Hermes agent tool."""


def register(ctx: HermesPluginContext) -> None:
    """Register the hook and command surface without patching Hermes source."""

    register_llm_cli(ctx)

    def handle_tool(args: Mapping[str, object], **kwargs: object) -> str:
        return handle_paper_tool(args, **kwargs)

    def handle_session_cleanup(**kwargs: object) -> None:
        handle_session_response_cleanup(**kwargs)
        discard_personal_turn_context(kwargs)

    ctx.register_hook("pre_gateway_dispatch", handle_pre_auth_paper_message)
    ctx.register_hook("pre_llm_call", handle_post_auth_paper_context)
    ctx.register_hook("transform_llm_output", handle_transform_llm_output)
    ctx.register_hook("on_session_end", handle_session_cleanup)
    ctx.register_hook("on_session_finalize", handle_session_cleanup)
    ctx.register_tool(
        name=PAPER_REQUEST_TOOL_NAME,
        toolset="scifetch",
        schema=PAPER_REQUEST_TOOL_SCHEMA,
        handler=handle_tool,
        description="Submit paper retrieval requests to SciFetch.",
        emoji="📄",
    )
    ctx.register_command(
        "scifetch",
        handle_scifetch_command,
        description="SciFetch account and status commands.",
        args_hint="login <code> [url] | status | logout",
    )
    ctx.register_command(
        "paper",
        handle_paper_command,
        description="Submit or inspect SciFetch paper requests.",
        args_hint="<query> | status <request_id> | replay <request_id>",
    )
    if _personal_digest_enabled():
        ctx.register_command(
            "paper-digest",
            handle_personal_digest_command,
            description="Personal Paper Digest commands.",
            args_hint="status | due [limit]",
        )
        logger.info("registered %s Hermes plugin with personal digest commands", PLUGIN_NAME)
        return
    logger.info("registered %s Hermes plugin; personal digest commands disabled", PLUGIN_NAME)


def _personal_digest_enabled(environ: Mapping[str, str] | None = None) -> bool:
    value = (
        profile_value(PERSONAL_DIGEST_ENABLE_ENV)
        if environ is None
        else environ.get(PERSONAL_DIGEST_ENABLE_ENV, "")
    )
    return value.strip().lower() in _TRUE_ENV_VALUES
