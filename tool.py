"""Official Hermes tool surface for personal-instance paper requests."""

from __future__ import annotations

from collections.abc import Mapping

from .detector import _strip_force_prefix, submit_paper_text
from .home_delivery import with_home_delivery_bridge
from .response_output import remember_tool_response, with_tool_ack_gate
from .trust import PERSONAL_INSTANCE_MODE, configured_trust_mode

PAPER_REQUEST_TOOL_NAME = "scifetch_paper_request"
PAPER_REQUEST_TOOL_SCHEMA: dict[str, object] = {
    "name": PAPER_REQUEST_TOOL_NAME,
    "description": (
        "Submit a DOI, scholarly URL, WeChat article URL, arXiv id, citation, or "
        "paper title to SciFetch. Use this only when the user is asking SciFetch "
        "to retrieve a paper. For a message containing multiple requests, invoke "
        "this tool once per occurrence in order, including repeated occurrences. "
        "Copy every result verbatim, preserving emojis and line breaks, and never "
        "claim that a file was sent."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The DOI, URL, citation, or paper title to retrieve.",
            },
            "force": {
                "type": "boolean",
                "description": "Bypass cached retrieval when the user explicitly requests it.",
            },
        },
        "required": ["query"],
        "additionalProperties": False,
    },
}


def handle_paper_tool(
    args: Mapping[str, object],
    **kwargs: object,
) -> str:
    """Submit through the profile-bound personal instance after gateway auth."""
    if configured_trust_mode() != PERSONAL_INSTANCE_MODE:
        return _tool_result(
            "trusted_message_context_unavailable: 多用户模式缺少官方可信的消息身份上下文，本次未提交文献请求。",
            kwargs,
        )
    query = args.get("query")
    if not isinstance(query, str) or not query.strip():
        return _tool_result("文献请求不能为空，请提供 DOI、文献链接或题名。", kwargs)
    query, marked_force = _strip_force_prefix(query)
    force = args.get("force") is True or marked_force
    raw_text = f"!! {query}" if force else query
    runtime_kwargs = with_tool_ack_gate(with_home_delivery_bridge(kwargs))
    result = submit_paper_text(raw_text, runtime_kwargs, require_paper_like=False)
    return _tool_result(result.get("message", "本次未提交文献请求。"), kwargs)


def _tool_result(message: str, kwargs: Mapping[str, object]) -> str:
    remember_tool_response(kwargs, message)
    return message
