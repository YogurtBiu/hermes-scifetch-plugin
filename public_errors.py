"""Pure Chinese presentation of acquisition failures; never a policy classifier.

This dependency-free module can also be distributed with the independent plugin.
Internal exceptions, stored events and scheduler inputs must retain their originals.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy

_API_MESSAGES = {
    "auth_required": "SciFetch 登录已失效或尚未登录，请重新登录后再试。",
    "permission_denied": "当前 SciFetch 账号、设备或课题组没有此操作的权限。",
    "quota_exceeded": "SciFetch 账号或设备的用量额度已达到上限。",
    "conflict": "请求与当前状态冲突，请查看请求详情。",
    "query_unresolved": "未能识别可用的文献目标，请提供 DOI 或论文官网链接。",
    "file_not_ready": "请求的文件暂不可用，请查看该请求的文件状态。",
    "server_unavailable": "SciFetch 服务暂时不可用，请稍后查看请求状态。",
    "not_found": "未找到请求的 SciFetch 记录或文件。",
    "job_failed": "文献获取未完成，请查看请求详情。",
}
_API_REASON_MESSAGES = {
    (
        "quota_exceeded",
        "job_queue_full",
    ): "SciFetch 获取队列已满，暂时无法接收新的获取任务，请稍后再试。",
    ("quota_exceeded", "quota_publisher_exceeded"): "SciFetch 对该出版商的请求用量已达到上限。",
    (
        "permission_denied",
        "missing_signed_url_token",
    ): "文件下载链接无效，请从请求详情重新获取下载链接。",
    (
        "permission_denied",
        "invalid_signed_url_token",
    ): "文件下载链接无效，请从请求详情重新获取下载链接。",
    (
        "permission_denied",
        "signed_url_revoked",
    ): "文件下载链接已撤销，请从请求详情重新获取下载链接。",
    ("file_not_ready", "signed_url_expired"): "文件下载链接已过期，请从请求详情重新获取下载链接。",
}
_MESSAGES = {
    "access_denied": "出版商拒绝了当前会话对本文的全文访问。",
    "authentication_required": "出版商要求登录，当前尚未确认本文的全文访问权限。",
    "publisher_challenge_incomplete": "出版商自动验证未能完成，本次获取已结束。",
    "publisher_human_verification_required": "出版商验证尚未稳定完成；请查看可见的服务 Chrome，若页面提示人工操作，请完成后重试；若页面仍在自动跳转，请等待文章页稳定后再试。",
    "publisher_paused": "出版商队列暂时暂停，管理员恢复后可重试。",
    "publisher_queue_paused": "出版商队列暂时暂停，管理员恢复后可重试。",
    "browser_queue_paused": "浏览器获取队列已暂停，请管理员核对当前状态。",
    "rate_limited": "出版商暂时限制了请求频率，本次获取未完成。",
    "publisher_rate_limited": "出版商暂时限制了请求频率，本次获取未完成。",
    "download_failed": "网络、传输或原始文件校验未能完成；尚未确认全文访问权限。",
    "download_exception": "文献获取流程未能完成，具体原因尚未确认。",
    "unknown_publisher": "本次未能取得经过验证的原始 PDF。",
    "publisher_pdf_unavailable": "本次未能取得经过验证的原始 PDF；不能据此判断出版商尚未发布全文。",
    "no_handler": "当前获取方式未能处理该文献来源。",
    "metadata_not_found": "未找到与输入匹配的可用文献信息。",
    "no_doi": "未能从输入中识别到可用的 DOI 或文献目标。",
    "arxiv_withdrawn": "这篇 arXiv 文献已被撤回。",
    "browser_required": "本次获取需要浏览器，当前未启用浏览器。",
    "weixin_no_browser": "读取公众号文章需要浏览器，当前未启用浏览器。",
    "browser_unavailable": "获取文献所需的浏览器当前不可用。",
    "browser_capability_unavailable": "当前浏览器未提供本次获取所需的功能。",
    "si_pdf_conversion_failed": "附件 PDF 转换未完成，原始文件已保留。",
    "browser_transport_failure": "浏览器操作未能确认完成，具体原因尚未确认。",
    "browser_source_changed": "浏览器页面已离开目标文献来源，无法继续核验和下载本文。",
    "browser_session_transport_unavailable": "浏览器会话连接不可用，本次操作未能确认完成。",
    "browser_observation_unavailable": "未能可靠读取浏览器页面或文件状态。",
    "browser_local_failure": "本地浏览器环境未能完成本次操作。",
    "browser_stop_unproven": "浏览器操作的停止状态尚未确认；请管理员核对后再继续，勿自动重试或重发。",
    "browser_resource_policy_uncertain": "浏览器资源安全处理未能确认，已停止获取；本次任务已结束，不会在后台继续。请管理员检查会话归属和资源处理记录，确认安全后再恢复。",
    "job_cancelled": "请求已取消。",
    "cancelled": "请求已取消。",
    "server_restart": "服务重启或任务中断，服务器已保留请求记录。",
    "interrupted": "服务重启或任务中断，服务器已保留请求记录。",
    "queue_timeout": "请求在队列中等待超时，本次未完成获取。",
    "job_queue_timeout": "请求在队列中等待超时，本次未完成获取。",
    "artifact_validation_failed": "已取得的文件未通过原始文件校验，不能将其作为有效结果提供。",
    "si_artifact_validation_failed": "部分附件未通过原始文件校验，附件获取未完成。",
    "si_discovery_incomplete": "附件清单检查未完成，暂不能确定本文附件是否齐全。",
    "si_discovery_failed": "附件清单检查未完成，暂不能确定本文附件是否齐全。",
    "si_discovery_unknown": "附件清单尚未确认。",
    "si_unsupported": "当前获取方式尚不支持本文的附件检测。",
    "aip_si_incomplete": "AIP 附件获取未完整完成。",
    "wiley_si_inspection_incomplete": "Wiley 附件清单检查未完成。",
    "science_si_discovery_unknown": "Science 附件清单尚未确认。",
    "science_session_ownership_unavailable": "Science 会话归属尚未确认，本次未发起新下载。需管理员核对旧任务与受管 Chrome，确认安全后显式恢复会话状态；反复提交或仅重启服务不会自动解除。",
    "science_acquisition_disabled": "Science 新下载已被配置关闭；普通请求仍可使用完整缓存。需管理员启用后再请求新下载。",
    "science_lifecycle_guard_unavailable": "Science 会话状态暂时无法可靠保存，本次未发起新下载；请管理员检查状态数据库和任务归属后再试。",
    "wiley_main_invalid": "Wiley 返回的正文未通过原始 PDF 校验。",
    "nature_invalid_pdf": "Nature 返回的正文未通过原始 PDF 校验。",
    "wiley_article_identity_unproven": "未能核验 Wiley 页面与目标文献的一致性。",
    "weixin_resolution_abstained": "公众号文献识别未得到可安全选择的 DOI，暂未开始论文下载。",
    "weixin_article_incomplete": "公众号文章内容未完整读取，暂未开始论文下载。",
    "weixin_article_unavailable": "公众号文章当前无法读取，暂未开始论文下载。",
    "cli_auth_required": "本机服务正在管理获取流程；请配置 [cli] 登录凭据（SCIFETCH_CLI_AUTH_TOKEN 和 SCIFETCH_CLI_DEVICE_ID）后使用服务。",
    "cli_remote_failed": "与 SciFetch 服务交互或接收结果时未能完成，请查看请求详情。",
    "internal_error": "文献获取遇到内部错误，请联系管理员查看请求详情。",
}
_TIMEOUTS = {"job_timeout", "timeout", "weixin_timeout", "browser_cdp_command_timeout"}
_PUBLISHERS = "nature|springer|science|aps|aip|iop|acs|wiley|elsevier|rsc|arxiv|generic"
_PUBLISHER_STAGE_PATTERN = rf"({_PUBLISHERS})_(?:uc_(identity|metadata|article|file|si)_unverified|pure_(article|file|main|si|runtime|cleanup)_failed)"
_ACCESS_AGGREGATES = {
    "wiley_access_or_entitlement",
    "rsc_access_or_entitlement",
    "aip_access_page_or_challenge",
    "science_access_page_or_challenge",
}
_CHALLENGE_AGGREGATES = {
    "wiley_challenge_unresolved",
    "science_challenge_unresolved",
    "weixin_automatic_challenge_unresolved",
}
_STAGE_LABELS = {
    "identity": "文献身份",
    "metadata": "文献信息",
    "article": "文章页面",
    "file": "原始文件",
    "main": "正文",
    "si": "附件",
    "runtime": "浏览器运行",
    "cleanup": "浏览器清理",
}
_WEIXIN = {
    "weixin_article_incomplete": "公众号文章内容未完整读取，暂未开始论文下载。",
    "weixin_article_unavailable": "公众号文章当前无法读取，暂未开始论文下载。",
    "no_doi_or_publisher_url_in_read_more": "未能从公众号正文及“阅读原文”中识别到可用的 DOI 或出版商文献链接。请提供 DOI 或论文官网链接后重试。",
    "no_doi_or_publisher_url": "未能从公众号文章中识别到可用的 DOI 或出版商文献链接。请提供 DOI 或论文官网链接后重试。",
}
_WILEY_EPISODE_MESSAGES = {
    "runtime": "Wiley 官网自动验证运行失败，本次获取已结束。",
    "cleanup": "Wiley 官网自动验证的结束状态未能确认，本次获取失败；勿自动重试或重发。",
}
_PARTIAL_MAIN_PREFIX = "正文已保留，附件获取未完成。"
_RETAINED_FILES_SUFFIX = "已获取文件仍保留。"
_TERMINAL_TIMEOUT = "获取超时；本次任务已结束。"
_UNCERTAIN_TIMEOUT = "本次操作超时，结果尚未确认，请查看请求详情。"


_BROWSER_STOP_REASON_MESSAGES = {
    "acquisition_pure_io_unsettled": "本次浏览器操作的完成及退出状态尚未确认，文献识别或获取未完成；请管理员核对，勿自动重试或重发。",
    "acquisition_pure_owner_exit_unproven": "本次浏览器进程退出状态尚未确认；请管理员核对，勿自动重试或重发。",
    "acquisition_pure_previous_owner_unsettled": "前一次浏览器任务尚未确认退出，本次未启动新的浏览器操作；请管理员核对，勿反复提交或重发。",
    "acquisition_uc_previous_owner_unsettled": "前一次浏览器任务尚未确认退出，本次未启动新的浏览器操作；请管理员核对，勿反复提交或重发。",
}
_BROWSER_STOP_BUSINESS_SUFFIXES = (
    "",
    " The current session does not have full-text access to this article.",
    " The publisher requires authentication for this article; full-text permission is not yet known.",
    " The publisher requires article access authorization.",
)


def browser_stop_explanation(raw_message: str) -> str | None:
    """Preserve only complete same-type diagnostic or canonical presentation."""
    raw = raw_message.strip()
    for reason, message in _BROWSER_STOP_REASON_MESSAGES.items():
        # Core may preserve a closed business cause after the stop diagnostic.
        # Match the complete finite producer forms; cleanup remains primary.
        if raw in {
            "Browser I/O settlement could not be confirmed. reason=" + reason + suffix
            for suffix in _BROWSER_STOP_BUSINESS_SUFFIXES
        }:
            return message
        if raw in {message, message + _RETAINED_FILES_SUFFIX}:
            return raw
    return None


def public_error_message(
    error_type: str,
    raw_message: object = "",
    *,
    context: Mapping[str, object] | None = None,
    origin: str = "acquisition",
) -> str:
    """Render existing facts without modifying them or inferring lost causes."""
    kind = error_type.strip().lower()
    raw = raw_message.strip() if isinstance(raw_message, str) else ""
    facts = context or {}
    count = facts.get("file_count", 0)
    retained = (
        type(count) is int
        and count > 0
        or isinstance(facts.get("files"), (list, tuple))
        and bool(facts["files"])
    )
    # These canonical existing producer shapes explicitly follow MAIN readiness.
    partial = raw == "Supporting information delivery incomplete after main file was ready."
    retained = retained or partial
    if origin == "api":
        details = facts.get("details")
        reason = details.get("reason") if isinstance(details, Mapping) else None
        if isinstance(reason, str) and (kind, reason) in _API_REASON_MESSAGES:
            return _API_REASON_MESSAGES[kind, reason]
        return _API_MESSAGES.get(kind, "SciFetch 请求未能完成，请查看请求详情。")
    if origin == "delivery":
        if kind in {"unknown", "delivery_unknown", "timeout", "browser_stop_unproven"}:
            return "文件发送结果尚未确认；文件仍保留，请先核对投递记录，勿自动重复发送。"
        if kind == "unsupported":
            return "当前消息通道不支持发送该文件；文件仍保留。"
        return "文件发送未完成或结果尚未确认；请查看投递记录，勿自动重复发送。"
    if _wiley_cleanup_unconfirmed(facts):
        # Existing structured original facts take precedence over their primary
        # acquisition failure. This changes prose only, never the machine type.
        canonical = _WILEY_EPISODE_MESSAGES["cleanup"]
        retained = retained or raw == canonical + _RETAINED_FILES_SUFFIX
        return canonical + (_RETAINED_FILES_SUFFIX if retained else "")
    if kind == "browser_stop_unproven":
        explanation = browser_stop_explanation(raw)
        if explanation is not None:
            return explanation + (
                _RETAINED_FILES_SUFFIX
                if retained and not explanation.endswith(_RETAINED_FILES_SUFFIX)
                else ""
            )
    if kind == "cli_remote_failed" and _safe_chinese(raw):
        return raw
    if raw == "browser CDP command timed out; uncertain tab quarantined without navigation":
        kind = "browser_cdp_command_timeout"
    if kind == "browser_resource_policy_uncertain":
        retained_message = _MESSAGES[kind].replace(
            "本次任务已结束", "已获取文件仍可用；本次任务已结束"
        )
        if retained or raw == retained_message:
            return retained_message
    if kind == "weixin_resolve_failed":
        code = raw.partition(": ")[2] if raw.startswith("[WeChat] ") else raw
        if code in _WEIXIN:
            return _WEIXIN[code]
    if kind in _TIMEOUTS:
        terminal_retained = _TERMINAL_TIMEOUT + "已获取文件仍可用。"
        retained = retained or raw in {
            terminal_retained,
            _UNCERTAIN_TIMEOUT + _RETAINED_FILES_SUFFIX,
        }
        if (
            kind == "job_timeout"
            or facts.get("terminal") is True
            or raw
            in {
                terminal_retained,
                _TERMINAL_TIMEOUT + "请查看请求详情核对结果。",
            }
        ):
            return _TERMINAL_TIMEOUT + (
                "已获取文件仍可用。" if retained else "请查看请求详情核对结果。"
            )
        return _UNCERTAIN_TIMEOUT + (_RETAINED_FILES_SUFFIX if retained else "")
    # Closed legacy producer envelopes and their exact public copies retain the
    # episode meaning even when the coarse machine type is download_exception.
    episode = (
        re.fullmatch(
            r"(?:BrowserContractError: )?worker_uncertain:wiley_episode_(runtime|cleanup)_failure",
            raw,
        )
        if kind == "download_exception"
        else None
    )
    canonical_episode = (
        next(
            (
                message
                for message in _WILEY_EPISODE_MESSAGES.values()
                if raw in {message, message + "已获取文件仍保留。"}
            ),
            None,
        )
        if kind == "download_exception"
        else None
    )
    if episode:
        message = _WILEY_EPISODE_MESSAGES[episode[1]]
    elif canonical_episode:
        message = raw
    elif kind in _ACCESS_AGGREGATES:
        message = "出版商当前会话未能取得全文；访问权限或页面验证状态尚未确认。"
    elif kind in _CHALLENGE_AGGREGATES:
        message = "页面自动验证未能完成，本次获取已结束。"
    elif kind in _MESSAGES:
        message = _MESSAGES[kind]
    else:
        match = re.fullmatch(_PUBLISHER_STAGE_PATTERN, kind)
        if match:
            stage = match[2] or match[3]
            message = f"未能完成{_STAGE_LABELS[stage]}的核验，本次获取未完成。"
        elif _safe_chinese(raw):
            message = raw
        else:
            message = "本次获取未完成，具体原因尚未确认，请查看请求详情。"
    # Public SSE may be rendered again by the independent consumer without its
    # earlier FILE_READY events. Keep only this exact canonical equivalence;
    # arbitrary Chinese prefixes are not evidence of MAIN readiness.
    if raw == message + _RETAINED_FILES_SUFFIX:
        message = raw
    if partial or raw == _PARTIAL_MAIN_PREFIX + message:
        message = _PARTIAL_MAIN_PREFIX + message
    elif retained and "已获取文件" not in message and "正文已保留" not in message:
        message += _RETAINED_FILES_SUFFIX
    return message


def _wiley_cleanup_unconfirmed(context: Mapping[str, object]) -> bool:
    """Read only the original exact marker for public cleanup guidance."""
    errors = context.get("artifact_errors")
    return isinstance(errors, list) and any(
        isinstance(error, Mapping)
        and error.get("error_type") == "acquisition_finish_stop_unproven"
        and error.get("publisher") == "wiley"
        and error.get("retryable") is False
        for error in errors
    )


def canonical_retained_fact(error_type: str, raw_message: str) -> bool:
    """Recognize finite same-type public copies for rendering, never state policy."""
    kind = error_type.strip().lower()
    if not (
        kind in _MESSAGES
        or kind in _TIMEOUTS
        or kind in _ACCESS_AGGREGATES
        or kind in _CHALLENGE_AGGREGATES
        or re.fullmatch(_PUBLISHER_STAGE_PATTERN, kind)
    ):
        return False
    copies = {
        public_error_message(kind, raw, context={"file_count": 1, "terminal": terminal})
        for terminal in (False, True)
        for raw in ("", "Supporting information delivery incomplete after main file was ready.")
    }
    if kind == "browser_stop_unproven":
        copies.update(
            message + _RETAINED_FILES_SUFFIX for message in _BROWSER_STOP_REASON_MESSAGES.values()
        )
    if kind == "download_exception":
        copies.update(
            message + _RETAINED_FILES_SUFFIX for message in _WILEY_EPISODE_MESSAGES.values()
        )
    return raw_message.strip() in copies


def _safe_chinese(text: str) -> bool:
    """Preserve Chinese prose with known product terms, not arbitrary mixed errors."""
    if not re.search(r"[\u4e00-\u9fff]", text):
        return False
    if re.search(
        r"https?://|/(?:Users|home|srv|tmp|var|private)/|Traceback|Bearer\s|token\s*[=:]|signature\s*[=:]|[A-Za-z]:\\|[a-z]+_[a-z_]+|原始信息",
        text,
        re.I,
    ):
        return False
    remaining = re.sub(
        r"\b(?:SciFetch|Science|Wiley|AIP|RSC|IOP|Nature|Springer|APS|ACS|Elsevier|arXiv|Chrome|Cloudflare|Turnstile|SeleniumBase|DOI|PDF|SI|DOCX|ZIP|API|HTTP|LLM)\b",
        "",
        text,
    )
    return re.search(r"[A-Za-z]", remaining) is None


def public_error_payload(
    data: Mapping[str, object], *, context: Mapping[str, object] | None = None
) -> dict[str, object]:
    """Copy only error-bearing fields; source names, titles and machine fields stay intact."""
    result = deepcopy(dict(data))
    facts = {**(context or {}), **data}
    kind = str(data.get("error_type") or "")
    for field in ("error", "error_message", "message"):
        if (
            isinstance(data.get(field), str)
            and data[field]
            and (kind or field != "message" or data.get("ok") is not True)
        ):
            result[field] = public_error_message(kind, data[field], context=facts)
    errors = data.get("artifact_errors")
    if isinstance(errors, list):
        # Parent retained-file facts apply to nested errors; its aggregate cleanup
        # marker must not replace a nested access/validation cause's own meaning.
        nested_context = {key: value for key, value in facts.items() if key != "artifact_errors"}
        result["artifact_errors"] = [
            public_error_payload(item, context=nested_context)
            if isinstance(item, dict)
            else deepcopy(item)
            for item in errors
        ]
    return result
