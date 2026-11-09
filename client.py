"""HTTP client for the scifetch-paper Hermes plugin."""

from __future__ import annotations

import json as json_module
from dataclasses import dataclass
from typing import Any

import httpx

from .public_errors import public_error_message


@dataclass(frozen=True)
class SciFetchClientConfig:
    """Server pointer shared by plugin command handlers."""

    server_url: str
    timeout_seconds: float = 10.0


class SciFetchPluginError(Exception):
    """User-visible server/client error without token material."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class SciFetchPluginClient:
    """Small synchronous client used by Hermes slash-command handlers."""

    config: SciFetchClientConfig
    transport: httpx.BaseTransport | None = None

    def pair_device(
        self,
        *,
        pairing_code: str,
        platform: str,
        platform_user_id: str,
        chat_id: str,
        trust_mode: str,
        installation_id: str,
        plugin_version: str,
        display_name: str,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/devices/pair",
            json={
                "pairing_code": pairing_code,
                "platform": platform,
                "platform_user_id": platform_user_id,
                "chat_id": chat_id,
                "trust_mode": trust_mode,
                "installation_id": installation_id,
                "plugin_version": plugin_version,
                "display_name": display_name,
            },
        )

    def get_me(
        self,
        *,
        access_token: str,
        device_id: str,
        trust_mode: str = "",
        installation_id: str = "",
        platform: str = "",
        platform_user_id: str = "",
        chat_id: str = "",
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/v1/me",
            headers=_auth_headers(
                access_token=access_token,
                device_id=device_id,
                trust_mode=trust_mode,
                installation_id=installation_id,
                platform=platform,
                platform_user_id=platform_user_id,
                chat_id=chat_id,
            ),
        )

    def revoke_device(
        self,
        *,
        access_token: str,
        device_id: str,
        trust_mode: str = "",
        installation_id: str = "",
        platform: str = "",
        platform_user_id: str = "",
        chat_id: str = "",
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/devices/revoke",
            headers=_auth_headers(
                access_token=access_token,
                device_id=device_id,
                trust_mode=trust_mode,
                installation_id=installation_id,
                platform=platform,
                platform_user_id=platform_user_id,
                chat_id=chat_id,
            ),
            json={},
        )

    def refresh_device_token(
        self,
        *,
        refresh_token: str,
        device_id: str,
        trust_mode: str = "",
        installation_id: str = "",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "refresh_token": refresh_token,
            "device_id": device_id,
        }
        if trust_mode:
            payload["trust_mode"] = trust_mode
        if installation_id:
            payload["installation_id"] = installation_id
        return self._request_json(
            "POST",
            "/v1/devices/refresh-token",
            headers={"X-Client-ID": "hermes-scifetch-paper"},
            json=payload,
        )

    def submit_paper_request(
        self,
        *,
        raw_text: str,
        access_token: str,
        device_id: str,
        client_request_id: str,
        platform: str = "",
        platform_user_id: str = "",
        chat_id: str = "",
        trust_mode: str = "",
        installation_id: str = "",
        force: bool = False,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/paper-requests",
            headers=_auth_headers(
                access_token=access_token,
                device_id=device_id,
                platform=platform,
                platform_user_id=platform_user_id,
                chat_id=chat_id,
                trust_mode=trust_mode,
                installation_id=installation_id,
            ),
            json={
                "client_request_id": client_request_id or None,
                "query": {
                    "raw_text": raw_text,
                    "detected_queries": [],
                    "force": force,
                },
            },
        )

    def list_paper_request_events(
        self,
        *,
        request_id: str,
        access_token: str,
        device_id: str,
        trust_mode: str = "",
        installation_id: str = "",
        platform: str = "",
        platform_user_id: str = "",
        chat_id: str = "",
    ) -> list[dict[str, Any]]:
        text = self._request_text(
            "GET",
            f"/v1/paper-requests/{request_id}/events",
            headers=_auth_headers(
                access_token=access_token,
                device_id=device_id,
                trust_mode=trust_mode,
                installation_id=installation_id,
                platform=platform,
                platform_user_id=platform_user_id,
                chat_id=chat_id,
            ),
        )
        return _parse_sse_events(text)

    def list_paper_request_files(
        self,
        *,
        request_id: str,
        access_token: str,
        device_id: str,
        trust_mode: str = "",
        installation_id: str = "",
        platform: str = "",
        platform_user_id: str = "",
        chat_id: str = "",
    ) -> list[dict[str, Any]]:
        return self._request_json_list(
            "GET",
            f"/v1/paper-requests/{request_id}/files",
            headers=_auth_headers(
                access_token=access_token,
                device_id=device_id,
                trust_mode=trust_mode,
                installation_id=installation_id,
                platform=platform,
                platform_user_id=platform_user_id,
                chat_id=chat_id,
            ),
        )

    def list_my_scipaper_notifications(
        self,
        *,
        access_token: str,
        device_id: str,
        limit: int = 20,
        trust_mode: str = "",
        installation_id: str = "",
        platform: str = "",
        platform_user_id: str = "",
        chat_id: str = "",
    ) -> list[dict[str, Any]]:
        return self._request_json_list(
            "GET",
            "/v1/me/notifications/due",
            headers=_auth_headers(
                access_token=access_token,
                device_id=device_id,
                trust_mode=trust_mode,
                installation_id=installation_id,
                platform=platform,
                platform_user_id=platform_user_id,
                chat_id=chat_id,
            ),
            params={"limit": str(limit)},
        )

    def acknowledge_my_scipaper_notification(
        self,
        *,
        notification_id: str,
        ack_token: str,
        access_token: str,
        device_id: str,
        channel: str = "hermes_plugin",
        status: str = "sent",
        attempt: int = 1,
        metadata: dict[str, Any] | None = None,
        trust_mode: str = "",
        installation_id: str = "",
        platform: str = "",
        platform_user_id: str = "",
        chat_id: str = "",
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            f"/v1/me/notifications/{notification_id}/ack",
            headers=_auth_headers(
                access_token=access_token,
                device_id=device_id,
                trust_mode=trust_mode,
                installation_id=installation_id,
                platform=platform,
                platform_user_id=platform_user_id,
                chat_id=chat_id,
            ),
            json={
                "notification_id": notification_id,
                "ack_token": ack_token,
                "channel": channel,
                "status": status,
                "attempt": attempt,
                "metadata": metadata or {},
            },
        )

    def health(self) -> dict[str, Any]:
        return self._request_json("GET", "/healthz")

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        data = self._request_json_data(method, path, headers=headers, json=json, params=params)
        if not isinstance(data, dict):
            raise SciFetchPluginError("SciFetch 服务返回的结果无法解析，请查看请求状态。")
        return data

    def _request_json_list(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        data = self._request_json_data(method, path, headers=headers, json=json, params=params)
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise SciFetchPluginError("SciFetch 服务返回的结果无法解析，请查看请求状态。")
        return data

    def _request_json_data(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> Any:
        try:
            with httpx.Client(
                base_url=self.config.server_url.rstrip("/"),
                timeout=self.config.timeout_seconds,
                transport=self.transport,
                trust_env=False,
            ) as client:
                response = client.request(
                    method,
                    path,
                    headers=headers,
                    json=json,
                    params=params,
                )
        except httpx.RequestError as exc:
            raise SciFetchPluginError(
                "与 SciFetch 服务的通信未完成，结果尚未确认，请查看请求状态。"
            ) from exc

        if response.status_code >= 400:
            raise SciFetchPluginError(
                _api_error_message(response), status_code=response.status_code
            )
        try:
            return response.json()
        except ValueError as exc:
            raise SciFetchPluginError("SciFetch 服务返回的结果无法解析，请查看请求状态。") from exc

    def _request_text(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> str:
        try:
            with httpx.Client(
                base_url=self.config.server_url.rstrip("/"),
                timeout=self.config.timeout_seconds,
                transport=self.transport,
                trust_env=False,
            ) as client:
                response = client.request(
                    method,
                    path,
                    headers=headers,
                    json=json,
                    params=params,
                )
        except httpx.ReadTimeout as exc:
            raise SciFetchPluginError(
                "SciFetch 状态请求超时，结果尚未确认，请稍后查看请求详情。"
            ) from exc
        except httpx.RequestError as exc:
            raise SciFetchPluginError(
                "与 SciFetch 服务的通信未完成，结果尚未确认，请查看请求状态。"
            ) from exc

        if response.status_code >= 400:
            raise SciFetchPluginError(
                _api_error_message(response), status_code=response.status_code
            )
        return response.text


def _api_error_message(response: httpx.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        data = {}
    body = data if isinstance(data, dict) else {}
    code = body.get("code", "")
    return public_error_message(
        code if isinstance(code, str) else "",
        body.get("message", ""),
        origin="api",
        context={**body, "status_code": response.status_code},
    )


def _auth_headers(
    *,
    access_token: str,
    device_id: str,
    platform: str = "",
    platform_user_id: str = "",
    chat_id: str = "",
    trust_mode: str = "",
    installation_id: str = "",
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Scifetch-Device-ID": device_id,
        "X-Client-ID": "hermes-scifetch-paper",
    }
    if platform:
        headers["X-Scifetch-Platform"] = platform
    if platform_user_id:
        headers["X-Scifetch-Platform-User-ID"] = platform_user_id
    if chat_id:
        headers["X-Scifetch-Chat-ID"] = chat_id
    if trust_mode:
        headers["X-Scifetch-Trust-Mode"] = trust_mode
    if installation_id:
        headers["X-Scifetch-Installation-ID"] = installation_id
    return headers


def _parse_sse_events(text: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    event_name = "message"
    data_lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip("\r")
        if not line:
            _append_sse_event(events, event_name, data_lines)
            event_name = "message"
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    _append_sse_event(events, event_name, data_lines)
    return events


def _append_sse_event(
    events: list[dict[str, Any]],
    event_name: str,
    data_lines: list[str],
) -> None:
    if not data_lines:
        return
    try:
        decoded = json_module.loads("\n".join(data_lines))
    except json_module.JSONDecodeError:
        return
    if not isinstance(decoded, dict):
        return
    decoded.setdefault("event_type", event_name)
    events.append(decoded)
