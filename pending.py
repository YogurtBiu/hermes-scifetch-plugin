"""Durable pending-delivery state for the scifetch-paper Hermes plugin."""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

PENDING_FILENAME = "pending_deliveries.json"
_ACTIVE_STATUSES = frozenset({"pending", "watching", "deferred"})
_STORE_LOCKS: dict[Path, threading.RLock] = {}
_STORE_LOCKS_LOCK = threading.Lock()


class PendingDeliveryStoreError(Exception):
    """Raised when pending delivery state cannot be decoded."""


@dataclass(frozen=True)
class PendingDelivery:
    """Path-free, token-free recovery record for one request delivery."""

    request_id: str
    device_id: str
    platform: str
    chat_id: str
    channel: str
    user_id: str = ""
    last_event_cursor: int = 0
    delivered_file_ids: tuple[str, ...] = ()
    dead_letter_file_ids: tuple[str, ...] = ()
    terminal_texts: tuple[str, ...] = ()
    terminal_outcome: str = ""
    status: str = "pending"
    created_at: str = ""
    updated_at: str = ""

    @property
    def is_active(self) -> bool:
        return self.status in _ACTIVE_STATUSES


class PendingDeliveryStore:
    """JSON-backed per-Hermes-profile pending delivery store."""

    def __init__(self, path: Path):
        self.path = path
        resolved = path.resolve()
        with _STORE_LOCKS_LOCK:
            self._lock = _STORE_LOCKS.setdefault(resolved, threading.RLock())

    def list_active(self) -> list[PendingDelivery]:
        with self._lock:
            return [record for record in self._load().values() if record.is_active]

    def get(self, request_id: str) -> PendingDelivery | None:
        with self._lock:
            return self._load().get(request_id)

    def upsert(
        self,
        *,
        request_id: str,
        device_id: str,
        platform: str,
        chat_id: str,
        channel: str,
        user_id: str = "",
    ) -> PendingDelivery:
        with self._lock:
            records = self._load()
            now = _now_iso()
            existing = records.get(request_id)
            delivered = existing.delivered_file_ids if existing is not None else ()
            dead_letter = existing.dead_letter_file_ids if existing is not None else ()
            created_at = (
                existing.created_at if existing is not None and existing.created_at else now
            )
            record = PendingDelivery(
                request_id=request_id,
                device_id=device_id,
                platform=platform,
                chat_id=chat_id,
                channel=channel,
                user_id=(
                    existing.user_id if existing is not None and existing.user_id else user_id
                ),
                last_event_cursor=existing.last_event_cursor if existing is not None else 0,
                delivered_file_ids=delivered,
                dead_letter_file_ids=dead_letter,
                status="pending",
                created_at=created_at,
                updated_at=now,
            )
            records[request_id] = record
            self._save(records)
            return record

    def persist_terminal_text(
        self,
        request_id: str,
        texts: Iterable[str],
        *,
        outcome: str = "unavailable",
        user_id: str = "",
    ) -> None:
        """Make actionable terminal text durable before its callback is attempted."""
        terminal_texts = tuple(text for text in texts if isinstance(text, str) and text)
        if not terminal_texts:
            return
        updates: dict[str, object] = {
            "status": "terminal_text_pending",
            "terminal_texts": terminal_texts,
            "terminal_outcome": outcome,
        }
        if user_id:
            updates["user_id"] = user_id
        self._replace(request_id, **updates)

    def mark_terminal_outcome(self, request_id: str, outcome: str) -> None:
        self._replace(request_id, terminal_outcome=outcome)

    def mark_watching(self, request_id: str) -> None:
        self._replace(request_id, status="watching")

    def mark_deferred(self, request_id: str) -> None:
        self._replace(request_id, status="deferred")

    def mark_auth_required(self, request_id: str) -> None:
        self._replace(request_id, status="auth_required")

    def mark_active_auth_required(self) -> int:
        with self._lock:
            records = self._load()
            updated = 0
            now = _now_iso()
            for request_id, record in tuple(records.items()):
                if not record.is_active:
                    continue
                records[request_id] = replace(
                    record,
                    status="auth_required",
                    updated_at=now,
                )
                updated += 1
            if updated:
                self._save(records)
            return updated

    def reactivate_auth_required(self, *, device_id: str) -> int:
        with self._lock:
            records = self._load()
            reactivated = 0
            for request_id, record in tuple(records.items()):
                if record.status != "auth_required":
                    continue
                records[request_id] = replace(
                    record,
                    device_id=device_id or record.device_id,
                    status="pending",
                    updated_at=_now_iso(),
                )
                reactivated += 1
            if reactivated:
                self._save(records)
            return reactivated

    def mark_event_seen(self, request_id: str, cursor: int) -> None:
        self._replace(request_id, last_event_cursor=max(0, cursor))

    def mark_delivered(self, request_id: str, file_id: str) -> None:
        if not file_id:
            return
        with self._lock:
            record = self.get(request_id)
            if record is None:
                return
            delivered = tuple(sorted({*record.delivered_file_ids, file_id}))
            self._replace(request_id, delivered_file_ids=delivered)

    def mark_dead_letter(self, request_id: str, file_id: str) -> None:
        if not file_id:
            return
        with self._lock:
            record = self.get(request_id)
            if record is None:
                return
            dead_letter = tuple(sorted({*record.dead_letter_file_ids, file_id}))
            self._replace(request_id, dead_letter_file_ids=dead_letter)

    def remove(self, request_id: str) -> None:
        with self._lock:
            records = self._load()
            if records.pop(request_id, None) is not None:
                self._save(records)

    def _replace(self, request_id: str, **updates: object) -> None:
        with self._lock:
            records = self._load()
            record = records.get(request_id)
            if record is None:
                return
            values = {
                "request_id": record.request_id,
                "device_id": record.device_id,
                "platform": record.platform,
                "chat_id": record.chat_id,
                "channel": record.channel,
                "user_id": record.user_id,
                "last_event_cursor": record.last_event_cursor,
                "delivered_file_ids": record.delivered_file_ids,
                "dead_letter_file_ids": record.dead_letter_file_ids,
                "terminal_texts": record.terminal_texts,
                "terminal_outcome": record.terminal_outcome,
                "status": record.status,
                "created_at": record.created_at,
                "updated_at": _now_iso(),
            }
            values.update(updates)
            records[request_id] = PendingDelivery(
                request_id=cast(str, values["request_id"]),
                device_id=cast(str, values["device_id"]),
                platform=cast(str, values["platform"]),
                chat_id=cast(str, values["chat_id"]),
                channel=cast(str, values["channel"]),
                user_id=cast(str, values["user_id"]),
                last_event_cursor=cast(int, values["last_event_cursor"]),
                delivered_file_ids=cast(tuple[str, ...], values["delivered_file_ids"]),
                dead_letter_file_ids=cast(tuple[str, ...], values["dead_letter_file_ids"]),
                terminal_texts=cast(tuple[str, ...], values["terminal_texts"]),
                terminal_outcome=cast(str, values["terminal_outcome"]),
                status=cast(str, values["status"]),
                created_at=cast(str, values["created_at"]),
                updated_at=cast(str, values["updated_at"]),
            )
            self._save(records)

    def _load(self) -> dict[str, PendingDelivery]:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PendingDeliveryStoreError(
                "SciFetch pending delivery state is corrupted."
            ) from exc
        if not isinstance(data, dict):
            raise PendingDeliveryStoreError("SciFetch pending delivery state is corrupted.")
        raw_records = data.get("deliveries", {})
        if not isinstance(raw_records, dict):
            raise PendingDeliveryStoreError("SciFetch pending delivery state is corrupted.")
        records: dict[str, PendingDelivery] = {}
        for request_id, raw_record in raw_records.items():
            if not isinstance(request_id, str) or not isinstance(raw_record, dict):
                continue
            record = _record_from_json(request_id, raw_record)
            if record is not None:
                records[record.request_id] = record
        return records

    def _save(self, records: Mapping[str, PendingDelivery]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": 2,
            "updated_at": _now_iso(),
            "deliveries": {
                request_id: _record_to_json(record)
                for request_id, record in sorted(records.items())
            },
        }
        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.chmod(tmp_path, 0o600)
        tmp_path.replace(self.path)


def resolve_pending_path(state_path: Path) -> Path:
    return state_path.with_name(PENDING_FILENAME)


def _record_from_json(request_id: str, data: Mapping[str, object]) -> PendingDelivery | None:
    delivered = data.get("delivered_file_ids")
    delivered_file_ids = _string_tuple(delivered if isinstance(delivered, list) else [])
    dead_letter = data.get("dead_letter_file_ids")
    dead_letter_file_ids = _string_tuple(dead_letter if isinstance(dead_letter, list) else [])
    terminal = data.get("terminal_texts")
    record = PendingDelivery(
        request_id=request_id,
        device_id=_string_value(data, "device_id"),
        platform=_string_value(data, "platform") or "hermes",
        chat_id=_string_value(data, "chat_id"),
        channel=_string_value(data, "channel") or "hermes_unknown",
        user_id=_string_value(data, "user_id"),
        last_event_cursor=_int_value(data, "last_event_cursor"),
        delivered_file_ids=delivered_file_ids,
        dead_letter_file_ids=dead_letter_file_ids,
        terminal_texts=_ordered_string_tuple(terminal if isinstance(terminal, list) else []),
        terminal_outcome=_string_value(data, "terminal_outcome"),
        status=_string_value(data, "status") or "pending",
        created_at=_string_value(data, "created_at"),
        updated_at=_string_value(data, "updated_at"),
    )
    if not record.request_id or not record.chat_id:
        return None
    return record


def _record_to_json(record: PendingDelivery) -> dict[str, object]:
    return {
        "device_id": record.device_id,
        "platform": record.platform,
        "chat_id": record.chat_id,
        "channel": record.channel,
        "user_id": record.user_id,
        "last_event_cursor": record.last_event_cursor,
        "delivered_file_ids": list(record.delivered_file_ids),
        "dead_letter_file_ids": list(record.dead_letter_file_ids),
        "terminal_texts": list(record.terminal_texts),
        "terminal_outcome": record.terminal_outcome,
        "status": record.status,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _string_tuple(values: Iterable[object]) -> tuple[str, ...]:
    return tuple(sorted({value for value in values if isinstance(value, str) and value}))


def _ordered_string_tuple(values: Iterable[object]) -> tuple[str, ...]:
    return tuple(value for value in values if isinstance(value, str) and value)


def _string_value(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else ""


def _int_value(data: Mapping[str, object], key: str) -> int:
    value = data.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
