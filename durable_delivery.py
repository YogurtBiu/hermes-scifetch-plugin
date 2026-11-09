"""Immutable event ingestion and installation-owned native delivery workers.

Only the ingestion transaction runs in an SSE reader. Network staging, native
invocation and the remote audit projection have independent bounded workers.
Credentials and native callbacks are ephemeral and never enter the ledger.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import threading
import time
import uuid
from collections.abc import Mapping
from concurrent.futures import Future, InvalidStateError
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import httpx

from . import delivery as legacy
from .outbox import (
    Artifact,
    DeliveryRecord,
    GateScope,
    NativeAttempt,
    NativeReceipt,
    OutboxError,
    OutboxStore,
    TargetBinding,
)
from .state import PluginState, load_state

logger = logging.getLogger(__name__)
_NATIVE_TIMEOUT = 180.0
_WORKER_POLL = 0.25
_STAGE_TIMEOUT = 120.0
_AUDIT_TIMEOUT = 20.0
_MAX_NATIVE_CALLS = 4
_STARTUP_TIMEOUT = 5.0
_REGISTRY_LOCK = threading.Lock()
_SUPERVISORS: dict[str, DeliverySupervisor] = {}


def _binding(runtime: legacy.DeliveryRuntime) -> TargetBinding:
    target = runtime.target
    return TargetBinding(
        installation_id=runtime.state.installation_id,
        platform=legacy._canonical_delivery_platform(target.platform),
        chat_id=target.chat_id,
        user_id=target.user_id,
        chat_type=target.chat_type,
        thread_id=target.thread_id,
    )


def _valid_runtime(runtime: legacy.DeliveryRuntime) -> bool:
    return bool(
        runtime.state.is_authenticated
        and runtime.state.trust_mode == "personal_instance"
        and runtime.state.installation_id
        and runtime.state_path is not None
        and runtime.target.platform
        and runtime.target.chat_id
        and runtime.target.user_id
        and runtime.target.chat_type
        and not runtime.target.thread_id  # No audited Weixin/QQ threaded native route.
    )


def open_store(runtime: legacy.DeliveryRuntime) -> OutboxStore:
    if not _valid_runtime(runtime) or runtime.state_path is None:
        raise ValueError("Durable delivery requires an authenticated installation and exact target")
    store = OutboxStore(
        runtime.state_path.with_name("delivery_outbox.sqlite3"),
        runtime.state.installation_id,
    )
    if runtime.pending_path is not None and runtime.pending_path.exists():
        store.import_legacy_json(runtime.pending_path)
    return store


def persist_request_intent(runtime: legacy.DeliveryRuntime) -> bool:
    """Save the initially closed intent before a watcher or native worker starts."""
    if not _valid_runtime(runtime):
        return False
    try:
        store = open_store(runtime)
        store.create_request_intent(
            runtime.state.server_url.rstrip("/"),
            runtime.state.device_id,
            runtime.request_id,
            _binding(runtime),
        )
        return True
    except Exception:
        logger.warning("could not persist durable delivery intent")
        return False


def _runtime_records(store: OutboxStore, runtime: legacy.DeliveryRuntime) -> list[DeliveryRecord]:
    return [
        r
        for r in store.list_deliveries(runtime.request_id)
        if r.scope.server_identity == runtime.state.server_url.rstrip("/")
        and r.scope.account_scope == runtime.state.device_id
        and r.scope.installation_id == runtime.state.installation_id
        and r.target.binding_id == _binding(runtime).binding_id
    ]


def _qualified_native_result(result: object) -> bool:
    receipt = legacy._normalize_send_result(result)
    method = legacy._result_field(result, "delivery_method")
    # The public bridge calls the concrete send_document method. Stock receipts
    # omit a method marker; an explicit marker must name that same operation.
    native_method = method is None or (
        isinstance(method, str) and method.strip().casefold() == "send_document"
    )
    return receipt.success and not receipt.error_kind and native_method


def _timestamp(value: object) -> float:
    if not isinstance(value, str) or not value:
        raise ValueError("Missing immutable artifact retention deadline")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Artifact retention deadline must include timezone")
    return parsed.timestamp()


def _required(data: Mapping[str, object], name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing immutable artifact {name}")
    return value


def ingest_file(
    store: OutboxStore,
    runtime: legacy.DeliveryRuntime,
    payload: Mapping[str, object],
) -> DeliveryRecord:
    """Perform one short durable identity upsert; never fetch bytes or send here."""
    request_id = payload.get("request_id", runtime.request_id)
    if request_id != runtime.request_id:
        raise ValueError("Artifact request identity mismatch")
    target = _binding(runtime)
    scope = GateScope(
        server_identity=runtime.state.server_url.rstrip("/"),
        account_scope=runtime.state.device_id,
        installation_id=runtime.state.installation_id,
        request_id=runtime.request_id,
        request_item_id=_required(payload, "item_id"),
        acquisition_id=_required(payload, "acquisition_id"),
        generation_id=_required(payload, "generation_id"),
        target_binding_id=target.binding_id,
    )
    size = payload.get("size_bytes")
    revision = payload.get("readiness_revision")
    ordinal = payload.get("si_index", 0) or 0
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise ValueError("Invalid immutable artifact size")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision <= 0:
        raise ValueError("Missing durable readiness revision")
    if not isinstance(ordinal, int) or isinstance(ordinal, bool) or ordinal < 0:
        raise ValueError("Invalid attachment ordinal")
    artifact = Artifact(
        logical_file_id=_required(payload, "file_id"),
        artifact_id=_required(payload, "artifact_id"),
        role=_required(payload, "role").upper(),
        sha256=_required(payload, "sha256"),
        size=size,
        media_type=_required(payload, "content_type"),
        ordinal=ordinal,
        integrity_status=_required(payload, "integrity_status"),
        filename=legacy._string_value(payload, "filename"),
    )
    record = store.enqueue(
        scope,
        target,
        artifact,
        revision=revision,
        expires_at=_timestamp(payload.get("retention_expires_at")),
    )
    if artifact.integrity_status == "rejected":
        store.block_delivery(record.key, "integrity_rejected")
        return store.get_delivery(record.key) or record
    return record


async def _request_json(
    runtime: legacy.DeliveryRuntime,
    method: str,
    path: str,
    payload: Mapping[str, object] | None = None,
) -> object:
    async with legacy._async_client(runtime) as client:
        response = await client.request(
            method, path, headers=legacy._auth_headers(runtime), json=payload
        )
    if response.status_code in {401, 403}:
        raise legacy.WatcherAuthenticationError("SciFetch delivery authentication required")
    response.raise_for_status()
    return response.json()


def apply_closed_scopes(
    store: OutboxStore,
    runtime: legacy.DeliveryRuntime,
    payload: Mapping[str, object],
) -> None:
    """Apply only authoritative exact server cancellation/MAIN-failure scopes."""
    scopes = payload.get("closed_scopes", [])
    if not isinstance(scopes, list):
        raise ValueError("Invalid closed delivery scopes")
    for value in scopes:
        if not isinstance(value, dict) or value.get("request_id") != runtime.request_id:
            raise ValueError("Closed scope request mismatch")
        status = value.get("status")
        if status not in {"cancelled", "failed"}:
            raise ValueError("Unqualified terminal delivery revocation")
        store.revoke_item(
            runtime.state.server_url.rstrip("/"),
            runtime.state.device_id,
            runtime.request_id,
            _required(value, "item_id"),
            _binding(runtime).binding_id,
            reason="cancelled" if status == "cancelled" else "main_failed",
            acquisition_id=_required(value, "acquisition_id"),
            generation_id=_required(value, "generation_id"),
        )


async def reconcile_snapshot(
    store: OutboxStore,
    runtime: legacy.DeliveryRuntime,
    metadata: dict[str, dict[str, object]] | None = None,
) -> int:
    data = await _request_json(
        runtime, "GET", f"/v1/paper-requests/{runtime.request_id}/delivery-snapshot"
    )
    if not isinstance(data, dict) or data.get("request_id") != runtime.request_id:
        raise ValueError("Invalid delivery snapshot identity")
    apply_closed_scopes(store, runtime, data)
    files = data.get("files")
    if not isinstance(files, list):
        raise ValueError("Invalid delivery snapshot files")
    for record in files:
        if not isinstance(record, dict):
            raise ValueError("Invalid delivery snapshot artifact")
        delivery = ingest_file(store, runtime, record)
        if metadata is not None:
            metadata[delivery.key] = legacy._with_public_file_metadata(
                dict(record), metadata.get(delivery.key, {})
            )
    revision = data.get("readiness_revision", 0)
    if not isinstance(revision, int):
        raise ValueError("Invalid delivery snapshot revision")
    return revision


class DeliverySupervisor:
    """One native owner per installation; routes are refreshed only post-auth."""

    def __init__(self, store: OutboxStore) -> None:
        self.store = store
        self.owner = "sender_" + uuid.uuid4().hex
        self._routes: dict[tuple[str, str, str, str], legacy.DeliveryRuntime] = {}
        self._routes_lock = threading.Lock()
        self._reconciliations: dict[tuple[str, str, str, str], legacy.DeliveryRuntime] = {}
        self._wake: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._notifications: set[asyncio.Task[None]] = set()
        self._native: set[asyncio.Task[None]] = set()
        self._staging: set[str] = set()
        self._stage_tasks: set[asyncio.Task[None]] = set()
        self._stage_retry: dict[str, float] = {}
        self._pin_retry: dict[str, float] = {}
        self._fatal = False
        self._stopping = False
        self._lock = store.sender_lock()

    def register(self, runtime: legacy.DeliveryRuntime) -> None:
        if not _valid_runtime(runtime):
            raise ValueError("Invalid authenticated native route")
        key = (
            runtime.state.server_url.rstrip("/"),
            runtime.state.device_id,
            runtime.request_id,
            _binding(runtime).binding_id,
        )
        with self._routes_lock:
            previous = self._routes.get(key)
            self._routes[key] = runtime
        if runtime.native_document_sender is not None and (
            previous is None
            or previous.native_document_sender is not runtime.native_document_sender
        ):
            # Only a fresh authenticated callback installed after its ACK may
            # revive QQ/context-deferred work, never a reconnect with the old one.
            self.store.resume_target(
                _binding(runtime),
                request_id=runtime.request_id,
                server_identity=runtime.state.server_url.rstrip("/"),
                account_scope=runtime.state.device_id,
            )
        self.wake()

    def unregister(self, runtime: legacy.DeliveryRuntime) -> None:
        key = (
            runtime.state.server_url.rstrip("/"),
            runtime.state.device_id,
            runtime.request_id,
            _binding(runtime).binding_id,
        )
        with self._routes_lock:
            self._routes.pop(key, None)
        for record in _runtime_records(self.store, runtime):
            if record.state == "pending":
                self.store.defer_delivery(record.key, "waiting_context")

    def authentication_failed(self, runtime: legacy.DeliveryRuntime) -> None:
        """A current server denial fences every callback sharing that credential owner."""
        server, account = runtime.state.server_url.rstrip("/"), runtime.state.device_id
        with self._routes_lock:
            for key in list(self._routes):
                if key[:2] == (server, account):
                    self._routes.pop(key, None)
        for record in self.store.list_deliveries():
            if (
                record.scope.server_identity == server
                and record.scope.account_scope == account
                and record.state == "pending"
            ):
                self.store.defer_delivery(record.key, "waiting_context")

    def wake(self) -> None:
        if self._loop is not None and self._wake is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._wake.set)

    def request_reconciliation(self, runtime: legacy.DeliveryRuntime) -> None:
        with self._routes_lock:
            key = (
                runtime.state.server_url,
                runtime.state.device_id,
                runtime.request_id,
                _binding(runtime).binding_id,
            )
            self._reconciliations[key] = runtime
        self.wake()

    async def _reconcile_loop(self) -> None:
        while not self._stopping:
            with self._routes_lock:
                work = list(self._reconciliations.values())
                self._reconciliations.clear()
            for runtime in work:
                try:
                    runtime = await legacy._ensure_fresh_runtime(runtime)
                    await asyncio.wait_for(
                        reconcile_snapshot(self.store, runtime), timeout=_AUDIT_TIMEOUT
                    )
                    self.wake()
                except asyncio.CancelledError:
                    raise
                except legacy.WatcherAuthenticationError:
                    self.authentication_failed(runtime)
                except Exception:
                    # Normal reconnect still performs a full permission-scoped
                    # snapshot; a gap never causes DOI submission or native replay.
                    logger.info("readiness gap awaits authenticated snapshot reconciliation")
            await self._pause()

    def notify(self, runtime: legacy.DeliveryRuntime, message: str) -> None:
        """Queue current progress text without blocking the SSE reader."""
        if self._loop is None or self._loop.is_closed() or runtime.send_text is None:
            return

        async def send() -> None:
            try:
                await self.send_text(runtime, message)
            except Exception:
                logger.info("current delivery progress notification unavailable")

        def schedule() -> None:
            task = asyncio.create_task(send())
            self._notifications.add(task)
            task.add_done_callback(self._notifications.discard)

        self._loop.call_soon_threadsafe(schedule)

    def _route(self, record: DeliveryRecord) -> legacy.DeliveryRuntime | None:
        scope = record.scope
        with self._routes_lock:
            runtime = self._routes.get(
                (
                    scope.server_identity,
                    scope.account_scope,
                    scope.request_id,
                    scope.target_binding_id,
                )
            )
        if runtime is None:
            return None
        if runtime.state_path is not None:
            latest = load_state(runtime.state_path)
            if latest is None or not latest.is_authenticated:
                return None
            if (
                latest.server_url.rstrip("/") != scope.server_identity
                or latest.device_id != scope.account_scope
                or latest.installation_id != scope.installation_id
                or latest.trust_mode != "personal_instance"
            ):
                return None
        return replace(runtime, request_id=scope.request_id)

    async def start(self) -> bool:
        if not self._lock.acquire():
            return False
        try:
            self.store.recover(self.owner)
            self._loop = asyncio.get_running_loop()
            self._wake = asyncio.Event()
            self._tasks = [
                asyncio.create_task(self._stage_loop()),
                asyncio.create_task(self._send_loop()),
                asyncio.create_task(self._audit_loop()),
                asyncio.create_task(self._retention_loop()),
                asyncio.create_task(self._reconcile_loop()),
            ]
        except BaseException:
            await self.close()
            raise
        return True

    async def close(self) -> None:
        """Stop admission; do not cancel or forget an actual native invocation."""
        self._stopping = True
        self.wake()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for task in self._stage_tasks:
            task.cancel()
        await asyncio.gather(*self._stage_tasks, return_exceptions=True)
        if self._notifications:
            await asyncio.gather(*self._notifications, return_exceptions=True)
        if self._native:
            await asyncio.gather(*self._native, return_exceptions=True)
        self._lock.release()

    async def _pause(self) -> None:
        assert self._wake is not None
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=_WORKER_POLL)
        self._wake.clear()

    async def _stage_loop(self) -> None:
        while not self._stopping:
            records = sorted(self.store.list_deliveries(), key=lambda r: r.artifact.role != "MAIN")
            for record in records:
                if self._fatal or self._stopping or len(self._staging) >= 2:
                    break
                if (
                    record.state != "pending"
                    or record.key in self._staging
                    or record.block_reason in {"cancelled", "main_failed"}
                ):
                    continue
                if record.staged_path:
                    if await asyncio.to_thread(Path(record.staged_path).is_file):
                        continue
                    self.store.clear_staged(record.key)
                if (
                    record.artifact.integrity_status != "validated"
                    or record.expires_at <= time.time()
                ):
                    continue
                if self._stage_retry.get(record.key, 0) > time.time():
                    continue
                runtime = self._route(record)
                if runtime is None:
                    continue
                self._staging.add(record.key)
                task = asyncio.create_task(self._stage_one(record, runtime))
                self._stage_tasks.add(task)
                task.add_done_callback(self._stage_tasks.discard)
            await self._pause()

    async def _stage_one(self, record: DeliveryRecord, runtime: legacy.DeliveryRuntime) -> None:
        try:
            await asyncio.wait_for(self._stage(record, runtime), timeout=_STAGE_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except legacy.WatcherAuthenticationError:
            self.authentication_failed(runtime)
        except Exception:
            self._stage_retry[record.key] = time.time() + 5.0
            logger.warning("immutable artifact staging deferred")
        finally:
            self._staging.discard(record.key)
            self.wake()

    async def _stage(self, record: DeliveryRecord, runtime: legacy.DeliveryRuntime) -> None:
        runtime = await legacy._ensure_fresh_runtime(runtime)
        if self._route(record) is None:
            return
        base = (
            f"/v1/paper-requests/{record.scope.request_id}/artifacts/{record.artifact.artifact_id}"
        )
        pin = await _request_json(
            runtime,
            "POST",
            base + "/pin",
            {
                "action": "claim",
                "generation_id": record.scope.generation_id,
                "item_id": record.scope.request_item_id,
            },
        )
        if not isinstance(pin, dict):
            raise ValueError("Invalid artifact retention claim")
        self.store.record_server_pin(
            record.key,
            _required(pin, "owner_token"),
            int(pin["owner_version"]),
            _timestamp(pin.get("expires_at")),
            _timestamp(pin.get("hard_expires_at")),
        )
        data = await _request_json(runtime, "GET", base + "/download-url")
        if not isinstance(data, dict):
            raise ValueError("Invalid immutable download grant")
        for name, expected in {
            "artifact_id": record.artifact.artifact_id,
            "generation_id": record.scope.generation_id,
            "acquisition_id": record.scope.acquisition_id,
            "item_id": record.scope.request_item_id,
            "sha256": record.artifact.sha256,
            "size_bytes": record.artifact.size,
            "role": record.artifact.role,
            "content_type": record.artifact.media_type,
        }.items():
            observed = data.get(name)
            if name == "role" and isinstance(observed, str):
                observed = observed.upper()
            if observed != expected:
                raise ValueError("Immutable download grant identity changed")
        preflight = legacy._preflight_delivery(runtime, data)
        if preflight is not None or runtime.native_document_sender is None:
            self.store.block_delivery(record.key, "unsupported_native")
            return
        assert runtime.state_path is not None
        directory = runtime.state_path.parent / "delivery-staging" / record.key
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(directory, 0o700)
        filename = Path(legacy._string_value(data, "filename") or "document").name
        if filename in {"", ".", ".."}:
            raise ValueError("Invalid artifact filename")
        destination = directory / filename
        await legacy._download_file(runtime, data, destination)
        os.chmod(destination, 0o600)
        if legacy._requires_weixin_zip_validation(runtime, data):
            import zipfile

            if not await asyncio.to_thread(zipfile.is_zipfile, destination):
                self.store.block_delivery(record.key, "invalid_zip")
                return
        self.store.record_staged(record.key, destination)

    async def _send_loop(self) -> None:
        while not self._stopping:
            if not self._fatal and len(self._native) < _MAX_NATIVE_CALLS:
                # Missing context must not claim or churn attempts after restart.
                allowed_targets = {
                    r.target.binding_id
                    for r in self.store.list_deliveries()
                    if r.state == "pending" and r.staged_path and self._route(r) is not None
                }
                for _ in range(_MAX_NATIVE_CALLS - len(self._native)):
                    attempt = self.store.claim_next(self.owner, allowed_target_ids=allowed_targets)
                    if attempt is None:
                        continue
                    runtime = self._route(attempt.delivery)
                    if runtime is None:
                        self.store.fail(
                            attempt.attempt_id,
                            "waiting_context",
                            proven_not_accepted=True,
                            retryable=True,
                        )
                        continue
                    task = asyncio.create_task(self._send(attempt, runtime))
                    self._native.add(task)
                    task.add_done_callback(self._native.discard)
                    if len(self._native) >= _MAX_NATIVE_CALLS:
                        break
            await self._pause()

    async def _send(self, attempt: NativeAttempt, runtime: legacy.DeliveryRuntime) -> None:
        record = attempt.delivery
        started = False
        result_received = False
        staging_invalid = False
        route_changed = False
        route_missing = False
        call: asyncio.Task[object] | None = None

        def before_native() -> bool:
            nonlocal started, staging_invalid, route_changed, route_missing
            current = self._route(record)
            route_missing = current is None
            route_changed = (
                current is not None
                and current.native_document_sender is not runtime.native_document_sender
            )
            if self._fatal or self._stopping or route_missing or route_changed:
                return False
            assert current is not None
            if (
                legacy._preflight_delivery(
                    current,
                    {
                        "filename": record.artifact.filename,
                        "content_type": record.artifact.media_type,
                        "size_bytes": record.artifact.size,
                    },
                )
                is not None
            ):
                return False
            # Recheck bytes after staging and immediately before native submission.
            try:
                self.store.record_staged(record.key, Path(record.staged_path))
            except (OSError, OutboxError):
                staging_invalid = True
                return False
            try:
                started = self.store.begin_native(attempt.attempt_id, attempt.start_token)
            except Exception:
                self._fatal = True
                return False
            return started

        try:
            sender = runtime.native_document_sender
            if sender is None:
                self.store.fail(attempt.attempt_id, "unsupported_native", proven_not_accepted=True)
                return
            call = asyncio.create_task(
                legacy._maybe_await(sender(record.staged_path, before_native))
            )
            # shield preserves ownership: a timeout is not proof the native upload stopped.
            try:
                result = await asyncio.wait_for(asyncio.shield(call), timeout=_NATIVE_TIMEOUT)
            except asyncio.TimeoutError:
                self.store.mark_unknown(attempt.attempt_id, "native_timeout", stopped=False)
                result = await asyncio.shield(call)
            result_received = True
            receipt = legacy._normalize_send_result(result)
            if receipt.retry_after is not None:
                self.store.set_cooldown(record.target, receipt.retry_after, "native_retry_after")
            if started and _qualified_native_result(result):
                self.store.accept(
                    attempt.attempt_id,
                    NativeReceipt(
                        method="send_document",
                        capability_revision=f"{runtime.target.platform}:{runtime.target.hermes_version}:{runtime.target.chat_type}",
                        message_id=receipt.message_id,
                    ),
                    stopped=True,
                )
            elif not started:
                self.store.fail(
                    attempt.attempt_id,
                    receipt.error_kind or "preflight_refused",
                    proven_not_accepted=True,
                    retryable=staging_invalid
                    or route_changed
                    or route_missing
                    or receipt.error_kind.startswith("qq_passive_reply_"),
                )
                if staging_invalid:
                    self.store.clear_staged(record.key)
                if route_missing or receipt.error_kind.startswith("qq_passive_reply_"):
                    self.store.defer_delivery(record.key, "waiting_context")
            else:
                # False/None/exception after possible submission is never a blind retry.
                self.store.mark_unknown(
                    attempt.attempt_id,
                    receipt.error_kind or "ambiguous_native_result",
                    stopped=True,
                )
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                self.store.mark_unknown(attempt.attempt_id, "sender_cancelled", stopped=False)
            if call is not None:

                async def reconcile_late() -> None:
                    late_received = False
                    try:
                        late = await asyncio.shield(call)
                        late_received = True
                        receipt = legacy._normalize_send_result(late)
                        if started and _qualified_native_result(late):
                            self.store.accept(
                                attempt.attempt_id,
                                NativeReceipt(
                                    method="send_document",
                                    capability_revision=f"{runtime.target.platform}:{runtime.target.hermes_version}:{runtime.target.chat_type}",
                                    message_id=receipt.message_id,
                                ),
                                stopped=True,
                            )
                        else:
                            self.store.mark_unknown(
                                attempt.attempt_id, "late_ambiguous_result", stopped=True
                            )
                    except Exception:
                        if late_received:
                            self._fatal = True
                        with contextlib.suppress(Exception):
                            self.store.mark_unknown(
                                attempt.attempt_id, "late_native_error", stopped=late_received
                            )
                    self.wake()

                late_task = asyncio.create_task(reconcile_late())
                self._native.add(late_task)
                late_task.add_done_callback(self._native.discard)
            raise
        except Exception:
            with contextlib.suppress(Exception):
                if not started:
                    self.store.fail(
                        attempt.attempt_id,
                        "preinvoke_error",
                        proven_not_accepted=True,
                        retryable=staging_invalid or route_changed or route_missing,
                    )
                    if route_missing:
                        self.store.defer_delivery(record.key, "waiting_context")
                    if staging_invalid:
                        self.store.clear_staged(record.key)
                else:
                    self.store.mark_unknown(
                        attempt.attempt_id,
                        "native_or_receipt_error",
                        stopped=result_received,
                    )
            # A native exception remains unknown; only failure to persist a
            # returned receipt poisons installation-wide write health.
            if result_received:
                self._fatal = True
            logger.warning("native delivery stopped pending durable receipt reconciliation")
        finally:
            self.wake()

    async def _audit_loop(self) -> None:
        while not self._stopping:
            for audit in self.store.pending_audits():
                record = audit.delivery
                runtime = self._route(record)
                if runtime is None:
                    continue
                try:
                    runtime = await legacy._ensure_fresh_runtime(runtime)
                    payload: dict[str, object] = {
                        "status": audit.payload["status"],
                        "attempt": audit.payload["attempt"],
                        "error_code": audit.payload.get("error_code"),
                        "metadata": {
                            "filename": record.artifact.filename,
                            "role": record.artifact.role.lower(),
                            **audit.payload,
                            "artifact_id": record.artifact.artifact_id,
                            "acquisition_id": record.scope.acquisition_id,
                            "generation_id": record.scope.generation_id,
                            "chat_id": record.target.chat_id,
                        },
                    }
                    payload.update(
                        {
                            "audit_id": audit.audit_id,
                            "payload_version": 1,
                            "request_id": record.scope.request_id,
                            "item_id": record.scope.request_item_id,
                            "file_id": record.artifact.logical_file_id,
                            "channel": f"hermes_{record.target.platform}",
                        }
                    )
                    await asyncio.wait_for(
                        _request_json(runtime, "POST", "/v1/delivery-attempts", payload),
                        timeout=_AUDIT_TIMEOUT,
                    )
                    self.store.mark_audit_synced(audit.audit_id)
                except asyncio.CancelledError:
                    raise
                except legacy.WatcherAuthenticationError:
                    self.authentication_failed(runtime)
                except Exception:
                    self.store.defer_audit(audit.audit_id, retry_after=5.0)
                    logger.info("durable native receipt awaits remote audit repair")
            await self._pause()

    async def _retention_loop(self) -> None:
        while not self._stopping:
            now = time.time()
            audit_keys = {
                audit.delivery.key for audit in self.store.pending_audits(now=float("inf"))
            }
            records = self.store.list_deliveries()
            pins = {pin.delivery_key: pin for pin in self.store.list_server_pins()}
            handled: set[tuple[str, str, str]] = set()
            for record in records:
                pin = pins.get(record.key)
                if pin is None or self._pin_retry.get(record.key, 0) > now:
                    continue
                identity = (
                    record.scope.server_identity,
                    record.scope.account_scope,
                    record.artifact.artifact_id,
                )
                if identity in handled:
                    continue
                handled.add(identity)
                dependencies = [
                    r
                    for r in records
                    if (r.scope.server_identity, r.scope.account_scope, r.artifact.artifact_id)
                    == identity
                ]
                runtime = next(
                    (
                        route
                        for dependency in dependencies
                        if (route := self._route(dependency)) is not None
                    ),
                    None,
                )
                if runtime is None:
                    continue
                # Server pins are artifact/device scoped; a second target is
                # another delivery dependency, never another independent pin.
                release = all(
                    r.state == "accepted" and r.key not in audit_keys for r in dependencies
                )
                if not release and (pin.expires_at > now + 300 or pin.hard_expires_at <= now):
                    continue
                action = "release" if release else "renew"
                try:
                    runtime = await legacy._ensure_fresh_runtime(runtime)
                    data = await asyncio.wait_for(
                        _request_json(
                            runtime,
                            "POST",
                            f"/v1/paper-requests/{record.scope.request_id}/artifacts/{record.artifact.artifact_id}/pin",
                            {
                                "action": action,
                                "item_id": record.scope.request_item_id,
                                "generation_id": record.scope.generation_id,
                                "owner_token": pin.owner_token,
                                "owner_version": pin.owner_version,
                            },
                        ),
                        timeout=_AUDIT_TIMEOUT,
                    )
                    for dependency in dependencies:
                        previous = self.store.get_server_pin(dependency.key)
                        if previous is None:
                            continue
                        if release:
                            self.store.release_server_pin(
                                dependency.key, previous.owner_token, previous.owner_version
                            )
                        elif isinstance(data, dict):
                            self.store.record_server_pin(
                                dependency.key,
                                _required(data, "owner_token"),
                                int(data["owner_version"]),
                                _timestamp(data.get("expires_at")),
                                _timestamp(data.get("hard_expires_at")),
                            )
                except asyncio.CancelledError:
                    raise
                except legacy.WatcherAuthenticationError:
                    self.authentication_failed(runtime)
                except Exception:
                    for dependency in dependencies:
                        self._pin_retry[dependency.key] = now + 30
            for path in self.store.expire_payloads():
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(Path(path).unlink)
            await self._pause()

    async def send_text(self, runtime: legacy.DeliveryRuntime, message: str) -> object:
        """Reserve the same durable physical lane used by native files."""
        try:
            legacy._runtime_with_latest_state(runtime)
        except legacy.WatcherAuthenticationError:
            return {"success": False, "error_kind": "authentication_required"}
        target = _binding(runtime)
        deadline = time.monotonic() + 30
        token = None
        while token is None and time.monotonic() < deadline and not self._fatal:
            token = self.store.claim_text(target, self.owner)
            if token is None:
                await self._pause()
        if token is None or runtime.send_text is None:
            if token is not None:
                self.store.finish_text(target, token)
            return {"success": False, "error_kind": "delivery_lane_deferred"}
        try:
            legacy._runtime_with_latest_state(runtime)
        except legacy.WatcherAuthenticationError:
            self.store.finish_text(target, token)
            return {"success": False, "error_kind": "authentication_required"}
        task = asyncio.create_task(legacy._maybe_await(runtime.send_text(message)))
        try:
            # Caller cancellation must not free a still-running callback's lane.
            result = await asyncio.shield(task)
            receipt = legacy._normalize_send_result(result, operation="send_text")
            self.store.finish_text(
                target, token, retry_after=receipt.retry_after or 0, stopped=True
            )
            return result
        except asyncio.CancelledError:

            async def drain() -> None:
                try:
                    result = await task
                    receipt = legacy._normalize_send_result(result, operation="send_text")
                    self.store.finish_text(target, token, retry_after=receipt.retry_after or 0)
                except Exception:
                    self.store.finish_text(target, token, stopped=False)

            draining = asyncio.create_task(drain())
            self._native.add(draining)
            draining.add_done_callback(self._native.discard)
            raise
        except Exception:
            self.store.finish_text(target, token, stopped=False)
            return {"success": False, "error_kind": "text_outcome_unknown"}
        finally:
            self.wake()

    def request_busy(self, runtime: legacy.DeliveryRuntime) -> bool:
        records = _runtime_records(self.store, runtime)
        return any(
            r.state == "in_flight"
            or r.key in self._staging
            or (
                r.state == "pending"
                and r.expires_at > time.time()
                and r.artifact.integrity_status == "validated"
                and r.block_reason
                not in {
                    "blocked_on_main",
                    "blocked_on_prior",
                    "waiting_context",
                    "unsupported_native",
                    "expired",
                    "invalid_zip",
                    "cancelled",
                    "main_failed",
                }
            )
            for r in records
        )


def supervisor_for(runtime: legacy.DeliveryRuntime) -> DeliverySupervisor:
    """Start a daemon owner only from the authenticated, ACK-gated watcher."""
    identity = runtime.state.installation_id
    with _REGISTRY_LOCK:
        supervisor = _SUPERVISORS.get(identity)
        if supervisor is None:
            supervisor = DeliverySupervisor(open_store(runtime))
            ready: Future[None] = Future()

            def run() -> None:
                async def own() -> None:
                    assert supervisor is not None
                    error: Exception | None = None
                    try:
                        if not await supervisor.start():
                            raise RuntimeError("Durable sender is owned by another process")
                        try:
                            ready.set_result(None)
                        except InvalidStateError:
                            # The caller timed out. Never leave a late owner running.
                            return
                        await asyncio.gather(*supervisor._tasks, return_exceptions=True)
                    except Exception as exc:
                        error = exc
                    finally:
                        await supervisor.close()
                    if error is not None:
                        with contextlib.suppress(InvalidStateError):
                            ready.set_exception(error)

                asyncio.run(own())

            threading.Thread(target=run, daemon=True, name="scifetch-native-outbox").start()
            try:
                ready.result(timeout=_STARTUP_TIMEOUT)
            except FutureTimeoutError as exc:
                if ready.cancel():
                    raise RuntimeError("Durable sender initialization did not finish") from exc
                # Completion won the deadline race; consume that exact outcome.
                ready.result()
            _SUPERVISORS[identity] = supervisor
        if (
            runtime.state_path is None
            or supervisor.store.path.resolve()
            != runtime.state_path.with_name("delivery_outbox.sqlite3").resolve()
        ):
            raise ValueError(
                "Installation delivery ledger relocation requires an explicit migration"
            )
        return supervisor


async def watch_durable_request(
    runtime: legacy.DeliveryRuntime,
    *,
    max_reconnects: int | None,
    retry_delay_seconds: float,
    terminal_timeout_seconds: float,
) -> None:
    if not persist_request_intent(runtime):
        logger.warning("immutable delivery disabled: no durable exact installation binding")
        return
    supervisor = supervisor_for(runtime)
    store = supervisor.store
    deadline = time.monotonic() + terminal_timeout_seconds
    reconnects = 0
    terminal: dict[str, object] | None = None
    failed_terminal: dict[str, object] | None = None
    terminal_payloads: list[dict[str, object]] = []
    error_payloads: list[dict[str, object]] = []
    last_error: dict[str, object] | None = None
    metadata: dict[str, dict[str, object]] = {}
    attention_seen = False
    initial_accepted = {r.key for r in _runtime_records(store, runtime) if r.state == "accepted"}
    while time.monotonic() < deadline:
        try:
            runtime = await legacy._ensure_fresh_runtime(runtime)
            revision = await reconcile_snapshot(store, runtime, metadata)
            supervisor.register(runtime)
            supervisor.wake()
            async with (
                legacy._async_client(runtime) as client,
                client.stream(
                    "GET",
                    f"/v1/paper-requests/{runtime.request_id}/events",
                    headers=legacy._auth_headers(runtime),
                ) as response,
            ):
                if response.status_code in {401, 403}:
                    raise legacy.WatcherAuthenticationError(
                        "SciFetch delivery authentication required"
                    )
                response.raise_for_status()
                async for name, data in legacy._iter_sse(response.aiter_lines(), deadline=deadline):
                    event_type, payload, event_job_id = legacy._parse_event_payload(name, data)
                    if event_job_id:
                        payload = {**payload, "job_id": event_job_id}
                    if event_type == "file_ready":
                        record = ingest_file(store, runtime, payload)
                        metadata[record.key] = dict(payload)
                        observed_revision = payload["readiness_revision"]
                        if isinstance(observed_revision, int):
                            if observed_revision > revision + 1:
                                supervisor.request_reconciliation(runtime)
                            revision = max(revision, observed_revision)
                        supervisor.wake()
                    elif event_type == "attention_required" and not attention_seen:
                        attention_seen = True
                        supervisor.notify(
                            runtime,
                            "Science 页面仍在验证，请查看可见的服务 Chrome；如出现勾选框请手动完成。本次请求仍在等待，验证页不会因提示而重新打开。",
                        )
                    elif event_type == "error":
                        last_error = payload
                        error_payloads.append(payload)
                    elif event_type == "done":
                        apply_closed_scopes(store, runtime, payload)
                        terminal = payload
                        terminal_payloads.append(payload)
                        if legacy._event_failed(payload):
                            failed_terminal = payload
            if terminal is not None:
                await reconcile_snapshot(store, runtime, metadata)
                supervisor.wake()
                break
            raise httpx.RemoteProtocolError("Readiness stream interrupted")
        except asyncio.CancelledError:
            # Watcher shutdown is not user cancellation and does not revoke accepted MAIN.
            raise
        except legacy.WatcherAuthenticationError:
            supervisor.authentication_failed(runtime)
            await legacy._send_text(
                runtime,
                legacy._watcher_auth_required_message(
                    runtime,
                    {
                        r.artifact.logical_file_id
                        for r in _runtime_records(store, runtime)
                        if r.state == "accepted"
                    },
                ),
            )
            return
        except legacy.WatcherDeadlineError:
            supervisor.unregister(runtime)
            await legacy._send_text(
                runtime,
                legacy._watcher_timeout_message(
                    runtime,
                    {
                        r.artifact.logical_file_id
                        for r in _runtime_records(store, runtime)
                        if r.state == "accepted"
                    },
                ),
            )
            return
        except Exception:
            reconnects += 1
            if max_reconnects is not None and reconnects > max_reconnects:
                supervisor.unregister(runtime)
                await legacy._send_text(
                    runtime, legacy._watcher_issue_message(runtime, reason="与服务器进度流断开")
                )
                return
            await asyncio.sleep(min(retry_delay_seconds * reconnects, 30.0))
    # Event ingestion is finished. This bounded terminal waiter is not the sender.
    while supervisor.request_busy(runtime) and time.monotonic() < deadline:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.Event().wait(), timeout=_WORKER_POLL)
    if terminal is None and time.monotonic() >= deadline:
        supervisor.unregister(runtime)
        await legacy._send_text(
            runtime,
            legacy._watcher_timeout_message(
                runtime,
                {
                    r.artifact.logical_file_id
                    for r in _runtime_records(store, runtime)
                    if r.state == "accepted"
                },
            ),
        )
        return
    records = _runtime_records(store, runtime)
    latest_terminals = legacy._latest_item_terminals(terminal_payloads)
    error_payloads = [legacy._bind_item_from_job(e, terminal_payloads) for e in error_payloads]
    failed_terminal = next((t for t in reversed(latest_terminals) if legacy._event_failed(t)), None)
    last_error = None
    for error in reversed(error_payloads):
        history = [
            t
            for t in terminal_payloads
            if len(latest_terminals) == 1 or legacy._same_request_item(error, t)
        ]
        if (
            history
            and history[-1].get("ok") is True
            and any(legacy._event_failed(t) for t in history[:-1])
        ):
            continue
        last_error = error
        break
    accepted = [r for r in records if r.state == "accepted"]
    rendered = [
        {
            **metadata.get(r.key, {}),
            "role": r.artifact.role.lower(),
            "filename": r.artifact.filename,
            "file_id": r.artifact.logical_file_id,
            "item_id": r.scope.request_item_id,
        }
        for r in accepted
    ]
    if failed_terminal is not None and not accepted:
        if len(latest_terminals) > 1:
            await legacy._send_item_delivery_outcomes(
                runtime, rendered, terminals=terminal_payloads, errors=error_payloads
            )
        else:
            await legacy._send_failure_notification(
                runtime, failed_terminal, fallback_payload=last_error
            )
    elif records and any(r.state == "unknown" for r in records):
        await legacy._send_terminal_text(
            runtime,
            "原生文件发送结果尚未确认；已保留本地记录，不会自动重发。请按 request_id 查看状态。",
            actionable=True,
        )
    elif accepted and (
        any(r.state != "accepted" for r in records)
        or failed_terminal is not None
        or (last_error is not None and legacy._is_partial_delivery_error(last_error))
    ):
        await legacy._send_item_delivery_outcomes(
            runtime, rendered, terminals=terminal_payloads, errors=error_payloads
        )
    elif accepted and any(r.key not in initial_accepted for r in accepted):
        await legacy._send_delivery_summary(runtime, rendered, terminal_payloads=terminal_payloads)

    safe_pending = any(
        r.state == "pending"
        and r.expires_at > time.time()
        and r.block_reason
        not in {"blocked_on_main", "blocked_on_prior", "cancelled", "main_failed", "expired"}
        for r in records
    )
    if terminal is not None and not safe_pending:
        store.complete_request_intent(
            runtime.request_id,
            runtime.state.server_url.rstrip("/"),
            runtime.state.device_id,
            _binding(runtime).binding_id,
        )


def resume_durable_deliveries(kwargs: Mapping[str, object]) -> int:
    state_path = legacy._state_path_from_kwargs(kwargs)
    if state_path is None:
        return 0
    state = load_state(state_path)
    if state is None or not state.is_authenticated:
        return 0
    target = legacy._target_from_kwargs(kwargs)
    transport = kwargs.get("transport")
    runtime = legacy.DeliveryRuntime(
        state=state,
        request_id="resume",
        target=target,
        send_document=legacy._document_sender_from_kwargs(kwargs, target),
        send_text=legacy._text_sender_from_kwargs(kwargs, target),
        native_document_sender=legacy._native_sender_from_kwargs(kwargs),
        state_path=state_path,
        pending_path=legacy._pending_path_from_state_path(state_path),
        transport=transport if isinstance(transport, httpx.AsyncBaseTransport) else None,
    )
    if not _valid_runtime(runtime):
        return 0
    store = open_store(runtime)
    request_ids = {
        intent.request_id
        for intent in store.list_request_intents()
        if not intent.revoked
        and not intent.completed
        and intent.server_identity == state.server_url.rstrip("/")
        and intent.account_scope == state.device_id
        and intent.target.binding_id == _binding(runtime).binding_id
    }
    scheduled = 0
    for request_id in sorted(request_ids):
        if legacy._schedule_runtime_watcher(replace(runtime, request_id=request_id), kwargs):
            scheduled += 1
    return scheduled


async def controlled_text(runtime: legacy.DeliveryRuntime, message: str) -> object:
    supervisor = supervisor_for(runtime)
    if supervisor._loop is None:
        return {"success": False, "error_kind": "sender_unavailable"}
    if asyncio.get_running_loop() is supervisor._loop:
        return await supervisor.send_text(runtime, message)
    future = asyncio.run_coroutine_threadsafe(
        supervisor.send_text(runtime, message), supervisor._loop
    )
    return await asyncio.wrap_future(future)


def local_delivery_status(state_path: Path, state: PluginState, request_id: str) -> str:
    """Authenticated profile-level read-only reconciliation; no resend side effect."""
    path = state_path.with_name("delivery_outbox.sqlite3")
    if not state.installation_id:
        return ""
    missing_observation = (
        "汇总原文记录未保留；以上为依据现存记录生成的当前状态，不能据此确认原汇总已送达。"
    )
    if not path.exists():
        return missing_observation
    store = OutboxStore(path, state.installation_id)
    records = [
        r
        for r in store.list_deliveries(request_id=request_id)
        if r.scope.server_identity == state.server_url.rstrip("/")
        and r.scope.account_scope == state.device_id
    ]
    segments = store.list_terminal_segments(
        request_id,
        state.server_url.rstrip("/"),
        state.device_id,
    )
    if not records and not segments:
        return missing_observation
    labels = {
        "accepted": "原生适配器已接受（非收件人已读）",
        "unknown": "结果未知，禁止自动重发",
        "pending": "等待投递",
        "in_flight": "原生发送处理中",
        "failed": "未完成",
    }
    lines = ["本地原生文件投递："] if records else []
    reasons = {
        "waiting_context": "等待当前会话的有效发送上下文",
        "blocked_on_main": "等待正文的原生接受回执及附件校验",
        "blocked_on_prior": "等待前序附件完成投递",
        "legacy_incomplete": "历史投递记录不完整，结果尚未确认",
        "cancelled": "本次投递已取消",
        "main_failed": "正文未完成，附件投递未继续",
        "expired": "本地文件保留期限已到",
        "integrity_rejected": "文件未通过完整性校验",
        "invalid_zip": "压缩文件未通过校验",
        "unsupported_native": "当前通道不支持该文件的原生发送",
    }
    for record in records:
        lines.append(
            f"{record.artifact.role}（目标 {record.target.binding_id}）：{labels[record.state]}"
            + (
                f"（{reasons.get(record.block_reason, '投递未完成或结果尚未确认，请核对投递记录')}）"
                if record.block_reason
                else ""
            )
        )
        if record.state == "unknown":
            lines.append(
                f"文件：{record.artifact.artifact_id}；目标绑定：{record.target.binding_id}"
            )
    if any(r.state == "unknown" for r in records):
        lines.append(
            "未知结果须等待原调用的明确回执或核实原调用已经停止后，另行明确授权重发；重发可能重复。用户确认收到或远程审计不能解锁附件。"
        )
    outcomes = {
        "not_attempted": "尚未尝试发送",
        "unavailable": "发送不可用或结果尚未确认",
        "rejected": "发送被拒绝",
        "timeout": "发送超时，结果尚未确认",
        "legacy_unconfirmed": "缺少明确回执，结果尚未确认",
        "adapter_accepted_unverified": "适配器已接受，未验证收件人收到或已读",
    }
    if segments:
        lines.append("历史汇总及发送记录（仅查询，不重发文件）：")
        for segment in segments:
            lines.append(
                f"第 {segment['ordinal'] + 1} 段："
                + outcomes.get(segment["outcome"], "结果尚未确认")
                + "\n"
                + legacy._scrub_terminal_text(segment["text"])
            )
    else:
        lines.append(missing_observation)
    return "\n".join(lines)
