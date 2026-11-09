"""Installation-owned native delivery ledger; no network or adapter calls.

The caller authenticates routes and qualifies native results. This module owns
crash-safe intent/receipt ordering, exact identity, lanes, and audit compensation.
An OS sender lock is required for every native transition, not for ingestion.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from .platform_io import (
    canonical_path,
    lock_sender,
    open_private_file,
    process_exited,
    unlock_sender,
)

SCHEMA_VERSION = 1
OUTBOX_FILENAME = "delivery_outbox.sqlite3"


class OutboxError(Exception):
    """Ledger operation was refused without a native side effect."""


class IdentityConflictError(OutboxError):
    """An immutable binding was reused with different content or target."""


class SenderBusyError(OutboxError):
    """Another owner, or a different ledger, owns this installation."""


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _key(value: object) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _nonempty(*values: str) -> None:
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise OutboxError("Incomplete delivery identity")


@dataclass(frozen=True)
class TargetBinding:
    installation_id: str
    platform: str
    chat_id: str
    user_id: str
    chat_type: str = "direct"
    thread_id: str = ""

    @property
    def binding_id(self) -> str:
        return _key(asdict(self))

    @property
    def lane_id(self) -> str:
        return _key((self.installation_id, self.platform, self.chat_id, self.thread_id))


@dataclass(frozen=True)
class GateScope:
    server_identity: str
    account_scope: str
    installation_id: str
    request_id: str
    request_item_id: str
    acquisition_id: str
    generation_id: str
    target_binding_id: str

    @property
    def key(self) -> str:
        return _key(asdict(self))


@dataclass(frozen=True)
class Artifact:
    logical_file_id: str
    artifact_id: str
    role: str
    sha256: str
    size: int
    media_type: str
    ordinal: int = 0
    integrity_status: str = "validated"
    filename: str = ""

    @property
    def content_identity(self) -> tuple[object, ...]:
        return (self.artifact_id, self.sha256, self.size, self.media_type, self.ordinal)


@dataclass(frozen=True)
class DeliveryRecord:
    key: str
    scope: GateScope
    target: TargetBinding
    artifact: Artifact
    state: str
    block_reason: str
    staged_path: str
    expires_at: float
    attempt_count: int
    enqueued_at: float


@dataclass(frozen=True)
class RequestIntent:
    intent_id: str
    server_identity: str
    account_scope: str
    request_id: str
    target: TargetBinding
    revoked: bool
    revision: int
    completed: bool = False


@dataclass(frozen=True)
class NativeAttempt:
    attempt_id: str
    start_token: str
    delivery: DeliveryRecord
    owner: str
    stage: str
    prior_attempt_id: str = ""


@dataclass(frozen=True)
class NativeReceipt:
    method: str
    capability_revision: str
    result_class: str = "native_file_accepted"
    message_id: str = ""


@dataclass(frozen=True)
class ServerPin:
    delivery_key: str
    owner_token: str
    owner_version: int
    expires_at: float
    hard_expires_at: float


@dataclass(frozen=True)
class AuditRecord:
    audit_id: str
    attempt_id: str
    delivery: DeliveryRecord
    payload: dict[str, Any]
    tries: int


_SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS request_intents (
 intent_id TEXT PRIMARY KEY, server_identity TEXT NOT NULL, account_scope TEXT NOT NULL,
 request_id TEXT NOT NULL, target_id TEXT NOT NULL, target_json TEXT NOT NULL,
 revoked INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
 created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS completed_intents (
 intent_id TEXT PRIMARY KEY, completed_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS gates (
 gate_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, scope_json TEXT NOT NULL,
 main_key TEXT, accepted_receipt TEXT, revoked INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS deliveries (
 delivery_key TEXT PRIMARY KEY, gate_id TEXT NOT NULL, lane_id TEXT NOT NULL,
 target_json TEXT NOT NULL, artifact_json TEXT NOT NULL, role TEXT NOT NULL,
 ordinal INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
 block_reason TEXT NOT NULL DEFAULT '', staged_path TEXT NOT NULL DEFAULT '',
 expires_at REAL NOT NULL, not_before REAL NOT NULL DEFAULT 0,
 attempt_count INTEGER NOT NULL DEFAULT 0, continuation INTEGER NOT NULL DEFAULT 0,
 enqueued_at REAL NOT NULL, updated_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS deliveries_gate ON deliveries(gate_id, ordinal);
CREATE TABLE IF NOT EXISTS attempts (
 attempt_id TEXT PRIMARY KEY, delivery_key TEXT NOT NULL, owner TEXT NOT NULL,
 start_token TEXT NOT NULL UNIQUE, stage TEXT NOT NULL, outcome TEXT NOT NULL DEFAULT '',
 reason TEXT NOT NULL DEFAULT '', native_active INTEGER NOT NULL DEFAULT 0,
 created_at REAL NOT NULL, native_started_at REAL, result_at REAL, stopped_at REAL);
CREATE TABLE IF NOT EXISTS receipts (
 receipt_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL UNIQUE,
 delivery_key TEXT NOT NULL, payload_json TEXT NOT NULL, accepted_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS attempt_owners (
 attempt_id TEXT PRIMARY KEY, process_id INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS stop_proofs (
 attempt_id TEXT PRIMARY KEY, reason TEXT NOT NULL,
 process_id INTEGER NOT NULL, observed_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS audits (
 audit_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL UNIQUE,
 delivery_key TEXT NOT NULL, payload_json TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', tries INTEGER NOT NULL DEFAULT 0,
 not_before REAL NOT NULL DEFAULT 0, created_at REAL NOT NULL, synced_at REAL);
CREATE TABLE IF NOT EXISTS lanes (
 lane_id TEXT PRIMARY KEY, attempt_id TEXT, next_allowed_at REAL NOT NULL DEFAULT 0,
 cooldown_reason TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS text_attempts (
 token TEXT PRIMARY KEY, owner TEXT NOT NULL, process_id INTEGER NOT NULL,
 created_at REAL NOT NULL, stopped_at REAL, outcome TEXT NOT NULL DEFAULT 'unknown');
CREATE TABLE IF NOT EXISTS server_pins (
 delivery_key TEXT PRIMARY KEY, owner_token TEXT NOT NULL, owner_version INTEGER NOT NULL,
 expires_at REAL NOT NULL, hard_expires_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS terminal_observations (
 request_id TEXT NOT NULL, target_id TEXT NOT NULL, texts_json TEXT NOT NULL,
 outcome TEXT NOT NULL, updated_at REAL NOT NULL,
 PRIMARY KEY(request_id,target_id));
CREATE TABLE IF NOT EXISTS terminal_segments (
 batch_id TEXT NOT NULL, ordinal INTEGER NOT NULL, request_id TEXT NOT NULL,
 target_id TEXT NOT NULL, server_identity TEXT NOT NULL, account_scope TEXT NOT NULL,
 text TEXT NOT NULL, outcome TEXT NOT NULL, updated_at REAL NOT NULL,
 PRIMARY KEY(batch_id,ordinal));
CREATE INDEX IF NOT EXISTS terminal_segments_request
 ON terminal_segments(request_id,server_identity,account_scope);
CREATE TABLE IF NOT EXISTS resend_authorizations (
 authorization_id TEXT PRIMARY KEY, delivery_key TEXT NOT NULL,
 prior_attempt_id TEXT NOT NULL, created_at REAL NOT NULL, next_attempt_id TEXT);
CREATE TABLE IF NOT EXISTS late_receipts (
 attempt_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, observed_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS revocations (
 revocation_id TEXT PRIMARY KEY, server_identity TEXT NOT NULL, account_scope TEXT NOT NULL,
 request_id TEXT NOT NULL, item_id TEXT NOT NULL, target_id TEXT NOT NULL,
 acquisition_id TEXT NOT NULL, generation_id TEXT NOT NULL, reason TEXT NOT NULL,
 created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS conflicts (
 conflict_id TEXT PRIMARY KEY, delivery_key TEXT NOT NULL, observed_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS imports (
 source_id TEXT PRIMARY KEY, digest TEXT NOT NULL, imported_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS legacy_observations (
 observation_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, request_id TEXT NOT NULL,
 payload_json TEXT NOT NULL, disposition TEXT NOT NULL);
"""


class SenderLock:
    """Stable installation OS lock with a persistent canonical-ledger binding.

    Changing the SQLite path is a migration, not permission to start an empty
    sender. The binding survives release, preventing alternate-path replay.
    """

    def __init__(self, store: OutboxStore):
        self.store = store
        self._fd: int | None = None
        self._pid = 0

    @property
    def held(self) -> bool:
        return self._fd is not None and self._pid == os.getpid()

    def acquire(self) -> bool:
        if self.held:
            return True
        root = self.store.coordination_dir
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        lock_path = root / f"{_key(self.store.installation_id)}.lock"
        fd = open_private_file(lock_path, os.O_RDWR | os.O_CREAT)
        try:
            lock_sender(fd)
            os.lseek(fd, 0, os.SEEK_SET)
            with os.fdopen(fd, "r+", encoding="utf-8", closefd=False) as stream:
                bound = stream.read().strip()
                expected = canonical_path(self.store.path)
                if bound and bound != expected:
                    raise SenderBusyError("Installation ledger relocation requires migration")
                if not bound:
                    stream.seek(0)
                    stream.write(expected)
                    stream.flush()
                    os.fsync(fd)
        except (BlockingIOError, SenderBusyError):
            os.close(fd)
            return False
        except BaseException:
            os.close(fd)
            raise
        self._fd, self._pid = fd, os.getpid()
        return True

    def release(self) -> None:
        if self._fd is not None:
            try:
                # A forked child must not unlock the parent's inherited descriptor.
                if self._pid == os.getpid():
                    unlock_sender(self._fd)
            finally:
                os.close(self._fd)
                self._fd = None

    def __enter__(self) -> SenderLock:
        if not self.acquire():
            raise SenderBusyError("Installation sender is already owned")
        return self

    def __exit__(self, *args: object) -> None:
        self.release()


class OutboxStore:
    def __init__(self, path: Path, installation_id: str, coordination_dir: Path | None = None):
        _nonempty(installation_id)
        self.path = path.resolve()
        self.installation_id = installation_id
        self.coordination_dir = (
            coordination_dir or Path.home() / ".cache" / "scifetch-delivery-locks"
        ).resolve()
        self._mutex = threading.RLock()
        self._sender_lock = SenderLock(self)
        self._healthy = True
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = open_private_file(self.path, os.O_RDWR | os.O_CREAT)
        os.close(fd)
        os.chmod(self.path, 0o600)
        with self._connection() as conn:
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise OutboxError("Unsupported outbox schema")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_SCHEMA)
            conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            existing = conn.execute(
                "SELECT value FROM metadata WHERE key='installation_id'"
            ).fetchone()
            if existing and existing[0] != installation_id:
                raise IdentityConflictError("Ledger belongs to another installation")
            conn.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('installation_id', ?)",
                (installation_id,),
            )
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{self.path}{suffix}")
            # SQLite may remove these when another connection closes.
            with suppress(FileNotFoundError):
                os.chmod(sidecar, 0o600)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=0.25)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._mutex, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            yield conn

    def sender_lock(self) -> SenderLock:
        return self._sender_lock

    def _require_sender(self) -> None:
        if not self._sender_lock.held:
            raise SenderBusyError("Native transition requires installation sender ownership")
        if not self._healthy:
            raise OutboxError("Receipt persistence is unhealthy; stop native dispatch")

    def create_request_intent(
        self, server_identity: str, account_scope: str, request_id: str, target: TargetBinding
    ) -> str:
        _nonempty(server_identity, account_scope, request_id)
        self._validate_target(target)
        intent_id = _key((server_identity, account_scope, request_id, target.binding_id))
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO request_intents "
                "(intent_id,server_identity,account_scope,request_id,target_id,target_json,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    intent_id,
                    server_identity,
                    account_scope,
                    request_id,
                    target.binding_id,
                    _json(asdict(target)),
                    time.time(),
                ),
            )
        return intent_id

    def _validate_target(self, target: TargetBinding) -> None:
        _nonempty(
            target.installation_id,
            target.platform,
            target.chat_id,
            target.user_id,
            target.chat_type,
        )
        if target.installation_id != self.installation_id:
            raise IdentityConflictError("Target installation mismatch")

    def enqueue(
        self,
        scope: GateScope,
        target: TargetBinding,
        artifact: Artifact,
        *,
        revision: int = 0,
        expires_at: float | None = None,
    ) -> DeliveryRecord:
        _nonempty(*asdict(scope).values())
        self._validate_target(target)
        if (
            scope.installation_id != self.installation_id
            or scope.target_binding_id != target.binding_id
        ):
            raise IdentityConflictError("Artifact target mismatch")
        _nonempty(artifact.logical_file_id, artifact.artifact_id, artifact.media_type)
        if (
            artifact.role not in {"MAIN", "SI"}
            or artifact.size <= 0
            or len(artifact.sha256) != 64
            or any(c not in "0123456789abcdef" for c in artifact.sha256)
            or artifact.ordinal < 0
            or artifact.integrity_status not in {"validated", "blocked_on_main", "rejected"}
        ):
            raise OutboxError("Invalid artifact association")
        intent_id = self.create_request_intent(
            scope.server_identity, scope.account_scope, scope.request_id, target
        )
        key = _key((scope.key, artifact.logical_file_id, artifact.role))
        now = time.time()
        conflict = False
        with self._transaction() as conn:
            intent = conn.execute(
                "SELECT * FROM request_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            conn.execute(
                "INSERT OR IGNORE INTO gates(gate_id,intent_id,scope_json,revoked) VALUES (?,?,?,?)",
                (scope.key, intent_id, _json(asdict(scope)), intent["revoked"]),
            )
            tombstone = conn.execute(
                "SELECT reason FROM revocations WHERE server_identity=? AND account_scope=? AND request_id=? AND item_id=? AND target_id=? AND (acquisition_id='' OR acquisition_id=?) AND (generation_id='' OR generation_id=?) LIMIT 1",
                (
                    scope.server_identity,
                    scope.account_scope,
                    scope.request_id,
                    scope.request_item_id,
                    scope.target_binding_id,
                    scope.acquisition_id,
                    scope.generation_id,
                ),
            ).fetchone()
            if tombstone:
                conn.execute("UPDATE gates SET revoked=1 WHERE gate_id=?", (scope.key,))
            gate = conn.execute("SELECT * FROM gates WHERE gate_id=?", (scope.key,)).fetchone()
            existing = conn.execute(
                "SELECT * FROM deliveries WHERE delivery_key=?", (key,)
            ).fetchone()
            if artifact.role == "MAIN" and gate["main_key"] not in (None, key):
                conflict = True
            if existing:
                previous = Artifact(**json.loads(existing["artifact_json"]))
                conflict = conflict or previous.content_identity != artifact.content_identity
                # Integrity can progress once; a late stale event cannot undo validation.
                if not conflict and previous.integrity_status == "blocked_on_main":
                    conn.execute(
                        "UPDATE deliveries SET artifact_json=?,updated_at=? WHERE delivery_key=?",
                        (_json(asdict(artifact)), now, key),
                    )
            elif not conflict:
                expiry = expires_at if expires_at is not None else now + 86400
                conn.execute(
                    "INSERT INTO deliveries(delivery_key,gate_id,lane_id,target_json,artifact_json,role,ordinal,expires_at,enqueued_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        key,
                        scope.key,
                        target.lane_id,
                        _json(asdict(target)),
                        _json(asdict(artifact)),
                        artifact.role,
                        artifact.ordinal,
                        expiry,
                        now,
                        now,
                    ),
                )
                if artifact.role == "MAIN":
                    conn.execute("UPDATE gates SET main_key=? WHERE gate_id=?", (key, scope.key))
            if artifact.role == "SI" and not conflict:
                reason = (
                    ""
                    if gate["accepted_receipt"] and artifact.integrity_status == "validated"
                    else "blocked_on_main"
                )
                conn.execute(
                    "UPDATE deliveries SET block_reason=? WHERE delivery_key=? AND state='pending' AND block_reason IN ('','blocked_on_main')",
                    (reason, key),
                )
            self._refresh_si_blocks(conn, scope.key)
            if gate["revoked"]:
                conn.execute(
                    "UPDATE deliveries SET block_reason=? WHERE delivery_key=? AND state='pending'",
                    (tombstone["reason"] if tombstone else "cancelled", key),
                )
            legacy = conn.execute(
                "SELECT 1 FROM legacy_observations WHERE request_id=? LIMIT 1", (scope.request_id,)
            ).fetchone()
            if legacy:
                conn.execute(
                    "UPDATE deliveries SET state='unknown',block_reason='legacy_incomplete' WHERE delivery_key=? AND state='pending'",
                    (key,),
                )
            if conflict:
                conn.execute("UPDATE gates SET revoked=1 WHERE gate_id=?", (scope.key,))
                conn.execute(
                    "INSERT OR IGNORE INTO conflicts VALUES (?,?,?)",
                    (_key((key, artifact.content_identity)), key, now),
                )
            else:
                conn.execute(
                    "UPDATE request_intents SET revision=MAX(revision,?) WHERE intent_id=?",
                    (revision, intent_id),
                )
        if conflict:
            raise IdentityConflictError("Immutable artifact association conflict")
        result = self.get_delivery(key)
        assert result is not None
        return result

    def _record(self, row: sqlite3.Row) -> DeliveryRecord:
        return DeliveryRecord(
            key=row["delivery_key"],
            scope=GateScope(**json.loads(row["scope_json"])),
            target=TargetBinding(**json.loads(row["target_json"])),
            artifact=Artifact(**json.loads(row["artifact_json"])),
            state=row["state"],
            block_reason=row["block_reason"],
            staged_path=row["staged_path"],
            expires_at=row["expires_at"],
            attempt_count=row["attempt_count"],
            enqueued_at=row["enqueued_at"],
        )

    def get_delivery(self, key: str) -> DeliveryRecord | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT d.*,g.scope_json FROM deliveries d JOIN gates g USING(gate_id) WHERE delivery_key=?",
                (key,),
            ).fetchone()
            return self._record(row) if row else None

    def list_deliveries(self, request_id: str | None = None) -> list[DeliveryRecord]:
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT d.*,g.scope_json,i.request_id FROM deliveries d JOIN gates g USING(gate_id) JOIN request_intents i USING(intent_id) ORDER BY d.enqueued_at,d.ordinal"
            ).fetchall()
            return [
                self._record(row)
                for row in rows
                if request_id is None or row["request_id"] == request_id
            ]

    def list_pending(self, request_id: str | None = None) -> list[DeliveryRecord]:
        return [
            record
            for record in self.list_deliveries(request_id)
            if record.state == "pending" and record.expires_at > time.time()
        ]

    def list_request_intents(self) -> list[RequestIntent]:
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT i.*,(c.intent_id IS NOT NULL) AS completed FROM request_intents i LEFT JOIN completed_intents c USING(intent_id) ORDER BY i.created_at"
            ).fetchall()
            return [
                RequestIntent(
                    row["intent_id"],
                    row["server_identity"],
                    row["account_scope"],
                    row["request_id"],
                    TargetBinding(**json.loads(row["target_json"])),
                    bool(row["revoked"]),
                    row["revision"],
                    bool(row["completed"]),
                )
                for row in rows
            ]

    def complete_request_intent(
        self, request_id: str, server_identity: str, account_scope: str, target_binding_id: str
    ) -> None:
        """Retire terminal watcher recovery; native/audit evidence stays durable."""
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO completed_intents SELECT intent_id,? FROM request_intents WHERE request_id=? AND server_identity=? AND account_scope=? AND target_id=?",
                (time.time(), request_id, server_identity, account_scope, target_binding_id),
            )

    def block_delivery(self, key: str, reason: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE deliveries SET state='failed',block_reason=?,continuation=0 WHERE delivery_key=? AND state='pending'",
                (reason, key),
            )
            row = conn.execute(
                "SELECT gate_id FROM deliveries WHERE delivery_key=?", (key,)
            ).fetchone()
            if row:
                self._refresh_si_blocks(conn, row["gate_id"])

    def defer_delivery(self, key: str, reason: str = "waiting_context") -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE deliveries SET block_reason=?,continuation=0 WHERE delivery_key=? AND state='pending'",
                (reason, key),
            )

    def resume_target(
        self,
        target: TargetBinding,
        *,
        request_id: str | None = None,
        server_identity: str | None = None,
        account_scope: str | None = None,
    ) -> int:
        self._validate_target(target)
        with self._transaction() as conn:
            return conn.execute(
                "UPDATE deliveries SET block_reason='' WHERE state='pending' AND block_reason='waiting_context' AND gate_id IN (SELECT g.gate_id FROM gates g JOIN request_intents i USING(intent_id) WHERE i.target_id=? AND g.revoked=0 AND (? IS NULL OR i.request_id=?) AND (? IS NULL OR i.server_identity=?) AND (? IS NULL OR i.account_scope=?))",
                (
                    target.binding_id,
                    request_id,
                    request_id,
                    server_identity,
                    server_identity,
                    account_scope,
                    account_scope,
                ),
            ).rowcount

    def checkpoint(self, intent_id: str) -> int:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT revision FROM request_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            return int(row[0]) if row else 0

    def record_staged(self, key: str, path: Path) -> None:
        record = self.get_delivery(key)
        if record is None:
            raise OutboxError("Unknown delivery")
        if path.is_symlink() or not path.is_file():
            raise OutboxError("Staged artifact missing or unsafe")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        if (
            path.stat().st_size != record.artifact.size
            or digest.hexdigest() != record.artifact.sha256
        ):
            raise IdentityConflictError("Staged artifact integrity mismatch")
        os.chmod(path, 0o600)
        with self._transaction() as conn:
            conn.execute(
                "UPDATE deliveries SET staged_path=?,updated_at=? WHERE delivery_key=? AND state='pending'",
                (str(path.resolve()), time.time(), key),
            )

    def clear_staged(self, key: str) -> bool:
        """Forget a lost local payload only before invocation, preserving identity."""
        with self._transaction() as conn:
            return bool(
                conn.execute(
                    "UPDATE deliveries SET staged_path='' WHERE delivery_key=? AND state='pending' AND NOT EXISTS (SELECT 1 FROM attempts WHERE delivery_key=? AND native_active=1)",
                    (key, key),
                ).rowcount
            )

    mark_staged = record_staged
    verify_staged = record_staged

    def _refresh_si_blocks(self, conn: sqlite3.Connection, gate_id: str) -> None:
        gate = conn.execute(
            "SELECT accepted_receipt FROM gates WHERE gate_id=?", (gate_id,)
        ).fetchone()
        rows = conn.execute(
            "SELECT * FROM deliveries WHERE gate_id=? AND role='SI' ORDER BY ordinal", (gate_id,)
        ).fetchall()
        expected = 1
        prior_complete = True
        for row in rows:
            prior_complete = prior_complete and row["ordinal"] == expected
            expected = row["ordinal"] + 1
            artifact = Artifact(**json.loads(row["artifact_json"]))
            reason = ""
            if not gate["accepted_receipt"] or artifact.integrity_status != "validated":
                reason = "blocked_on_main"
            elif not prior_complete:
                reason = "blocked_on_prior"
            if row["state"] == "pending" and row["block_reason"] in {
                "",
                "blocked_on_main",
                "blocked_on_prior",
            }:
                conn.execute(
                    "UPDATE deliveries SET block_reason=? WHERE delivery_key=?",
                    (reason, row["delivery_key"]),
                )
            prior_complete = prior_complete and row["state"] in {"accepted", "failed"}

    def _eligible(self, conn: sqlite3.Connection, row: sqlite3.Row, now: float) -> bool:
        if row["revoked"] or row["expires_at"] <= now or row["not_before"] > now:
            return False
        artifact = Artifact(**json.loads(row["artifact_json"]))
        if artifact.integrity_status != "validated" or not row["staged_path"]:
            return False
        lane = conn.execute("SELECT * FROM lanes WHERE lane_id=?", (row["lane_id"],)).fetchone()
        if lane and (lane["attempt_id"] or lane["next_allowed_at"] > now):
            return False
        if row["role"] == "SI":
            if not row["accepted_receipt"]:
                return False
            earlier = conn.execute(
                "SELECT 1 FROM deliveries WHERE gate_id=? AND role='SI' AND ordinal<? AND state NOT IN ('accepted','failed') LIMIT 1",
                (row["gate_id"], row["ordinal"]),
            ).fetchone()
            completed = conn.execute(
                "SELECT COUNT(DISTINCT ordinal) FROM deliveries WHERE gate_id=? AND role='SI' AND ordinal<? AND state IN ('accepted','failed')",
                (row["gate_id"], row["ordinal"]),
            ).fetchone()[0]
            if earlier or completed != row["ordinal"] - 1:
                return False
        return True

    def claim_next(
        self,
        owner: str,
        now: float | None = None,
        request_id: str | None = None,
        target_binding_id: str | None = None,
        allowed_target_ids: set[str] | None = None,
    ) -> NativeAttempt | None:
        self._require_sender()
        _nonempty(owner)
        current = time.time() if now is None else now
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT d.*,g.scope_json,g.revoked,g.accepted_receipt,i.request_id FROM deliveries d JOIN gates g USING(gate_id) JOIN request_intents i USING(intent_id) WHERE d.state='pending' ORDER BY (d.continuation>0) DESC,d.continuation,d.enqueued_at,d.ordinal"
            ).fetchall()
            for row in rows:
                if request_id is not None and row["request_id"] != request_id:
                    continue
                if (
                    target_binding_id is not None
                    and GateScope(**json.loads(row["scope_json"])).target_binding_id
                    != target_binding_id
                ):
                    continue
                if (
                    allowed_target_ids is not None
                    and GateScope(**json.loads(row["scope_json"])).target_binding_id
                    not in allowed_target_ids
                ):
                    continue
                if row["block_reason"] in {
                    "waiting_context",
                    "unsupported",
                    "integrity_error",
                    "legacy_incomplete",
                }:
                    continue
                if not self._eligible(conn, row, current):
                    continue
                attempt_id, token = f"attempt_{uuid.uuid4().hex}", uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO attempts(attempt_id,delivery_key,owner,start_token,stage,created_at) VALUES (?,?,?,?,?,?)",
                    (attempt_id, row["delivery_key"], owner, token, "prepared", current),
                )
                conn.execute("INSERT INTO attempt_owners VALUES (?,?)", (attempt_id, os.getpid()))
                conn.execute(
                    "UPDATE deliveries SET state='in_flight',attempt_count=attempt_count+1,updated_at=? WHERE delivery_key=?",
                    (current, row["delivery_key"]),
                )
                conn.execute(
                    "INSERT INTO lanes(lane_id,attempt_id) VALUES (?,?) ON CONFLICT(lane_id) DO UPDATE SET attempt_id=excluded.attempt_id",
                    (row["lane_id"], attempt_id),
                )
                authorization = conn.execute(
                    "SELECT * FROM resend_authorizations WHERE delivery_key=? AND next_attempt_id IS NULL ORDER BY created_at DESC LIMIT 1",
                    (row["delivery_key"],),
                ).fetchone()
                prior_attempt_id = ""
                if authorization:
                    prior_attempt_id = authorization["prior_attempt_id"]
                    conn.execute(
                        "UPDATE resend_authorizations SET next_attempt_id=? WHERE authorization_id=?",
                        (attempt_id, authorization["authorization_id"]),
                    )
                return NativeAttempt(
                    attempt_id, token, self._record(row), owner, "prepared", prior_attempt_id
                )
        return None

    def begin_native(self, attempt_id: str, start_token: str) -> bool:
        """Consume once on the owning gateway loop immediately before invocation."""
        self._require_sender()
        now = time.time()
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT a.*,d.lane_id,d.expires_at,g.revoked,l.next_allowed_at FROM attempts a JOIN deliveries d USING(delivery_key) JOIN gates g USING(gate_id) JOIN lanes l USING(lane_id) WHERE a.attempt_id=?",
                (attempt_id,),
            ).fetchone()
            if row is None or row["stage"] != "prepared" or row["start_token"] != start_token:
                return False
            if row["revoked"] or row["expires_at"] <= now or row["next_allowed_at"] > now:
                self._finish_nonacceptance(conn, row, "preflight_fenced", True, now)
                return False
            conn.execute(
                "UPDATE attempts SET stage='invoking',native_active=1,native_started_at=? WHERE attempt_id=?",
                (now, attempt_id),
            )
            return True

    def _attempt(self, conn: sqlite3.Connection, attempt_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT a.*,d.gate_id,d.lane_id,d.role,g.revoked FROM attempts a JOIN deliveries d USING(delivery_key) JOIN gates g USING(gate_id) WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise OutboxError("Unknown native attempt")
        return cast(sqlite3.Row, row)

    def _release(self, conn: sqlite3.Connection, row: sqlite3.Row, now: float) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO stop_proofs VALUES (?,?,?,?)",
            (row["attempt_id"], "local_call_stopped", os.getpid(), now),
        )
        conn.execute(
            "UPDATE attempts SET native_active=0,stopped_at=? WHERE attempt_id=?",
            (now, row["attempt_id"]),
        )
        conn.execute("UPDATE lanes SET attempt_id=NULL WHERE attempt_id=?", (row["attempt_id"],))

    def accept(self, attempt_id: str, receipt: NativeReceipt, *, stopped: bool = True) -> bool:
        self._require_sender()
        if (
            receipt.method != "send_document"
            or not receipt.capability_revision
            or receipt.result_class != "native_file_accepted"
        ):
            raise OutboxError("Unqualified native file receipt")
        now = time.time()
        with self._transaction() as conn:
            replacement = conn.execute(
                "SELECT 1 FROM resend_authorizations WHERE prior_attempt_id=? AND next_attempt_id IS NOT NULL LIMIT 1",
                (attempt_id,),
            ).fetchone()
            if replacement:
                conn.execute(
                    "INSERT OR IGNORE INTO late_receipts VALUES (?,?,?)",
                    (attempt_id, _json(asdict(receipt)), now),
                )
        if replacement:
            raise IdentityConflictError("Late receipt belongs to superseded native attempt")
        try:
            with self._transaction() as conn:
                row = self._attempt(conn, attempt_id)
                if conn.execute(
                    "SELECT 1 FROM resend_authorizations WHERE prior_attempt_id=? AND next_attempt_id IS NOT NULL",
                    (attempt_id,),
                ).fetchone():
                    raise IdentityConflictError("Late receipt belongs to superseded native attempt")
                previous = conn.execute(
                    "SELECT payload_json FROM receipts WHERE attempt_id=?", (attempt_id,)
                ).fetchone()
                payload = _json(asdict(receipt))
                if previous:
                    if previous[0] != payload:
                        raise IdentityConflictError("Native receipt conflict")
                    return False
                if row["stage"] not in {"invoking", "unknown"} or row["outcome"] == "failed":
                    raise OutboxError("Receipt has no matching invocation")
                receipt_id = f"receipt_{attempt_id}"
                conn.execute(
                    "INSERT INTO receipts VALUES (?,?,?,?,?)",
                    (receipt_id, attempt_id, row["delivery_key"], payload, now),
                )
                conn.execute(
                    "UPDATE attempts SET stage='completed',outcome='accepted',result_at=? WHERE attempt_id=?",
                    (now, attempt_id),
                )
                conn.execute(
                    "UPDATE deliveries SET state='accepted',block_reason='',continuation=0,updated_at=? WHERE delivery_key=?",
                    (now, row["delivery_key"]),
                )
                if row["role"] == "MAIN" and not row["revoked"]:
                    conn.execute(
                        "UPDATE gates SET accepted_receipt=? WHERE gate_id=?",
                        (receipt_id, row["gate_id"]),
                    )
                    conn.execute(
                        "UPDATE deliveries SET block_reason='' WHERE gate_id=? AND role='SI' AND state='pending' AND block_reason='blocked_on_main' AND json_extract(artifact_json,'$.integrity_status')='validated'",
                        (row["gate_id"],),
                    )
                    self._reserve_continuation(conn, row["gate_id"], now)
                audit_payload = {
                    "receipt_id": receipt_id,
                    "attempt": int(
                        conn.execute(
                            "SELECT attempt_count FROM deliveries WHERE delivery_key=?",
                            (row["delivery_key"],),
                        ).fetchone()[0]
                    ),
                    "status": "sent",
                    "payload_version": 1,
                    "method": receipt.method,
                    "capability_revision": receipt.capability_revision,
                    "result_class": receipt.result_class,
                    "message_id": receipt.message_id,
                }
                conn.execute(
                    "INSERT INTO audits(audit_id,attempt_id,delivery_key,payload_json,created_at) VALUES (?,?,?,?,?)",
                    (
                        f"audit_{attempt_id}",
                        attempt_id,
                        row["delivery_key"],
                        _json(audit_payload),
                        now,
                    ),
                )
                self._refresh_si_blocks(conn, row["gate_id"])
                if stopped:
                    self._release(conn, row, now)
        except sqlite3.Error:
            # Durable pre-invocation intent survives. No in-memory accepted gate.
            self._healthy = False
            raise
        return True

    def _reserve_continuation(self, conn: sqlite3.Connection, gate_id: str, now: float) -> None:
        sequence = (
            int(conn.execute("SELECT COALESCE(MAX(continuation),0) FROM deliveries").fetchone()[0])
            + 1
        )
        rows = conn.execute(
            "SELECT * FROM deliveries WHERE gate_id=? AND role='SI' ORDER BY ordinal", (gate_id,)
        ).fetchall()
        expected = 1
        for row in rows:
            if row["ordinal"] != expected:
                break
            expected += 1
            if row["state"] in {"accepted", "failed"}:
                continue
            artifact = Artifact(**json.loads(row["artifact_json"]))
            if (
                row["state"] != "pending"
                or row["block_reason"] not in {"", "blocked_on_prior"}
                or not row["staged_path"]
                or artifact.integrity_status != "validated"
                or row["expires_at"] <= now
                or row["not_before"] > now
            ):
                break
            conn.execute(
                "UPDATE deliveries SET continuation=? WHERE delivery_key=?",
                (sequence, row["delivery_key"]),
            )
            sequence += 1

    def _finish_nonacceptance(
        self, conn: sqlite3.Connection, row: sqlite3.Row, reason: str, retry: bool, now: float
    ) -> None:
        conn.execute(
            "UPDATE attempts SET stage='completed',outcome='failed',reason=?,result_at=? WHERE attempt_id=?",
            (reason, now, row["attempt_id"]),
        )
        conn.execute(
            "UPDATE deliveries SET state=?,block_reason=?,continuation=0,updated_at=? WHERE delivery_key=?",
            ("pending" if retry else "failed", reason, now, row["delivery_key"]),
        )
        payload = {
            "status": "failed",
            "payload_version": 1,
            "error_code": reason,
            "attempt": int(
                conn.execute(
                    "SELECT attempt_count FROM deliveries WHERE delivery_key=?",
                    (row["delivery_key"],),
                ).fetchone()[0]
            ),
        }
        conn.execute(
            "INSERT OR IGNORE INTO audits(audit_id,attempt_id,delivery_key,payload_json,created_at) VALUES (?,?,?,?,?)",
            (
                f"audit_{row['attempt_id']}",
                row["attempt_id"],
                row["delivery_key"],
                _json(payload),
                now,
            ),
        )
        self._release(conn, row, now)

    def fail(
        self,
        attempt_id: str,
        reason: str,
        *,
        proven_not_accepted: bool = False,
        retryable: bool = False,
        retry_after: float = 0,
        stopped: bool = True,
    ) -> None:
        self._require_sender()
        now = time.time()
        with self._transaction() as conn:
            row = self._attempt(conn, attempt_id)
            if row["outcome"] in {"accepted", "failed"}:
                return
            if retry_after > 0:
                conn.execute(
                    "UPDATE lanes SET next_allowed_at=MAX(next_allowed_at,?),cooldown_reason=? WHERE lane_id=?",
                    (now + retry_after, reason, row["lane_id"]),
                )
            if not stopped or (row["stage"] != "prepared" and not proven_not_accepted):
                self._unknown(conn, row, reason, stopped, now)
                return
            self._finish_nonacceptance(conn, row, reason, retryable, now)
            self._refresh_si_blocks(conn, row["gate_id"])
            if retry_after > 0:
                conn.execute(
                    "UPDATE deliveries SET not_before=? WHERE delivery_key=?",
                    (now + retry_after, row["delivery_key"]),
                )
                conn.execute(
                    "UPDATE lanes SET next_allowed_at=MAX(next_allowed_at,?),cooldown_reason=? WHERE lane_id=?",
                    (now + retry_after, reason, row["lane_id"]),
                )
            # Backoff ends this finite continuation, releasing unrelated work.
            conn.execute("UPDATE deliveries SET continuation=0 WHERE gate_id=?", (row["gate_id"],))

    def _unknown(
        self, conn: sqlite3.Connection, row: sqlite3.Row, reason: str, stopped: bool, now: float
    ) -> None:
        conn.execute(
            "UPDATE attempts SET stage='unknown',outcome='unknown',reason=?,result_at=? WHERE attempt_id=?",
            (reason, now, row["attempt_id"]),
        )
        conn.execute(
            "UPDATE deliveries SET state='unknown',block_reason=?,continuation=0,updated_at=? WHERE delivery_key=?",
            (reason, now, row["delivery_key"]),
        )
        self._refresh_si_blocks(conn, row["gate_id"])
        if stopped:
            self._release(conn, row, now)

    def mark_unknown(self, attempt_id: str, reason: str, *, stopped: bool = False) -> None:
        self._require_sender()
        with self._transaction() as conn:
            row = self._attempt(conn, attempt_id)
            if row["outcome"] not in {"accepted", "failed"}:
                self._unknown(conn, row, reason, stopped, time.time())

    def mark_stopped(self, attempt_id: str) -> None:
        """Caller supplies actual stop/drain proof, never merely an expired lease."""
        self._require_sender()
        with self._transaction() as conn:
            row = self._attempt(conn, attempt_id)
            self._release(conn, row, time.time())

    @staticmethod
    def _process_exited(process_id: int) -> bool:
        return process_exited(process_id)

    def recover(self, owner: str) -> int:
        self._require_sender()
        recovered = 0
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT attempt_id FROM attempts WHERE (stage IN ('prepared','invoking') OR (outcome='unknown' AND native_active=1)) AND owner != ?",
                (owner,),
            ).fetchall()
            for raw in rows:
                row = self._attempt(conn, raw["attempt_id"])
                if row["stage"] == "prepared":
                    self._finish_nonacceptance(
                        conn, row, "recovered_never_invoked", True, time.time()
                    )
                else:
                    self._unknown(conn, row, "owner_lost", False, time.time())
                    process = conn.execute(
                        "SELECT process_id FROM attempt_owners WHERE attempt_id=?",
                        (row["attempt_id"],),
                    ).fetchone()
                    if process and self._process_exited(process[0]):
                        # Actual process absence proves the local call cannot
                        # continue. Remote acceptance remains unknown.
                        now = time.time()
                        self._release(conn, row, now)
                        conn.execute(
                            "UPDATE stop_proofs SET reason='owner_process_exited',process_id=?,observed_at=? WHERE attempt_id=?",
                            (process[0], now, row["attempt_id"]),
                        )
                recovered += 1
            text_rows = conn.execute(
                "SELECT t.* FROM text_attempts t JOIN lanes l ON l.attempt_id=t.token WHERE t.stopped_at IS NULL"
            ).fetchall()
            for text_row in text_rows:
                if self._process_exited(text_row["process_id"]):
                    now = time.time()
                    conn.execute(
                        "UPDATE lanes SET attempt_id=NULL WHERE attempt_id=?", (text_row["token"],)
                    )
                    conn.execute(
                        "UPDATE text_attempts SET stopped_at=? WHERE token=?",
                        (now, text_row["token"]),
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO stop_proofs VALUES (?,?,?,?)",
                        (text_row["token"], "owner_process_exited", text_row["process_id"], now),
                    )
                    recovered += 1
        return recovered

    def explicit_retry_unknown(
        self,
        delivery_key: str,
        *,
        expected_target_binding_id: str,
        user_requested: bool,
        duplicate_risk_ack: bool,
    ) -> str:
        """Queue a new lineage only after authenticated, explicit duplicate-risk consent.

        This is an authorization mutation, never native acceptance or stop proof.
        The route must still be reacquired by the normal authenticated sender.
        """
        if not user_requested or not duplicate_risk_ack:
            raise OutboxError("Explicit duplicate-risk acknowledgement required")
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT d.*,g.revoked,g.scope_json,g.intent_id FROM deliveries d JOIN gates g USING(gate_id) WHERE delivery_key=?",
                (delivery_key,),
            ).fetchone()
            if row is None or row["state"] != "unknown":
                raise OutboxError("Only unknown delivery can be explicitly retried")
            exact = GateScope(**json.loads(row["scope_json"]))
            if exact.target_binding_id != expected_target_binding_id:
                raise IdentityConflictError("Explicit retry target mismatch")
            if row["revoked"] or row["expires_at"] <= time.time():
                raise OutboxError("Revoked or expired delivery cannot be retried")
            previous = conn.execute(
                "SELECT * FROM attempts WHERE delivery_key=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                (delivery_key,),
            ).fetchone()
            if (
                previous is None
                or previous["stage"] != "unknown"
                or previous["native_active"]
                or previous["stopped_at"] is None
            ):
                raise OutboxError("Original native invocation lacks durable actual stop proof")
            authorization_id = "resend_" + uuid.uuid4().hex
            conn.execute(
                "INSERT INTO resend_authorizations VALUES (?,?,?,?,NULL)",
                (authorization_id, delivery_key, previous["attempt_id"], time.time()),
            )
            conn.execute(
                "UPDATE deliveries SET state='pending',block_reason='waiting_context',continuation=0 WHERE delivery_key=?",
                (delivery_key,),
            )
            conn.execute("DELETE FROM completed_intents WHERE intent_id=?", (row["intent_id"],))
            return authorization_id

    def revoke_scope(self, scope: GateScope, reason: str = "cancelled") -> None:
        self.revoke_item(
            scope.server_identity,
            scope.account_scope,
            scope.request_id,
            scope.request_item_id,
            scope.target_binding_id,
            reason=reason,
            acquisition_id=scope.acquisition_id,
            generation_id=scope.generation_id,
        )

    def revoke_item(
        self,
        server_identity: str,
        account_scope: str,
        request_id: str,
        request_item_id: str,
        target_binding_id: str,
        *,
        reason: str = "cancelled",
        acquisition_id: str | None = None,
        generation_id: str | None = None,
    ) -> None:
        """Persist a scoped cancellation tombstone even before artifact IDs arrive."""
        _nonempty(
            server_identity, account_scope, request_id, request_item_id, target_binding_id, reason
        )
        binding = (
            server_identity,
            account_scope,
            request_id,
            request_item_id,
            target_binding_id,
            acquisition_id or "",
            generation_id or "",
        )
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO revocations VALUES (?,?,?,?,?,?,?,?,?,?)",
                (_key(binding), *binding, reason, time.time()),
            )
            for gate in conn.execute("SELECT * FROM gates").fetchall():
                scope = GateScope(**json.loads(gate["scope_json"]))
                if (
                    scope.server_identity,
                    scope.account_scope,
                    scope.request_id,
                    scope.request_item_id,
                    scope.target_binding_id,
                ) != binding[:5]:
                    continue
                if acquisition_id is not None and scope.acquisition_id != acquisition_id:
                    continue
                if generation_id is not None and scope.generation_id != generation_id:
                    continue
                conn.execute("UPDATE gates SET revoked=1 WHERE gate_id=?", (gate["gate_id"],))
                conn.execute(
                    "UPDATE deliveries SET block_reason=?,continuation=0 WHERE gate_id=? AND state='pending'",
                    (reason, gate["gate_id"]),
                )

    def revoke_request(self, request_id: str, *, target_binding_id: str | None = None) -> None:
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT intent_id FROM request_intents WHERE request_id=? AND (? IS NULL OR target_id=?)",
                (request_id, target_binding_id, target_binding_id),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE request_intents SET revoked=1 WHERE intent_id=?", (row["intent_id"],)
                )
                conn.execute("UPDATE gates SET revoked=1 WHERE intent_id=?", (row["intent_id"],))

    def pending_audits(self, now: float | None = None) -> list[AuditRecord]:
        current = time.time() if now is None else now
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT a.* FROM audits a WHERE state='pending' AND not_before<=? ORDER BY created_at",
                (current,),
            ).fetchall()
        results = []
        for row in rows:
            delivery = self.get_delivery(row["delivery_key"])
            assert delivery is not None
            results.append(
                AuditRecord(
                    row["audit_id"],
                    row["attempt_id"],
                    delivery,
                    json.loads(row["payload_json"]),
                    row["tries"],
                )
            )
        return results

    def mark_audit_synced(self, audit_id: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE audits SET state='synced',synced_at=? WHERE audit_id=?",
                (time.time(), audit_id),
            )

    def defer_audit(self, audit_id: str, *, retry_after: float = 5) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE audits SET tries=tries+1,not_before=? WHERE audit_id=? AND state='pending'",
                (time.time() + max(0, retry_after), audit_id),
            )

    def record_server_pin(
        self,
        key: str,
        owner_token: str,
        owner_version: int,
        expires_at: float,
        hard_expires_at: float,
    ) -> None:
        _nonempty(owner_token)
        if owner_version <= 0 or expires_at > hard_expires_at:
            raise OutboxError("Invalid server retention pin")
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM server_pins WHERE delivery_key=?", (key,)).fetchone()
            if row and (
                row["owner_token"] != owner_token
                or owner_version < row["owner_version"]
                or row["hard_expires_at"] != hard_expires_at
            ):
                raise IdentityConflictError("Server retention pin conflict")
            conn.execute(
                "INSERT INTO server_pins VALUES (?,?,?,?,?) ON CONFLICT(delivery_key) DO UPDATE SET owner_version=excluded.owner_version,expires_at=excluded.expires_at",
                (key, owner_token, owner_version, expires_at, hard_expires_at),
            )
            conn.execute(
                "UPDATE deliveries SET expires_at=? WHERE delivery_key=?",
                (min(expires_at, hard_expires_at), key),
            )

    def get_server_pin(self, key: str) -> ServerPin | None:
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM server_pins WHERE delivery_key=?", (key,)).fetchone()
            return ServerPin(*tuple(row)) if row else None

    def list_server_pins(self) -> list[ServerPin]:
        with self._connection() as conn:
            return [ServerPin(*tuple(row)) for row in conn.execute("SELECT * FROM server_pins")]

    def release_server_pin(self, key: str, owner_token: str, owner_version: int) -> bool:
        with self._transaction() as conn:
            return bool(
                conn.execute(
                    "DELETE FROM server_pins WHERE delivery_key=? AND owner_token=? AND owner_version=?",
                    (key, owner_token, owner_version),
                ).rowcount
            )

    def claim_text(self, target: TargetBinding, owner: str) -> str | None:
        self._require_sender()
        self._validate_target(target)
        _nonempty(owner)
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM lanes WHERE lane_id=?", (target.lane_id,)).fetchone()
            if row and (row["attempt_id"] or row["next_allowed_at"] > time.time()):
                return None
            # Ready MAIN continuations have priority over undispatched text.
            continuation = conn.execute(
                "SELECT 1 FROM deliveries WHERE lane_id=? AND state='pending' AND continuation>0 AND expires_at>? LIMIT 1",
                (target.lane_id, time.time()),
            ).fetchone()
            if continuation:
                return None
            token = "text_" + uuid.uuid4().hex
            conn.execute(
                "INSERT INTO text_attempts(token,owner,process_id,created_at) VALUES (?,?,?,?)",
                (token, owner, os.getpid(), time.time()),
            )
            conn.execute(
                "INSERT INTO lanes(lane_id,attempt_id) VALUES (?,?) ON CONFLICT(lane_id) DO UPDATE SET attempt_id=excluded.attempt_id",
                (target.lane_id, token),
            )
            return token

    def finish_text(
        self, target: TargetBinding, token: str, *, retry_after: float = 0, stopped: bool = True
    ) -> None:
        self._require_sender()
        self._validate_target(target)
        with self._transaction() as conn:
            if stopped:
                conn.execute(
                    "UPDATE text_attempts SET stopped_at=?,outcome='stopped' WHERE token=? AND EXISTS (SELECT 1 FROM lanes WHERE lane_id=? AND attempt_id=?)",
                    (time.time(), token, target.lane_id, token),
                )
                conn.execute(
                    "UPDATE lanes SET attempt_id=NULL,next_allowed_at=MAX(next_allowed_at,?) WHERE lane_id=? AND attempt_id=?",
                    (time.time() + max(0, retry_after), target.lane_id, token),
                )
            elif retry_after > 0:
                conn.execute(
                    "UPDATE lanes SET next_allowed_at=MAX(next_allowed_at,?) WHERE lane_id=? AND attempt_id=?",
                    (time.time() + retry_after, target.lane_id, token),
                )

    def set_cooldown(self, target: TargetBinding, retry_after: float, reason: str) -> None:
        self._validate_target(target)
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO lanes(lane_id,next_allowed_at,cooldown_reason) VALUES (?,?,?) ON CONFLICT(lane_id) DO UPDATE SET next_allowed_at=MAX(next_allowed_at,excluded.next_allowed_at),cooldown_reason=excluded.cooldown_reason",
                (target.lane_id, time.time() + max(0, retry_after), reason),
            )

    def native_allowed(self, target: TargetBinding, now: float | None = None) -> bool:
        self._validate_target(target)
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM lanes WHERE lane_id=?", (target.lane_id,)).fetchone()
            return not row or (
                not row["attempt_id"]
                and row["next_allowed_at"] <= (time.time() if now is None else now)
            )

    def expire_payloads(self, now: float | None = None) -> list[str]:
        """Detach expired inactive staging paths for caller cleanup; keep all facts."""
        current = time.time() if now is None else now
        released: list[str] = []
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT d.* FROM deliveries d WHERE expires_at<=? AND staged_path!='' AND NOT EXISTS (SELECT 1 FROM attempts a WHERE a.delivery_key=d.delivery_key AND a.native_active=1)",
                (current,),
            ).fetchall()
            for row in rows:
                released.append(row["staged_path"])
                conn.execute(
                    "UPDATE deliveries SET staged_path='',block_reason=CASE WHEN state='pending' THEN 'expired' ELSE block_reason END WHERE delivery_key=?",
                    (row["delivery_key"],),
                )
        # A content-shared path cannot be deleted while another live pin uses it.
        with self._connection() as conn:
            pinned = {
                row[0]
                for row in conn.execute("SELECT staged_path FROM deliveries WHERE staged_path!=''")
            }
        return [path for path in released if path not in pinned]

    def save_terminal_batch(
        self,
        request_id: str,
        target: TargetBinding,
        server_identity: str,
        account_scope: str,
        texts: list[str],
    ) -> str:
        """Store a complete sanitized batch; observations never schedule sends."""
        self._validate_target(target)
        _nonempty(request_id, server_identity, account_scope, *texts)
        batch_id = uuid.uuid4().hex
        with self._transaction() as conn:
            conn.executemany(
                "INSERT INTO terminal_segments VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        batch_id,
                        index,
                        request_id,
                        target.binding_id,
                        server_identity,
                        account_scope,
                        text,
                        "not_attempted",
                        time.time(),
                    )
                    for index, text in enumerate(texts)
                ],
            )
        return batch_id

    def update_terminal_segment(self, batch_id: str, ordinal: int, outcome: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE terminal_segments SET outcome=?,updated_at=? WHERE batch_id=? AND ordinal=?",
                (outcome, time.time(), batch_id, ordinal),
            )

    def list_terminal_segments(
        self,
        request_id: str,
        server_identity: str,
        account_scope: str,
    ) -> list[dict[str, Any]]:
        with self._connection() as conn:
            segments = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM terminal_segments WHERE request_id=? AND server_identity=? AND account_scope=? ORDER BY rowid",
                    (request_id, server_identity, account_scope),
                )
            ]
            # Legacy observations have no server/device columns. Only an actual
            # persisted request intent can qualify their scope for status lookup.
            legacy_rows = conn.execute(
                "SELECT DISTINCT t.* FROM terminal_observations t JOIN request_intents i ON i.request_id=t.request_id AND i.target_id=t.target_id WHERE t.request_id=? AND i.server_identity=? AND i.account_scope=?",
                (request_id, server_identity, account_scope),
            ).fetchall()
            new_targets = {row["target_id"] for row in segments}
            for row in legacy_rows:
                if row["target_id"] in new_targets:
                    continue
                for ordinal, text in enumerate(json.loads(row["texts_json"])):
                    segments.append(
                        {
                            "target_id": row["target_id"],
                            "ordinal": ordinal,
                            "text": text,
                            "outcome": row["outcome"],
                            "updated_at": row["updated_at"],
                        }
                    )
            return segments

    def save_terminal(
        self,
        request_id: str,
        target: TargetBinding,
        texts: tuple[str, ...] | list[str],
        outcome: str,
    ) -> None:
        """Persist already-sanitized current failure text for explicit status only.

        The caller applies the existing terminal sanitizer. No worker reads this
        table to send, resume or append historical text to a new request.
        """
        self._validate_target(target)
        _nonempty(request_id, outcome)
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO terminal_observations VALUES (?,?,?,?,?) ON CONFLICT(request_id,target_id) DO UPDATE SET texts_json=excluded.texts_json,outcome=excluded.outcome,updated_at=excluded.updated_at",
                (request_id, target.binding_id, _json(texts), outcome, time.time()),
            )

    def update_terminal_outcome(self, request_id: str, target: TargetBinding, outcome: str) -> None:
        self._validate_target(target)
        with self._transaction() as conn:
            conn.execute(
                "UPDATE terminal_observations SET outcome=?,updated_at=? WHERE request_id=? AND target_id=?",
                (outcome, time.time(), request_id, target.binding_id),
            )

    def get_terminal(self, request_id: str, target: TargetBinding) -> dict[str, Any] | None:
        self._validate_target(target)
        with self._connection() as conn:
            segments = conn.execute(
                "SELECT * FROM terminal_segments WHERE request_id=? AND target_id=? AND batch_id=(SELECT batch_id FROM terminal_segments WHERE request_id=? AND target_id=? ORDER BY rowid DESC LIMIT 1) ORDER BY ordinal",
                (request_id, target.binding_id, request_id, target.binding_id),
            ).fetchall()
            if segments:
                return {
                    "texts": [row["text"] for row in segments],
                    "outcome": segments[0]["outcome"] if len(segments) == 1 else "mixed",
                    "outcomes": [row["outcome"] for row in segments],
                    "updated_at": max(row["updated_at"] for row in segments),
                }
            row = conn.execute(
                "SELECT * FROM terminal_observations WHERE request_id=? AND target_id=?",
                (request_id, target.binding_id),
            ).fetchone()
            return (
                {
                    "texts": json.loads(row["texts_json"]),
                    "outcome": row["outcome"],
                    "updated_at": row["updated_at"],
                }
                if row
                else None
            )

    def import_legacy(self, source: Path) -> int:
        """Import observations only: old file IDs prove neither bytes nor acceptance.

        The restricted original snapshot is retained. Unknown/terminal records
        never enter the native queue, and tokens/URLs/raw terminal text are not
        copied into the ledger. No old/new sender dual-write is supported.
        """
        if not source.exists():
            return 0
        if source.is_symlink():
            raise OutboxError("Unsafe legacy state path")
        raw = source.read_bytes()
        if len(raw) > 8 * 1024 * 1024:
            raise OutboxError("Legacy state exceeds import limit")
        data = json.loads(raw)
        if not isinstance(data, dict) or not isinstance(data.get("deliveries", {}), dict):
            raise OutboxError("Invalid legacy delivery state")
        source_id = _key(str(source.resolve()))
        digest = hashlib.sha256(raw).hexdigest()
        snapshot = self.path.parent / f"legacy-pending-{digest}.json"
        if not snapshot.exists():
            fd = open_private_file(snapshot, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        count = 0
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT digest FROM imports WHERE source_id=?", (source_id,)
            ).fetchone()
            if existing:
                if existing[0] != digest:
                    raise IdentityConflictError("Legacy source changed after cutover")
                return 0
            for request_id, record in data.get("deliveries", {}).items():
                if not isinstance(request_id, str) or not isinstance(record, dict):
                    continue
                safe = {
                    key: record[key]
                    for key in (
                        "platform",
                        "chat_id",
                        "user_id",
                        "device_id",
                        "status",
                        "delivered_file_ids",
                        "dead_letter_file_ids",
                    )
                    if key in record
                }
                disposition = (
                    "dormant_terminal"
                    if record.get("status") == "terminal_text_pending"
                    else "quarantined_incomplete_identity"
                )
                conn.execute(
                    "INSERT OR IGNORE INTO legacy_observations VALUES (?,?,?,?,?)",
                    (
                        _key((source_id, request_id)),
                        source_id,
                        request_id,
                        _json(safe),
                        disposition,
                    ),
                )
                count += 1
            conn.execute("INSERT INTO imports VALUES (?,?,?)", (source_id, digest, time.time()))
        return count

    import_legacy_json = import_legacy


def resolve_outbox_path(state_path: Path) -> Path:
    return state_path.with_name(OUTBOX_FILENAME)
