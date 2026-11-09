"""Standalone OS contracts for an exported or officially installed paper plugin."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

PLUGIN = Path(os.environ.get("SCIFETCH_TEST_PLUGIN", Path(__file__).resolve().parents[1]))


def load_module(name: str) -> ModuleType:
    package_name = "scifetch_native_test"
    if package_name not in sys.modules:
        package = ModuleType(package_name)
        package.__path__ = [str(PLUGIN)]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.{name}")


def child(mode: str, root: Path) -> None:
    store_module = load_module("outbox")
    if mode == "idle":
        print("ready", flush=True)
        sys.stdin.read(1)
        return
    store = store_module.OutboxStore(root / "ledger.sqlite3", "native-install", root / "locks")
    with store.sender_lock():
        print("locked", flush=True)
        instruction = sys.stdin.read(1)
        if instruction == "x":
            os._exit(0)


class NativeContracts(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="scifetch-native-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "资料 空间"
        self.root.mkdir()
        self.outbox = load_module("outbox")
        self.platform = load_module("platform_io")

    def store(self, path: Path | None = None) -> Any:
        return self.outbox.OutboxStore(
            path or self.root / "ledger.sqlite3", "native-install", self.root / "locks"
        )

    def start_child(self, mode: str) -> subprocess.Popen[str]:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--child", mode, str(self.root)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.addCleanup(self.stop_child, process)
        assert process.stdout is not None
        ready = process.stdout.readline().strip()
        if not ready:
            output, error = process.communicate("q", timeout=15)
            self.fail(f"Child exited before readiness ({process.returncode}): {output}\n{error}")
        self.assertEqual(ready, "ready" if mode == "idle" else "locked")
        return process

    @staticmethod
    def stop_child(process: subprocess.Popen[str]) -> None:
        if process.poll() is None:
            process.communicate("q", timeout=15)

    def test_real_process_sender_contention_and_release(self) -> None:
        process = self.start_child("sender")
        store = self.store()
        self.assertFalse(store.sender_lock().acquire())
        output, error = process.communicate("q", timeout=15)
        self.assertEqual(process.returncode, 0, output + error)
        self.assertTrue(store.sender_lock().acquire())
        store.sender_lock().release()

    def test_process_exit_releases_os_lock(self) -> None:
        process = self.start_child("sender")
        process.communicate("x", timeout=15)
        self.assertEqual(process.returncode, 0)
        store = self.store()
        self.assertTrue(store.sender_lock().acquire())
        store.sender_lock().release()

    def test_canonical_ledger_binding_survives_release(self) -> None:
        store = self.store()
        with store.sender_lock():
            self.assertTrue(store.sender_lock().held)
        other = self.store(self.root / "other.sqlite3")
        self.assertFalse(other.sender_lock().acquire())
        with self.store().sender_lock():
            pass

    @unittest.skipUnless(os.name == "nt", "Windows case-insensitive path contract")
    def test_windows_case_alias_refers_to_same_ledger(self) -> None:
        with self.store().sender_lock():
            pass
        alias = Path(str(self.root / "ledger.sqlite3").swapcase())
        with self.store(alias).sender_lock():
            pass

    def test_readonly_probe_preserves_running_process(self) -> None:
        process = self.start_child("idle")
        self.assertFalse(self.platform.process_exited(process.pid))
        self.assertIsNone(process.poll())
        process.communicate("q", timeout=15)
        self.assertEqual(process.returncode, 0)
        self.assertTrue(self.platform.process_exited(process.pid))

    def test_current_process_has_no_exit_proof(self) -> None:
        self.assertFalse(self.platform.process_exited(os.getpid()))
        self.assertFalse(self.platform.process_exited(0))

    def test_private_create_preserves_unicode_path_and_exclusive_creation(self) -> None:
        path = self.root / "记录 文件.json"
        descriptor = self.platform.open_private_file(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write("保留内容".encode())
        self.assertEqual(path.read_bytes(), "保留内容".encode())
        with self.assertRaises(FileExistsError):
            self.platform.open_private_file(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)

    def test_leaf_symlink_cannot_be_opened(self) -> None:
        target = self.root / "original.json"
        target.write_text("preserved", encoding="utf-8")
        link = self.root / "linked.json"
        link.symlink_to(target)
        with self.assertRaises(OSError):
            self.platform.open_private_file(link, os.O_RDWR | os.O_CREAT)
        self.assertEqual(target.read_text(encoding="utf-8"), "preserved")

    def test_state_atomic_replacement_uses_same_installation(self) -> None:
        state_module = load_module("state")
        state = state_module.PluginState(
            server_url="https://offline.example.test",
            device_id="device-test",
            access_token="offline-access",
            refresh_token="offline-refresh",
            access_expires_at="2099-01-01T00:00:00+00:00",
            refresh_expires_at="2099-02-01T00:00:00+00:00",
            trust_mode="personal_instance",
            installation_id="native-install",
        )
        path = self.root / "state.json"
        state_module.save_state(path, state)
        state_module.save_state(path, state)
        self.assertEqual(state_module.load_state(path), state)
        self.assertFalse(path.with_name("state.json.tmp").exists())

    def add_main(self, store: Any) -> Any:
        target = self.outbox.TargetBinding("native-install", "weixin", "chat", "user", "dm")
        scope = self.outbox.GateScope(
            "server",
            "account",
            "native-install",
            "request",
            "item",
            "acquisition",
            "generation",
            target.binding_id,
        )
        body = b"offline-artifact"
        artifact = self.outbox.Artifact(
            "logical-file",
            "artifact",
            "MAIN",
            hashlib.sha256(body).hexdigest(),
            len(body),
            "application/pdf",
        )
        record = store.enqueue(scope, target, artifact)
        path = self.root / "正文 文件.pdf"
        path.write_bytes(body)
        store.record_staged(record.key, path)
        return record

    def test_sqlite_record_survives_reopen(self) -> None:
        store = self.store()
        record = self.add_main(store)
        reopened = self.store()
        recovered = reopened.get_delivery(record.key)
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.state, "pending")
        self.assertEqual(recovered.artifact.sha256, record.artifact.sha256)

    def test_unknown_native_attempt_stays_unclaimable_after_recovery(self) -> None:
        store = self.store()
        record = self.add_main(store)
        with store.sender_lock():
            attempt = store.claim_next("old-owner")
            self.assertIsNotNone(attempt)
            self.assertTrue(store.begin_native(attempt.attempt_id, attempt.start_token))
            store.mark_unknown(attempt.attempt_id, "offline-unconfirmed", stopped=True)
        reopened = self.store()
        with reopened.sender_lock():
            reopened.recover("new-owner")
            self.assertEqual(reopened.get_delivery(record.key).state, "unknown")
            self.assertIsNone(reopened.claim_next("new-owner"))

    def test_local_native_receipt_and_audit_survive_reopen(self) -> None:
        store = self.store()
        record = self.add_main(store)
        with store.sender_lock():
            attempt = store.claim_next("old-owner")
            self.assertIsNotNone(attempt)
            self.assertTrue(store.begin_native(attempt.attempt_id, attempt.start_token))
            # A fake receipt exercises ledger durability without invoking a platform.
            receipt = self.outbox.NativeReceipt("send_document", "offline-test-v1")
            self.assertTrue(store.accept(attempt.attempt_id, receipt))
        reopened = self.store()
        with reopened.sender_lock():
            reopened.recover("new-owner")
            self.assertEqual(reopened.get_delivery(record.key).state, "accepted")
            self.assertEqual(len(reopened.pending_audits()), 1)
            self.assertIsNone(reopened.claim_next("new-owner"))

    @unittest.skipUnless(
        os.environ.get("SCIFETCH_TEST_HERMES"), "Run inside the official Hermes CI host"
    )
    def test_official_installed_plugin_registration(self) -> None:
        host = Path(os.environ["SCIFETCH_TEST_HERMES"])
        sys.path.insert(0, str(host))
        manager_class = importlib.import_module("hermes_cli.plugins").PluginManager
        get_version_info = importlib.import_module("hermes_cli.version_info").get_version_info

        self.assertIsNone(importlib.util.find_spec("scifetch"))
        manager = manager_class()
        manager.discover_and_load()
        loaded = manager._plugins["scifetch-paper"]
        self.assertTrue(loaded.enabled, loaded.error)
        self.assertIsNotNone(loaded.module)
        self.assertEqual(set(loaded.commands_registered), {"scifetch", "paper"})
        self.assertEqual(loaded.tools_registered, ["scifetch_paper_request"])
        self.assertEqual(len(loaded.hooks_registered), 5)
        self.assertEqual(get_version_info().derived_version, "0.23.0")
        source = json.loads((PLUGIN / "SOURCE.json").read_text(encoding="utf-8"))
        self.assertEqual(source["plugin_version"], "0.2.28")
        for name, digest in source["sha256"].items():
            self.assertEqual(hashlib.sha256((PLUGIN / name).read_bytes()).hexdigest(), digest)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--child"]:
        child(sys.argv[2], Path(sys.argv[3]))
    else:
        unittest.main(verbosity=2)
