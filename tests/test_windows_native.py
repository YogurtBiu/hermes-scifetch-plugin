"""Standalone OS contracts for an exported or officially installed paper plugin."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

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


def upgrade_state(profile: Path, proof_path: Path, *, verify: bool) -> None:
    """Seed or inspect an installed plugin using offline credentials and receipts."""
    state_module = load_module("state")
    outbox_module = load_module("outbox")
    plugin = profile / "plugins" / "scifetch-paper"
    names = [
        ".env",
        "config.yaml",
        "plugins/scifetch-paper/state.json",
        "plugins/scifetch-paper/delivery_outbox.sqlite3",
    ]
    if verify:
        proof = json.loads(proof_path.read_text(encoding="utf-8"))
        for name, digest in proof["sha256"].items():
            actual = hashlib.sha256((profile / name).read_bytes()).hexdigest()
            if actual != digest:
                raise AssertionError(f"Official update changed retained state: {name}")
        store = outbox_module.OutboxStore(plugin / "delivery_outbox.sqlite3", "upgrade-install")
        for key, expected in proof["records"].items():
            if store.get_delivery(key).state != expected:
                raise AssertionError(f"Delivery observation changed: {key}")
        if len(store.pending_audits()) != 1:
            raise AssertionError("The native acceptance audit was not retained")
        with store.sender_lock():
            store.recover("after-upgrade")
            if store.claim_next("after-upgrade") is not None:
                raise AssertionError("Upgrade requeued an accepted or unknown native attempt")
        print(
            "Official Git update retained profile, device state, native receipt and unknown attempt."
        )
        return

    (profile / ".env").write_text(
        "SCIFETCH_TRUST_MODE=personal_instance\nSCIFETCH_SERVER_URL=https://offline.example.test\n",
        encoding="utf-8",
    )
    state_module.save_state(
        plugin / "state.json",
        state_module.PluginState(
            server_url="https://offline.example.test",
            device_id="upgrade-device",
            access_token="offline-access",
            refresh_token="offline-refresh",
            access_expires_at="2099-01-01T00:00:00+00:00",
            refresh_expires_at="2099-02-01T00:00:00+00:00",
            trust_mode="personal_instance",
            installation_id="upgrade-install",
        ),
    )
    store = outbox_module.OutboxStore(plugin / "delivery_outbox.sqlite3", "upgrade-install")
    target = outbox_module.TargetBinding("upgrade-install", "weixin", "chat", "user", "dm")
    records: dict[str, str] = {}
    with store.sender_lock():
        for outcome in ["accepted", "unknown"]:
            body = f"offline-{outcome}".encode()
            artifact = outbox_module.Artifact(
                f"file-{outcome}",
                f"artifact-{outcome}",
                "MAIN",
                hashlib.sha256(body).hexdigest(),
                len(body),
                "application/pdf",
            )
            scope = outbox_module.GateScope(
                "server",
                "account",
                "upgrade-install",
                f"request-{outcome}",
                "item",
                "acquisition",
                "generation",
                target.binding_id,
            )
            record = store.enqueue(scope, target, artifact)
            staged = profile / f"{outcome}.pdf"
            staged.write_bytes(body)
            store.record_staged(record.key, staged)
            attempt = store.claim_next("before-upgrade")
            if attempt is None or not store.begin_native(attempt.attempt_id, attempt.start_token):
                raise AssertionError("Could not seed the offline native attempt")
            if outcome == "accepted":
                receipt = outbox_module.NativeReceipt("send_document", "offline-upgrade-test")
                if not store.accept(attempt.attempt_id, receipt):
                    raise AssertionError("Could not seed the offline native receipt")
            else:
                store.mark_unknown(attempt.attempt_id, "offline-unconfirmed", stopped=True)
            records[record.key] = outcome
    proof_path.write_text(
        json.dumps(
            {
                "records": records,
                "sha256": {
                    name: hashlib.sha256((profile / name).read_bytes()).hexdigest()
                    for name in names
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )


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

    def runtime(self) -> Any:
        environment = patch.dict(
            os.environ,
            {
                "HOME": str(self.root),
                "USERPROFILE": str(self.root),
                "HERMES_HOME": str(self.root / "profile"),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)
        durable = load_module("durable_delivery")
        state_module = load_module("state")
        state = state_module.PluginState(
            server_url="https://offline.example.test",
            device_id="offline-device",
            access_token="offline-access",
            refresh_token="offline-refresh",
            access_expires_at="2099-01-01T00:00:00+00:00",
            refresh_expires_at="2099-02-01T00:00:00+00:00",
            trust_mode="personal_instance",
            installation_id="native-install",
        )
        path = self.root / "state.json"
        state_module.save_state(path, state)
        return durable.legacy.DeliveryRuntime(
            state=state,
            state_path=path,
            request_id="request",
            target=durable.legacy.DeliveryTarget(
                platform="weixin",
                chat_id="chat",
                user_id="user",
                chat_type="private",
                hermes_version="0.23.0",
            ),
            send_document=lambda path: self.fail("No native sends in a startup/text test"),
        )

    @staticmethod
    def close_daemon(durable: Any, worker: Any) -> None:
        asyncio.run_coroutine_threadsafe(worker.close(), worker._loop).result(timeout=5)
        durable._SUPERVISORS.pop(worker.store.installation_id, None)

    def test_supervisor_busy_start_retries_without_reviving_unknown_or_fatal(self) -> None:
        runtime = self.runtime()
        durable = load_module("durable_delivery")
        incumbent = durable.open_store(runtime)
        record = self.add_main(incumbent)
        with incumbent.sender_lock():
            attempt = incumbent.claim_next("old-owner")
            self.assertTrue(incumbent.begin_native(attempt.attempt_id, attempt.start_token))
            incumbent.mark_unknown(attempt.attempt_id, "offline-unconfirmed", stopped=True)
            with self.assertRaisesRegex(RuntimeError, "owned by another process"):
                durable.supervisor_for(runtime)
            self.assertNotIn(runtime.state.installation_id, durable._SUPERVISORS)
        worker = durable.supervisor_for(runtime)
        try:
            self.assertTrue(worker._loop.is_running() and worker._lock.held)
            self.assertEqual(len(worker._tasks), 5)
            self.assertEqual(worker.store.get_delivery(record.key).state, "unknown")
            self.assertIsNone(worker.store.claim_next(worker.owner))
            worker._fatal = True
            self.assertIs(durable.supervisor_for(runtime), worker)
            self.assertTrue(worker._fatal)
        finally:
            self.close_daemon(durable, worker)

    def test_supervisor_start_exception_releases_real_lock(self) -> None:
        runtime = self.runtime()
        durable = load_module("durable_delivery")
        with (
            patch.object(
                durable.OutboxStore, "recover", side_effect=RuntimeError("offline failure")
            ),
            self.assertRaisesRegex(RuntimeError, "offline failure"),
        ):
            durable.supervisor_for(runtime)
        self.assertNotIn(runtime.state.installation_id, durable._SUPERVISORS)
        worker = durable.supervisor_for(runtime)
        try:
            self.assertTrue(worker._lock.held and worker._loop.is_running())
        finally:
            self.close_daemon(durable, worker)

    def test_supervisor_start_timeout_releases_late_owner(self) -> None:
        runtime = self.runtime()
        durable = load_module("durable_delivery")
        release, entered, closed = threading.Event(), threading.Event(), threading.Event()
        recover = durable.OutboxStore.recover
        close = durable.DeliverySupervisor.close

        def delayed_recover(store: Any, owner: str) -> None:
            recover(store, owner)
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("Test did not release startup")

        async def observed_close(worker: Any) -> None:
            await close(worker)
            closed.set()

        with (
            patch.object(durable.OutboxStore, "recover", delayed_recover),
            patch.object(durable.DeliverySupervisor, "close", observed_close),
            patch.object(durable, "_STARTUP_TIMEOUT", 0.5),
        ):
            try:
                with self.assertRaisesRegex(RuntimeError, "initialization did not finish"):
                    durable.supervisor_for(runtime)
                self.assertTrue(entered.is_set())
                self.assertNotIn(runtime.state.installation_id, durable._SUPERVISORS)
            finally:
                release.set()
            self.assertTrue(closed.wait(timeout=5))
        worker = durable.supervisor_for(runtime)
        try:
            self.assertTrue(worker._lock.held and worker._loop.is_running())
        finally:
            self.close_daemon(durable, worker)

    def test_windows_paths_redacted_in_real_summary_storage_and_callback(self) -> None:
        runtime = self.runtime()
        durable = load_module("durable_delivery")
        sent: list[str] = []
        store = durable.open_store(runtime)

        async def text(message: str) -> dict[str, bool]:
            rows = store.list_terminal_segments(
                runtime.request_id, runtime.state.server_url, runtime.state.device_id
            )
            self.assertEqual(rows[-1]["text"], message)
            sent.append(message)
            return {"success": True}

        runtime = replace(runtime, send_text=text)

        async def exercise() -> None:
            worker = durable.DeliverySupervisor(store)
            worker.register(runtime)
            self.assertTrue(await worker.start())
            try:
                with patch.object(durable, "supervisor_for", return_value=worker):
                    for path in [
                        r"C:\Users\Fictional\private.pdf",
                        r"C:\\Users\\Fictional\\private.pdf",
                        r'"C:\Users\Fictional User\private file.pdf"',
                        r"'D:\Fictional Files\private file.pdf'",
                        r"C:\Users\Fictional User\private file.pdf",
                        r"'D:\Fictional O'Connor\private file.pdf'",
                    ]:
                        self.assertTrue(
                            await durable.legacy._send_delivery_summary(
                                runtime,
                                [
                                    {
                                        "role": "main",
                                        "filename": "main.pdf",
                                        "title": "Synthetic study " + path,
                                        "doi": "10.9999/offline",
                                        "journal": "Synthetic Journal",
                                    }
                                ],
                            )
                        )
            finally:
                await worker.close()

        asyncio.run(exercise())
        self.assertEqual(len(sent), 6)
        rows = store.list_terminal_segments(
            runtime.request_id, runtime.state.server_url, runtime.state.device_id
        )
        self.assertEqual([row["text"] for row in rows], sent)
        for message in sent:
            self.assertIn("<server-path>", message)
            self.assertNotIn("Fictional", message)
            self.assertNotIn("private", message)
            self.assertIn("Synthetic study", message)
            self.assertIn("10.9999/offline", message)
            self.assertIn("Synthetic Journal", message)

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

    @unittest.skipUnless(os.name == "nt", "Windows API error branch contract")
    def test_windows_uncertain_process_queries_preserve_unknown(self) -> None:
        api = self.platform._kernel32
        for error in [5, 299]:
            with (
                self.subTest(error=error),
                patch.object(api, "OpenProcess", return_value=None),
                patch.object(self.platform.ctypes, "get_last_error", return_value=error),
            ):
                self.assertFalse(self.platform.process_exited(12345))
        with (
            patch.object(api, "OpenProcess", return_value=123),
            patch.object(api, "WaitForSingleObject", return_value=0xFFFFFFFF),
            patch.object(api, "CloseHandle") as close,
        ):
            self.assertFalse(self.platform.process_exited(12345))
            close.assert_called_once_with(123)

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
        installed_commit = subprocess.check_output(
            ["git", "-C", str(PLUGIN), "rev-parse", "HEAD"], text=True, encoding="utf-8"
        ).strip()
        self.assertEqual(installed_commit, os.environ["SCIFETCH_TEST_EXPECTED_COMMIT"])
        print(f"Official installed plugin commit matches event: {installed_commit}")
        source = json.loads((PLUGIN / "SOURCE.json").read_text(encoding="utf-8"))
        self.assertEqual(source["plugin_version"], "0.2.29")
        for name, digest in source["sha256"].items():
            self.assertEqual(hashlib.sha256((PLUGIN / name).read_bytes()).hexdigest(), digest)

    @unittest.skipUnless(
        os.environ.get("SCIFETCH_TEST_HERMES"), "Run inside the official Hermes CI host"
    )
    def test_official_git_update_preserves_native_state(self) -> None:
        source = self.root / "local-source"
        source.mkdir()
        manifest = json.loads((PLUGIN / "SOURCE.json").read_text(encoding="utf-8"))
        for name in [*manifest["sha256"], "SOURCE.json"]:
            destination = source / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(PLUGIN / name, destination)

        def git(*args: str) -> str:
            return subprocess.check_output(
                ["git", "-C", str(source), *args], text=True, encoding="utf-8"
            ).strip()

        git("init", "-q", "-b", "main")
        git("config", "user.name", "Offline upgrade test")
        git("config", "user.email", "offline@example.test")
        git("add", ".")
        git("commit", "-qm", "Initial offline plugin snapshot")
        home = self.root / "upgrade-home"
        home.mkdir()
        profile = home / "profile"
        env = dict(os.environ)
        env.update(
            {
                "HOME": str(home),
                "USERPROFILE": str(home),
                "HERMES_HOME": str(profile),
                "LOCALAPPDATA": str(home / "AppData/Local"),
                "APPDATA": str(home / "AppData/Roaming"),
                "SCIFETCH_TEST_PLUGIN": str(profile / "plugins/scifetch-paper"),
            }
        )
        env.pop("GITHUB_TOKEN", None)
        env.pop("GH_TOKEN", None)

        def command(*args: str) -> None:
            result = subprocess.run(
                [sys.executable, *args],
                env=env,
                input="y\ny\n",
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=180,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        command("-m", "hermes_cli.main", "plugins", "install", source.as_uri(), "--no-enable")
        command("-m", "hermes_cli.main", "plugins", "enable", "scifetch-paper")
        proof = self.root / "upgrade-proof.json"
        script = str(Path(__file__).resolve())
        command(script, "--seed-upgrade", str(profile), str(proof))
        readme = source / "README.md"
        readme.write_text(
            readme.read_text(encoding="utf-8") + "\nOffline update fixture.\n", encoding="utf-8"
        )
        git("add", "README.md")
        git("commit", "-qm", "Second offline plugin snapshot")
        command("-m", "hermes_cli.main", "plugins", "update", "scifetch-paper")
        installed = profile / "plugins/scifetch-paper"
        installed_head = subprocess.check_output(
            ["git", "-C", str(installed), "rev-parse", "HEAD"], text=True
        ).strip()
        self.assertEqual(installed_head, git("rev-parse", "HEAD"))
        command(script, "--verify-upgrade", str(profile), str(proof))


if __name__ == "__main__":
    if sys.argv[1:2] == ["--child"]:
        child(sys.argv[2], Path(sys.argv[3]))
    elif sys.argv[1:2] in [["--seed-upgrade"], ["--verify-upgrade"]]:
        upgrade_state(
            Path(sys.argv[2]), Path(sys.argv[3]), verify=sys.argv[1] == "--verify-upgrade"
        )
    else:
        unittest.main(verbosity=2)
