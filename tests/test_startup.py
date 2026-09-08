import asyncio
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
import types

import pytest


PROFILE_ROOT = Path(__file__).resolve().parents[1] / "C2_Profiles" / "discordx"


def load_startup_module():
    fake_root = types.ModuleType("mythic_container")
    fake_root.C2_RPC_RESYNC_ROUTING_KEY = "c2_rpc_resync"
    fake_root.RabbitmqConnection = SimpleNamespace(
        SendRPCDictMessage=None,
        conn=None,
        futures={},
    )
    fake_root.C2ProfileBase = SimpleNamespace(c2Profiles={})

    sys.path.insert(0, str(PROFILE_ROOT))
    try:
        if "startup" in sys.modules:
            del sys.modules["startup"]
        sys.modules["mythic_container"] = fake_root
        return importlib.import_module("startup")
    finally:
        try:
            sys.path.remove(str(PROFILE_ROOT))
        except ValueError:
            pass


def test_restore_bind_mount_ownership_uses_mount_owner(tmp_path, monkeypatch):
    startup = load_startup_module()
    calls = []

    def fake_run(command, check):
        calls.append((command, check))

    monkeypatch.setattr(startup.subprocess, "run", fake_run)

    startup.restore_bind_mount_ownership(tmp_path)

    assert calls == [
        (
            ["chown", "-R", f"--reference={tmp_path}", str(tmp_path)],
            True,
        )
    ]


def test_build_server_binary_restores_ownership_after_publish_failure(monkeypatch):
    startup = load_startup_module()
    calls = []

    def fake_run(command, cwd, check):
        calls.append(("publish", command, cwd, check))
        raise RuntimeError("publish failed")

    def fake_restore(path):
        calls.append(("restore", path))

    monkeypatch.setattr(startup.subprocess, "run", fake_run)
    monkeypatch.setattr(startup, "restore_bind_mount_ownership", fake_restore)

    with pytest.raises(RuntimeError, match="publish failed"):
        startup.build_server_binary()

    assert calls[-1] == ("restore", startup.MYTHIC_ROOT)


def test_bootstrap_runtime_config_from_env_writes_blank_config(tmp_path, monkeypatch):
    startup = load_startup_module()
    config_path = tmp_path / "config.json"
    config_path.write_text('{"botToken": "", "channelID": ""}\n')
    monkeypatch.setenv("BOT_TOKEN", "env-bot-token")
    monkeypatch.setenv("CHANNEL_ID", "1234567890")

    changed = startup.bootstrap_runtime_config_from_env(config_path)

    assert changed is True
    assert json.loads(config_path.read_text()) == {
        "botToken": "env-bot-token",
        "channelID": "1234567890",
    }


def test_bootstrap_runtime_config_from_env_keeps_existing_valid_config(tmp_path, monkeypatch):
    startup = load_startup_module()
    config_path = tmp_path / "config.json"
    config_path.write_text('{"botToken": "existing-token", "channelID": "1234567890"}\n')
    monkeypatch.setenv("BOT_TOKEN", "env-bot-token")
    monkeypatch.setenv("CHANNEL_ID", "9999999999")

    changed = startup.bootstrap_runtime_config_from_env(config_path)

    assert changed is False
    assert json.loads(config_path.read_text()) == {
        "botToken": "existing-token",
        "channelID": "1234567890",
    }


def test_rpc_healthcheck_uses_debug_output_queue(monkeypatch):
    startup = load_startup_module()
    calls = {}

    async def fake_send(*, queue, body):
        calls["queue"] = queue
        calls["body"] = body
        return {"success": True, "server_running": True}

    monkeypatch.setattr(
        startup.mythic_container,
        "RabbitmqConnection",
        SimpleNamespace(SendRPCDictMessage=fake_send),
        raising=False,
    )

    assert asyncio.run(startup.rpc_healthcheck("discordx")) is True
    assert calls == {
        "queue": "discordx_c2_rpc_get_server_debug_output",
        "body": {"c2_profile_name": "discordx", "message": ""},
    }


@pytest.mark.asyncio
async def test_rpc_health_status_reports_reachable_without_server_running(monkeypatch):
    startup = load_startup_module()

    async def fake_send(*, queue, body):
        return {"success": True, "server_running": False}

    monkeypatch.setattr(
        startup.mythic_container,
        "RabbitmqConnection",
        SimpleNamespace(SendRPCDictMessage=fake_send),
        raising=False,
    )

    assert await startup.rpc_health_status("discordx") == {
        "reachable": True,
        "server_running": False,
        "response": {"success": True, "server_running": False},
    }
    assert await startup.rpc_healthcheck("discordx", require_running=False) is True
    assert await startup.rpc_healthcheck("discordx", require_running=True) is False


@pytest.mark.asyncio
async def test_rpc_health_status_treats_valid_unsuccessful_reply_as_reachable(monkeypatch):
    startup = load_startup_module()

    async def fake_send(*, queue, body):
        return {"success": False, "error": "debug RPC is unavailable"}

    monkeypatch.setattr(
        startup.mythic_container,
        "RabbitmqConnection",
        SimpleNamespace(SendRPCDictMessage=fake_send),
        raising=False,
    )

    assert await startup.rpc_health_status("discordx") == {
        "reachable": True,
        "server_running": False,
        "response": {"success": False, "error": "debug RPC is unavailable"},
    }
    assert await startup.rpc_healthcheck("discordx", require_running=False) is True
    assert await startup.rpc_healthcheck("discordx", require_running=True) is False


@pytest.mark.asyncio
async def test_rpc_healthcheck_times_out_and_returns_false(monkeypatch):
    startup = load_startup_module()

    async def fake_send(*, queue, body):
        await asyncio.sleep(3600)

    monkeypatch.setattr(
        startup.mythic_container,
        "RabbitmqConnection",
        SimpleNamespace(SendRPCDictMessage=fake_send),
        raising=False,
    )

    assert await startup.rpc_healthcheck("discordx", timeout=0.01) is False


def test_ensure_service_ready_recovers_unhealthy_startup(monkeypatch):
    startup = load_startup_module()
    fake_c2 = object()
    calls = []
    statuses = iter(
        [
            {"reachable": False, "server_running": False, "response": None},
            {"reachable": False, "server_running": False, "response": None},
            {"reachable": True, "server_running": True, "response": {"success": True, "server_running": True}},
        ]
    )

    async def fake_wait(profile_name, *, require_running, wait_timeout, poll_interval):
        calls.append(("wait", profile_name, require_running, wait_timeout, poll_interval))
        return next(statuses)

    async def fake_start(c2profile):
        calls.append(("start", c2profile))

    async def fake_sync(c2profile):
        calls.append(("sync", c2profile))

    async def fake_status(profile_name, running, error=""):
        calls.append(("status", profile_name, running, error))

    async def fake_cancel():
        calls.append("cancel")

    monkeypatch.setattr(startup, "wait_for_rpc_state", fake_wait)
    monkeypatch.setattr(startup, "publish_running_status", fake_status)
    monkeypatch.setattr(startup, "cancel_queue_tasks", fake_cancel)
    monkeypatch.setattr(
        startup,
        "get_mythic_service",
        lambda: SimpleNamespace(
            startC2RabbitMQ=fake_start,
            syncC2ProfileData=fake_sync,
        ),
    )
    monkeypatch.setattr(
        startup.mythic_container.C2ProfileBase,
        "c2Profiles",
        {"discordx": fake_c2},
        raising=False,
    )

    asyncio.run(
        startup.ensure_service_ready(
            "discordx",
            retries=2,
            retry_delay=0,
            startup_grace_period=7,
            poll_interval=0.5,
        )
    )

    assert calls == [
        ("wait", "discordx", True, 7, 0.5),
        "cancel",
        ("start", fake_c2),
        ("sync", fake_c2),
        ("wait", "discordx", True, 7, 0.5),
        "cancel",
        ("start", fake_c2),
        ("sync", fake_c2),
        ("wait", "discordx", True, 7, 0.5),
        ("status", "discordx", True, ""),
    ]


def test_ensure_service_ready_marks_running_on_immediate_health(monkeypatch):
    startup = load_startup_module()
    fake_c2 = object()
    calls = []

    async def healthy(profile_name, *, require_running, wait_timeout, poll_interval):
        calls.append(("wait", profile_name, require_running, wait_timeout, poll_interval))
        return {"reachable": True, "server_running": True, "response": {"success": True, "server_running": True}}

    async def fake_status(profile_name, running, error=""):
        calls.append(("status", profile_name, running, error))

    async def fake_sync(c2profile):
        calls.append(("sync", c2profile))

    monkeypatch.setattr(startup, "wait_for_rpc_state", healthy)
    monkeypatch.setattr(startup, "publish_running_status", fake_status)
    monkeypatch.setattr(
        startup,
        "get_mythic_service",
        lambda: SimpleNamespace(syncC2ProfileData=fake_sync),
    )
    monkeypatch.setattr(
        startup.mythic_container.C2ProfileBase,
        "c2Profiles",
        {"discordx": fake_c2},
        raising=False,
    )

    asyncio.run(startup.ensure_service_ready("discordx", startup_grace_period=9, poll_interval=0.25))

    assert calls == [
        ("wait", "discordx", True, 9, 0.25),
        ("sync", fake_c2),
        ("status", "discordx", True, ""),
    ]


def test_ensure_service_ready_raises_when_healthcheck_never_recovers(monkeypatch):
    startup = load_startup_module()
    fake_c2 = object()

    async def always_unhealthy(profile_name, *, require_running, wait_timeout, poll_interval):
        return {"reachable": False, "server_running": False, "response": None}

    async def fake_start(c2profile):
        return None

    async def fake_sync(c2profile):
        return None

    monkeypatch.setattr(startup, "wait_for_rpc_state", always_unhealthy)
    monkeypatch.setattr(
        startup,
        "get_mythic_service",
        lambda: SimpleNamespace(
            startC2RabbitMQ=fake_start,
            syncC2ProfileData=fake_sync,
        ),
    )
    monkeypatch.setattr(
        startup.mythic_container.C2ProfileBase,
        "c2Profiles",
        {"discordx": fake_c2},
        raising=False,
    )

    with pytest.raises(RuntimeError, match="discordx startup failed"):
        asyncio.run(
            startup.ensure_service_ready(
                "discordx",
                retries=2,
                retry_delay=0,
                startup_grace_period=5,
                poll_interval=0.5,
            )
        )


def test_ensure_service_ready_waits_for_running_before_recycling_healthy_queues(monkeypatch):
    startup = load_startup_module()
    fake_c2 = object()
    calls = []
    statuses = iter(
        [
            {"reachable": True, "server_running": False, "response": {"success": True, "server_running": False}},
            {"reachable": True, "server_running": True, "response": {"success": True, "server_running": True}},
        ]
    )

    async def fake_wait(profile_name, *, require_running, wait_timeout, poll_interval):
        calls.append(("wait", profile_name, require_running, wait_timeout, poll_interval))
        return next(statuses)

    async def fake_start(c2profile):
        calls.append(("start", c2profile))

    async def fake_sync(c2profile):
        calls.append(("sync", c2profile))

    async def fake_cancel():
        calls.append("cancel")

    async def fake_status(profile_name, running, error=""):
        calls.append(("status", profile_name, running, error))

    monkeypatch.setattr(startup, "wait_for_rpc_state", fake_wait)
    monkeypatch.setattr(startup, "cancel_queue_tasks", fake_cancel)
    monkeypatch.setattr(startup, "publish_running_status", fake_status)
    monkeypatch.setattr(
        startup,
        "get_mythic_service",
        lambda: SimpleNamespace(
            startC2RabbitMQ=fake_start,
            syncC2ProfileData=fake_sync,
        ),
    )
    monkeypatch.setattr(
        startup.mythic_container.C2ProfileBase,
        "c2Profiles",
        {"discordx": fake_c2},
        raising=False,
    )

    asyncio.run(
        startup.ensure_service_ready(
            "discordx",
            retries=1,
            retry_delay=0,
            startup_grace_period=6,
            poll_interval=0.5,
        )
    )

    assert calls == [
        ("wait", "discordx", True, 6, 0.5),
        ("sync", fake_c2),
        ("wait", "discordx", True, 6, 0.5),
        ("status", "discordx", True, ""),
    ]


@pytest.mark.asyncio
async def test_reset_rabbitmq_connection_closes_stale_connection_and_clears_futures():
    startup = load_startup_module()
    calls = []

    class FakeFuture:
        def __init__(self):
            self.cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

    class FakeConnection:
        async def close(self):
            calls.append("close")

    future = FakeFuture()
    startup.mythic_container.RabbitmqConnection = SimpleNamespace(
        conn=FakeConnection(),
        futures={"pending": future},
    )

    await startup.reset_rabbitmq_connection()

    assert calls == ["close"]
    assert future.cancelled is True
    assert startup.mythic_container.RabbitmqConnection.conn is None
    assert startup.mythic_container.RabbitmqConnection.futures == {}


@pytest.mark.asyncio
async def test_recover_service_resets_connection_and_resyncs(monkeypatch):
    startup = load_startup_module()
    calls = []

    async def fake_cancel():
        calls.append("cancel")

    async def fake_reset():
        calls.append("reset")

    async def fake_ready(profile_name, *, retries, retry_delay):
        calls.append(("ready", profile_name, retries, retry_delay))

    monkeypatch.setattr(startup, "cancel_queue_tasks", fake_cancel)
    monkeypatch.setattr(startup, "reset_rabbitmq_connection", fake_reset)
    monkeypatch.setattr(startup, "ensure_service_ready", fake_ready)

    await startup.recover_service("discordx", retries=4, retry_delay=1)

    assert calls == [
        "cancel",
        "reset",
        ("ready", "discordx", 4, 1),
    ]


def test_rabbitmq_socket_stuck_in_close_wait_detects_closed_socket(tmp_path):
    startup = load_startup_module()
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    (proc_root / "net" / "tcp").write_text(
        "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
        "   0: 0100007F:9BF6 0100007F:1628 08 00000000:00000000 00:00000000 00000000 0 0 5202781 1 0000000000000000\n"
    )
    (proc_root / "net" / "tcp6").write_text(
        "  sl  local_address remote_address st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
    )

    assert startup.rabbitmq_socket_stuck_in_close_wait(proc_root=proc_root) is True


def test_monitor_close_wait_exits_when_socket_is_stuck(monkeypatch):
    startup = load_startup_module()

    monkeypatch.setattr(startup, "rabbitmq_socket_stuck_in_close_wait", lambda **kwargs: True)
    monkeypatch.setattr(startup.time, "sleep", lambda _: None)

    def fake_exit(code):
        raise SystemExit(code)

    monkeypatch.setattr(startup.os, "_exit", fake_exit)

    with pytest.raises(SystemExit, match="1"):
        startup.monitor_close_wait(interval=0)


@pytest.mark.asyncio
async def test_monitor_service_health_recovers_late_consumer_drop(monkeypatch):
    startup = load_startup_module()
    calls = []
    health_results = iter([True, False])

    async def fake_healthcheck(profile_name, *, require_running):
        calls.append(("health", profile_name, require_running))
        return next(health_results)

    async def fake_recover(profile_name, *, retries, retry_delay):
        calls.append(("recover", profile_name, retries, retry_delay))

    async def fake_sleep(delay):
        calls.append(("sleep", delay))
        if len([entry for entry in calls if entry[0] == "sleep"]) >= 3:
            raise RuntimeError("stop-monitor")

    monkeypatch.setattr(startup, "rpc_healthcheck", fake_healthcheck)
    monkeypatch.setattr(startup, "recover_service", fake_recover)
    monkeypatch.setattr(startup.asyncio, "sleep", fake_sleep)

    with pytest.raises(RuntimeError, match="stop-monitor"):
        await startup.monitor_service_health(
            "discordx",
            check_interval=5,
            retries=4,
            retry_delay=1,
            failure_threshold=1,
        )

    assert calls == [
        ("sleep", 5),
        ("health", "discordx", False),
        ("sleep", 5),
        ("health", "discordx", False),
        ("recover", "discordx", 4, 1),
        ("sleep", 5),
    ]


def test_monitor_service_health_checks_rabbitmq_reachability_not_server_state(monkeypatch):
    startup = load_startup_module()
    calls = []

    async def fake_healthcheck(profile_name, *, require_running):
        calls.append(("health", profile_name, require_running))
        return True

    async def fake_sleep(delay):
        calls.append(("sleep", delay))
        if len([entry for entry in calls if entry[0] == "sleep"]) >= 2:
            raise RuntimeError("stop-monitor")

    monkeypatch.setattr(startup, "rpc_healthcheck", fake_healthcheck)
    monkeypatch.setattr(startup.asyncio, "sleep", fake_sleep)

    with pytest.raises(RuntimeError, match="stop-monitor"):
        asyncio.run(startup.monitor_service_health("discordx", check_interval=5))

    assert calls == [
        ("sleep", 5),
        ("health", "discordx", False),
        ("sleep", 5),
    ]


@pytest.mark.asyncio
async def test_monitor_service_health_waits_for_consecutive_failures(monkeypatch):
    startup = load_startup_module()
    calls = []
    health_results = iter([True, False, False, False])

    async def fake_healthcheck(profile_name, *, require_running):
        calls.append(("health", profile_name, require_running))
        return next(health_results)

    async def fake_recover(profile_name, *, retries, retry_delay):
        calls.append(("recover", profile_name, retries, retry_delay))

    async def fake_sleep(delay):
        calls.append(("sleep", delay))
        if len([entry for entry in calls if entry[0] == "sleep"]) >= 5:
            raise RuntimeError("stop-monitor")

    monkeypatch.setattr(startup, "rpc_healthcheck", fake_healthcheck)
    monkeypatch.setattr(startup, "recover_service", fake_recover)
    monkeypatch.setattr(startup.asyncio, "sleep", fake_sleep)

    with pytest.raises(RuntimeError, match="stop-monitor"):
        await startup.monitor_service_health(
            "discordx",
            check_interval=5,
            retries=4,
            retry_delay=1,
            failure_threshold=3,
        )

    assert calls == [
        ("sleep", 5),
        ("health", "discordx", False),
        ("sleep", 5),
        ("health", "discordx", False),
        ("sleep", 5),
        ("health", "discordx", False),
        ("sleep", 5),
        ("health", "discordx", False),
        ("recover", "discordx", 4, 1),
        ("sleep", 5),
    ]


@pytest.mark.asyncio
async def test_patch_async_c2_server_handlers_serializes_without_sync_mutex():
    startup = load_startup_module()
    calls = []

    class ExplodingMutex:
        def __enter__(self):
            raise AssertionError("sync mutex should be bypassed")

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_module = SimpleNamespace()
    fake_module.c2Mutex = ExplodingMutex()

    async def fake_start_server(msg):
        with fake_module.c2Mutex:
            calls.append(("start_enter", msg))
            await asyncio.sleep(0)
            calls.append(("start_exit", msg))
        return msg

    async def fake_stop_server(msg):
        with fake_module.c2Mutex:
            calls.append(("stop_enter", msg))
            await asyncio.sleep(0)
            calls.append(("stop_exit", msg))
        return msg

    fake_module.startServer = fake_start_server
    fake_module.stopServer = fake_stop_server

    startup.patch_async_c2_server_handlers(fake_module)

    first = asyncio.create_task(fake_module.startServer(b"one"))
    await asyncio.sleep(0)
    second = asyncio.create_task(fake_module.startServer(b"two"))
    assert await first == b"one"
    assert await second == b"two"

    assert calls == [
        ("start_enter", b"one"),
        ("start_exit", b"one"),
        ("start_enter", b"two"),
        ("start_exit", b"two"),
    ]

    calls.clear()
    assert await fake_module.stopServer(b"three") == b"three"
    assert calls == [
        ("stop_enter", b"three"),
        ("stop_exit", b"three"),
    ]


def test_start_service_skips_close_wait_monitor_and_starts_health_monitor(monkeypatch):
    startup = load_startup_module()
    calls = []

    async def fake_start_services():
        calls.append("start_services")

    async def fake_ensure_service_ready():
        calls.append("ensure_service_ready")

    class FakeTask:
        def add_done_callback(self, callback):
            calls.append(("add_done_callback", callback.__name__))

    class FakeLoop:
        def run_until_complete(self, coro):
            try:
                coro.send(None)
            except StopIteration:
                pass

        def create_task(self, coro):
            calls.append(("create_task", getattr(coro, "__name__", type(coro).__name__)))
            coro.close()
            return FakeTask()

        def run_forever(self):
            calls.append("run_forever")

    monkeypatch.setattr(startup, "get_mythic_service", lambda: SimpleNamespace(start_services=fake_start_services))
    monkeypatch.setattr(startup, "ensure_service_ready", fake_ensure_service_ready)
    monkeypatch.setattr(startup, "patch_async_c2_server_handlers", lambda: calls.append("patch_async_c2_server_handlers"))
    monkeypatch.setattr(startup, "bootstrap_runtime_config_from_env", lambda: calls.append("bootstrap_runtime_config_from_env"))
    monkeypatch.setattr(startup.asyncio, "get_event_loop", lambda: FakeLoop())
    monkeypatch.setattr(
        startup,
        "start_close_wait_monitor",
        lambda *args, **kwargs: calls.append("start_close_wait_monitor"),
    )

    startup.start_service()

    assert calls == [
        "patch_async_c2_server_handlers",
        "bootstrap_runtime_config_from_env",
        "start_services",
        "ensure_service_ready",
        ("create_task", "monitor_service_health"),
        ("add_done_callback", "_crash_on_background_failure"),
        "run_forever",
    ]
