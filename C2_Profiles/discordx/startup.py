import asyncio
from contextlib import nullcontext
import inspect
import json
import logging
import os
from pathlib import Path
import subprocess
import threading
import time

import mythic_container


PROFILE_NAME = "discordx"
RESYNC_ROUTING_KEY = getattr(mythic_container, "C2_RPC_RESYNC_ROUTING_KEY", "c2_rpc_resync")
DEBUG_OUTPUT_ROUTING_KEY = "c2_rpc_get_server_debug_output"
HEALTHCHECK_TIMEOUT = 5.0
STARTUP_GRACE_PERIOD = 15.0
HEALTHCHECK_POLL_INTERVAL = 1.0
CLOSE_WAIT_STATE = "08"
MYTHIC_ROOT = Path("/Mythic")
RUNTIME_CONFIG_PATH = Path("/Mythic/discordx/c2_code/config.json")
logger = logging.getLogger(__name__)
ASYNC_SERVER_PATCH_FLAG = "_discord_async_server_handlers_patched"


def get_mythic_service():
    import mythic_container.mythic_service as mythic_service

    return mythic_service


async def publish_running_status(
    profile_name: str,
    running: bool,
    error: str = "",
) -> None:
    try:
        from mythic_container.MythicGoRPC.send_mythic_rpc_c2_update_status import (
            MythicRPCC2UpdateStatusMessage,
            SendMythicRPCC2UpdateStatus,
        )

        await SendMythicRPCC2UpdateStatus(
            MythicRPCC2UpdateStatusMessage(
                C2Profile=profile_name,
                InternalServerRunning=running,
                Error=error or None,
            )
        )
    except Exception:
        logger.exception(
            "Failed to publish C2 running status",
            extra={"profile_name": profile_name, "running": running},
        )


def patch_async_c2_server_handlers(c2_utils_module=None):
    if c2_utils_module is None:
        import mythic_container.c2_utils as c2_utils_module

    if getattr(c2_utils_module, ASYNC_SERVER_PATCH_FLAG, False):
        return c2_utils_module

    server_rpc_lock = asyncio.Lock()
    original_start_server = c2_utils_module.startServer
    original_stop_server = c2_utils_module.stopServer

    async def _run_with_async_lock(handler, msg: bytes) -> bytes:
        async with server_rpc_lock:
            original_mutex = c2_utils_module.c2Mutex
            c2_utils_module.c2Mutex = nullcontext()
            try:
                return await handler(msg)
            finally:
                c2_utils_module.c2Mutex = original_mutex

    async def start_server(msg: bytes) -> bytes:
        return await _run_with_async_lock(original_start_server, msg)

    async def stop_server(msg: bytes) -> bytes:
        return await _run_with_async_lock(original_stop_server, msg)

    start_server.__name__ = getattr(original_start_server, "__name__", "startServer")
    stop_server.__name__ = getattr(original_stop_server, "__name__", "stopServer")
    c2_utils_module.startServer = start_server
    c2_utils_module.stopServer = stop_server
    setattr(c2_utils_module, ASYNC_SERVER_PATCH_FLAG, True)
    return c2_utils_module


def restore_bind_mount_ownership(root: Path = MYTHIC_ROOT) -> None:
    subprocess.run(
        ["chown", "-R", f"--reference={root}", str(root)],
        check=True,
    )


def build_server_binary() -> None:
    try:
        subprocess.run(
            ["dotnet", "publish", "-c", "Release", "-o", "/Mythic/discordx/c2_code/"],
            cwd="/Mythic/discordx/c2_code/src/discordx",
            check=True,
        )
    finally:
        restore_bind_mount_ownership(MYTHIC_ROOT)


def runtime_config_is_valid(config: dict[str, object] | None) -> bool:
    if not isinstance(config, dict):
        return False
    bot_token = str(config.get("botToken", "")).strip()
    channel_id = str(config.get("channelID", "")).strip()
    return bool(bot_token) and channel_id.isdigit()


def bootstrap_runtime_config_from_env(config_path: Path = RUNTIME_CONFIG_PATH) -> bool:
    existing_config = None
    if config_path.exists():
        try:
            existing_config = json.loads(config_path.read_text())
        except json.JSONDecodeError:
            logger.warning("Discord runtime config is invalid JSON; attempting env bootstrap")
    if runtime_config_is_valid(existing_config):
        return False

    bot_token = os.environ.get("BOT_TOKEN", "").strip()
    channel_id = os.environ.get("CHANNEL_ID", "").strip()
    if not bot_token or not channel_id.isdigit():
        logger.warning(
            "Discord runtime config is unavailable and env bootstrap is incomplete",
            extra={
                "has_bot_token": bool(bot_token),
                "has_numeric_channel_id": channel_id.isdigit(),
                "config_path": str(config_path),
            },
        )
        return False

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                "botToken": bot_token,
                "channelID": channel_id,
            },
            indent=2,
        )
        + "\n"
    )
    logger.info("Bootstrapped Discord runtime config from environment", extra={"config_path": str(config_path)})
    return True


async def rpc_health_status(
    profile_name: str = PROFILE_NAME,
    *,
    timeout: float = HEALTHCHECK_TIMEOUT,
) -> dict[str, object]:
    try:
        response = await asyncio.wait_for(
            mythic_container.RabbitmqConnection.SendRPCDictMessage(
                queue=f"{profile_name}_{DEBUG_OUTPUT_ROUTING_KEY}",
                body={"c2_profile_name": profile_name, "message": ""},
            ),
            timeout=timeout,
        )
    except Exception:
        logger.exception("RabbitMQ healthcheck raised an exception", extra={"profile_name": profile_name})
        return {"reachable": False, "server_running": False, "response": None}
    # A response proves that the RabbitMQ RPC path is alive.  The debug-RPC
    # success field describes the C2 server operation, not transport
    # reachability; using it for recovery repeatedly cancels a healthy push
    # stream whenever debug output is unavailable.
    reachable = isinstance(response, dict)
    server_running = reachable and response.get("success") is True and response.get("server_running") is True
    if reachable:
        output = response.get("message", "")
        if isinstance(output, str):
            received = output.count("[MythicClient] Received outbound Mythic message")
            delivered = output.count("[MythicClient] Delivered outbound Mythic message to Discord")
            if received or delivered:
                # The profile runtime records C2 child-process stdout at debug
                # level, so keep this intentionally content-free counter at a
                # visible level while diagnosing a live handoff failure.
                logger.warning(
                    "Discord C2 outbound delivery activity received=%d delivered=%d",
                    received,
                    delivered,
                    extra={"profile_name": profile_name},
                )
    if not reachable:
        logger.warning(
            "RabbitMQ healthcheck returned an unhealthy response",
            extra={"profile_name": profile_name, "response": response},
        )
    return {
        "reachable": reachable,
        "server_running": server_running,
        "response": response,
    }


async def rpc_healthcheck(
    profile_name: str = PROFILE_NAME,
    *,
    timeout: float = HEALTHCHECK_TIMEOUT,
    require_running: bool = True,
) -> bool:
    status = await rpc_health_status(profile_name, timeout=timeout)
    if require_running:
        return bool(status["server_running"])
    return bool(status["reachable"])


async def wait_for_rpc_state(
    profile_name: str = PROFILE_NAME,
    *,
    require_running: bool = True,
    wait_timeout: float = STARTUP_GRACE_PERIOD,
    poll_interval: float = HEALTHCHECK_POLL_INTERVAL,
) -> dict[str, object]:
    deadline = time.monotonic() + wait_timeout
    status = {"reachable": False, "server_running": False, "response": None}
    while True:
        status = await rpc_health_status(profile_name)
        if status["reachable"] and (status["server_running"] or not require_running):
            return status
        if time.monotonic() >= deadline:
            return status
        await asyncio.sleep(poll_interval)


async def reset_rabbitmq_connection() -> None:
    rabbitmq = mythic_container.RabbitmqConnection
    connection = getattr(rabbitmq, "conn", None)
    close = getattr(connection, "close", None)
    if callable(close):
        try:
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception("Failed to close stale RabbitMQ connection")
    futures = getattr(rabbitmq, "futures", None)
    if isinstance(futures, dict):
        for future in list(futures.values()):
            done = getattr(future, "done", None)
            cancel = getattr(future, "cancel", None)
            if callable(done) and callable(cancel) and not done():
                cancel()
        futures.clear()
    if hasattr(rabbitmq, "conn"):
        rabbitmq.conn = None


async def cancel_queue_tasks() -> None:
    mythic_service = get_mythic_service()
    queue_tasks = list(getattr(mythic_service, "payloadQueueTasks", []))
    for task in queue_tasks:
        if isinstance(task, asyncio.Task) and not task.done():
            task.cancel()
    if queue_tasks:
        await asyncio.gather(*queue_tasks, return_exceptions=True)
    if hasattr(mythic_service, "payloadQueueTasks"):
        mythic_service.payloadQueueTasks.clear()


async def ensure_service_ready(
    profile_name: str = PROFILE_NAME,
    *,
    retries: int = 3,
    retry_delay: float = 2.0,
    startup_grace_period: float = STARTUP_GRACE_PERIOD,
    poll_interval: float = HEALTHCHECK_POLL_INTERVAL,
) -> None:
    status = await wait_for_rpc_state(
        profile_name,
        require_running=True,
        wait_timeout=startup_grace_period,
        poll_interval=poll_interval,
    )
    c2profile = mythic_container.C2ProfileBase.c2Profiles.get(profile_name)
    if c2profile is None:
        raise RuntimeError(f"{profile_name} startup failed: c2 profile not registered")

    if status["server_running"]:
        await get_mythic_service().syncC2ProfileData(c2profile)
        await publish_running_status(profile_name, True)
        return

    for _ in range(retries):
        mythic_service = get_mythic_service()
        if not status["reachable"]:
            await cancel_queue_tasks()
            await mythic_service.startC2RabbitMQ(c2profile)
        await mythic_service.syncC2ProfileData(c2profile)
        status = await wait_for_rpc_state(
            profile_name,
            require_running=True,
            wait_timeout=startup_grace_period,
            poll_interval=poll_interval,
        )
        if status["server_running"]:
            await publish_running_status(profile_name, True)
            return
        await asyncio.sleep(retry_delay)

    raise RuntimeError(f"{profile_name} startup failed: rpc queues never became healthy")


async def recover_service(
    profile_name: str = PROFILE_NAME,
    *,
    retries: int = 3,
    retry_delay: float = 2.0,
) -> None:
    logger.warning("RabbitMQ healthcheck failed, resetting connection", extra={"profile_name": profile_name})
    await cancel_queue_tasks()
    await reset_rabbitmq_connection()
    await ensure_service_ready(profile_name, retries=retries, retry_delay=retry_delay)


def rabbitmq_socket_stuck_in_close_wait(
    *,
    proc_root: Path = Path("/proc/self"),
    rabbitmq_port: int = 5672,
) -> bool:
    remote_suffix = f":{rabbitmq_port:04X}"
    for net_path in (proc_root / "net" / "tcp", proc_root / "net" / "tcp6"):
        if not net_path.exists():
            continue
        for line in net_path.read_text().splitlines()[1:]:
            cols = line.split()
            if len(cols) > 3 and cols[2].upper().endswith(remote_suffix) and cols[3] == CLOSE_WAIT_STATE:
                return True
    return False


def monitor_close_wait(
    *,
    interval: float = 15.0,
    proc_root: Path = Path("/proc/self"),
    rabbitmq_port: int = 5672,
) -> None:
    while True:
        time.sleep(interval)
        if not rabbitmq_socket_stuck_in_close_wait(proc_root=proc_root, rabbitmq_port=rabbitmq_port):
            continue
        logger.error("Detected RabbitMQ socket stuck in CLOSE_WAIT; exiting for container restart")
        os._exit(1)


def start_close_wait_monitor(*, interval: float = 15.0) -> threading.Thread:
    monitor_thread = threading.Thread(
        target=monitor_close_wait,
        kwargs={"interval": interval},
        name="discordx-rabbitmq-closewait-monitor",
        daemon=True,
    )
    monitor_thread.start()
    return monitor_thread


async def monitor_service_health(
    profile_name: str = PROFILE_NAME,
    *,
    check_interval: float = 15.0,
    retries: int = 3,
    retry_delay: float = 2.0,
    failure_threshold: int = 3,
) -> None:
    consecutive_failures = 0
    while True:
        await asyncio.sleep(check_interval)
        # This monitor repairs the container's RabbitMQ RPC client.  The
        # server_running field is a lifecycle signal, not an RPC-connectivity
        # signal; treating it as unhealthy tears down a live Discord task
        # delivery stream during ordinary server status transitions.
        if await rpc_healthcheck(profile_name, require_running=False):
            if consecutive_failures > 0:
                logger.info(
                    "RabbitMQ healthcheck recovered",
                    extra={"profile_name": profile_name, "consecutive_failures": consecutive_failures},
                )
            consecutive_failures = 0
            continue
        consecutive_failures += 1
        logger.warning(
            "RabbitMQ healthcheck failed",
            extra={
                "profile_name": profile_name,
                "consecutive_failures": consecutive_failures,
                "failure_threshold": failure_threshold,
            },
        )
        if consecutive_failures < failure_threshold:
            continue
        await recover_service(profile_name, retries=retries, retry_delay=retry_delay)
        consecutive_failures = 0


def _crash_on_background_failure(task: asyncio.Task) -> None:
    try:
        exception = task.exception()
    except asyncio.CancelledError:
        return

    if exception is None:
        return

    logger.error(
        "Discord startup background task failed",
        exc_info=(type(exception), exception, exception.__traceback__),
    )
    os._exit(1)


def start_service() -> None:
    mythic_service = get_mythic_service()
    patch_async_c2_server_handlers()
    bootstrap_runtime_config_from_env()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mythic_service.start_services())
    loop.run_until_complete(ensure_service_ready())
    health_monitor = loop.create_task(monitor_service_health())
    health_monitor.add_done_callback(_crash_on_background_failure)
    loop.run_forever()
