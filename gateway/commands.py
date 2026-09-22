"""命令下发与离线队列。

命令状态机：
    设备在线 issue ───────────────> SENT
    设备离线 issue -> QUEUED --(重连补发, FIFO, 每条仅一次)--> SENT
    QUEUED  --cancel--> CANCELLED
    SENT    --ack------> ACKED       (终态；重复 ack 幂等返回)
    SENT    --timeout--> TIMEOUT     (终态；超过 timeout_at 自动到期)
    SENT    --cancel---> CANCELLED   (终态)
非法迁移（如对 TIMEOUT/CANCELLED 的命令再 ack，或对 QUEUED 命令 timeout）
一律 INVALID_COMMAND_TRANSITION 拒绝，绝不静默吞掉或回退状态。
所有迁移在同一把 Store 锁内完成，同一设备并发的生命周期操作不会互相覆盖。
"""

from __future__ import annotations

import logging
import time
import uuid

from gateway.errors import ErrorCode, GatewayError
from gateway.models import Command, CommandState, DeviceState, Store

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 30.0

_TERMINAL_STATES = {CommandState.ACKED, CommandState.TIMEOUT, CommandState.CANCELLED}


def _now(now: float | None) -> float:
    return time.time() if now is None else float(now)


def get_command(store: Store, command_id: str, now: float | None = None) -> Command:
    with store.lock:
        command = store.commands.get(command_id)
        if command is None:
            raise GatewayError(
                ErrorCode.COMMAND_NOT_FOUND,
                f"command not found: {command_id!r}",
            )
        # 查询同样触发确定性到期：deadline 过后任何读取都得到 TIMEOUT，结论一致。
        _expire_if_needed_locked(command, _now(now))
        return command


def _expire_if_needed_locked(command: Command, at: float) -> None:
    """SENT 且超过 deadline 的命令在此刻确定地进入 TIMEOUT。"""
    if (
        command.state == CommandState.SENT
        and command.timeout_at is not None
        and at >= command.timeout_at
    ):
        command.state = CommandState.TIMEOUT
        command.closed_at = at
        logger.info("command=%s SENT->TIMEOUT at=%.3f deadline=%.3f",
                    command.command_id, at, command.timeout_at)


def issue_command(
    store: Store,
    device_id: str,
    payload: str,
    now: float | None = None,
    ttl_seconds: float = DEFAULT_TTL_SECONDS,
) -> Command:
    at = _now(now)
    with store.lock:
        device = store.devices.get(device_id)
        if device is None:
            raise GatewayError(
                ErrorCode.DEVICE_NOT_FOUND,
                f"device not registered: {device_id!r}",
            )
        command = Command(
            command_id=uuid.uuid4().hex,
            device_id=device_id,
            payload=payload,
            state=CommandState.QUEUED,
            created_at=at,
        )
        store.commands[command.command_id] = command
        if device.state == DeviceState.ONLINE:
            command.state = CommandState.SENT
            command.sent_at = at
            command.timeout_at = at + ttl_seconds
            command.delivered_version = device.connected_version
            logger.info(
                "issue command=%s device=%s state=SENT deadline=%.3f",
                command.command_id, device_id, command.timeout_at,
            )
        else:
            store.device_queues[device_id].append(command.command_id)
            logger.info(
                "issue command=%s device=%s state=QUEUED queue_pos=%d",
                command.command_id, device_id, len(store.device_queues[device_id]),
            )
        return command


def drain_on_reconnect(store: Store, device_id: str, now: float | None = None) -> list[Command]:
    """重连补发：FIFO 取出离线队列，每条恰好转一次 SENT，不重复、不乱序。"""
    at = _now(now)
    delivered: list[Command] = []
    with store.lock:
        device = store.devices.get(device_id)
        if device is None:
            raise GatewayError(
                ErrorCode.DEVICE_NOT_FOUND,
                f"device not registered: {device_id!r}",
            )
        queue = store.device_queues.setdefault(device_id, [])
        while queue:
            command_id = queue.pop(0)
            command = store.commands[command_id]
            if command.state != CommandState.QUEUED:
                # 已被取消等：不再补发（正常情况下取消时已出队，这里是防御性保证）。
                logger.info("drain skip command=%s state=%s", command_id, command.state.value)
                continue
            command.state = CommandState.SENT
            command.sent_at = at
            command.timeout_at = at + DEFAULT_TTL_SECONDS
            command.delivered_version = device.connected_version
            delivered.append(command)
            logger.info("drain command=%s device=%s QUEUED->SENT order=%d",
                        command_id, device_id, len(delivered))
        return delivered


# 向后兼容的语义别名
reconnect_device = drain_on_reconnect


def ack_command(store: Store, command_id: str, now: float | None = None) -> Command:
    at = _now(now)
    with store.lock:
        command = get_command(store, command_id, now=at)
        _expire_if_needed_locked(command, at)
        if command.state == CommandState.ACKED:
            # 重复确认：幂等返回当前态，不重复执行、不产生副作用。
            logger.info("command=%s duplicate ACK ignored (already ACKED at=%s)",
                        command_id, command.acked_at)
            return command
        if command.state == CommandState.SENT:
            command.state = CommandState.ACKED
            command.acked_at = at
            command.closed_at = at
            logger.info("command=%s SENT->ACKED at=%.3f", command_id, at)
            return command
        raise GatewayError(
            ErrorCode.INVALID_COMMAND_TRANSITION,
            f"cannot ack command {command_id!r} in state {command.state.value}",
        )


def cancel_command(store: Store, command_id: str, now: float | None = None) -> Command:
    at = _now(now)
    with store.lock:
        command = get_command(store, command_id, now=at)
        _expire_if_needed_locked(command, at)
        if command.state in (CommandState.QUEUED, CommandState.SENT):
            command.state = CommandState.CANCELLED
            command.closed_at = at
            queue = store.device_queues.get(command.device_id)
            if queue and command.command_id in queue:
                queue.remove(command.command_id)
            logger.info("command=%s %s->CANCELLED at=%.3f",
                        command_id, command.state.value, at)
            return command
        raise GatewayError(
            ErrorCode.INVALID_COMMAND_TRANSITION,
            f"cannot cancel command {command_id!r} in state {command.state.value}",
        )


def timeout_command(store: Store, command_id: str, now: float | None = None) -> Command:
    at = _now(now)
    with store.lock:
        command = get_command(store, command_id, now=at)
        _expire_if_needed_locked(command, at)
        if command.state == CommandState.TIMEOUT:
            return command  # 幂等
        if command.state == CommandState.SENT:
            command.state = CommandState.TIMEOUT
            command.closed_at = at
            logger.info("command=%s SENT->TIMEOUT(manual) at=%.3f", command_id, at)
            return command
        raise GatewayError(
            ErrorCode.INVALID_COMMAND_TRANSITION,
            f"cannot timeout command {command_id!r} in state {command.state.value}",
        )
