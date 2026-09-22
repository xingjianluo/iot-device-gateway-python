"""设备注册、鉴权、上下线状态机。

设备状态机：
    REGISTERED --connect--> ONLINE
    ONLINE     --disconnect--> OFFLINE
    OFFLINE    --connect(重连, 幂等)--> ONLINE
    ONLINE     --connect(重复连接, 幂等)--> ONLINE
重复注册以 DUPLICATE_DEVICE 拒绝；连接前必须通过密钥校验。
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from gateway.auth import authenticate, register_token
from gateway.errors import ErrorCode, GatewayError
from gateway.models import Device, DeviceState, Store
from gateway.versions import parse_version

if TYPE_CHECKING:
    from gateway.models import Command

logger = logging.getLogger(__name__)


def register_device(store: Store, device_id: str, protocol_version: str | None) -> Device:
    version = parse_version(protocol_version)  # 未知版本在注册时即拒绝
    with store.lock:
        if device_id in store.devices:
            raise GatewayError(
                ErrorCode.DUPLICATE_DEVICE,
                f"device already registered: {device_id!r}",
            )
        now = time.time()
        device = Device(
            device_id=device_id,
            secret=register_token(store, device_id),
            protocol_version=version,
            state=DeviceState.REGISTERED,
            registered_at=now,
        )
        store.devices[device_id] = device
        store.device_queues.setdefault(device_id, [])
        store.telemetry.setdefault(device_id, [])
        logger.info("register device=%s version=%s at=%.3f", device_id, version, now)
        return device


def connect(store: Store, device_id: str, secret: str, version: str | None) -> tuple[Device, list[Command]]:
    """鉴权 -> ONLINE。连接使用的版本必须在白名单内；重连/重复连接幂等。

    返回 (设备, 本次补发的命令列表)。重复连接且队列已空时列表为空。
    """
    from gateway.commands import drain_on_reconnect

    requested = parse_version(version)
    with store.lock:
        device = authenticate(store, device_id, secret)
        if requested != device.protocol_version:
            # 设备注册版本是其档案版本；连接版本不一致不静默改写，明确拒绝以便解释。
            raise GatewayError(
                ErrorCode.UNSUPPORTED_VERSION,
                f"device {device_id!r} registered as {device.protocol_version}, "
                f"but connected with {requested!r}",
            )
        device.state = DeviceState.ONLINE
        device.last_seen = time.time()
        device.connected_version = requested
        # 重连补发在同一把锁内完成：状态翻 ONLINE 与队列排空对外是原子的。
        delivered: list[Command] = drain_on_reconnect(store, device_id, now=device.last_seen)
        logger.info(
            "connect device=%s version=%s state=ONLINE redelivered=%d",
            device_id, requested, len(delivered),
        )
        return device, delivered


def disconnect(store: Store, device_id: str) -> Device:
    with store.lock:
        device = store.devices.get(device_id)
        if device is None:
            raise GatewayError(
                ErrorCode.DEVICE_NOT_FOUND,
                f"device not registered: {device_id!r}",
            )
        if device.state == DeviceState.ONLINE:
            device.state = DeviceState.OFFLINE
            logger.info("disconnect device=%s state=OFFLINE", device_id)
        return device


def get_device(store: Store, device_id: str) -> Device:
    with store.lock:
        device = store.devices.get(device_id)
        if device is None:
            raise GatewayError(
                ErrorCode.DEVICE_NOT_FOUND,
                f"device not registered: {device_id!r}",
            )
        return device
