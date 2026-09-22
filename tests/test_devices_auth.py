"""设备注册/鉴权/状态机：失败原因可区分，重连幂等。"""

from __future__ import annotations

import logging

import pytest

from gateway.devices import connect, disconnect, register_device
from gateway.errors import ErrorCode, GatewayError
from gateway.models import DeviceState

logger = logging.getLogger(__name__)


def test_register_and_duplicate(store) -> None:
    device = register_device(store, "dev-x", "v1")
    logger.info("register input=device-x,v1 -> state=%s secret_len=%d",
                device.state.value, len(device.secret))
    assert device.state is DeviceState.REGISTERED
    with pytest.raises(GatewayError) as exc:
        register_device(store, "dev-x", "v1")
    assert exc.value.code is ErrorCode.DUPLICATE_DEVICE


def test_unknown_device_vs_bad_credential_are_distinguishable(store, registered_device) -> None:
    device_id, _secret = registered_device
    with pytest.raises(GatewayError) as exc_unknown:
        connect(store, "ghost", "whatever", "v2")
    with pytest.raises(GatewayError) as exc_bad:
        connect(store, device_id, "wrong-secret", "v2")
    logger.info("unknown input=device=ghost -> %s; bad-secret device=%s -> %s",
                exc_unknown.value.code.value, device_id, exc_bad.value.code.value)
    assert exc_unknown.value.code is ErrorCode.DEVICE_NOT_FOUND
    assert exc_bad.value.code is ErrorCode.INVALID_CREDENTIAL


def test_connect_disconnect_reconnect_idempotent(store, registered_device) -> None:
    device_id, secret = registered_device
    device, _delivered = connect(store, device_id, secret, "v2")
    assert device.state is DeviceState.ONLINE
    first_seen = device.last_seen

    # 重复连接幂等，不报错、不改变设备档案版本
    again, delivered2 = connect(store, device_id, secret, "v2")
    assert again.state is DeviceState.ONLINE
    assert again.protocol_version == "v2"
    assert delivered2 == []
    logger.info("reconnect while online -> state=%s redelivered=%d (idempotent)",
                again.state.value, len(delivered2))

    disconnect(store, device_id)
    assert store.devices[device_id].state is DeviceState.OFFLINE
    device3, _ = connect(store, device_id, secret, "v2")
    assert device3.state is DeviceState.ONLINE
    assert device3.last_seen >= first_seen


def test_connect_version_mismatch_rejected(store, registered_device) -> None:
    device_id, secret = registered_device  # registered as v2
    with pytest.raises(GatewayError) as exc:
        connect(store, device_id, secret, "v1")
    logger.info("connect input=v1 on v2-registered device -> reject %s", exc.value.code.value)
    assert exc.value.code is ErrorCode.UNSUPPORTED_VERSION
    assert store.devices[device_id].state is not DeviceState.ONLINE
