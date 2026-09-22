"""设备凭据校验（本地演示：注册时确定性派生根密钥）。"""

from __future__ import annotations

import hashlib
import hmac

from gateway.errors import raise_bad_credential, raise_not_found
from gateway.models import Device, Store

_SERVER_SALT = "iot-gateway-local-demo-v1"


def register_token(store: Store, device_id: str) -> str:
    """注册时派生设备密钥（本地演示用确定性派生，便于脚本复现）。"""
    digest = hashlib.sha256(f"{_SERVER_SALT}:{device_id}".encode())
    return digest.hexdigest()


def verify_secret(device: Device | None, secret: str) -> Device:
    """恒定时间比较；未知设备与凭据无效给出可区分原因。

    注意：先判存在性（DEVICE_NOT_FOUND），再判密钥（INVALID_CREDENTIAL），
    两类失败调用方必须能区分，因此这里不做“统一返回认证失败”的混淆。
    """
    if device is None:
        raise_not_found("<unknown>")
    assert device is not None
    if not isinstance(secret, str) or not hmac.compare_digest(device.secret, secret):
        raise_bad_credential(device.device_id)
    return device


def authenticate(store: Store, device_id: str, secret: str) -> Device:
    """查设备并校验密钥（调用方无需先判存在性）。"""
    device = store.devices.get(device_id)
    return verify_secret(device, secret)
