"""pytest 公共夹具：每个测试独立 Store，并将网关关键判定日志打到 stdout。"""

from __future__ import annotations

import logging
import sys

import pytest

from gateway.models import Store

# 让“关键输入 + 判定依据”在单测日志中可见（pytest -s 或失败时回显）。
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s"))
_gateway_logger = logging.getLogger("gateway")
_gateway_logger.setLevel(logging.INFO)
if not any(getattr(h, "_pytest_gateway", False) for h in _gateway_logger.handlers):
    _handler._pytest_gateway = True  # type: ignore[attr-defined]
    _gateway_logger.addHandler(_handler)
_gateway_logger.propagate = False


@pytest.fixture
def store() -> Store:
    return Store()


@pytest.fixture
def registered_device(store: Store):
    """已注册 v2 设备：返回 (device_id, secret)。"""
    from gateway.devices import register_device

    device = register_device(store, "dev-1", "v2")
    return device.device_id, device.secret
