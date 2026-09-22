"""错误码与可区分的业务异常。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """所有失败原因的稳定编码（不允许调用方靠文案判断）。"""

    DEVICE_NOT_FOUND = "DEVICE_NOT_FOUND"
    DUPLICATE_DEVICE = "DUPLICATE_DEVICE"
    INVALID_CREDENTIAL = "INVALID_CREDENTIAL"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    DEVICE_OFFLINE = "DEVICE_OFFLINE"
    COMMAND_NOT_FOUND = "COMMAND_NOT_FOUND"
    INVALID_COMMAND_TRANSITION = "INVALID_COMMAND_TRANSITION"
    TELEMETRY_DUPLICATE = "TELEMETRY_DUPLICATE"
    TIMESTAMP_OUT_OF_RANGE = "TIMESTAMP_OUT_OF_RANGE"
    MISSING_FIELD = "MISSING_FIELD"


class GatewayError(Exception):
    """带稳定错误码的业务异常。"""

    def __init__(self, code: ErrorCode, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"[{code.value}] {message}")


def raise_not_found(device_id: str) -> None:
    raise GatewayError(
        ErrorCode.DEVICE_NOT_FOUND,
        f"device not registered: {device_id!r}",
    )


def raise_bad_credential(device_id: str) -> None:
    raise GatewayError(
        ErrorCode.INVALID_CREDENTIAL,
        f"invalid secret for device: {device_id!r}",
    )
