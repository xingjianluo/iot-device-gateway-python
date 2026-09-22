"""Core type definitions: devices, telemetry, commands, reject reasons."""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any, Callable


class DeviceStatus(str, enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class CommandStatus(str, enum.Enum):
    QUEUED = "queued"        # waiting: device offline or not yet delivered
    SENT = "sent"            # delivered to device, awaiting ack
    ACKED = "acked"          # device acknowledged (terminal)
    TIMED_OUT = "timed_out"  # ack deadline passed (terminal)
    CANCELLED = "cancelled"  # cancelled before ack (terminal)


TERMINAL_COMMAND_STATES = frozenset(
    {CommandStatus.ACKED, CommandStatus.TIMED_OUT, CommandStatus.CANCELLED}
)


class RejectReason(str, enum.Enum):
    UNKNOWN_DEVICE = "unknown_device"
    INVALID_CREDENTIALS = "invalid_credentials"
    TIMESTAMP_OUT_OF_RANGE = "timestamp_out_of_range"
    DUPLICATE_TELEMETRY = "duplicate_telemetry"
    UNSUPPORTED_VERSION = "unsupported_version"
    UNKNOWN_COMMAND = "unknown_command"
    INVALID_STATE = "invalid_state"


@dataclass
class Device:
    device_id: str
    secret: str
    status: DeviceStatus = DeviceStatus.OFFLINE
    protocol_version: str = "1.0"
    last_seen: float = 0.0


@dataclass
class TelemetryRecord:
    device_id: str
    message_id: str
    timestamp: float
    payload: dict[str, Any]
    seq: int = 0
    received_at: float = 0.0


@dataclass
class Command:
    command_id: str
    device_id: str
    name: str
    params: dict[str, Any] = field(default_factory=dict)
    status: CommandStatus = CommandStatus.QUEUED
    created_at: float = 0.0
    sent_at: float | None = None
    ack_deadline: float | None = None
    ack_count: int = 0


@dataclass
class IngestResult:
    accepted: bool
    reason: RejectReason | None = None
    duplicate: bool = False


@dataclass
class AckResult:
    accepted: bool
    reason: RejectReason | None = None
    duplicate: bool = False


Clock = Callable[[], float]


def system_clock() -> float:
    return time.time()
