"""Core type definitions: devices, telemetry, commands, reject reasons."""
from __future__ import annotations

import enum
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


class DeviceStatus(str, enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class CommandStatus(str, enum.Enum):
    QUEUED = "queued"              # waiting: device offline or not yet delivered
    SENT = "sent"                  # delivered to device, awaiting ack
    ACKED = "acked"                # device acknowledged receipt
    # v2.0: ACKED is non-terminal; the device then reports an execution result.
    SUCCEEDED = "succeeded"        # reported success (terminal)
    FAILED = "failed"              # reported failure, no retry policy (terminal)
    RETRIES_EXHAUSTED = "retries_exhausted"  # failed and retry cap reached (terminal)
    TIMED_OUT = "timed_out"        # ack deadline passed (terminal)
    CANCELLED = "cancelled"        # cancelled before ack (terminal)


TERMINAL_COMMAND_STATES = frozenset(
    {
        CommandStatus.SUCCEEDED,
        CommandStatus.FAILED,
        CommandStatus.RETRIES_EXHAUSTED,
        CommandStatus.TIMED_OUT,
        CommandStatus.CANCELLED,
    }
)
# ACKED is terminal only for legacy v1.0 clients that cannot report results.
TERMINAL_AFTER_ACK_VERSIONS = frozenset({"1.0"})


class RejectReason(str, enum.Enum):
    UNKNOWN_DEVICE = "unknown_device"
    INVALID_CREDENTIALS = "invalid_credentials"
    TIMESTAMP_OUT_OF_RANGE = "timestamp_out_of_range"
    DUPLICATE_TELEMETRY = "duplicate_telemetry"
    UNSUPPORTED_VERSION = "unsupported_version"
    UNKNOWN_COMMAND = "unknown_command"
    INVALID_STATE = "invalid_state"
    # command execution result reporting
    RESULT_UNSUPPORTED_VERSION = "result_unsupported_version"
    RESULT_BEFORE_ACK = "result_before_ack"
    RESULT_AFTER_TIMEOUT = "result_after_timeout"
    RESULT_AFTER_CANCEL = "result_after_cancel"
    CONFLICTING_RESULT = "conflicting_result"
    # offline queue capacity
    QUEUE_FULL = "queue_full"


@dataclass
class AttemptResult:
    """One execution-result report for one delivery attempt."""

    attempt: int
    success: bool
    duration_ms: int
    error_code: str | None = None
    reported_at: float = 0.0


@dataclass
class Device:
    device_id: str
    secret: str
    status: DeviceStatus = DeviceStatus.OFFLINE
    protocol_version: str = "1.0"
    last_seen: float = 0.0
    # per-device command policy
    max_retries: int = 0       # automatic retries after a reported failure
    queue_capacity: int | None = None  # max QUEUED commands; None = unlimited


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
    # delivery attempts: 0 while QUEUED, 1 on first delivery, n on nth retry
    attempt: int = 0
    result_count: int = 0
    # one record per reported attempt (append-only)
    results: list[AttemptResult] = field(default_factory=list)


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


@dataclass
class ResultReportResult:
    accepted: bool
    reason: RejectReason | None = None
    duplicate: bool = False
    attempt: int = 0
    terminal: bool = False
    # commands requeued for retry as a consequence of this report
    retried: bool = False


@dataclass
class QueueStatus:
    device_id: str
    queued: int
    capacity: int | None
    full: bool


Clock = Callable[[], float]


def system_clock() -> float:
    return time.time()
