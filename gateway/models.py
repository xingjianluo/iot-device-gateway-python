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
    QUEUED = "queued"  # waiting: device offline or requeued for retry
    SENT = "sent"  # delivered to device, awaiting ack
    ACKED = "acked"  # device acked: terminal for v1, awaiting result for v2
    SUCCEEDED = "succeeded"  # execution succeeded (terminal, v2)
    FAILED = "failed"  # execution failed with retries exhausted (terminal, v2)
    TIMED_OUT = "timed_out"  # ack deadline passed (terminal)
    CANCELLED = "cancelled"  # cancelled before completion (terminal)


# States whose conclusion can never change afterwards.
FINAL_COMMAND_STATES = frozenset(
    {
        CommandStatus.SUCCEEDED,
        CommandStatus.FAILED,
        CommandStatus.TIMED_OUT,
        CommandStatus.CANCELLED,
    }
)

# States from which no further lifecycle transition is legal. ACKED is
# included because v1 devices settle there; for v2 devices ACKED is an
# intermediate "executing" state that only accepts result reports.
TERMINAL_COMMAND_STATES = frozenset({CommandStatus.ACKED}) | FINAL_COMMAND_STATES


class RejectReason(str, enum.Enum):
    UNKNOWN_DEVICE = "unknown_device"
    INVALID_CREDENTIALS = "invalid_credentials"
    TIMESTAMP_OUT_OF_RANGE = "timestamp_out_of_range"
    DUPLICATE_TELEMETRY = "duplicate_telemetry"
    UNSUPPORTED_VERSION = "unsupported_version"
    UNKNOWN_COMMAND = "unknown_command"
    INVALID_STATE = "invalid_state"
    # command execution result reporting
    RESULT_NOT_ACKED = "result_not_acked"  # result before any ack
    RESULT_AFTER_TIMEOUT = "result_after_timeout"  # command already timed out
    RESULT_AFTER_CANCELLED = "result_after_cancelled"  # command already cancelled
    INVALID_RESULT = "invalid_result"  # malformed result payload
    # offline queue capacity
    QUEUE_FULL = "queue_full"


@dataclass
class Device:
    device_id: str
    secret: str
    status: DeviceStatus = DeviceStatus.OFFLINE
    protocol_version: str = "1.0"
    last_seen: float = 0.0
    # Per-device policies.
    max_queue_size: int = 100  # offline queue capacity (QUEUED commands)
    max_retries: int = 3  # failed-execution retry cap, v2 devices
    retry_backoff: float = 0.0  # delay before a requeued retry is eligible


@dataclass
class TelemetryRecord:
    device_id: str
    message_id: str
    timestamp: float
    payload: dict[str, Any]
    seq: int = 0
    received_at: float = 0.0


@dataclass
class ExecutionResult:
    """Outcome of one execution attempt as reported by the device."""

    success: bool
    duration: float
    error_code: str | None = None
    attempt: int = 1


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
    # Attempt lifecycle: 1 for the first delivery, incremented on each retry.
    attempt: int = 1
    # Snapshot of the policy that governs this command (fixed at dispatch).
    max_retries: int = 3
    retry_backoff: float = 0.0
    not_before: float | None = None  # earliest deliver time (retry backoff)
    # Settled execution conclusion (None until a terminal v2 result lands).
    result: ExecutionResult | None = None
    result_received_at: float | None = None
    # Last per-attempt result, used for idempotent duplicate detection:
    # a retransmission of the same (attempt, outcome) never settles twice.
    last_result: ExecutionResult | None = None
    # Ordered history of failed attempts: 1-based attempt numbers.
    failed_attempts: list[int] = field(default_factory=list)


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
    """Verdict of a device-reported execution result."""

    accepted: bool
    reason: RejectReason | None = None
    duplicate: bool = False
    retried: bool = False  # failure accepted but under cap -> requeued
    command_status: CommandStatus | None = None


@dataclass
class QueueStatus:
    """Observable offline-queue occupancy for one device."""

    device_id: str
    queued: int
    capacity: int
    in_flight: int  # SENT + ACKED (delivered, not settled)
    full: bool


Clock = Callable[[], float]


def system_clock() -> float:
    return time.time()
