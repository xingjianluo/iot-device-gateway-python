"""核心数据模型：设备 / 遥测 / 命令。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum


class DeviceState(str, Enum):
    REGISTERED = "REGISTERED"
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class CommandState(str, Enum):
    QUEUED = "QUEUED"
    SENT = "SENT"
    ACKED = "ACKED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"


@dataclass
class Device:
    device_id: str
    secret: str
    protocol_version: str
    state: DeviceState = DeviceState.REGISTERED
    registered_at: float = 0.0
    last_seen: float | None = None
    connected_version: str | None = None


@dataclass
class TelemetryRecord:
    device_id: str
    message_id: str
    ts: float
    metrics: dict
    protocol_version: str
    ingested_seq: int


@dataclass
class Command:
    command_id: str
    device_id: str
    payload: str
    state: CommandState
    created_at: float
    sent_at: float | None = None
    acked_at: float | None = None
    timeout_at: float | None = None
    closed_at: float | None = None
    delivered_version: str | None = None


@dataclass
class Store:
    """进程内状态，所有可变访问均经 Store 自带的锁串行化。"""

    devices: dict[str, Device] = field(default_factory=dict)
    telemetry: dict[str, list[TelemetryRecord]] = field(default_factory=dict)
    seen_message_ids: set[str] = field(default_factory=set)
    commands: dict[str, Command] = field(default_factory=dict)
    device_queues: dict[str, list[str]] = field(default_factory=dict)
    global_seq: int = 0
    lock: threading.RLock = field(default_factory=threading.RLock)

    def next_seq(self) -> int:
        with self.lock:
            self.global_seq += 1
            return self.global_seq
