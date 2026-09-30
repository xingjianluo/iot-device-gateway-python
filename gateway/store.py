"""Thread-safe in-memory store with optional JSON file persistence.

Concurrency model: one global RLock guards the maps; per-device locks
serialize lifecycle operations (telemetry ingest, command transitions)
for a single device so concurrent operations on the same device cannot
overwrite each other, while different devices proceed in parallel.
"""
from __future__ import annotations

import json
import threading
from collections import OrderedDict
from pathlib import Path

from .models import (
    AttemptResult,
    Command,
    CommandStatus,
    Device,
    DeviceStatus,
    TelemetryRecord,
)


class Store:
    def __init__(self, persist_path: str | Path | None = None) -> None:
        self._lock = threading.RLock()
        self._device_locks: dict[str, threading.RLock] = {}
        self.devices: dict[str, Device] = {}
        # device_id -> OrderedDict[message_id, TelemetryRecord] (insertion order)
        self.telemetry: dict[str, OrderedDict[str, TelemetryRecord]] = {}
        # device_id -> OrderedDict[command_id, Command] (FIFO order)
        self.commands: dict[str, OrderedDict[str, Command]] = {}
        self.persist_path = Path(persist_path) if persist_path else None
        if self.persist_path and self.persist_path.exists():
            self._load()

    # -- locking ---------------------------------------------------------
    def device_lock(self, device_id: str) -> threading.RLock:
        with self._lock:
            return self._device_locks.setdefault(device_id, threading.RLock())

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # -- devices ---------------------------------------------------------
    def add_device(self, device: Device) -> None:
        with self._lock:
            self.devices[device.device_id] = device
            self.telemetry.setdefault(device.device_id, OrderedDict())
            self.commands.setdefault(device.device_id, OrderedDict())
            self._persist()

    def get_device(self, device_id: str) -> Device | None:
        with self._lock:
            return self.devices.get(device_id)

    # -- telemetry -------------------------------------------------------
    def has_telemetry(self, device_id: str, message_id: str) -> bool:
        with self._lock:
            return message_id in self.telemetry.get(device_id, {})

    def add_telemetry(self, record: TelemetryRecord) -> None:
        with self._lock:
            self.telemetry.setdefault(record.device_id, OrderedDict())[
                record.message_id
            ] = record
            self._persist()

    def telemetry_count(self, device_id: str) -> int:
        with self._lock:
            return len(self.telemetry.get(device_id, {}))

    # -- commands --------------------------------------------------------
    def add_command(self, command: Command) -> None:
        with self._lock:
            self.commands.setdefault(command.device_id, OrderedDict())[
                command.command_id
            ] = command
            self._persist()

    def get_command(self, device_id: str, command_id: str) -> Command | None:
        with self._lock:
            return self.commands.get(device_id, {}).get(command_id)

    def queued_commands(self, device_id: str) -> list[Command]:
        """Commands awaiting (re)delivery, in FIFO order."""
        with self._lock:
            return [
                c
                for c in self.commands.get(device_id, {}).values()
                if c.status == CommandStatus.QUEUED
            ]

    def queue_size(self, device_id: str) -> int:
        """Number of QUEUED commands currently occupying the offline queue."""
        with self._lock:
            return sum(
                1
                for c in self.commands.get(device_id, {}).values()
                if c.status == CommandStatus.QUEUED
            )

    def requeue_command(self, command: Command) -> None:
        """Move an existing command back to the tail of the FIFO queue.

        Used for failure-driven retries: the same command_id keeps one slot
        and one ordered position, so it can neither be duplicated nor cover
        any other pending command.
        """
        with self._lock:
            bucket = self.commands.setdefault(command.device_id, OrderedDict())
            bucket[command.command_id] = command
            bucket.move_to_end(command.command_id)
            self._persist()

    # -- persistence -----------------------------------------------------
    def _persist(self) -> None:
        if not self.persist_path:
            return
        data = {
            "devices": {
                d.device_id: {
                    "secret": d.secret,
                    "status": d.status.value,
                    "protocol_version": d.protocol_version,
                    "last_seen": d.last_seen,
                    "max_retries": d.max_retries,
                    "queue_capacity": d.queue_capacity,
                }
                for d in self.devices.values()
            },
            "telemetry": {
                dev: [
                    {
                        "message_id": r.message_id,
                        "timestamp": r.timestamp,
                        "payload": r.payload,
                        "seq": r.seq,
                        "received_at": r.received_at,
                    }
                    for r in recs.values()
                ]
                for dev, recs in self.telemetry.items()
            },
            "commands": {
                dev: [
                    {
                        "command_id": c.command_id,
                        "name": c.name,
                        "params": c.params,
                        "status": c.status.value,
                        "created_at": c.created_at,
                        "sent_at": c.sent_at,
                        "ack_deadline": c.ack_deadline,
                        "ack_count": c.ack_count,
                        "attempt": c.attempt,
                        "result_count": c.result_count,
                        "results": [
                            {
                                "attempt": r.attempt,
                                "success": r.success,
                                "duration_ms": r.duration_ms,
                                "error_code": r.error_code,
                                "reported_at": r.reported_at,
                            }
                            for r in c.results
                        ],
                    }
                    for c in cmds.values()
                ]
                for dev, cmds in self.commands.items()
            },
        }
        tmp = self.persist_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=1))
        tmp.replace(self.persist_path)

    def _load(self) -> None:
        data = json.loads(self.persist_path.read_text())
        for device_id, d in data.get("devices", {}).items():
            self.devices[device_id] = Device(
                device_id=device_id,
                secret=d["secret"],
                status=DeviceStatus(d["status"]),
                protocol_version=d.get("protocol_version", "1.0"),
                last_seen=d.get("last_seen", 0.0),
                max_retries=d.get("max_retries", 0),
                queue_capacity=d.get("queue_capacity"),
            )
        for device_id, recs in data.get("telemetry", {}).items():
            bucket = self.telemetry.setdefault(device_id, OrderedDict())
            for r in recs:
                bucket[r["message_id"]] = TelemetryRecord(
                    device_id=device_id,
                    message_id=r["message_id"],
                    timestamp=r["timestamp"],
                    payload=r["payload"],
                    seq=r.get("seq", 0),
                    received_at=r.get("received_at", 0.0),
                )
        for device_id, cmds in data.get("commands", {}).items():
            bucket = self.commands.setdefault(device_id, OrderedDict())
            for c in cmds:
                bucket[c["command_id"]] = Command(
                    command_id=c["command_id"],
                    device_id=device_id,
                    name=c["name"],
                    params=c.get("params", {}),
                    status=CommandStatus(c["status"]),
                    created_at=c.get("created_at", 0.0),
                    sent_at=c.get("sent_at"),
                    ack_deadline=c.get("ack_deadline"),
                    ack_count=c.get("ack_count", 0),
                    attempt=c.get("attempt", 0),
                    result_count=c.get("result_count", 0),
                    results=[
                        AttemptResult(
                            attempt=r["attempt"],
                            success=r["success"],
                            duration_ms=r["duration_ms"],
                            error_code=r.get("error_code"),
                            reported_at=r.get("reported_at", 0.0),
                        )
                        for r in c.get("results", [])
                    ],
                )
