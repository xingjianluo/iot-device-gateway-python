"""Gateway facade wiring registry, telemetry and command services."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .commands import CommandService
from .telemetry import TelemetryService
from .devices import DeviceRegistry
from .models import (
    AckResult,
    Clock,
    Command,
    Device,
    IngestResult,
    RejectReason,
    system_clock,
)
from .store import Store


class Gateway:
    def __init__(
        self,
        persist_path: str | Path | None = None,
        clock: Clock = system_clock,
    ) -> None:
        self.store = Store(persist_path)
        self.clock = clock
        self.devices = DeviceRegistry(self.store, clock)
        self.telemetry = TelemetryService(self.store, self.devices, clock)
        self.commands = CommandService(self.store, self.devices, clock)

    # convenience pass-throughs ------------------------------------------
    def register(self, device_id: str, secret: str, protocol_version: str = "1.0") -> Device:
        return self.devices.register(device_id, secret, protocol_version)

    def connect(self, device_id: str) -> list[Command]:
        """Mark device online and flush its offline queue, FIFO."""
        self.devices.connect(device_id)
        return self.commands.flush(device_id)

    def disconnect(self, device_id: str) -> None:
        self.devices.disconnect(device_id)

    def ingest(
        self, device_id: str, secret: str, version: str, message: dict[str, Any]
    ) -> IngestResult:
        return self.telemetry.ingest(device_id, secret, version, message)

    def dispatch(
        self, device_id: str, name: str, params: dict[str, Any] | None = None
    ) -> tuple[Command | None, RejectReason | None]:
        return self.commands.dispatch(device_id, name, params)

    def ack(self, device_id: str, secret: str, command_id: str) -> AckResult:
        return self.commands.ack(device_id, secret, command_id)

    def cancel(self, device_id: str, command_id: str):
        return self.commands.cancel(device_id, command_id)

    def expire(self, device_id: str | None = None) -> list[Command]:
        return self.commands.expire(device_id)
