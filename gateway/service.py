"""Gateway facade wiring registry, telemetry and command services."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .commands import CommandService
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
from .telemetry import TelemetryService


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
    def register(
        self,
        device_id: str,
        secret: str,
        protocol_version: str = "1.0",
        max_retries: int = 0,
        queue_capacity: int | None = None,
    ) -> Device:
        return self.devices.register(
            device_id,
            secret,
            protocol_version,
            max_retries=max_retries,
            queue_capacity=queue_capacity,
        )

    def connect(self, device_id: str) -> list[Command]:
        """Mark device online and flush its offline queue, FIFO."""
        self.devices.connect(device_id)
        return self.commands.flush(device_id)

    def disconnect(self, device_id: str) -> None:
        self.devices.disconnect(device_id)

    def flush(self, device_id: str) -> list[Command]:
        """Redeliver the device's QUEUED commands in FIFO order."""
        return self.commands.flush(device_id)

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

    def report_result(
        self,
        device_id: str,
        secret: str,
        command_id: str,
        success: bool,
        duration_ms: int,
        error_code: str | None = None,
    ):
        return self.commands.report_result(
            device_id, secret, command_id, success, duration_ms, error_code
        )

    def queue_status(self, device_id: str):
        return self.commands.queue_status(device_id)

    def configure_policy(
        self,
        device_id: str,
        max_retries: int | None = None,
        queue_capacity: int | None = None,
    ):
        return self.devices.configure_policy(
            device_id, max_retries, queue_capacity
        )

    def expire(self, device_id: str | None = None) -> list[Command]:
        return self.commands.expire(device_id)
