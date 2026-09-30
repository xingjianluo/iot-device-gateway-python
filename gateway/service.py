"""Gateway facade wiring registry, telemetry and command services."""

from __future__ import annotations

from pathlib import Path

from .commands import CommandService
from .devices import DeviceRegistry
from .models import (
    AckResult,
    Clock,
    Command,
    Device,
    IngestResult,
    QueueStatus,
    RejectReason,
    ResultReportResult,
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
        max_queue_size: int = 100,
        max_retries: int = 3,
        retry_backoff: float = 0.0,
    ) -> Device:
        return self.devices.register(
            device_id,
            secret,
            protocol_version,
            max_queue_size=max_queue_size,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
        )

    def configure_policy(
        self,
        device_id: str,
        max_queue_size: int | None = None,
        max_retries: int | None = None,
        retry_backoff: float | None = None,
    ) -> Device:
        return self.devices.configure_policy(
            device_id,
            max_queue_size=max_queue_size,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
        )

    def connect(self, device_id: str) -> list[Command]:
        """Mark device online and flush its offline queue, FIFO."""
        self.devices.connect(device_id)
        return self.commands.flush(device_id)

    def disconnect(self, device_id: str) -> None:
        self.devices.disconnect(device_id)

    def ingest(
        self, device_id: str, secret: str, version: str, message: dict
    ) -> IngestResult:
        return self.telemetry.ingest(device_id, secret, version, message)

    def dispatch(
        self, device_id: str, name: str, params: dict | None = None
    ) -> tuple[Command | None, RejectReason | None]:
        return self.commands.dispatch(device_id, name, params)

    def ack(self, device_id: str, secret: str, command_id: str) -> AckResult:
        return self.commands.ack(device_id, secret, command_id)

    def report_result(
        self,
        device_id: str,
        secret: str,
        command_id: str,
        success: bool,
        duration: float,
        error_code: str | None = None,
        attempt: int | None = None,
    ) -> ResultReportResult:
        return self.commands.report_result(
            device_id,
            secret,
            command_id,
            success=success,
            duration=duration,
            error_code=error_code,
            attempt=attempt,
        )

    def queue_status(self, device_id: str) -> QueueStatus | None:
        return self.commands.queue_status(device_id)

    def cancel(self, device_id: str, command_id: str):
        return self.commands.cancel(device_id, command_id)

    def expire(self, device_id: str | None = None) -> list[Command]:
        return self.commands.expire(device_id)
