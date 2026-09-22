"""Command dispatch with offline queue and a guarded state machine.

State machine (terminal states: ACKED, TIMED_OUT, CANCELLED):

    QUEUED --deliver--> SENT --ack--> ACKED
      |                  |
      |cancel            +--expire(deadline passed)--> TIMED_OUT
      |                  |
      v                  +--cancel--> CANCELLED
    CANCELLED

Rules:
- Dispatch to an offline device leaves the command QUEUED; on reconnect,
  flush() delivers queued commands in FIFO order. A command is delivered
  at most once per SENT transition, and QUEUED->SENT happens exactly once
  per flush pass, so nothing is executed twice.
- ack() is idempotent: a repeated ack for an already-ACKED command returns
  duplicate=True without changing state; ack for TIMED_OUT/CANCELLED or
  unknown commands is rejected with a distinguishable reason.
- cancel() is only legal from QUEUED or SENT.
- expire() is driven by an injected clock (no background threads), making
  timeouts deterministic and testable.
- All transitions on one device are serialized by the per-device lock, so
  concurrent lifecycle operations cannot overwrite each other.
"""
from __future__ import annotations

import itertools
from typing import Any

from .devices import DeviceRegistry
from .models import (
    TERMINAL_COMMAND_STATES,
    AckResult,
    Clock,
    Command,
    CommandStatus,
    DeviceStatus,
    RejectReason,
    system_clock,
)
from .store import Store

DEFAULT_ACK_TIMEOUT = 30.0


class CommandService:
    def __init__(
        self,
        store: Store,
        registry: DeviceRegistry,
        clock: Clock = system_clock,
        ack_timeout: float = DEFAULT_ACK_TIMEOUT,
    ) -> None:
        self._store = store
        self._registry = registry
        self._clock = clock
        self._ack_timeout = ack_timeout
        self._seq = itertools.count(1)

    # -- dispatch --------------------------------------------------------
    def dispatch(
        self, device_id: str, name: str, params: dict[str, Any] | None = None
    ) -> tuple[Command | None, RejectReason | None]:
        if self._store.get_device(device_id) is None:
            return None, RejectReason.UNKNOWN_DEVICE
        with self._store.device_lock(device_id):
            command = Command(
                command_id=f"cmd-{next(self._seq)}",
                device_id=device_id,
                name=name,
                params=dict(params or {}),
                created_at=self._clock(),
            )
            self._store.add_command(command)
            device = self._store.get_device(device_id)
            if device is not None and device.status == DeviceStatus.ONLINE:
                self._deliver(command)
            return command, None

    def _deliver(self, command: Command) -> None:
        """QUEUED -> SENT. Caller must hold the device lock."""
        command.status = CommandStatus.SENT
        command.sent_at = self._clock()
        command.ack_deadline = command.sent_at + self._ack_timeout
        self._store._persist()

    def flush(self, device_id: str) -> list[Command]:
        """Deliver queued commands in FIFO order (call on reconnect)."""
        with self._store.device_lock(device_id):
            delivered = []
            for command in self._store.queued_commands(device_id):
                self._deliver(command)
                delivered.append(command)
            return delivered

    # -- ack -------------------------------------------------------------
    def ack(
        self, device_id: str, secret: str, command_id: str
    ) -> AckResult:
        _, reason = self._registry.authenticate(device_id, secret)
        if reason is not None:
            return AckResult(accepted=False, reason=reason)
        with self._store.device_lock(device_id):
            command = self._store.get_command(device_id, command_id)
            if command is None:
                return AckResult(
                    accepted=False, reason=RejectReason.UNKNOWN_COMMAND
                )
            if command.status == CommandStatus.ACKED:
                command.ack_count += 1  # observed, but state unchanged
                return AckResult(accepted=True, duplicate=True)
            if command.status in TERMINAL_COMMAND_STATES:
                return AckResult(
                    accepted=False, reason=RejectReason.INVALID_STATE
                )
            if command.status == CommandStatus.QUEUED:
                # ack for a command never delivered: protocol violation
                return AckResult(
                    accepted=False, reason=RejectReason.INVALID_STATE
                )
            command.status = CommandStatus.ACKED
            command.ack_count += 1
            self._store._persist()
            return AckResult(accepted=True)

    # -- cancel ----------------------------------------------------------
    def cancel(
        self, device_id: str, command_id: str
    ) -> tuple[bool, RejectReason | None]:
        with self._store.device_lock(device_id):
            command = self._store.get_command(device_id, command_id)
            if command is None:
                return False, RejectReason.UNKNOWN_COMMAND
            if command.status in TERMINAL_COMMAND_STATES:
                return False, RejectReason.INVALID_STATE
            command.status = CommandStatus.CANCELLED
            self._store._persist()
            return True, None

    # -- timeout ---------------------------------------------------------
    def expire(self, device_id: str | None = None) -> list[Command]:
        """Move SENT commands past their ack deadline to TIMED_OUT."""
        now = self._clock()
        expired: list[Command] = []
        device_ids = (
            [device_id] if device_id else list(self._store.commands.keys())
        )
        for dev_id in device_ids:
            with self._store.device_lock(dev_id):
                for command in self._store.commands.get(dev_id, {}).values():
                    if (
                        command.status == CommandStatus.SENT
                        and command.ack_deadline is not None
                        and command.ack_deadline <= now
                    ):
                        command.status = CommandStatus.TIMED_OUT
                        expired.append(command)
                self._store._persist()
        return expired
