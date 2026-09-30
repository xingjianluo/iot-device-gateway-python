"""Command dispatch with offline queue, retries and a guarded state machine.

State machine::

    QUEUED --deliver--> SENT --ack--> ACKED
      |                  |              |
      |cancel            |cancel        +--v2 report success--> SUCCEEDED  (T)
      v                  v              |
    CANCELLED (T)     CANCELLED (T)     +--v2 report failure, retries left
                                        |      -> QUEUED (tail) -> SENT ...
                                        +--v2 report failure, retries used
                                        |      -> FAILED / RETRIES_EXHAUSTED (T)
                                        +--cancel--> CANCELLED              (T)
                       SENT --expire(deadline passed)--> TIMED_OUT          (T)

    (T) = terminal. v1.0 clients cannot report results: ACKED is *their*
    terminal verdict ("delivered; outcome unknown"), never upgraded/retry.

Rules:
- Dispatch to an offline device leaves the command QUEUED; on reconnect,
  flush() delivers queued commands in FIFO order. Each QUEUED->SENT
  transition increments ``attempt`` (1 = first delivery, n = nth retry),
  so a command is executed at most once per delivery and retries are
  distinguishable from the original attempt.
- ack() is idempotent and version-aware: a repeated ack for an ACKED
  command returns duplicate=True; ack for QUEUED/terminal commands is
  rejected with INVALID_STATE. For v1.0 devices ACKED is terminal, so a
  racing cancel can no longer overwrite it; for v2.0 devices ACKED is an
  intermediate "executing" state.
- report_result() (v2.0 only) legal only from ACKED. Reports arriving out
  of order are each rejected with a distinguishable reason: before ack ->
  RESULT_BEFORE_ACK, after timeout -> RESULT_AFTER_TIMEOUT, after cancel
  -> RESULT_AFTER_CANCEL, legacy client -> RESULT_UNSUPPORTED_VERSION.
  Repeating the same (success, error_code) verdict for a terminal command
  is a duplicate and never changes the recorded conclusion; a *different*
  verdict is rejected with CONFLICTING_RESULT. (A duplicate of an older
  attempt's verdict that arrives only after the command was requeued for
  retry lands while the command is QUEUED/SENT: only the currently ACKED
  attempt accepts results, so it is reported as RESULT_BEFORE_ACK and
  changes nothing -- the earlier verdict was already recorded once.)
  Every lifecycle action is serialized by the per-device lock, so however
  ack/result/cancel/expire interleave, a command lands in exactly one
  terminal state.
- A reported failure is requeued to the FIFO tail while
  attempt <= max_retries; retries reuse the same command_id and never
  exceed the capacity admission for *new* dispatches. Once the cap is
  reached the command ends in FAILED (no retry policy) or
  RETRIES_EXHAUSTED (cap reached), both stable and distinguishable.
- New dispatches are rejected with QUEUE_FULL once a device's QUEUED
  occupancy reaches its queue_capacity; already queued commands and
  in-flight retries are never dropped.
- expire() is driven by an injected clock (no background threads), making
  timeouts deterministic and testable.
"""
from __future__ import annotations

import itertools
from typing import Any

from .devices import DeviceRegistry
from .models import (
    TERMINAL_AFTER_ACK_VERSIONS,
    AckResult,
    AttemptResult,
    Clock,
    Command,
    CommandStatus,
    Device,
    DeviceStatus,
    QueueStatus,
    RejectReason,
    ResultReportResult,
    system_clock,
)
from .protocol import supports_result_report
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
        device = self._store.get_device(device_id)
        if device is None:
            return None, RejectReason.UNKNOWN_DEVICE
        with self._store.device_lock(device_id):
            # Capacity only gates commands that would actually wait in the
            # queue: an offline device cannot take delivery now. Commands
            # dispatched while online go straight to SENT and occupy no
            # QUEUED slot. Retry requeues are exempt (already admitted).
            if (
                device.status == DeviceStatus.OFFLINE
                and device.queue_capacity is not None
                and self._store.queue_size(device_id) >= device.queue_capacity
            ):
                return None, RejectReason.QUEUE_FULL
            command = Command(
                command_id=f"cmd-{next(self._seq)}",
                device_id=device_id,
                name=name,
                params=dict(params or {}),
                created_at=self._clock(),
            )
            self._store.add_command(command)
            if device.status == DeviceStatus.ONLINE:
                self._deliver(command)
            return command, None

    def _deliver(self, command: Command) -> None:
        """QUEUED -> SENT. Caller must hold the device lock."""
        command.status = CommandStatus.SENT
        command.attempt += 1
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
        _device, reason = self._registry.authenticate(device_id, secret)
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
            # ack for a never-delivered (QUEUED) or already settled command
            if command.status is not CommandStatus.SENT:
                return AckResult(accepted=False, reason=RejectReason.INVALID_STATE)
            command.status = CommandStatus.ACKED
            command.ack_count += 1
            self._store._persist()
            return AckResult(accepted=True)

    # -- cancel ----------------------------------------------------------
    def cancel(
        self, device_id: str, command_id: str
    ) -> tuple[bool, RejectReason | None]:
        device = self._store.get_device(device_id)
        if device is None:
            return False, RejectReason.UNKNOWN_DEVICE
        with self._store.device_lock(device_id):
            command = self._store.get_command(device_id, command_id)
            if command is None:
                return False, RejectReason.UNKNOWN_COMMAND
            if command.status in (
                CommandStatus.QUEUED,
                CommandStatus.SENT,
            ):
                command.status = CommandStatus.CANCELLED
                self._store._persist()
                return True, None
            if command.status == CommandStatus.ACKED:
                # v1.0: ACKED is the terminal verdict; v2.0: command is
                # still executing and may be cancelled before its result.
                if device.protocol_version in TERMINAL_AFTER_ACK_VERSIONS:
                    return False, RejectReason.INVALID_STATE
                command.status = CommandStatus.CANCELLED
                self._store._persist()
                return True, None
            return False, RejectReason.INVALID_STATE

    # -- execution results (v2.0) ---------------------------------------
    def report_result(
        self,
        device_id: str,
        secret: str,
        command_id: str,
        success: bool,
        duration_ms: int,
        error_code: str | None = None,
    ) -> ResultReportResult:
        device, reason = self._registry.authenticate(device_id, secret)
        if reason is not None:
            return ResultReportResult(accepted=False, reason=reason)
        # Legacy clients cannot report results: their ACKED is the fixed,
        # definitive terminal -- never silently turn it into success or
        # failure, and never retry on their behalf.
        if not supports_result_report(device.protocol_version):
            return ResultReportResult(
                accepted=False,
                reason=RejectReason.RESULT_UNSUPPORTED_VERSION,
            )
        if duration_ms < 0 or (not success and not error_code):
            return ResultReportResult(
                accepted=False, reason=RejectReason.INVALID_STATE
            )
        with self._store.device_lock(device_id):
            command = self._store.get_command(device_id, command_id)
            if command is None:
                return ResultReportResult(
                    accepted=False, reason=RejectReason.UNKNOWN_COMMAND
                )
            status = command.status
            if status in (CommandStatus.QUEUED, CommandStatus.SENT):
                return ResultReportResult(
                    accepted=False, reason=RejectReason.RESULT_BEFORE_ACK
                )
            if status == CommandStatus.TIMED_OUT:
                return ResultReportResult(
                    accepted=False, reason=RejectReason.RESULT_AFTER_TIMEOUT
                )
            if status == CommandStatus.CANCELLED:
                return ResultReportResult(
                    accepted=False, reason=RejectReason.RESULT_AFTER_CANCEL
                )
            if status == CommandStatus.ACKED:
                return self._record_result(device, command, success,
                                           duration_ms, error_code)
            # already settled: the conclusion is fixed. An identical
            # verdict is a duplicate; a contradictory one is rejected.
            last = command.results[-1]
            command.result_count += 1  # observation only
            self._store._persist()
            if last.success == success and last.error_code == error_code:
                return ResultReportResult(
                    accepted=True,
                    duplicate=True,
                    attempt=last.attempt,
                    terminal=True,
                )
            return ResultReportResult(
                accepted=False,
                reason=RejectReason.CONFLICTING_RESULT,
                attempt=last.attempt,
                terminal=True,
            )

    def _record_result(
        self,
        device: Device,
        command: Command,
        success: bool,
        duration_ms: int,
        error_code: str | None,
    ) -> ResultReportResult:
        """ACKED -> terminal, or back to QUEUED for one more retry.

        Caller must hold the device lock.
        """
        record = self._make_attempt_result(
            command, success, duration_ms, error_code
        )
        command.results.append(record)
        command.result_count += 1
        if success:
            command.status = CommandStatus.SUCCEEDED
            self._store._persist()
            return ResultReportResult(
                accepted=True, attempt=record.attempt, terminal=True
            )
        if command.attempt <= device.max_retries:
            # retries remain: requeue to the tail behind other pending
            # work; same command_id, same single slot -- no duplication,
            # no clobbering of other commands, delivery via next flush.
            command.status = CommandStatus.QUEUED
            command.sent_at = None
            command.ack_deadline = None
            self._store.requeue_command(command)
            return ResultReportResult(
                accepted=True,
                attempt=record.attempt,
                terminal=False,
                retried=True,
            )
        command.status = (
            CommandStatus.RETRIES_EXHAUSTED
            if device.max_retries > 0
            else CommandStatus.FAILED
        )
        self._store._persist()
        return ResultReportResult(
            accepted=True, attempt=record.attempt, terminal=True
        )

    def _make_attempt_result(
        self,
        command: Command,
        success: bool,
        duration_ms: int,
        error_code: str | None,
    ) -> AttemptResult:
        return AttemptResult(
            attempt=command.attempt,
            success=success,
            duration_ms=duration_ms,
            error_code=error_code,
            reported_at=self._clock(),
        )

    # -- offline queue capacity -----------------------------------------
    def queue_status(self, device_id: str) -> QueueStatus | None:
        device = self._store.get_device(device_id)
        if device is None:
            return None
        with self._store.device_lock(device_id):
            queued = self._store.queue_size(device_id)
            capacity = device.queue_capacity
            return QueueStatus(
                device_id=device_id,
                queued=queued,
                capacity=capacity,
                full=capacity is not None and queued >= capacity,
            )

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
