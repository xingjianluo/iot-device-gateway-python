"""Command dispatch with offline queue, guarded state machine and retries.

State machine (final states: SUCCEEDED, FAILED, TIMED_OUT, CANCELLED):

  v1.0 devices (cannot report execution results):
    QUEUED --deliver--> SENT --ack--> ACKED                  (final)
      |                  |
      |cancel            +--expire(deadline passed)--> TIMED_OUT (final)
      |                  |
      v                  +--cancel--> CANCELLED           (final)
    CANCELLED (final)

  v2.0 devices (report execution results; failed commands retry):
    QUEUED --deliver--> SENT --ack--> ACKED --result ok------> SUCCEEDED (final)
      |                  |              |
      |cancel            |cancel        +--result fail, retry left--> QUEUED (retry, tail)
      |                  v              |
      v               CANCELLED         +--result fail, retries exhausted--> FAILED (final)
    CANCELLED
                         SENT --expire(deadline passed)--> TIMED_OUT (final)

Ordering/interleaving rules (the key invariant: one command settles into
exactly one final state regardless of network reordering):
- Every lifecycle operation on one device is serialized by the per-device
  lock, so ack / result / cancel / expire racing on the wire can never
  produce two contradictory conclusions.
- report_result() only settles from ACKED. A result arriving before the
  ack is rejected RESULT_NOT_ACKED; after timeout RESULT_AFTER_TIMEOUT;
  after cancellation RESULT_AFTER_CANCELLED. The reasons never overlap.
- A repeated result for the same (attempt, outcome, error_code) is a
  duplicate: accepted with duplicate=True and the stored conclusion is
  never changed. A *different* outcome for a settled command is rejected
  INVALID_STATE (the first conclusion wins).
- Failed v2 commands are requeued to the TAIL of the FIFO queue, up to
  the per-device max_retries cap; attempt numbers distinguish retries.
  Retries are never capacity-rejected and never overwrite pending work.
- Each per-device offline queue has a configurable capacity. New
  dispatches against a full queue are rejected QUEUE_FULL; queued
  commands are never silently dropped.
- expire() is driven by an injected clock (no background threads),
  making timeouts deterministic and testable.
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
    Device,
    DeviceStatus,
    ExecutionResult,
    QueueStatus,
    RejectReason,
    ResultReportResult,
    system_clock,
)
from .protocol import RESULT_CAPABLE_VERSIONS
from .store import Store

DEFAULT_ACK_TIMEOUT = 30.0

# Malformed result payloads (wrong field type or invalid value).
_RESULT_VALIDATION_ERRORS = (TypeError, ValueError)


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
            current = self._store.get_device(device_id)
            online = current is not None and current.status == DeviceStatus.ONLINE
            # Capacity only bounds waiting work (QUEUED). A command
            # delivered immediately to an online device never occupies the
            # queue, so a zero-capacity device can still be commanded while
            # connected. Internal retry requeues bypass this check too
            # (never drop).
            if (
                not online
                and self._store.queued_count(device_id) >= device.max_queue_size
            ):
                return None, RejectReason.QUEUE_FULL
            command = Command(
                command_id=f"cmd-{next(self._seq)}",
                device_id=device_id,
                name=name,
                params=dict(params or {}),
                created_at=self._clock(),
                max_retries=device.max_retries,
                retry_backoff=device.retry_backoff,
            )
            self._store.add_command(command)
            if online:
                self._deliver(command)
            return command, None

    def _deliver(self, command: Command) -> None:
        """QUEUED -> SENT. Caller must hold the device lock."""
        command.status = CommandStatus.SENT
        command.sent_at = self._clock()
        command.ack_deadline = command.sent_at + self._ack_timeout
        self._store._persist()

    def flush(self, device_id: str) -> list[Command]:
        """Deliver queued commands in FIFO order (call on reconnect).

        A retry parked with a backoff (not_before) keeps its position; the
        FIFO pass stops at the first not-yet-eligible command so delivery
        order always matches queue order and remains explainable.
        """
        with self._store.device_lock(device_id):
            delivered = []
            now = self._clock()
            for command in self._store.queued_commands(device_id):
                if command.not_before is not None and command.not_before > now:
                    break
                self._deliver(command)
                delivered.append(command)
            return delivered

    # -- ack -------------------------------------------------------------
    def ack(self, device_id: str, secret: str, command_id: str) -> AckResult:
        _, reason = self._registry.authenticate(device_id, secret)
        if reason is not None:
            return AckResult(accepted=False, reason=reason)
        with self._store.device_lock(device_id):
            command = self._store.get_command(device_id, command_id)
            if command is None:
                return AckResult(accepted=False, reason=RejectReason.UNKNOWN_COMMAND)
            if command.status == CommandStatus.ACKED:
                command.ack_count += 1  # observed, but state unchanged
                return AckResult(accepted=True, duplicate=True)
            if command.status in TERMINAL_COMMAND_STATES:
                return AckResult(accepted=False, reason=RejectReason.INVALID_STATE)
            if command.status == CommandStatus.QUEUED:
                # ack for a command never delivered: protocol violation
                return AckResult(accepted=False, reason=RejectReason.INVALID_STATE)
            command.status = CommandStatus.ACKED
            command.ack_count += 1
            self._store._persist()
            return AckResult(accepted=True)

    # -- execution result ------------------------------------------------
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
        """Settle a v2 command with its execution outcome.

        See the module docstring for the interleaving/ordering rules.
        """
        device, reason = self._registry.authenticate(device_id, secret)
        if reason is not None:
            return ResultReportResult(accepted=False, reason=reason)

        # Version semantics: only v2+ clients can report results. For a
        # v1 device, ACKED is already the final conclusion.
        if device.protocol_version not in RESULT_CAPABLE_VERSIONS:
            return ResultReportResult(
                accepted=False, reason=RejectReason.UNSUPPORTED_VERSION
            )

        try:
            parsed = self._parse_result(success, duration, error_code, attempt)
        except _RESULT_VALIDATION_ERRORS:
            return ResultReportResult(
                accepted=False,
                reason=RejectReason.INVALID_RESULT,
            )

        with self._store.device_lock(device_id):
            command = self._store.get_command(device_id, command_id)
            if command is None:
                return ResultReportResult(
                    accepted=False, reason=RejectReason.UNKNOWN_COMMAND
                )
            status = command.status

            # 1. Retransmission of the same attempt result is a duplicate,
            #    regardless of the command's current state: it never
            #    changes the conclusion and never triggers a second retry.
            if self._same_result(command.last_result, parsed):
                return ResultReportResult(
                    accepted=True,
                    duplicate=True,
                    command_status=status,
                )

            # 2. A different outcome for an already-settled command is a
            #    contradiction: the first conclusion wins.
            if status in (
                CommandStatus.SUCCEEDED,
                CommandStatus.FAILED,
            ):
                return ResultReportResult(
                    accepted=False, reason=RejectReason.INVALID_STATE
                )
            if status == CommandStatus.TIMED_OUT:
                return ResultReportResult(
                    accepted=False, reason=RejectReason.RESULT_AFTER_TIMEOUT
                )
            if status == CommandStatus.CANCELLED:
                return ResultReportResult(
                    accepted=False, reason=RejectReason.RESULT_AFTER_CANCELLED
                )
            if status == CommandStatus.QUEUED:
                # Result for a command never delivered/acked.
                return ResultReportResult(
                    accepted=False, reason=RejectReason.RESULT_NOT_ACKED
                )
            if status == CommandStatus.SENT:
                # Result without a preceding ack.
                return ResultReportResult(
                    accepted=False, reason=RejectReason.RESULT_NOT_ACKED
                )

            # status == ACKED: the only state from which a result settles.
            if parsed.attempt != command.attempt:
                # An attempt number that is not currently executing cannot
                # be settled (stale/duplicate of an older delivery, or a
                # future attempt that has not happened).
                return ResultReportResult(
                    accepted=False, reason=RejectReason.INVALID_STATE
                )

            now = self._clock()
            command.last_result = parsed
            if parsed.success:
                return self._settle_success(command, parsed, now)
            return self._settle_failure(device, command, parsed, now)

    @staticmethod
    def _parse_result(
        success: Any,
        duration: Any,
        error_code: Any,
        attempt: Any,
    ) -> ExecutionResult:
        if not isinstance(success, bool):
            raise TypeError("success must be a boolean")
        if isinstance(duration, bool) or not isinstance(duration, (int, float)):
            raise TypeError("duration must be a number")
        duration = float(duration)
        if duration < 0:
            raise ValueError("duration must be non-negative")
        if not success:
            if not isinstance(error_code, str) or not error_code:
                raise ValueError("error_code is required for a failed result")
        elif error_code is not None and not isinstance(error_code, str):
            raise TypeError("error_code must be a string or null")
        if attempt is None:
            attempt = 1
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise TypeError("attempt must be a positive int")
        return ExecutionResult(
            success=success,
            duration=duration,
            error_code=error_code if not success else None,
            attempt=attempt,
        )

    @staticmethod
    def _same_result(stored: ExecutionResult | None, new: ExecutionResult) -> bool:
        return (
            stored is not None
            and stored.attempt == new.attempt
            and stored.success == new.success
            and stored.error_code == new.error_code
        )

    def _settle_success(
        self, command: Command, result: ExecutionResult, now: float
    ) -> ResultReportResult:
        command.status = CommandStatus.SUCCEEDED
        command.result = result
        command.result_received_at = now
        self._store._persist()
        return ResultReportResult(accepted=True, command_status=CommandStatus.SUCCEEDED)

    def _settle_failure(
        self,
        device: Device,
        command: Command,
        result: ExecutionResult,
        now: float,
    ) -> ResultReportResult:
        command.failed_attempts.append(result.attempt)
        # A retry is another *attempt* of the same command_id. The cap
        # counts failures: total deliveries = 1 + max_retries.
        if result.attempt <= command.max_retries:
            command.attempt += 1
            command.status = CommandStatus.QUEUED
            command.sent_at = None
            command.ack_deadline = None
            command.not_before = (
                now + command.retry_backoff if command.retry_backoff > 0 else None
            )
            self._store.requeue_command(command)
            # Deliver immediately if the device is connected and the
            # retry is eligible; otherwise it waits in the FIFO queue.
            if device.status == DeviceStatus.ONLINE and (
                command.not_before is None or command.not_before <= now
            ):
                self._deliver(command)
            return ResultReportResult(
                accepted=True,
                retried=True,
                command_status=command.status,
            )
        command.status = CommandStatus.FAILED
        command.result = result
        command.result_received_at = now
        self._store._persist()
        return ResultReportResult(accepted=True, command_status=CommandStatus.FAILED)

    # -- queue occupancy -------------------------------------------------
    def queue_status(self, device_id: str) -> QueueStatus | None:
        device = self._store.get_device(device_id)
        if device is None:
            return None
        queued = self._store.queued_count(device_id)
        return QueueStatus(
            device_id=device_id,
            queued=queued,
            capacity=device.max_queue_size,
            in_flight=self._store.in_flight_count(device_id),
            full=queued >= device.max_queue_size,
        )

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
        device_ids = [device_id] if device_id else list(self._store.commands.keys())
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
