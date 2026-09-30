"""Telemetry ingestion: idempotent, order-tolerant, concurrency-safe.

Rules:
- Auth and version checks happen before any state mutation.
- A message is deduplicated by (device_id, message_id): re-delivery of the
  same message_id is reported as duplicate and never double-counted.
- Out-of-order timestamps are accepted (buffered), as long as the timestamp
  lies within [now - max_past_skew, now + max_future_skew]. Timestamps
  outside the window are rejected with TIMESTAMP_OUT_OF_RANGE.
- The per-device lock serializes the check-then-insert so concurrent
  ingestion of the same message_id yields exactly one acceptance.
"""

from __future__ import annotations

from typing import Any

from .devices import DeviceRegistry
from .models import (
    Clock,
    IngestResult,
    RejectReason,
    TelemetryRecord,
    system_clock,
)
from .protocol import ProtocolError, normalize_message
from .store import Store

DEFAULT_MAX_PAST_SKEW = 24 * 3600.0  # accept data up to 24h old
DEFAULT_MAX_FUTURE_SKEW = 5 * 60.0  # accept at most 5min clock-ahead


class TelemetryService:
    def __init__(
        self,
        store: Store,
        registry: DeviceRegistry,
        clock: Clock = system_clock,
        max_past_skew: float = DEFAULT_MAX_PAST_SKEW,
        max_future_skew: float = DEFAULT_MAX_FUTURE_SKEW,
    ) -> None:
        self._store = store
        self._registry = registry
        self._clock = clock
        self._max_past = max_past_skew
        self._max_future = max_future_skew

    def ingest(
        self,
        device_id: str,
        secret: str,
        version: str,
        message: dict[str, Any],
    ) -> IngestResult:
        # 1. protocol version (rejected before auth so version errors are
        #    distinguishable and never silently downgraded)
        try:
            msg = normalize_message(version, message)
        except ProtocolError as exc:
            return IngestResult(accepted=False, reason=exc.reason)

        # 2. authentication
        device, reason = self._registry.authenticate(device_id, secret)
        if reason is not None:
            return IngestResult(accepted=False, reason=reason)

        # 3. timestamp window
        now = self._clock()
        ts = float(msg["timestamp"])
        if ts < now - self._max_past or ts > now + self._max_future:
            return IngestResult(
                accepted=False, reason=RejectReason.TIMESTAMP_OUT_OF_RANGE
            )

        # 4. idempotent insert under the per-device lock
        with self._store.device_lock(device_id):
            if self._store.has_telemetry(device_id, msg["message_id"]):
                return IngestResult(
                    accepted=False,
                    reason=RejectReason.DUPLICATE_TELEMETRY,
                    duplicate=True,
                )
            record = TelemetryRecord(
                device_id=device_id,
                message_id=msg["message_id"],
                timestamp=ts,
                payload=dict(msg.get("payload", {})),
                seq=int(msg["seq"]),
                received_at=now,
            )
            self._store.add_telemetry(record)
            device.last_seen = now
        return IngestResult(accepted=True)
