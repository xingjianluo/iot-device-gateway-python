"""Device registry: registration, authentication, connect/disconnect."""
from __future__ import annotations

from .models import Clock, Device, DeviceStatus, RejectReason, system_clock
from .protocol import SUPPORTED_VERSIONS
from .store import Store


class DeviceRegistry:
    def __init__(self, store: Store, clock: Clock = system_clock) -> None:
        self._store = store
        self._clock = clock

    def register(
        self,
        device_id: str,
        secret: str,
        protocol_version: str = "1.0",
        max_retries: int = 0,
        queue_capacity: int | None = None,
    ) -> Device:
        if protocol_version not in SUPPORTED_VERSIONS:
            raise ValueError(
                f"unsupported protocol version {protocol_version!r} "
                f"(supported: {sorted(SUPPORTED_VERSIONS)})"
            )
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if queue_capacity is not None and queue_capacity < 0:
            raise ValueError("queue_capacity must be >= 0 or None")
        device = Device(
            device_id=device_id,
            secret=secret,
            protocol_version=protocol_version,
            max_retries=max_retries,
            queue_capacity=queue_capacity,
        )
        self._store.add_device(device)
        return device

    def authenticate(
        self, device_id: str, secret: str
    ) -> tuple[Device | None, RejectReason | None]:
        """Returns (device, None) on success, (None, reason) on failure."""
        device = self._store.get_device(device_id)
        if device is None:
            return None, RejectReason.UNKNOWN_DEVICE
        if device.secret != secret:
            return None, RejectReason.INVALID_CREDENTIALS
        return device, None

    def connect(self, device_id: str) -> None:
        with self._store.device_lock(device_id):
            device = self._store.get_device(device_id)
            if device is None:
                raise KeyError(device_id)
            device.status = DeviceStatus.ONLINE
            device.last_seen = self._clock()
            self._store._persist()

    def disconnect(self, device_id: str) -> None:
        with self._store.device_lock(device_id):
            device = self._store.get_device(device_id)
            if device is None:
                raise KeyError(device_id)
            device.status = DeviceStatus.OFFLINE
            self._store._persist()

    def configure_policy(
        self,
        device_id: str,
        max_retries: int | None = None,
        queue_capacity: int | None = None,
    ) -> Device:
        """Set per-device retry cap and offline-queue capacity.

        ``None`` leaves a field unchanged. Capacity may be shrunk below the
        current occupancy: queued commands are never discarded, the limit
        only rejects *new* dispatches until occupancy drains (see
        CommandService.dispatch).
        """
        if max_retries is not None and max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if queue_capacity is not None and queue_capacity < 0:
            raise ValueError("queue_capacity must be >= 0 or None")
        with self._store.device_lock(device_id):
            device = self._store.get_device(device_id)
            if device is None:
                raise KeyError(device_id)
            if max_retries is not None:
                device.max_retries = max_retries
            if queue_capacity is not None:
                device.queue_capacity = queue_capacity
            self._store._persist()
            return device
