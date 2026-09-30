"""Device registry: registration, authentication, connect/disconnect."""

from __future__ import annotations

from .models import Clock, Device, DeviceStatus, RejectReason, system_clock
from .protocol import SUPPORTED_VERSIONS
from .store import Store


def _validate_policy(
    max_queue_size: int, max_retries: int, retry_backoff: float
) -> None:
    if not isinstance(max_queue_size, int) or max_queue_size < 0:
        raise ValueError(
            f"max_queue_size must be a non-negative int, got {max_queue_size!r}"
        )
    if not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError(f"max_retries must be a non-negative int, got {max_retries!r}")
    if retry_backoff < 0:
        raise ValueError(f"retry_backoff must be non-negative, got {retry_backoff!r}")


class DeviceRegistry:
    def __init__(self, store: Store, clock: Clock = system_clock) -> None:
        self._store = store
        self._clock = clock

    def register(
        self,
        device_id: str,
        secret: str,
        protocol_version: str = "1.0",
        max_queue_size: int = 100,
        max_retries: int = 3,
        retry_backoff: float = 0.0,
    ) -> Device:
        if protocol_version not in SUPPORTED_VERSIONS:
            raise ValueError(
                f"unsupported protocol version {protocol_version!r} "
                f"(supported: {sorted(SUPPORTED_VERSIONS)})"
            )
        _validate_policy(max_queue_size, max_retries, retry_backoff)
        device = Device(
            device_id=device_id,
            secret=secret,
            protocol_version=protocol_version,
            max_queue_size=max_queue_size,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
        )
        self._store.add_device(device)
        return device

    def configure_policy(
        self,
        device_id: str,
        max_queue_size: int | None = None,
        max_retries: int | None = None,
        retry_backoff: float | None = None,
    ) -> Device:
        """Update a device's queue/retry policy. Never shrinks the queue
        silently: a smaller capacity only rejects *new* dispatches until
        the existing queue drains."""
        with self._store.device_lock(device_id):
            device = self._store.get_device(device_id)
            if device is None:
                raise KeyError(device_id)
            new_queue = (
                device.max_queue_size if max_queue_size is None else max_queue_size
            )
            new_retries = device.max_retries if max_retries is None else max_retries
            new_backoff = (
                device.retry_backoff if retry_backoff is None else retry_backoff
            )
            _validate_policy(new_queue, new_retries, new_backoff)
            device.max_queue_size = new_queue
            device.max_retries = new_retries
            device.retry_backoff = new_backoff
            self._store._persist()
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
