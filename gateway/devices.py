"""Device registry: registration, authentication, connect/disconnect."""
from __future__ import annotations

from .models import Clock, Device, DeviceStatus, RejectReason, system_clock
from .protocol import SUPPORTED_VERSIONS
from .store import Store


class DeviceRegistry:
    def __init__(self, store: Store, clock: Clock = system_clock) -> None:
        self._store = store
        self._clock = clock

    def register(self, device_id: str, secret: str, protocol_version: str = "1.0") -> Device:
        if protocol_version not in SUPPORTED_VERSIONS:
            raise ValueError(
                f"unsupported protocol version {protocol_version!r} "
                f"(supported: {sorted(SUPPORTED_VERSIONS)})"
            )
        device = Device(
            device_id=device_id, secret=secret, protocol_version=protocol_version
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
