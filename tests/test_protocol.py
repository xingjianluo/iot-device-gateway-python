"""Protocol version compatibility: defaults, rejection, reproducibility."""
import pytest

from gateway.models import RejectReason
from gateway.protocol import ProtocolError, normalize_message


def test_v1_missing_fields_get_stable_defaults():
    msg = {"message_id": "m1", "timestamp": 123.0}
    out = normalize_message("1.0", msg)
    print(f"v1 defaults: seq={out['seq']} qos={out['qos']}")
    assert out["seq"] == 0 and out["qos"] == 0
    assert msg == {"message_id": "m1", "timestamp": 123.0}  # input untouched


def test_v2_requires_seq():
    with pytest.raises(ProtocolError) as exc:
        normalize_message("2.0", {"message_id": "m1", "timestamp": 1.0})
    print(f"v2 missing seq -> {exc.value.detail}")
    out = normalize_message("2.0", {"message_id": "m1", "timestamp": 1.0, "seq": 7})
    assert out["seq"] == 7 and out["qos"] == 0


def test_unknown_version_rejected_not_downgraded(gateway, clock):
    gateway.register("d1", "s")
    r = gateway.ingest("d1", "s", "0.9", {"message_id": "m1", "timestamp": clock.now})
    print(f"version '0.9' -> accepted={r.accepted} reason={r.reason}")
    assert not r.accepted and r.reason == RejectReason.UNSUPPORTED_VERSION
    assert gateway.store.telemetry_count("d1") == 0  # nothing silently stored


def test_version_rules_reproducible():
    """Same (version, message) always normalizes identically -- verdicts for
    historical messages are reproducible regardless of gateway state."""
    msg = {"message_id": "m1", "timestamp": 42.0, "payload": {"t": 21}}
    first = normalize_message("1.0", msg)
    second = normalize_message("1.0", msg)
    print(f"reproducibility: {first} == {second}")
    assert first == second


def test_device_registered_with_unsupported_version_rejected(gateway):
    with pytest.raises(ValueError):
        gateway.register("d1", "s", protocol_version="3.0")
    print("register with version '3.0' -> ValueError (explicit rejection)")
