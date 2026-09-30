"""Protocol version compatibility rules.

Rules (deterministic and stable):
- Supported versions: SUPPORTED_VERSIONS. Anything else is rejected with
  UNSUPPORTED_VERSION -- never silently downgraded.
- v1.0 clients may omit ``seq`` and ``qos``; defaults are seq=0, qos=0.
- v2.0 clients MUST provide ``seq``; ``qos`` defaults to 0.
- Normalization is a pure function of (version, message): the same input
  always yields the same normalized output. Rules are keyed by the
  *message's* declared version, not by gateway state, so after a rules
  upgrade the verdict for any stored/historical message is reproducible
  by re-running normalize_message with its recorded version.
"""

from __future__ import annotations

from typing import Any

from .models import RejectReason

SUPPORTED_VERSIONS: frozenset[str] = frozenset({"1.0", "2.0"})

# Client versions that can report command *execution* results (success /
# failure / error_code / duration). v1.0 clients only send acks: for them
# ACKED is the terminal conclusion (never retried, see commands.py).
RESULT_CAPABLE_VERSIONS: frozenset[str] = frozenset({"2.0"})

# Per-version defaults applied when the client omits a field.
_FIELD_DEFAULTS: dict[str, dict[str, Any]] = {
    "1.0": {"seq": 0, "qos": 0},
    "2.0": {"qos": 0},
}

# Fields that must be present (after defaults) for each version.
_REQUIRED_FIELDS: dict[str, frozenset[str]] = {
    "1.0": frozenset({"message_id", "timestamp"}),
    "2.0": frozenset({"message_id", "timestamp", "seq"}),
}


class ProtocolError(Exception):
    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def normalize_message(version: str, message: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a client message for the declared version.

    Returns a new dict with defaults applied. Raises ProtocolError with a
    distinguishable reason on unknown version or missing required fields.
    """
    if version not in SUPPORTED_VERSIONS:
        raise ProtocolError(
            RejectReason.UNSUPPORTED_VERSION,
            f"protocol version {version!r} is not supported "
            f"(supported: {sorted(SUPPORTED_VERSIONS)})",
        )
    normalized = dict(message)
    for key, default in _FIELD_DEFAULTS[version].items():
        normalized.setdefault(key, default)
    missing = [f for f in sorted(_REQUIRED_FIELDS[version]) if f not in normalized]
    if missing:
        raise ProtocolError(
            RejectReason.INVALID_STATE,
            f"version {version} message missing required fields: {missing}",
        )
    normalized["protocol_version"] = version
    return normalized
