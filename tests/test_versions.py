"""协议版本兼容：缺省语义稳定、未知版本明确拒绝、不静默降级。"""

from __future__ import annotations

import logging

import pytest

from gateway.errors import ErrorCode, GatewayError
from gateway.versions import DEFAULT_VERSION, get_policy, parse_version

logger = logging.getLogger(__name__)


def test_default_version_is_v1(store) -> None:
    from gateway.devices import register_device

    device = register_device(store, "d", None)
    logger.info("input protocol_version=None -> parsed=%s (DEFAULT_VERSION=%s)",
                device.protocol_version, DEFAULT_VERSION)
    assert device.protocol_version == "v1"


def test_v1_missing_fields_have_stable_defaults() -> None:
    policy = get_policy("v1")
    normalized = policy.normalize({"ts": None, "metrics": None})
    logger.info("v1 normalize input={} -> %s", normalized)
    assert normalized["ts"] is None      # 由服务端在摄入时刻补时间
    assert normalized["metrics"] == {}   # 空对象，不抛异常


def test_v2_requires_ts_and_metrics() -> None:
    policy = get_policy("v2")
    for missing in ("ts", "metrics"):
        raw = {"ts": 1.0, "metrics": {"a": 1}}
        raw[missing] = None
        with pytest.raises(GatewayError) as exc:
            policy.normalize(raw)
        logger.info("v2 normalize missing=%s -> reject code=%s", missing, exc.value.code.value)
        assert exc.value.code is ErrorCode.MISSING_FIELD


def test_unknown_version_rejected_not_silently_downgraded() -> None:
    for bad in ("v3", "v0", "v99", "weird", "V2 "):  # 注意 'V2 ' strip 后合法 -> 见下一个用例
        if bad.strip().lower() == "v2":
            continue
        with pytest.raises(GatewayError) as exc:
            parse_version(bad)
        logger.info("parse_version input=%r -> reject code=%s", bad, exc.value.code.value)
        assert exc.value.code is ErrorCode.UNSUPPORTED_VERSION


def test_version_string_is_trimmed_and_case_insensitive() -> None:
    assert parse_version("  V2 ") == "v2"
    assert parse_version(None) == DEFAULT_VERSION


def test_policies_are_immutable() -> None:
    import dataclasses

    with pytest.raises(dataclasses.FrozenInstanceError):
        get_policy("v1").ts_skew_seconds = 1.0  # type: ignore[misc]
