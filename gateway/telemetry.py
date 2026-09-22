"""遥测摄入：鉴权、版本归一化、时间戳校验、去重、乱序稳定存储。

判定顺序（每一步失败都有独立、可区分的原因）：
    1. 版本解析 + 必填字段（随版本归一化）：
       未知/不支持版本 -> UNSUPPORTED_VERSION；字段缺失 -> MISSING_FIELD
    2. message_id 必填：-> MISSING_FIELD
    3. 设备鉴权（锁内）：未知设备 -> DEVICE_NOT_FOUND；密钥错 -> INVALID_CREDENTIAL
    4. 幂等去重：(device_id, message_id) 已存在 -> TELEMETRY_DUPLICATE
       （重试报文即使时间戳异常也按重复处理，保证重试结论稳定）
    5. 时间戳窗口：超出版本策略窗口 -> TIMESTAMP_OUT_OF_RANGE
全部通过后在同一把全局锁内分配摄入序号并落库，因此并发摄入既不会重复计数，
乱序时间戳也不会破坏读取顺序（读取按 (ts, ingested_seq) 升序）。
"""

from __future__ import annotations

import logging
import time

from gateway.auth import authenticate
from gateway.errors import ErrorCode, GatewayError
from gateway.models import Store, TelemetryRecord
from gateway.versions import get_policy

logger = logging.getLogger(__name__)


def ingest_telemetry(
    store: Store,
    device_id: str,
    secret: str,
    message_id: str,
    ts: float | None,
    metrics: dict | None,
    version: str | None,
    now: float | None = None,
) -> TelemetryRecord:
    policy = get_policy(version)  # 未知版本先拒绝
    if not message_id or not isinstance(message_id, str):
        raise GatewayError(ErrorCode.MISSING_FIELD, "field 'message_id' is required")

    received_at = time.time() if now is None else float(now)
    normalized = policy.normalize({"ts": ts, "metrics": metrics})
    effective_ts = received_at if normalized["ts"] is None else normalized["ts"]
    dedupe_key = f"{device_id}\x00{message_id}"

    with store.lock:
        # 鉴权放在所有状态相关检查之前：未知设备与凭据错误优先且可区分。
        authenticate(store, device_id, secret)

        if dedupe_key in store.seen_message_ids:
            logger.info(
                "telemetry reject device=%s msg=%s reason=TELEMETRY_DUPLICATE",
                device_id, message_id,
            )
            raise GatewayError(
                ErrorCode.TELEMETRY_DUPLICATE,
                f"duplicate telemetry message_id={message_id!r} for device {device_id!r}",
            )

        skew = abs(effective_ts - received_at)
        if skew > policy.ts_skew_seconds:
            logger.info(
                "telemetry reject device=%s msg=%s reason=TIMESTAMP_OUT_OF_RANGE "
                "ts=%s received=%.3f skew=%.3f window=%.0f",
                device_id, message_id, effective_ts, received_at, skew,
                policy.ts_skew_seconds,
            )
            raise GatewayError(
                ErrorCode.TIMESTAMP_OUT_OF_RANGE,
                f"ts {effective_ts} outside ±{policy.ts_skew_seconds}s window "
                f"(received_at={received_at}, skew={skew:.3f})",
            )

        record = TelemetryRecord(
            device_id=device_id,
            message_id=message_id,
            ts=effective_ts,
            metrics=normalized["metrics"],
            protocol_version=policy.version,
            ingested_seq=store.next_seq(),
        )
        store.telemetry[device_id].append(record)
        store.seen_message_ids.add(dedupe_key)
        device = store.devices[device_id]
        device.last_seen = received_at
        logger.info(
            "telemetry accept device=%s msg=%s version=%s ts=%.3f seq=%d metrics_keys=%s",
            device_id, message_id, policy.version, effective_ts,
            record.ingested_seq, list(normalized["metrics"]),
        )
        return record


def list_telemetry(store: Store, device_id: str) -> list[TelemetryRecord]:
    """按 (ts, 摄入序号) 升序返回；乱序摄入不影响读取顺序。"""
    with store.lock:
        if device_id not in store.devices:
            raise GatewayError(
                ErrorCode.DEVICE_NOT_FOUND,
                f"device not registered: {device_id!r}",
            )
        return sorted(store.telemetry.get(device_id, []), key=lambda r: (r.ts, r.ingested_seq))
