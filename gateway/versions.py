"""协议版本兼容规则（不可变语义，拒绝静默降级）。

版本策略一旦发布即冻结：新版本只能新增策略，不能改动旧策略，
因此用某版本摄入并留存 protocol_version 的数据，其判定永远可复现。

- v1（默认版本，兼容最早期客户端）：
    * 请求未显式带版本时按 v1 处理（这是文档化的确定缺省，不是降级）；
    * ts 允许缺失：缺失时由服务端以摄入时刻补时间戳（旧设备无可靠时钟）；
    * metrics 允许缺失：缺省为空对象 {}；
    * 时间戳窗口宽松：±300s。
- v2：
    * ts、metrics 均为必填，缺失以 MISSING_FIELD 拒绝；
    * 时间戳窗口严格：±60s。
任何不在白名单内的显式版本一律 UNSUPPORTED_VERSION 拒绝，绝不回退。
"""

from __future__ import annotations

from dataclasses import dataclass

from gateway.errors import ErrorCode, GatewayError

SUPPORTED_VERSIONS: tuple[str, ...] = ("v1", "v2")
DEFAULT_VERSION = "v1"

_TS_FIELD = "ts"
_METRICS_FIELD = "metrics"


@dataclass(frozen=True)
class VersionPolicy:
    """某个版本的确定语义。"""

    version: str
    ts_skew_seconds: float
    metrics_required: bool
    ts_required: bool

    def normalize(self, raw: dict) -> dict:
        """按本版本规则归一化遥测字段，返回 {"ts": float|None, "metrics": dict}。

        v1 缺字段走稳定缺省；v2 缺必填字段明确拒绝。返回的 ts 为 None
        表示“由服务端在摄入时刻补时间”。
        """
        ts = raw.get(_TS_FIELD, None)
        if ts is None:
            if self.ts_required:
                raise GatewayError(
                    ErrorCode.MISSING_FIELD,
                    f"protocol {self.version} requires field {_TS_FIELD!r}",
                )
        elif not isinstance(ts, (int, float)) or isinstance(ts, bool):
            raise GatewayError(
                ErrorCode.MISSING_FIELD,
                f"field {_TS_FIELD!r} must be a number, got {type(ts).__name__}",
            )

        metrics = raw.get(_METRICS_FIELD, None)
        if metrics is None:
            if self.metrics_required:
                raise GatewayError(
                    ErrorCode.MISSING_FIELD,
                    f"protocol {self.version} requires field {_METRICS_FIELD!r}",
                )
            metrics = {}
        elif not isinstance(metrics, dict):
            raise GatewayError(
                ErrorCode.MISSING_FIELD,
                f"field {_METRICS_FIELD!r} must be an object, got {type(metrics).__name__}",
            )

        return {"ts": (float(ts) if ts is not None else None), "metrics": metrics}


_VERSION_POLICIES: dict[str, VersionPolicy] = {
    "v1": VersionPolicy(version="v1", ts_skew_seconds=300.0, metrics_required=False, ts_required=False),
    "v2": VersionPolicy(version="v2", ts_skew_seconds=60.0, metrics_required=True, ts_required=True),
}


def parse_version(version: str | None) -> str:
    """归一化版本标识：None 取文档化默认版本；白名单外明确拒绝。"""
    if version is None:
        return DEFAULT_VERSION
    if not isinstance(version, str):
        raise GatewayError(
            ErrorCode.UNSUPPORTED_VERSION,
            f"protocol version must be a string, got {type(version).__name__}",
        )
    normalized = version.strip().lower()
    if normalized not in _VERSION_POLICIES:
        raise GatewayError(
            ErrorCode.UNSUPPORTED_VERSION,
            f"unsupported protocol version: {version!r}; supported={list(_VERSION_POLICIES)}",
        )
    return normalized


def get_policy(version: str | None) -> VersionPolicy:
    """返回版本策略；未知/不支持版本明确拒绝，不静默降级。"""
    return _VERSION_POLICIES[parse_version(version)]
