"""遥测：幂等去重、乱序稳定、并发一致、失败原因可区分。"""

from __future__ import annotations

import logging
import threading

import pytest

from gateway.devices import connect, register_device
from gateway.errors import ErrorCode, GatewayError
from gateway.telemetry import ingest_telemetry, list_telemetry

logger = logging.getLogger(__name__)

BASE_TS = 1_000_000.0


def _register(store, device_id="dev-t", version="v2"):
    device = register_device(store, device_id, version)
    return device.device_id, device.secret


def test_duplicate_message_id_rejected(store) -> None:
    device_id, secret = _register(store)
    r1 = ingest_telemetry(store, device_id, secret, "m1", BASE_TS, {"t": 1}, "v2", now=BASE_TS)
    logger.info("first ingest msg=m1 -> seq=%d status=accepted", r1.ingested_seq)
    with pytest.raises(GatewayError) as exc:
        ingest_telemetry(store, device_id, secret, "m1", BASE_TS, {"t": 2}, "v2", now=BASE_TS)
    assert exc.value.code is ErrorCode.TELEMETRY_DUPLICATE
    records = list_telemetry(store, device_id)
    assert len(records) == 1
    assert records[0].metrics == {"t": 1}  # 重复内容没有覆盖原始数据


def test_timestamp_out_of_range_v2_window_60s(store) -> None:
    device_id, secret = _register(store)
    for label, ts, ok in [
        ("within +59s", BASE_TS + 59, True),
        ("exact +60s boundary", BASE_TS + 60, True),
        ("outside +61s", BASE_TS + 61, False),
        ("outside -300s old", BASE_TS - 300, False),
    ]:
        msg = f"m-{label.replace(' ', '-')}"
        if ok:
            ingest_telemetry(store, device_id, secret, msg, ts, {}, "v2", now=BASE_TS)
        else:
            with pytest.raises(GatewayError) as exc:
                ingest_telemetry(store, device_id, secret, msg, ts, {}, "v2", now=BASE_TS)
            logger.info("input ts=%s now=%.0f (%s) -> reject %s",
                        ts, BASE_TS, label, exc.value.code.value)
            assert exc.value.code is ErrorCode.TIMESTAMP_OUT_OF_RANGE
    assert len(list_telemetry(store, device_id)) == 2


def test_v1_allows_300s_window_and_fills_missing_ts(store) -> None:
    device_id, secret = _register(store, "dev-old", "v1")
    ingest_telemetry(store, device_id, secret, "wide", BASE_TS + 299, None, "v1", now=BASE_TS)
    r = ingest_telemetry(store, device_id, secret, "no-ts", None, None, "v1", now=BASE_TS)
    logger.info("v1 missing ts -> server stamps ts=%.3f", r.ts)
    assert r.ts == BASE_TS  # 缺省语义：服务端补摄入时刻
    assert r.metrics == {}


def test_out_of_order_ingestion_reads_sorted(store) -> None:
    device_id, secret = _register(store)
    # 以乱序时间戳上报
    ingest_telemetry(store, device_id, secret, "late", BASE_TS + 30, {}, "v2", now=BASE_TS + 30)
    ingest_telemetry(store, device_id, secret, "early", BASE_TS + 1, {}, "v2", now=BASE_TS + 1)
    ingest_telemetry(store, device_id, secret, "middle", BASE_TS + 10, {}, "v2", now=BASE_TS + 10)
    records = list_telemetry(store, device_id)
    logger.info("ingest order=[late,early,middle] read order=%s",
                [r.message_id for r in records])
    assert [r.message_id for r in records] == ["early", "middle", "late"]
    # 摄入序号保留真实到达顺序，可解释
    assert [r.ingested_seq for r in records] == [2, 3, 1]


def test_concurrent_ingestion_no_duplicate_count(store) -> None:
    device_id, secret = _register(store)
    connect(store, device_id, secret, "v2")

    # 场景 A：N 线程各自不同消息并发 -> 恰好 N 条，无序号空洞/重复
    n_threads = 32
    errors: list[GatewayError] = []
    barrier = threading.Barrier(n_threads)

    def worker(i: int) -> None:
        barrier.wait()
        try:
            ingest_telemetry(store, device_id, secret, f"cm-{i}",
                             BASE_TS + i * 0.001, {"i": i}, "v2", now=BASE_TS)
        except GatewayError as exc:  # pragma: no cover - 仅用于断言无此情况
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    records = list_telemetry(store, device_id)
    logger.info("concurrent distinct messages: threads=%d stored=%d seqs=%s",
                n_threads, len(records), sorted(r.ingested_seq for r in records))
    assert len(records) == n_threads
    seqs = sorted(r.ingested_seq for r in records)
    assert seqs == list(range(1, n_threads + 1))  # 序号连续，唯一


def test_concurrent_duplicate_same_message_only_one_wins(store) -> None:
    device_id, secret = _register(store)
    n_threads = 16
    results: list[str] = []
    result_lock = threading.Lock()
    barrier = threading.Barrier(n_threads)

    def worker() -> None:
        barrier.wait()
        try:
            ingest_telemetry(store, device_id, secret, "same-id",
                             BASE_TS, {"k": "v"}, "v2", now=BASE_TS)
            outcome = "accepted"
        except GatewayError as exc:
            outcome = exc.code.value
        with result_lock:
            results.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r == "accepted"]
    rejected = [r for r in results if r == "TELEMETRY_DUPLICATE"]
    logger.info("concurrent same message_id: accepted=%d duplicate-rejected=%d",
                len(accepted), len(rejected))
    assert len(accepted) == 1
    assert len(rejected) == n_threads - 1
    assert len(list_telemetry(store, device_id)) == 1


def test_unknown_device_and_bad_secret_rejected(store) -> None:
    device_id, _secret = _register(store)
    with pytest.raises(GatewayError) as e1:
        ingest_telemetry(store, "nope", "x", "m", BASE_TS, {}, "v2", now=BASE_TS)
    with pytest.raises(GatewayError) as e2:
        ingest_telemetry(store, device_id, "bad", "m", BASE_TS, {}, "v2", now=BASE_TS)
    logger.info("auth failures: unknown->%s bad-secret->%s",
                e1.value.code.value, e2.value.code.value)
    assert e1.value.code is ErrorCode.DEVICE_NOT_FOUND
    assert e2.value.code is ErrorCode.INVALID_CREDENTIAL
