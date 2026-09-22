"""HTTP 端到端：错误码可区分、完整状态流转、重连补发。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.models import Store

logger = logging.getLogger(__name__)


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app(Store()))


def _register(client: TestClient, device_id: str, version: str = "v2") -> str:
    resp = client.post("/devices/register", json={"device_id": device_id, "protocol_version": version})
    assert resp.status_code == 200, resp.text
    return resp.json()["secret"]


def test_full_device_telesmetry_command_flow(client: TestClient) -> None:
    secret = _register(client, "d1", "v2")

    # 未知设备与坏凭据在 HTTP 层依然可区分
    r_unknown = client.post(
        "/devices/ghost/telemetry",
        json={"secret": "x", "message_id": "m", "ts": 1.0, "metrics": {}, "version": "v2"},
    )
    r_bad = client.post(
        "/devices/d1/telemetry",
        json={"secret": "wrong", "message_id": "m", "ts": 1.0, "metrics": {}, "version": "v2"},
    )
    logger.info("HTTP unknown=%s bad-secret=%s",
                r_unknown.json()["error"], r_bad.json()["error"])
    assert r_unknown.status_code == 404 and r_unknown.json()["error"] == "DEVICE_NOT_FOUND"
    assert r_bad.status_code == 401 and r_bad.json()["error"] == "INVALID_CREDENTIAL"

    # 在线后上报
    conn = client.post("/devices/d1/connect", json={"secret": secret, "version": "v2"})
    assert conn.json()["state"] == "ONLINE"

    # 时间戳取当前时间，满足 v2 的 ±60s 窗口
    import time

    ts = time.time()
    r1 = client.post("/devices/d1/telemetry",
                     json={"secret": secret, "message_id": "m1", "ts": ts, "metrics": {"v": 1}, "version": "v2"})
    assert r1.status_code == 200
    r_dup = client.post("/devices/d1/telemetry",
                        json={"secret": secret, "message_id": "m1", "ts": ts, "metrics": {"v": 2}, "version": "v2"})
    assert r_dup.status_code == 409 and r_dup.json()["error"] == "TELEMETRY_DUPLICATE"
    logger.info("HTTP duplicate telemetry -> %s", r_dup.json()["error"])

    # 在线命令立即 SENT，重复 ack 幂等
    ci = client.post("/devices/d1/commands", json={"payload": "reboot", "ttl_seconds": 30})
    command_id = ci.json()["command_id"]
    assert ci.json()["state"] == "SENT"
    a1 = client.post(f"/commands/{command_id}/ack")
    a2 = client.post(f"/commands/{command_id}/ack")
    assert a1.json()["state"] == a2.json()["state"] == "ACKED"
    assert a1.json()["acked_at"] == a2.json()["acked_at"]


def test_offline_queue_redelivered_over_reconnect_http(client: TestClient) -> None:
    secret = _register(client, "d2", "v2")
    client.post("/devices/d2/connect", json={"secret": secret, "version": "v2"})
    client.post("/devices/d2/disconnect")

    c1 = client.post("/devices/d2/commands", json={"payload": "a"}).json()
    c2 = client.post("/devices/d2/commands", json={"payload": "b"}).json()
    assert c1["state"] == c2["state"] == "QUEUED"

    reconn = client.post("/devices/d2/connect", json={"secret": secret, "version": "v2"})
    order = [c["payload"] for c in reconn.json()["redelivered"]]
    logger.info("HTTP reconnect redelivered=%s", order)
    assert order == ["a", "b"]
    for cid in (c1["command_id"], c2["command_id"]):
        assert client.get(f"/commands/{cid}").json()["state"] == "SENT"


def test_unsupported_version_rejected_no_silent_fallback(client: TestClient) -> None:
    r = client.post("/devices/register", json={"device_id": "d3", "protocol_version": "v9"})
    logger.info("HTTP register v9 -> status=%s error=%s", r.status_code, r.json()["error"])
    assert r.status_code == 400 and r.json()["error"] == "UNSUPPORTED_VERSION"

    secret = _register(client, "d4", "v1")
    r2 = client.post("/devices/d4/telemetry",
                     json={"secret": secret, "message_id": "m", "version": "v9", "metrics": {}})
    assert r2.json()["error"] == "UNSUPPORTED_VERSION"


def test_v2_missing_field_rejected(client: TestClient) -> None:
    secret = _register(client, "d5", "v2")
    r = client.post("/devices/d5/telemetry",
                    json={"secret": secret, "message_id": "m", "version": "v2"})
    logger.info("HTTP v2 missing ts/metrics -> %s", r.json()["error"])
    assert r.status_code == 422 and r.json()["error"] == "MISSING_FIELD"


def test_timestamp_range_error_distinguishable(client: TestClient) -> None:
    secret = _register(client, "d6", "v2")
    r = client.post(
        "/devices/d6/telemetry",
        json={"secret": secret, "message_id": "m", "ts": 1.0, "metrics": {}, "version": "v2"},
    )
    logger.info("HTTP ts=1 vs now -> %s: %s", r.json()["error"], r.json()["message"])
    assert r.status_code == 422 and r.json()["error"] == "TIMESTAMP_OUT_OF_RANGE"
