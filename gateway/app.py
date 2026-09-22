"""FastAPI HTTP 层：注册/鉴权/遥测/上下线/命令生命周期。

错误响应统一形态：{"error": <稳定错误码>, "message": <人类可读原因>}，
调用方应依据 error 字段（而非 HTTP 文案）做分支。
"""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from gateway import commands as cmd_service
from gateway import devices as device_service
from gateway import telemetry as telemetry_service
from gateway.errors import ErrorCode, GatewayError
from gateway.models import Command, Device, Store

logger = logging.getLogger(__name__)

_STATUS_MAP: dict[ErrorCode, int] = {
    ErrorCode.DEVICE_NOT_FOUND: 404,
    ErrorCode.COMMAND_NOT_FOUND: 404,
    ErrorCode.DUPLICATE_DEVICE: 409,
    ErrorCode.TELEMETRY_DUPLICATE: 409,
    ErrorCode.INVALID_COMMAND_TRANSITION: 409,
    ErrorCode.INVALID_CREDENTIAL: 401,
    ErrorCode.UNSUPPORTED_VERSION: 400,
    ErrorCode.MISSING_FIELD: 422,
    ErrorCode.TIMESTAMP_OUT_OF_RANGE: 422,
    ErrorCode.DEVICE_OFFLINE: 409,
}


# ---------- 请求/响应模型 ----------

class RegisterIn(BaseModel):
    device_id: str = Field(min_length=1)
    protocol_version: str | None = None


class ConnectIn(BaseModel):
    secret: str
    version: str | None = None


class TelemetryIn(BaseModel):
    secret: str
    message_id: str = Field(min_length=1)
    ts: float | None = None
    metrics: dict | None = None
    version: str | None = None


class CommandIn(BaseModel):
    payload: str = ""
    ttl_seconds: float = cmd_service.DEFAULT_TTL_SECONDS


# ---------- 序列化 ----------

def _device_dto(device: Device) -> dict:
    return {
        "device_id": device.device_id,
        "state": device.state.value,
        "protocol_version": device.protocol_version,
        "registered_at": device.registered_at,
        "last_seen": device.last_seen,
        "connected_version": device.connected_version,
    }


def _command_dto(command: Command) -> dict:
    return {
        "command_id": command.command_id,
        "device_id": command.device_id,
        "payload": command.payload,
        "state": command.state.value,
        "created_at": command.created_at,
        "sent_at": command.sent_at,
        "acked_at": command.acked_at,
        "timeout_at": command.timeout_at,
        "closed_at": command.closed_at,
        "delivered_version": command.delivered_version,
    }


def create_app(store: Store | None = None) -> FastAPI:
    store = store if store is not None else Store()
    app = FastAPI(title="IoT Device Gateway (local)", version="0.1.0")
    app.state.store = store

    @app.exception_handler(GatewayError)
    async def _on_gateway_error(_request, exc: GatewayError) -> JSONResponse:
        logger.info("request rejected code=%s message=%s", exc.code.value, exc.message)
        return JSONResponse(
            status_code=_STATUS_MAP.get(exc.code, 400),
            content={"error": exc.code.value, "message": exc.message},
        )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "devices": len(store.devices), "commands": len(store.commands)}

    # ---------- 设备 ----------

    @app.post("/devices/register")
    def register(body: RegisterIn) -> dict:
        device = device_service.register_device(store, body.device_id, body.protocol_version)
        return {**_device_dto(device), "secret": device.secret}

    @app.post("/devices/{device_id}/connect")
    def connect(device_id: str, body: ConnectIn) -> dict:
        device, delivered = device_service.connect(store, device_id, body.secret, body.version)
        return {**_device_dto(device), "redelivered": [_command_dto(c) for c in delivered]}

    @app.post("/devices/{device_id}/disconnect")
    def disconnect(device_id: str) -> dict:
        return _device_dto(device_service.disconnect(store, device_id))

    @app.get("/devices/{device_id}")
    def get_device(device_id: str) -> dict:
        return _device_dto(device_service.get_device(store, device_id))

    # ---------- 遥测 ----------

    @app.post("/devices/{device_id}/telemetry")
    def telemetry(device_id: str, body: TelemetryIn) -> dict:
        record = telemetry_service.ingest_telemetry(
            store,
            device_id,
            body.secret,
            body.message_id,
            body.ts,
            body.metrics,
            body.version,
        )
        return {
            "device_id": record.device_id,
            "message_id": record.message_id,
            "ts": record.ts,
            "metrics": record.metrics,
            "protocol_version": record.protocol_version,
            "ingested_seq": record.ingested_seq,
            "status": "accepted",
        }

    @app.get("/devices/{device_id}/telemetry")
    def list_telemetry(device_id: str) -> dict:
        records = telemetry_service.list_telemetry(store, device_id)
        return {
            "device_id": device_id,
            "count": len(records),
            "items": [
                {
                    "message_id": r.message_id,
                    "ts": r.ts,
                    "metrics": r.metrics,
                    "protocol_version": r.protocol_version,
                    "ingested_seq": r.ingested_seq,
                }
                for r in records
            ],
        }

    # ---------- 命令（管理侧下发 / 设备侧确认） ----------

    @app.post("/devices/{device_id}/commands")
    def issue(device_id: str, body: CommandIn) -> dict:
        command = cmd_service.issue_command(store, device_id, body.payload, ttl_seconds=body.ttl_seconds)
        return _command_dto(command)

    @app.get("/commands/{command_id}")
    def get_command(command_id: str) -> dict:
        return _command_dto(cmd_service.get_command(store, command_id))

    @app.post("/commands/{command_id}/ack")
    def ack(command_id: str) -> dict:
        return _command_dto(cmd_service.ack_command(store, command_id))

    @app.post("/commands/{command_id}/cancel")
    def cancel(command_id: str) -> dict:
        return _command_dto(cmd_service.cancel_command(store, command_id))

    @app.post("/commands/{command_id}/timeout")
    def timeout(command_id: str) -> dict:
        return _command_dto(cmd_service.timeout_command(store, command_id))

    return app
