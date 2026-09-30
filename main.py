"""FastAPI HTTP front-end for the local IoT device gateway."""

from fastapi import FastAPI
from pydantic import BaseModel

from gateway.service import Gateway

app = FastAPI(title="IoT Device Gateway")
gateway = Gateway(persist_path="gateway_state.json")


class RegisterRequest(BaseModel):
    device_id: str
    secret: str
    protocol_version: str = "1.0"
    max_queue_size: int = 100
    max_retries: int = 3
    retry_backoff: float = 0.0


class PolicyRequest(BaseModel):
    max_queue_size: int | None = None
    max_retries: int | None = None
    retry_backoff: float | None = None


class TelemetryRequest(BaseModel):
    secret: str
    version: str = "1.0"
    message: dict


class DispatchRequest(BaseModel):
    name: str
    params: dict = {}


class AckRequest(BaseModel):
    secret: str


class ResultRequest(BaseModel):
    secret: str
    success: bool
    duration: float
    error_code: str | None = None
    attempt: int | None = None


@app.get("/")
def read_root():
    return {"service": "iot-device-gateway", "docs": "/docs"}


@app.post("/devices")
def register(req: RegisterRequest):
    try:
        d = gateway.register(
            req.device_id,
            req.secret,
            req.protocol_version,
            max_queue_size=req.max_queue_size,
            max_retries=req.max_retries,
            retry_backoff=req.retry_backoff,
        )
    except ValueError as exc:
        return {"registered": False, "reason": str(exc)}
    return {"registered": True, "device_id": d.device_id}


@app.put("/devices/{device_id}/policy")
def configure_policy(device_id: str, req: PolicyRequest):
    try:
        d = gateway.configure_policy(
            device_id,
            max_queue_size=req.max_queue_size,
            max_retries=req.max_retries,
            retry_backoff=req.retry_backoff,
        )
    except KeyError:
        return {"updated": False, "reason": "unknown_device"}
    except ValueError as exc:
        return {"updated": False, "reason": str(exc)}
    return {
        "updated": True,
        "max_queue_size": d.max_queue_size,
        "max_retries": d.max_retries,
        "retry_backoff": d.retry_backoff,
    }


@app.post("/devices/{device_id}/connect")
def connect(device_id: str):
    delivered = gateway.connect(device_id)
    return {"online": True, "delivered": [c.command_id for c in delivered]}


@app.post("/devices/{device_id}/disconnect")
def disconnect(device_id: str):
    gateway.disconnect(device_id)
    return {"online": False}


@app.post("/devices/{device_id}/telemetry")
def ingest(device_id: str, req: TelemetryRequest):
    result = gateway.ingest(device_id, req.secret, req.version, req.message)
    return {
        "accepted": result.accepted,
        "reason": result.reason.value if result.reason else None,
        "duplicate": result.duplicate,
    }


@app.post("/devices/{device_id}/commands")
def dispatch(device_id: str, req: DispatchRequest):
    command, reason = gateway.dispatch(device_id, req.name, req.params)
    if command is None:
        return {"dispatched": False, "reason": reason.value if reason else None}
    return {
        "dispatched": True,
        "command_id": command.command_id,
        "status": command.status.value,
    }


@app.post("/devices/{device_id}/commands/{command_id}/ack")
def ack(device_id: str, command_id: str, req: AckRequest):
    result = gateway.ack(device_id, req.secret, command_id)
    return {
        "accepted": result.accepted,
        "reason": result.reason.value if result.reason else None,
        "duplicate": result.duplicate,
    }


@app.post("/devices/{device_id}/commands/{command_id}/result")
def report_result(device_id: str, command_id: str, req: ResultRequest):
    result = gateway.report_result(
        device_id,
        req.secret,
        command_id,
        success=req.success,
        duration=req.duration,
        error_code=req.error_code,
        attempt=req.attempt,
    )
    return {
        "accepted": result.accepted,
        "reason": result.reason.value if result.reason else None,
        "duplicate": result.duplicate,
        "retried": result.retried,
        "status": result.command_status.value if result.command_status else None,
    }


@app.get("/devices/{device_id}/queue")
def queue_status(device_id: str):
    status = gateway.queue_status(device_id)
    if status is None:
        return {"found": False, "reason": "unknown_device"}
    return {
        "found": True,
        "queued": status.queued,
        "capacity": status.capacity,
        "in_flight": status.in_flight,
        "full": status.full,
    }


@app.post("/devices/{device_id}/commands/{command_id}/cancel")
def cancel(device_id: str, command_id: str):
    ok, reason = gateway.cancel(device_id, command_id)
    return {"cancelled": ok, "reason": reason.value if reason else None}


@app.post("/commands/expire")
def expire():
    expired = gateway.expire()
    return {"expired": [c.command_id for c in expired]}


@app.get("/devices/{device_id}/state")
def state(device_id: str):
    device = gateway.store.get_device(device_id)
    if device is None:
        return {"found": False}
    queue = gateway.queue_status(device_id)
    return {
        "found": True,
        "status": device.status.value,
        "protocol_version": device.protocol_version,
        "max_retries": device.max_retries,
        "queue": {
            "queued": queue.queued,
            "capacity": queue.capacity,
            "in_flight": queue.in_flight,
            "full": queue.full,
        },
        "telemetry_count": gateway.store.telemetry_count(device_id),
        "commands": {
            cid: {
                "status": c.status.value,
                "attempt": c.attempt,
                "failed_attempts": list(c.failed_attempts),
                "result": None
                if c.result is None
                else {
                    "success": c.result.success,
                    "error_code": c.result.error_code,
                    "duration": c.result.duration,
                    "attempt": c.result.attempt,
                },
            }
            for cid, c in gateway.store.commands.get(device_id, {}).items()
        },
    }
