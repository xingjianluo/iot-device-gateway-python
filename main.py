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
    max_retries: int = 0
    queue_capacity: int | None = None


class PolicyRequest(BaseModel):
    max_retries: int | None = None
    queue_capacity: int | None = None


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
    # delivery attempt this verdict answers (echoed from the dispatch);
    # required so late/duplicate results are attributed to the right round
    attempt: int
    success: bool
    duration_ms: int
    error_code: str | None = None


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
            max_retries=req.max_retries,
            queue_capacity=req.queue_capacity,
        )
    except ValueError as exc:
        return {"registered": False, "reason": str(exc)}
    return {"registered": True, "device_id": d.device_id}


@app.post("/devices/{device_id}/policy")
def configure_policy(device_id: str, req: PolicyRequest):
    try:
        d = gateway.configure_policy(
            device_id, req.max_retries, req.queue_capacity
        )
    except KeyError:
        return {"updated": False, "reason": "unknown_device"}
    except ValueError as exc:
        return {"updated": False, "reason": str(exc)}
    return {
        "updated": True,
        "max_retries": d.max_retries,
        "queue_capacity": d.queue_capacity,
    }


@app.post("/devices/{device_id}/connect")
def connect(device_id: str):
    delivered = gateway.connect(device_id)
    return {"online": True, "delivered": [c.command_id for c in delivered]}


@app.post("/devices/{device_id}/disconnect")
def disconnect(device_id: str):
    gateway.disconnect(device_id)
    return {"online": False}


@app.post("/devices/{device_id}/flush")
def flush(device_id: str):
    delivered = gateway.flush(device_id)
    return {
        "delivered": [
            {"command_id": c.command_id, "attempt": c.attempt} for c in delivered
        ]
    }


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
        # echo on the /result report so verdicts are attributed to this
        # delivery round (robust against retries and jittered re-delivery)
        "attempt": command.attempt,
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
        req.success,
        req.duration_ms,
        req.attempt,
        req.error_code,
    )
    return {
        "accepted": result.accepted,
        "reason": result.reason.value if result.reason else None,
        "duplicate": result.duplicate,
        "attempt": result.attempt,
        "current_attempt": result.current_attempt,
        "terminal": result.terminal,
        "retried": result.retried,
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
    return {
        "found": True,
        "status": device.status.value,
        "protocol_version": device.protocol_version,
        "max_retries": device.max_retries,
        "queue_capacity": device.queue_capacity,
        "telemetry_count": gateway.store.telemetry_count(device_id),
        "queue": {
            "queued": gateway.queue_status(device_id).queued,
            "full": gateway.queue_status(device_id).full,
        },
        "commands": {
            cid: {
                "status": c.status.value,
                "attempt": c.attempt,
                "results": [
                    {
                        "attempt": r.attempt,
                        "success": r.success,
                        "duration_ms": r.duration_ms,
                        "error_code": r.error_code,
                    }
                    for r in c.results
                ],
            }
            for cid, c in gateway.store.commands.get(device_id, {}).items()
        },
    }
