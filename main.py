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


class TelemetryRequest(BaseModel):
    secret: str
    version: str = "1.0"
    message: dict


class DispatchRequest(BaseModel):
    name: str
    params: dict = {}


class AckRequest(BaseModel):
    secret: str


@app.get("/")
def read_root():
    return {"service": "iot-device-gateway", "docs": "/docs"}


@app.post("/devices")
def register(req: RegisterRequest):
    try:
        d = gateway.register(req.device_id, req.secret, req.protocol_version)
    except ValueError as exc:
        return {"registered": False, "reason": str(exc)}
    return {"registered": True, "device_id": d.device_id}


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
    return {"dispatched": True, "command_id": command.command_id, "status": command.status.value}


@app.post("/devices/{device_id}/commands/{command_id}/ack")
def ack(device_id: str, command_id: str, req: AckRequest):
    result = gateway.ack(device_id, req.secret, command_id)
    return {
        "accepted": result.accepted,
        "reason": result.reason.value if result.reason else None,
        "duplicate": result.duplicate,
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
        "telemetry_count": gateway.store.telemetry_count(device_id),
        "commands": {
            cid: c.status.value
            for cid, c in gateway.store.commands.get(device_id, {}).items()
        },
    }
