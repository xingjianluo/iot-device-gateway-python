"""Local JSON persistence: state survives a gateway restart."""

from gateway.models import CommandStatus
from gateway.service import Gateway
from tests.conftest import FakeClock


def test_state_survives_restart(tmp_path):
    path = tmp_path / "state.json"
    clock = FakeClock()
    g1 = Gateway(persist_path=path, clock=clock)
    g1.register("d1", "s", protocol_version="2.0")
    g1.ingest("d1", "s", "2.0", {"message_id": "m1", "timestamp": clock.now, "seq": 1})
    g1.dispatch("d1", "reboot")  # offline -> QUEUED

    g2 = Gateway(persist_path=path, clock=clock)  # "restart"
    device = g2.store.get_device("d1")
    print(
        f"after restart: device={device.device_id} v={device.protocol_version} "
        f"telemetry={g2.store.telemetry_count('d1')} "
        f"commands={[(c.command_id, c.status.value) for c in g2.store.commands['d1'].values()]}"
    )
    assert device is not None and device.protocol_version == "2.0"
    assert g2.store.telemetry_count("d1") == 1
    cmd = next(iter(g2.store.commands["d1"].values()))
    assert cmd.status == CommandStatus.QUEUED

    # queued command is still delivered FIFO after the restart
    delivered = g2.connect("d1")
    assert [c.command_id for c in delivered] == [cmd.command_id]

    # duplicate telemetry is still deduplicated after the restart
    r = g2.ingest(
        "d1", "s", "2.0", {"message_id": "m1", "timestamp": clock.now, "seq": 1}
    )
    print(f"re-ingest m1 after restart: duplicate={r.duplicate}")
    assert r.duplicate
    assert g2.store.telemetry_count("d1") == 1
