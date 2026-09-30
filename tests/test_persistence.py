"""Local JSON persistence: state survives a gateway restart."""
from gateway.models import CommandStatus
from gateway.service import Gateway
from tests.conftest import FakeClock


def test_state_survives_restart(tmp_path):
    path = tmp_path / "state.json"
    clock = FakeClock()
    g1 = Gateway(persist_path=path, clock=clock)
    g1.register("d1", "s", protocol_version="2.0")
    g1.ingest("d1", "s", "2.0",
              {"message_id": "m1", "timestamp": clock.now, "seq": 1})
    g1.dispatch("d1", "reboot")  # offline -> QUEUED

    g2 = Gateway(persist_path=path, clock=clock)  # "restart"
    device = g2.store.get_device("d1")
    print(f"after restart: device={device.device_id} v={device.protocol_version} "
          f"telemetry={g2.store.telemetry_count('d1')} "
          f"commands={[(c.command_id, c.status.value) for c in g2.store.commands['d1'].values()]}")
    assert device is not None and device.protocol_version == "2.0"
    assert g2.store.telemetry_count("d1") == 1
    cmd = next(iter(g2.store.commands["d1"].values()))
    assert cmd.status == CommandStatus.QUEUED

    # queued command is still delivered FIFO after the restart
    delivered = g2.connect("d1")
    assert [c.command_id for c in delivered] == [cmd.command_id]

    # duplicate telemetry is still deduplicated after the restart
    r = g2.ingest("d1", "s", "2.0",
                  {"message_id": "m1", "timestamp": clock.now, "seq": 1})
    print(f"re-ingest m1 after restart: duplicate={r.duplicate}")
    assert r.duplicate
    assert g2.store.telemetry_count("d1") == 1


def test_results_attempts_and_retry_queue_survive_restart(tmp_path):
    path = tmp_path / "cmd.json"
    clock = FakeClock()
    g1 = Gateway(persist_path=path, clock=clock)
    g1.register("d1", "s", protocol_version="2.0",
                max_retries=2, queue_capacity=4)
    g1.connect("d1")
    cmd, _ = g1.dispatch("d1", "reboot")
    g1.ack("d1", "s", cmd.command_id)
    g1.report_result("d1", "s", cmd.command_id, False, 120, "E_BUSY")  # requeued
    # a terminal success on a second command survives with its result record
    other, _ = g1.dispatch("d1", "config")
    g1.ack("d1", "s", other.command_id)
    g1.report_result("d1", "s", other.command_id, True, 15)

    g2 = Gateway(persist_path=path, clock=clock)  # "restart"
    device = g2.store.get_device("d1")
    assert device.max_retries == 2 and device.queue_capacity == 4
    reboot = g2.store.get_command("d1", cmd.command_id)
    print(
        f"after restart: reboot status={reboot.status.value} "
        f"attempt={reboot.attempt} results={[(r.attempt, r.success, r.error_code) for r in reboot.results]}"
    )
    assert reboot.status == CommandStatus.QUEUED and reboot.attempt == 1
    assert reboot.results[0].error_code == "E_BUSY"
    assert reboot.results[0].duration_ms == 120

    config = g2.store.get_command("d1", other.command_id)
    assert config.status == CommandStatus.SUCCEEDED
    assert config.results[0].success is True

    # retry still redelivers FIFO after restart, as attempt 2
    delivered = g2.connect("d1")
    print(f"post-restart flush: {[(c.command_id, c.attempt) for c in delivered]}")
    assert [(c.command_id, c.attempt) for c in delivered] == [
        (cmd.command_id, 2)
    ]
