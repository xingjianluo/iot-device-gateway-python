"""Per-device offline queue capacity: boundary rejection, occupancy query,
policy change, and persistence of retry/result state across restart."""

from gateway.models import CommandStatus, RejectReason
from gateway.service import Gateway
from tests.conftest import FakeClock


def test_capacity_boundary_rejects_only_the_overflow(gateway):
    gateway.register("d1", "s", max_queue_size=3)
    accepted = []
    for i in range(3):
        cmd, reason = gateway.dispatch("d1", f"op{i}")
        accepted.append((cmd, reason))
    print(f"3 dispatches into capacity 3: {[(c.command_id, r) for c, r in accepted]}")
    assert all(r is None for _, r in accepted)

    overflow, reason = gateway.dispatch("d1", "op3")
    print(f"4th dispatch -> command={overflow} reason={reason}")
    assert overflow is None and reason == RejectReason.QUEUE_FULL

    qs = gateway.queue_status("d1")
    print(
        f"occupancy: queued={qs.queued}/{qs.capacity} full={qs.full} in_flight={qs.in_flight}"
    )
    assert qs.queued == 3 and qs.full and qs.in_flight == 0

    # queued commands are NOT dropped: all three flush in FIFO
    delivered = gateway.connect("d1")
    print(f"flush after overflow: {[c.name for c in delivered]}")
    assert [c.name for c in delivered] == ["op0", "op1", "op2"]


def test_capacity_frees_after_flush_then_accepts_again(gateway):
    gateway.register("d1", "s", max_queue_size=1)
    gateway.dispatch("d1", "first")
    _, reason = gateway.dispatch("d1", "second")
    print(f"before flush, overflow reason={reason}")
    assert reason == RejectReason.QUEUE_FULL
    gateway.connect("d1")  # first is delivered -> queue drains
    # device is online: delivery is immediate, capacity does not block it
    online_cmd, reason = gateway.dispatch("d1", "online-op")
    print(
        f"online dispatch after drain -> status={online_cmd.status.value} reason={reason}"
    )
    assert reason is None and online_cmd.status == CommandStatus.SENT

    # go offline again; capacity is available once more
    gateway.disconnect("d1")
    queued_cmd, reason = gateway.dispatch("d1", "queued-again")
    print(
        f"offline dispatch after drain -> status={queued_cmd.status.value} reason={reason}"
    )
    assert reason is None and queued_cmd.status == CommandStatus.QUEUED
    qs = gateway.queue_status("d1")
    assert qs.queued == 1 and qs.full
    # the in-flight commands from the previous connection settled/remain
    # tracked, but only QUEUED commands consume capacity
    assert qs.in_flight >= 0


def test_online_dispatch_never_counts_against_queue(gateway):
    gateway.register("d1", "s", max_queue_size=0)  # zero offline capacity
    gateway.connect("d1")
    cmd, reason = gateway.dispatch("d1", "immediate")
    print(f"capacity=0 online dispatch -> status={cmd.status.value} reason={reason}")
    assert reason is None and cmd.status == CommandStatus.SENT
    gateway.disconnect("d1")
    _, offline_reason = gateway.dispatch("d1", "blocked")
    print(f"capacity=0 offline dispatch -> reason={offline_reason}")
    assert offline_reason == RejectReason.QUEUE_FULL


def test_configure_policy_shrinks_capacity_without_dropping(gateway):
    gateway.register("d1", "s", max_queue_size=5)
    for i in range(4):
        gateway.dispatch("d1", f"op{i}")
    gateway.configure_policy("d1", max_queue_size=2)  # shrink below occupancy
    qs = gateway.queue_status("d1")
    print(f"shrink 5->2 with 4 waiting: queued={qs.queued} full={qs.full}")
    assert qs.queued == 4 and qs.full  # nothing dropped; new work blocked
    _, reason = gateway.dispatch("d1", "op4")
    assert reason == RejectReason.QUEUE_FULL
    delivered = gateway.connect("d1")
    assert len(delivered) == 4  # all waiting work preserved
    qs2 = gateway.queue_status("d1")
    print(f"after drain: queued={qs2.queued} full={qs2.full}")
    assert qs2.queued == 0 and not qs2.full


def test_in_flight_commands_excluded_from_queue_occupancy(gateway):
    gateway.register("d1", "s", protocol_version="2.0", max_queue_size=2)
    gateway.connect("d1")
    sent, _ = gateway.dispatch("d1", "sent")
    gateway.ack("d1", "s", sent.command_id)  # ACKED: still in flight
    gateway.disconnect("d1")
    q1, _ = gateway.dispatch("d1", "q1")
    q2, _ = gateway.dispatch("d1", "q2")
    _, reason = gateway.dispatch("d1", "q3")
    qs = gateway.queue_status("d1")
    print(
        f"1 acked in flight + 2 queued (capacity 2): "
        f"queued={qs.queued} in_flight={qs.in_flight} overflow={reason}"
    )
    assert qs.queued == 2 and qs.in_flight == 1
    assert reason == RejectReason.QUEUE_FULL
    # finishing the in-flight command does not touch the waiting ones
    r = gateway.report_result("d1", "s", sent.command_id, success=True, duration=1.0)
    assert r.command_status == CommandStatus.SUCCEEDED
    assert [c.command_id for c in gateway.store.queued_commands("d1")] == [
        q1.command_id,
        q2.command_id,
    ]


def test_queue_status_unknown_device(gateway):
    assert gateway.queue_status("ghost") is None
    print("queue_status for unknown device -> None")


def test_retry_and_result_state_survive_restart(tmp_path):
    path = tmp_path / "state.json"
    clock = FakeClock()
    g1 = Gateway(persist_path=path, clock=clock)
    g1.register(
        "d1",
        "s",
        protocol_version="2.0",
        max_queue_size=7,
        max_retries=2,
        retry_backoff=0.0,
    )
    g1.connect("d1")
    cmd, _ = g1.dispatch("d1", "ota")
    g1.ack("d1", "s", cmd.command_id)
    g1.report_result(
        "d1",
        "s",
        cmd.command_id,
        success=False,
        duration=0.5,
        error_code="EIO",
        attempt=1,
    )
    # the retry is in flight immediately; settle it as success
    g1.ack("d1", "s", cmd.command_id)
    g1.report_result("d1", "s", cmd.command_id, success=True, duration=1.25, attempt=2)

    g2 = Gateway(persist_path=path, clock=clock)  # "restart"
    device = g2.store.get_device("d1")
    restored = g2.store.get_command("d1", cmd.command_id)
    print(
        f"after restart: policy queue={device.max_queue_size} retries={device.max_retries}; "
        f"cmd status={restored.status.value} attempt={restored.attempt} "
        f"failed={restored.failed_attempts} duration={restored.result.duration}"
    )
    assert device.max_queue_size == 7 and device.max_retries == 2
    assert restored.status == CommandStatus.SUCCEEDED
    assert restored.attempt == 2 and restored.failed_attempts == [1]
    assert restored.result.success and restored.result.duration == 1.25
    assert g2.queue_status("d1").queued == 0


def test_failed_terminal_state_survives_restart(tmp_path):
    path = tmp_path / "state.json"
    clock = FakeClock()
    g1 = Gateway(persist_path=path, clock=clock)
    g1.register("d1", "s", protocol_version="2.0", max_retries=0)
    g1.connect("d1")
    cmd, _ = g1.dispatch("d1", "ota")
    g1.ack("d1", "s", cmd.command_id)
    g1.report_result(
        "d1",
        "s",
        cmd.command_id,
        success=False,
        duration=0.2,
        error_code="EBAD",
        attempt=1,
    )

    g2 = Gateway(persist_path=path, clock=clock)
    restored = g2.store.get_command("d1", cmd.command_id)
    print(
        f"FAILED after restart: status={restored.status.value} error={restored.result.error_code}"
    )
    assert restored.status == CommandStatus.FAILED
    assert restored.result.error_code == "EBAD"
