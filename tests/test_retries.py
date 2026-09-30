"""Failure-driven retries: bounded, ordered, distinguishable from the first run."""
from gateway.models import CommandStatus, RejectReason
from gateway.service import Gateway
from tests.conftest import FakeClock


def test_failure_requeued_and_redelivered_with_incremented_attempt(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    assert cmd.attempt == 1
    gateway.ack("d1", "s", cid)

    # first failure -> retry remains; command goes back to the queue tail
    r1 = gateway.report_result("d1", "s", cid, False, 100, "E_BUSY")
    print(f"failure #1: accepted={r1.accepted} retried={r1.retried} attempt={r1.attempt}")
    assert r1.accepted and r1.retried and r1.attempt == 1
    stored = gateway.store.get_command("d1", cid)
    assert stored.status == CommandStatus.QUEUED
    assert stored.attempt == 1  # not incremented until redelivery

    # redelivery (on reconnect) is attempt 2 of the SAME command id
    gateway.disconnect("d1")
    delivered = gateway.connect("d1")
    print(f"redelivered: {[(c.command_id, c.attempt) for c in delivered]}")
    assert [c.command_id for c in delivered] == [cid]  # no duplication
    assert delivered[0].attempt == 2
    gateway.ack("d1", "s", cid)
    r2 = gateway.report_result("d1", "s", cid, False, 80, "E_BUSY")
    assert r2.retried and r2.attempt == 2

    # attempt 3 (last allowed): another failure exhausts the policy
    gateway.disconnect("d1")
    redelivered = gateway.connect("d1")
    assert redelivered[0].attempt == 3
    gateway.ack("d1", "s", cid)
    r3 = gateway.report_result("d1", "s", cid, False, 60, "E_DEAD")
    print(f"failure #3: terminal={r3.terminal} retried={r3.retried}")
    assert r3.terminal and not r3.retried
    final = gateway.store.get_command("d1", cid)
    assert final.status == CommandStatus.RETRIES_EXHAUSTED
    assert [a.attempt for a in final.results] == [1, 2, 3]
    assert [a.error_code for a in final.results] == ["E_BUSY", "E_BUSY", "E_DEAD"]


def test_success_after_retry_succeeds_within_cap(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0", max_retries=3)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "ota")
    cid = cmd.command_id
    for attempt in (1, 2):
        gateway.ack("d1", "s", cid)
        r = gateway.report_result("d1", "s", cid, False, 10, "E_AGAIN")
        assert r.attempt == attempt and r.retried
        gateway.flush("d1")
    gateway.ack("d1", "s", cid)
    r = gateway.report_result("d1", "s", cid, True, 42)
    print(f"success on attempt {r.attempt} -> terminal={r.terminal}")
    assert r.attempt == 3 and r.terminal
    assert gateway.store.get_command("d1", cid).status == CommandStatus.SUCCEEDED
    # nothing left queued
    assert gateway.flush("d1") == []


def test_no_retry_policy_fails_terminal_without_requeue(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")  # default max_retries=0
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    gateway.ack("d1", "s", cmd.command_id)
    r = gateway.report_result("d1", "s", cmd.command_id, False, 5, "E_X")
    print(f"max_retries=0 failure: terminal={r.terminal} retried={r.retried}")
    assert r.terminal and not r.retried
    final = gateway.store.get_command("d1", cmd.command_id)
    assert final.status == CommandStatus.FAILED  # distinct from exhausted
    assert final.results[0].error_code == "E_X"
    assert gateway.flush("d1") == []


def test_retry_goes_behind_already_pending_commands(gateway, clock):
    """A retried command must not cover or jump ahead of other queued work."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=1)
    gateway.connect("d1")
    first, _ = gateway.dispatch("d1", "op-A")
    gateway.ack("d1", "s", first.command_id)
    # A fails while online: retry is queued, but B is dispatched offline
    # afterwards and must queue behind A's retry.
    r = gateway.report_result("d1", "s", first.command_id, False, 9, "E_X")
    assert r.retried
    gateway.disconnect("d1")
    queued_d, _ = gateway.dispatch("d1", "op-D")
    print(
        "queue while A retried, D newly queued: "
        f"{[c.command_id for c in gateway.store.queued_commands('d1')]}"
    )
    delivered = gateway.connect("d1")
    print(f"flush order: {[c.command_id for c in delivered]}")
    assert [c.command_id for c in delivered] == [
        first.command_id, queued_d.command_id
    ]
    assert delivered[0].attempt == 2  # A's retry, not a fresh execution


def test_capacity_full_and_retry_interplay_keeps_order_no_loss(gateway, clock):
    """capacity=2: new commands rejected at the boundary; an admitted retry
    survives the same full queue and is redelivered exactly once."""
    gateway.register(
        "d1", "s", protocol_version="2.0", max_retries=1, queue_capacity=2
    )
    # offline: two commands fill the queue exactly, the third is rejected
    a, _ = gateway.dispatch("d1", "op-A")
    b, _ = gateway.dispatch("d1", "op-B")
    c, reason_c = gateway.dispatch("d1", "op-C")
    print(f"offline admits A,B; C rejected with {reason_c}")
    assert c is None and reason_c == RejectReason.QUEUE_FULL
    queued = [x.command_id for x in gateway.store.queued_commands("d1")]
    assert queued == [a.command_id, b.command_id]  # nothing silently dropped

    delivered = gateway.connect("d1")  # A,B both SENT now
    assert [x.command_id for x in delivered] == [a.command_id, b.command_id]

    # A fails -> one retry requeued (bypasses admission: already accepted)
    gateway.ack("d1", "s", a.command_id)
    r = gateway.report_result("d1", "s", a.command_id, False, 3, "E_X")
    print(f"A retry requeued at full capacity: retried={r.retried}")
    assert r.retried
    # while A waits, a new command for the (online) device is delivered
    # immediately and does not occupy the QUEUED slot; the retry survives.
    print(f"queue while retry waits: "
          f"{[x.command_id for x in gateway.store.queued_commands('d1')]}")
    assert gateway.queue_status("d1").queued == 1

    # disconnect: the retry still occupies 1 of the 2 slots, exactly one
    # more new command fits, a further one is QUEUE_FULL -- retry not lost
    gateway.disconnect("d1")
    d, _ = gateway.dispatch("d1", "op-D")
    e, reason_e = gateway.dispatch("d1", "op-E")
    print(f"after reconnect queue: D admitted, E -> {reason_e}")
    assert e is None and reason_e == RejectReason.QUEUE_FULL
    order = gateway.connect("d1")
    print(f"flush order: {[x.command_id for x in order]}")
    assert [x.command_id for x in order] == [a.command_id, d.command_id]
    retry = order[0]
    assert retry.attempt == 2  # distinguishable retry, executed once
    assert gateway.store.get_command("d1", b.command_id).status == CommandStatus.SENT


def test_stale_duplicate_of_old_attempt_after_requeue_changes_nothing(gateway, clock):
    """Failure #1 was recorded once and requeued. A jitter-delivered repeat
    of that old verdict arrives while the retry is QUEUED: it is rejected
    (only the ACKED attempt accepts results), recorded zero extra times."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    gateway.ack("d1", "s", cid)
    first = gateway.report_result("d1", "s", cid, False, 100, "E_BUSY")
    assert first.retried
    # command is now QUEUED for attempt 2; old verdict's duplicate shows up
    stale = gateway.report_result("d1", "s", cid, False, 100, "E_BUSY")
    print(f"stale duplicate while QUEUED -> accepted={stale.accepted} reason={stale.reason}")
    assert not stale.accepted and stale.reason == RejectReason.RESULT_BEFORE_ACK
    stored = gateway.store.get_command("d1", cid)
    assert stored.status == CommandStatus.QUEUED
    assert len(stored.results) == 1 and stored.results[0].attempt == 1


def test_legacy_v1_never_retried_even_with_policy(gateway, clock):
    """Mixed-version fleet: a v1.0 device cannot report failure, so with the
    same per-device policy its ACKED stays the single fixed verdict."""
    gateway.register("old", "s", protocol_version="1.0", max_retries=5)
    gateway.connect("old")
    cmd, _ = gateway.dispatch("old", "reboot")
    gateway.ack("old", "s", cmd.command_id)
    r = gateway.report_result("old", "s", cmd.command_id, False, 1, "E_X")
    print(f"v1.0 failure report -> {r.reason}; status stays ACKED")
    assert r.reason == RejectReason.RESULT_UNSUPPORTED_VERSION
    stored = gateway.store.get_command("old", cmd.command_id)
    assert stored.status == CommandStatus.ACKED and stored.results == []
    assert gateway.flush("old") == []  # nothing requeued, nothing redelivered


def test_policy_configuration_update_and_persistence(tmp_path):
    path = tmp_path / "s.json"
    clock = FakeClock()
    g1 = Gateway(persist_path=path, clock=clock)
    g1.register("d1", "s", protocol_version="2.0")
    g1.configure_policy("d1", max_retries=4, queue_capacity=8)
    g2 = Gateway(persist_path=path, clock=clock)
    d = g2.store.get_device("d1")
    print(f"after restart: max_retries={d.max_retries} capacity={d.queue_capacity}")
    assert d.max_retries == 4 and d.queue_capacity == 8
