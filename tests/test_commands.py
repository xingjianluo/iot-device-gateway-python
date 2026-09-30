"""Command lifecycle: offline queue, FIFO redelivery, ack/cancel/timeout."""
from concurrent.futures import ThreadPoolExecutor

from gateway.models import CommandStatus, RejectReason


def test_dispatch_offline_queues_then_flush_fifo(gateway, clock):
    gateway.register("d1", "s")
    ids = []
    for name in ["reboot", "config", "ota"]:
        cmd, _reason = gateway.dispatch("d1", name)
        print(f"offline dispatch {name}: status={cmd.status.value}")
        assert cmd.status == CommandStatus.QUEUED
        ids.append(cmd.command_id)

    delivered = gateway.connect("d1")  # reconnect flushes the queue
    print(f"flush order: {[c.name for c in delivered]}")
    assert [c.command_id for c in delivered] == ids  # FIFO preserved
    assert all(c.status == CommandStatus.SENT for c in delivered)

    # reconnecting again must NOT redeliver (no duplicate execution)
    gateway.disconnect("d1")
    redelivered = gateway.connect("d1")
    print(f"second reconnect delivered: {redelivered}")
    assert redelivered == []


def test_dispatch_online_delivers_immediately(gateway, clock):
    gateway.register("d1", "s")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    print(f"online dispatch: status={cmd.status.value} deadline={cmd.ack_deadline}")
    assert cmd.status == CommandStatus.SENT
    assert cmd.ack_deadline == clock.now + 30.0


def test_ack_happy_path_and_duplicate_ack(gateway, clock):
    gateway.register("d1", "s")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    r1 = gateway.ack("d1", "s", cmd.command_id)
    r2 = gateway.ack("d1", "s", cmd.command_id)
    print(f"ack1: accepted={r1.accepted} dup={r1.duplicate}; "
          f"ack2: accepted={r2.accepted} dup={r2.duplicate}")
    assert r1.accepted and not r1.duplicate
    assert r2.duplicate  # idempotent: state unchanged
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.ACKED


def test_ack_unknown_command_and_bad_secret(gateway, clock):
    gateway.register("d1", "s")
    r_unknown = gateway.ack("d1", "s", "cmd-999")
    r_badauth = gateway.ack("d1", "nope", "cmd-999")
    print(f"unknown cmd -> {r_unknown.reason}; bad secret -> {r_badauth.reason}")
    assert r_unknown.reason == RejectReason.UNKNOWN_COMMAND
    assert r_badauth.reason == RejectReason.INVALID_CREDENTIALS


def test_timeout_transition(gateway, clock):
    gateway.register("d1", "s")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    clock.advance(31.0)  # past the 30s ack deadline
    expired = gateway.expire()
    print(f"expired after deadline: {[c.command_id for c in expired]}")
    assert [c.command_id for c in expired] == [cmd.command_id]
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.TIMED_OUT
    # ack after timeout is rejected with a distinguishable reason
    r = gateway.ack("d1", "s", cmd.command_id)
    print(f"ack after timeout -> accepted={r.accepted} reason={r.reason}")
    assert not r.accepted and r.reason == RejectReason.INVALID_STATE


def test_cancel_queued_and_terminal_rejection(gateway, clock):
    gateway.register("d1", "s")
    cmd, _ = gateway.dispatch("d1", "reboot")  # offline -> QUEUED
    ok, _ = gateway.cancel("d1", cmd.command_id)
    print(f"cancel queued: ok={ok}")
    assert ok
    ok2, reason2 = gateway.cancel("d1", cmd.command_id)  # already terminal
    print(f"cancel again: ok={ok2} reason={reason2}")
    assert not ok2 and reason2 == RejectReason.INVALID_STATE
    # cancelled command is never delivered on reconnect
    assert gateway.connect("d1") == []


def test_concurrent_ack_and_cancel_do_not_corrupt_state(gateway, clock):
    gateway.register("d1", "s")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    with ThreadPoolExecutor(max_workers=8) as pool:
        acks = pool.map(lambda _: gateway.ack("d1", "s", cmd.command_id), range(32))
        cancels = pool.map(lambda _: gateway.cancel("d1", cmd.command_id), range(32))
        ack_results = list(acks)
        list(cancels)
    final = gateway.store.get_command("d1", cmd.command_id)
    wins = sum(1 for r in ack_results if r.accepted and not r.duplicate)
    print(f"32 acks vs 32 cancels: first-acks={wins} final={final.status.value}")
    # exactly one transition wins; the state is a valid terminal state
    assert final.status in (CommandStatus.ACKED, CommandStatus.CANCELLED)
    if final.status == CommandStatus.ACKED:
        assert wins == 1
    else:
        assert wins == 0


def test_concurrent_dispatch_same_device_all_tracked(gateway, clock):
    gateway.register("d1", "s")
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda i: gateway.dispatch("d1", f"op{i}"), range(50)))
    cmds = [c for c, _ in results]
    ids = {c.command_id for c in cmds}
    print(f"50 concurrent dispatches: unique ids={len(ids)}")
    assert len(ids) == 50
    delivered = gateway.connect("d1")
    assert len(delivered) == 50  # all queued commands flushed exactly once
