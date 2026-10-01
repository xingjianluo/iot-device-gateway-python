r"""Execution-result reporting: verdict stability under out-of-order arrival.

v2.0 command lifecycle after delivery::

    SENT --ack--> ACKED --result(success)-----------> SUCCEEDED  (terminal)
                        \--result(failure, retries)--> QUEUED tail -> SENT ...
                        \--result(failure, no retry)-> FAILED     (terminal)
                        \--cancel--------------------> CANCELLED  (terminal)
    SENT --expire--> TIMED_OUT (terminal)
"""
from concurrent.futures import ThreadPoolExecutor

from gateway.models import CommandStatus, RejectReason


def test_success_result_after_ack_is_terminal(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    ack = gateway.ack("d1", "s", cmd.command_id)
    print(f"ack: accepted={ack.accepted} status=ACKED(intermediate for v2.0)")
    r = gateway.report_result("d1", "s", cmd.command_id, True, 120, attempt=1)
    print(f"result: accepted={r.accepted} attempt={r.attempt} terminal={r.terminal}")
    assert r.accepted and r.terminal and r.attempt == 1
    stored = gateway.store.get_command("d1", cmd.command_id)
    assert stored.status == CommandStatus.SUCCEEDED
    assert stored.results[0].duration_ms == 120
    assert stored.results[0].success is True


def test_failure_requires_error_code(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    gateway.ack("d1", "s", cmd.command_id)
    r = gateway.report_result(
        "d1", "s", cmd.command_id, False, 10, attempt=1, error_code=None
    )
    print(f"failure without error_code -> {r.reason}")
    assert not r.accepted and r.reason == RejectReason.INVALID_STATE
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.ACKED


def test_duplicate_same_result_recorded_once(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    gateway.ack("d1", "s", cmd.command_id)
    r1 = gateway.report_result(
        "d1", "s", cmd.command_id, False, 50, attempt=1, error_code="E_BUSY"
    )
    r2 = gateway.report_result(
        "d1", "s", cmd.command_id, False, 50, attempt=1, error_code="E_BUSY"
    )
    print(f"same result twice: r1.accepted={r1.accepted} r2.duplicate={r2.duplicate}")
    assert r1.accepted and not r1.duplicate
    assert r2.accepted and r2.duplicate  # idempotent
    stored = gateway.store.get_command("d1", cmd.command_id)
    assert stored.status == CommandStatus.FAILED  # no retry policy by default
    assert len(stored.results) == 1               # conclusion recorded once


def test_conflicting_result_rejected_and_verdict_unchanged(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    gateway.ack("d1", "s", cmd.command_id)
    gateway.report_result("d1", "s", cmd.command_id, True, 10, attempt=1)
    # network retry now delivers a different (failure) verdict for the same command
    r = gateway.report_result(
        "d1", "s", cmd.command_id, False, 10, attempt=1, error_code="E_OTHER"
    )
    print(f"contradicting verdict after SUCCEEDED -> {r.reason}")
    assert not r.accepted and r.reason == RejectReason.CONFLICTING_RESULT
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.SUCCEEDED


def test_result_before_ack_rejected(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    # result for the current delivered round while command is still SENT
    # (ack lost/in flight)
    r_sent = gateway.report_result(
        "d1", "s", cmd.command_id, True, 10, attempt=1
    )
    print(f"result for round 1 while SENT -> {r_sent.reason}")
    assert not r_sent.accepted and r_sent.reason == RejectReason.RESULT_BEFORE_ACK
    # never-delivered command: a report naming its future round 1 names a
    # round that does not exist yet, distinguishable from before-ack
    gateway.register("d2", "s", protocol_version="2.0")
    queued, _ = gateway.dispatch("d2", "reboot")
    r_queued = gateway.report_result(
        "d2", "s", queued.command_id, True, 10, attempt=1
    )
    print(
        f"result for an undelivered round (attempt=0, report claims 1) "
        f"-> {r_queued.reason}"
    )
    assert not r_queued.accepted
    assert r_queued.reason == RejectReason.UNKNOWN_RESULT_ATTEMPT


def test_result_after_timeout_rejected(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    clock.advance(31.0)
    expired = gateway.expire()
    print(f"expired: {[c.command_id for c in expired]}")
    r = gateway.report_result("d1", "s", cmd.command_id, True, 200, attempt=1)
    print(f"result after TIMED_OUT -> {r.reason}")
    assert not r.accepted and r.reason == RejectReason.RESULT_AFTER_TIMEOUT
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.TIMED_OUT


def test_result_after_cancel_rejected(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    gateway.ack("d1", "s", cmd.command_id)  # v2.0: executing, still cancellable
    ok, _ = gateway.cancel("d1", cmd.command_id)
    print(f"cancel while ACKED(v2.0): ok={ok}")
    assert ok
    r = gateway.report_result("d1", "s", cmd.command_id, True, 5, attempt=1)
    print(f"result after CANCELLED -> {r.reason}")
    assert not r.accepted and r.reason == RejectReason.RESULT_AFTER_CANCEL
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.CANCELLED


def test_legacy_v1_ack_is_terminal_and_results_unsupported(gateway, clock):
    gateway.register("old", "s", protocol_version="1.0")
    gateway.connect("old")
    cmd, _ = gateway.dispatch("old", "reboot")
    gateway.ack("old", "s", cmd.command_id)
    # v1.0 client cannot report a result: fixed semantics, never upgraded
    r = gateway.report_result("old", "s", cmd.command_id, True, 1, attempt=1)
    print(f"v1.0 result report -> {r.reason}")
    assert not r.accepted
    assert r.reason == RejectReason.RESULT_UNSUPPORTED_VERSION
    stored = gateway.store.get_command("old", cmd.command_id)
    assert stored.status == CommandStatus.ACKED  # stays the legacy terminal
    assert stored.results == []
    # a racing cancel cannot overturn the legacy terminal either
    ok, reason = gateway.cancel("old", cmd.command_id)
    print(f"cancel v1.0 ACKED -> ok={ok} reason={reason}")
    assert not ok and reason == RejectReason.INVALID_STATE


def test_ack_result_cancel_expire_interleave_lands_in_one_terminal(gateway, clock):
    """Network jitter: every action races the same command; regardless of
    arrival order exactly one terminal verdict survives and no verdict is
    ever contradictory."""
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    clock.advance(30.0)  # exactly on the ack deadline: expire may win too

    def do_ack(_):
        return ("ack", gateway.ack("d1", "s", cid))

    def do_result(_):
        return (
            "result",
            gateway.report_result("d1", "s", cid, True, 12, attempt=1),
        )

    def do_cancel(_):
        ok, reason = gateway.cancel("d1", cid)
        return ("cancel", (ok, reason))

    def do_expire(_):
        expired = gateway.expire("d1")
        return ("expire", [c.command_id for c in expired])

    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = (
            list(pool.map(do_ack, range(24)))
            + list(pool.map(do_result, range(24)))
            + list(pool.map(do_cancel, range(24)))
            + list(pool.map(do_expire, range(8)))
        )

    final = gateway.store.get_command("d1", cid)
    first_acks = [
        o for kind, o in outcomes
        if kind == "ack" and o.accepted and not o.duplicate
    ]
    winning_results = [
        o for kind, o in outcomes
        if kind == "result" and o.accepted and not o.duplicate
    ]
    dup_results = [
        o for kind, o in outcomes
        if kind == "result" and o.duplicate
    ]
    winning_cancels = [
        o for kind, o in outcomes if kind == "cancel" and o[0]
    ]
    expires_won = [
        ids for kind, ids in outcomes if kind == "expire" and cid in ids
    ]
    rejected_result_reasons = {
        o.reason
        for kind, o in outcomes
        if kind == "result" and not o.accepted
    }
    print(
        f"final={final.status.value} first_acks={len(first_acks)} "
        f"winning_results={len(winning_results)} dup_results={len(dup_results)} "
        f"winning_cancels={len(winning_cancels)} expires_won={len(expires_won)}\n"
        f"rejected result reasons: {[r.value for r in rejected_result_reasons]}\n"
        f"recorded results: {[(r.attempt, r.success) for r in final.results]}"
    )

    assert final.status in (
        CommandStatus.SUCCEEDED,
        CommandStatus.CANCELLED,
        CommandStatus.TIMED_OUT,
    )
    assert len(first_acks) <= 1
    if final.status == CommandStatus.SUCCEEDED:
        assert len(first_acks) == 1
        assert len(winning_results) == 1  # exactly one result fixes the verdict
        assert len(winning_cancels) == 0 and not expires_won
        assert len(final.results) == 1
    elif final.status == CommandStatus.CANCELLED:
        assert len(winning_results) == 0
        assert len(winning_cancels) == 1
        assert final.results == []
        assert rejected_result_reasons <= {
            RejectReason.RESULT_BEFORE_ACK,
            RejectReason.RESULT_AFTER_CANCEL,
        }
    else:  # TIMED_OUT
        assert len(winning_results) == 0
        assert len(winning_cancels) == 0
        assert len(expires_won) == 1
        assert final.results == []
        assert rejected_result_reasons <= {
            RejectReason.RESULT_BEFORE_ACK,
            RejectReason.RESULT_AFTER_TIMEOUT,
        }
    # duplicate echoes never change the fixed conclusion
    for o in dup_results:
        assert o.terminal and final.status == CommandStatus.SUCCEEDED
