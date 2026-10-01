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
    r = gateway.report_result("d1", "s", cmd.command_id, True, 120, 1)
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
    r = gateway.report_result("d1", "s", cmd.command_id, False, 10, 1, None)
    print(f"failure without error_code -> {r.reason}")
    assert not r.accepted and r.reason == RejectReason.INVALID_STATE
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.ACKED


def test_duplicate_same_result_recorded_once(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    gateway.ack("d1", "s", cmd.command_id)
    r1 = gateway.report_result("d1", "s", cmd.command_id, False, 50, 1, "E_BUSY")
    r2 = gateway.report_result("d1", "s", cmd.command_id, False, 50, 1, "E_BUSY")
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
    gateway.report_result("d1", "s", cmd.command_id, True, 10, 1)
    # network retry now delivers a different (failure) verdict for the same command
    r = gateway.report_result("d1", "s", cmd.command_id, False, 10, 1, "E_OTHER")
    print(f"contradicting verdict after SUCCEEDED -> {r.reason}")
    assert not r.accepted and r.reason == RejectReason.CONFLICTING_RESULT
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.SUCCEEDED


def test_result_before_ack_rejected(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    # result arrives while command is still SENT (ack lost/in flight)
    r_sent = gateway.report_result("d1", "s", cmd.command_id, True, 10, 1)
    print(f"result while SENT -> {r_sent.reason}")
    assert not r_sent.accepted and r_sent.reason == RejectReason.RESULT_BEFORE_ACK
    # and while QUEUED (offline, never delivered)
    gateway.register("d2", "s", protocol_version="2.0")
    queued, _ = gateway.dispatch("d2", "reboot")
    r_queued = gateway.report_result("d2", "s", queued.command_id, True, 10, 1)
    print(f"result while QUEUED -> {r_queued.reason}")
    assert not r_queued.accepted and r_queued.reason == RejectReason.RESULT_BEFORE_ACK


def test_result_after_timeout_rejected(gateway, clock):
    gateway.register("d1", "s", protocol_version="2.0")
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    clock.advance(31.0)
    expired = gateway.expire()
    print(f"expired: {[c.command_id for c in expired]}")
    r = gateway.report_result("d1", "s", cmd.command_id, True, 200, 1)
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
    r = gateway.report_result("d1", "s", cmd.command_id, True, 5, 1)
    print(f"result after CANCELLED -> {r.reason}")
    assert not r.accepted and r.reason == RejectReason.RESULT_AFTER_CANCEL
    assert gateway.store.get_command("d1", cmd.command_id).status == CommandStatus.CANCELLED


def test_legacy_v1_ack_is_terminal_and_results_unsupported(gateway, clock):
    gateway.register("old", "s", protocol_version="1.0")
    gateway.connect("old")
    cmd, _ = gateway.dispatch("old", "reboot")
    gateway.ack("old", "s", cmd.command_id)
    # v1.0 client cannot report a result: fixed semantics, never upgraded
    r = gateway.report_result("old", "s", cmd.command_id, True, 1, 1)
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
            gateway.report_result("d1", "s", cid, True, 12, 1),
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


# ---------------------------------------------------------------------------
# Network jitter across retry rounds: a verdict is attributed to the delivery
# attempt it answers (echoed ``attempt``), never to "the currently ACKED
# round". A late repeat of an older round is therefore always an idempotent
# duplicate: it cannot add a record, spend a retry or rewrite the conclusion.
# ---------------------------------------------------------------------------
def _snapshot(gateway, cid):
    c = gateway.store.get_command("d1", cid)
    return c.status.value, c.attempt, [
        (r.attempt, r.success, r.error_code) for r in c.results
    ]


def test_stale_failure_after_redelivery_and_ack_counts_once(gateway, clock):
    """Failure #1 requeued; the retry is redelivered and acked; *then* a
    jitter-delayed repeat of attempt 1's failure arrives, followed by the
    real success of attempt 2. The stale packet must be a duplicate, must
    not trigger another retry, and the command still succeeds once."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    gateway.ack("d1", "s", cid)
    first = gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    print(
        f"input: attempt=1 failure E_BUSY -> accepted={first.accepted} "
        f"retried={first.retried} (basis: first verdict of the ACKED round)"
    )
    assert first.retried

    gateway.disconnect("d1")
    redelivered = gateway.connect("d1")  # reconnect flush -> attempt 2
    print(f"input: reconnect redelivery -> attempt={redelivered[0].attempt}")
    ack2 = gateway.ack("d1", "s", cid)
    print(f"input: ack of attempt 2 -> accepted={ack2.accepted}")

    # the old failure packet finally shows up, answering attempt 1
    stale = gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    print(
        f"input:  LATE repeat attempt=1 failure E_BUSY while round 2 is ACKED\n"
        f"verdict: accepted={stale.accepted} duplicate={stale.duplicate} "
        f"attempt={stale.attempt} current_attempt={stale.current_attempt} "
        f"retried={stale.retried} terminal={stale.terminal}\n"
        f"basis: (command, attempt=1) already has this exact verdict -> "
        f"idempotent duplicate, state untouched"
    )
    assert stale.accepted and stale.duplicate
    assert stale.attempt == 1 and stale.current_attempt == 2
    assert not stale.retried and not stale.terminal
    mid = gateway.store.get_command("d1", cid)
    assert mid.status == CommandStatus.ACKED and mid.attempt == 2
    assert len(mid.results) == 1 and mid.results[0].attempt == 1

    # the genuine result of the current round is still accepted normally
    real = gateway.report_result("d1", "s", cid, True, 50, 2)
    print(
        f"input: real attempt=2 success -> accepted={real.accepted} "
        f"terminal={real.terminal} (basis: first verdict of ACKED round 2)"
    )
    assert real.accepted and real.terminal
    final = gateway.store.get_command("d1", cid)
    print(f"final: {_snapshot(gateway, cid)}")
    assert final.status == CommandStatus.SUCCEEDED
    assert [(r.attempt, r.success) for r in final.results] == [
        (1, False), (2, True)
    ]
    # no phantom third delivery was ever scheduled
    assert gateway.flush("d1") == []


def test_stale_failure_with_single_retry_does_not_exhaust_policy(gateway, clock):
    """max_retries=1: after the only retry is redelivered and acked, the
    repeated attempt-1 failure must not be mistaken for attempt 2's failure
    (which would pre-judge RETRIES_EXHAUSTED); round 2's later success must
    still land normally."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=1)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    gateway.ack("d1", "s", cid)
    only_retry = gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    print(f"input: attempt=1 failure with max_retries=1 -> retried={only_retry.retried}")
    assert only_retry.retried

    gateway.disconnect("d1")
    redelivered = gateway.connect("d1")
    print(f"input: reconnect redelivery -> attempt={redelivered[0].attempt} (the only retry)")
    gateway.ack("d1", "s", cid)

    stale = gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    print(
        f"input:  LATE repeat attempt=1 failure while round 2 is ACKED\n"
        f"verdict: accepted={stale.accepted} duplicate={stale.duplicate} "
        f"terminal={stale.terminal} retried={stale.retried} "
        f"attempt={stale.attempt} current_attempt={stale.current_attempt}\n"
        f"basis: duplicate of round 1 -> retry budget and round 2 untouched"
    )
    assert stale.accepted and stale.duplicate and not stale.terminal
    mid = gateway.store.get_command("d1", cid)
    # the command must NOT have been pre-judged retries_exhausted/failed
    assert mid.status == CommandStatus.ACKED and mid.attempt == 2
    assert len(mid.results) == 1

    real = gateway.report_result("d1", "s", cid, True, 30, 2)
    print(f"input: real attempt=2 success -> accepted={real.accepted} terminal={real.terminal}")
    assert real.accepted and real.terminal
    final = gateway.store.get_command("d1", cid)
    print(f"final: {_snapshot(gateway, cid)}")
    assert final.status == CommandStatus.SUCCEEDED
    assert [r.attempt for r in final.results] == [1, 2]
    assert gateway.flush("d1") == []


def test_stale_verdict_conflicting_with_recorded_round_rejected(gateway, clock):
    """Round 1 failed (E_BUSY, requeued); round 2's real success fixes the
    verdict. A delayed attempt-1 packet carrying a *different* failure
    (E_OTHER) is rejected CONFLICTING_RESULT (same attempt, contradictory
    verdict) -- distinguishable from an identical repeat (duplicate) and
    from the current round's result -- and changes nothing."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    gateway.ack("d1", "s", cid)
    gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    gateway.disconnect("d1")
    gateway.connect("d1")
    gateway.ack("d1", "s", cid)
    current = gateway.report_result("d1", "s", cid, True, 40, 2)
    print(f"input: attempt=2 success fixes verdict -> terminal={current.terminal}")
    assert current.terminal

    weird = gateway.report_result("d1", "s", cid, False, 90, 1, "E_OTHER")
    print(
        f"input:  LATE attempt=1 failure with a DIFFERENT code E_OTHER\n"
        f"verdict: accepted={weird.accepted} reason={weird.reason} "
        f"duplicate={weird.duplicate} attempt={weird.attempt} "
        f"current_attempt={weird.current_attempt} terminal={weird.terminal}\n"
        f"basis: attempt 1 already recorded E_BUSY; same attempt, different "
        f"verdict -> conflicting_result"
    )
    assert not weird.accepted
    assert weird.reason == RejectReason.CONFLICTING_RESULT
    assert not weird.duplicate and weird.attempt == 1

    # the identical old verdict repeated is still a benign duplicate, and a
    # contradictory verdict for the *current* round is rejected too
    same = gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    clash_now = gateway.report_result("d1", "s", cid, False, 40, 2, "E_X")
    print(
        f"basis checks: identical old repeat duplicate={same.duplicate}; "
        f"contradiction on round 2 -> {clash_now.reason}"
    )
    assert same.duplicate
    assert clash_now.reason == RejectReason.CONFLICTING_RESULT

    final = gateway.store.get_command("d1", cid)
    print(f"final: {_snapshot(gateway, cid)}")
    assert final.status == CommandStatus.SUCCEEDED  # conclusion never moved
    assert len(final.results) == 2                  # nothing appended
    assert gateway.flush("d1") == []


def test_stale_duplicate_before_new_round_acked_is_idempotent(gateway, clock):
    """Edge case: round 1 failed and was requeued; round 2 has been
    redelivered but NOT acked yet when attempt 1's verdict repeats. It must
    still be recognized as a duplicate of round 1 and change nothing."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    gateway.ack("d1", "s", cid)
    gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    gateway.disconnect("d1")
    redelivered = gateway.connect("d1")  # round 2 is SENT, not yet acked
    assert redelivered[0].status.value == "sent"

    stale = gateway.report_result("d1", "s", cid, False, 100, 1, "E_BUSY")
    print(
        f"input: LATE attempt=1 repeat while round 2 is SENT (unacked)\n"
        f"verdict: accepted={stale.accepted} duplicate={stale.duplicate} "
        f"attempt={stale.attempt} current_attempt={stale.current_attempt}\n"
        f"basis: round 1 already recorded; attributed by attempt before any "
        f"state-window check"
    )
    assert stale.accepted and stale.duplicate and stale.attempt == 1
    mid = gateway.store.get_command("d1", cid)
    assert mid.status == CommandStatus.SENT and len(mid.results) == 1

    # a *first* verdict for round 2 before its ack is still rejected as
    # before-ack -- the two cases stay distinguishable
    early = gateway.report_result("d1", "s", cid, True, 5, 2)
    print(f"basis check: first attempt=2 verdict before ack -> {early.reason}")
    assert early.reason == RejectReason.RESULT_BEFORE_ACK

    gateway.ack("d1", "s", cid)
    real = gateway.report_result("d1", "s", cid, True, 5, 2)
    assert real.accepted and real.terminal
    print(f"final: {_snapshot(gateway, cid)}")
    final = gateway.store.get_command("d1", cid)
    assert final.status == CommandStatus.SUCCEEDED
    assert [r.attempt for r in final.results] == [1, 2]
    assert gateway.flush("d1") == []


def test_unknown_or_future_attempt_rejected(gateway, clock):
    """A verdict answering a delivery round that does not exist yet (or an
    invalid attempt number) is rejected invalid_state and records nothing."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    gateway.ack("d1", "s", cid)
    for bad in (0, -1, 5):
        r = gateway.report_result("d1", "s", cid, True, 5, bad)
        print(f"input: attempt={bad} -> accepted={r.accepted} reason={r.reason}")
        assert not r.accepted and r.reason == RejectReason.INVALID_STATE
    final = gateway.store.get_command("d1", cid)
    assert final.status == CommandStatus.ACKED and final.results == []
