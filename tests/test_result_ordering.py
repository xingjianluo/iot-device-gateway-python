r"""Stale/duplicate execution results across retry rounds.

Bug being locked down: every report_result names the delivery *round*
(``attempt``) it answers. A result belongs to its round, not to the command's
current lifecycle state, so a jitter-delayed retransmission of an old round's
verdict is recognised however late it shows up and at whatever stage
(QUEUED / SENT / ACKED / terminal):

* identical verdict for a recorded round  -> accepted duplicate, nothing moves
* different verdict for a recorded round  -> CONFLICTING_RESULT, nothing moves
* verdict for an undelivered round        -> UNKNOWN_RESULT_ATTEMPT

It can never consume the retry budget, trigger another requeue/execution, or
replace the current round's verdict. The current round's own result is always
accepted exactly once. Any command ends with exactly one definite conclusion.
"""
from gateway.models import CommandStatus, RejectReason


def _show(label, r):
    print(
        f"{label}: accepted={r.accepted} duplicate={r.duplicate} "
        f"reason={r.reason.value if r.reason else None} "
        f"attempt={r.attempt} retried={r.retried} terminal={r.terminal}"
    )


def test_failure_requeued_redelivered_acked_then_old_failure_duplicate_arrives(gateway):
    """Scenario 1: round 1 failed and was requeued; round 2 was redelivered
    and acked; only THEN the (identical) round-1 failure shows up again.
    It must be a duplicate, not round 2's new verdict. Round 2 then succeeds
    and the command is executed exactly twice (one retry), no more."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id

    gateway.ack("d1", "s", cid)  # round 1 executing
    f1 = gateway.report_result(
        "d1", "s", cid, False, 100, attempt=1, error_code="E_BUSY"
    )
    _show("round1 failure", f1)
    assert f1.accepted and f1.retried and f1.attempt == 1

    # reconnect re-delivers the SAME command id as round 2; device acks it
    gateway.disconnect("d1")
    redelivered = gateway.connect("d1")
    assert [c.command_id for c in redelivered] == [cid]
    assert redelivered[0].attempt == 2
    gateway.ack("d1", "s", cid)

    # the old round-1 failure retransmitted by the network arrives NOW
    stale = gateway.report_result(
        "d1", "s", cid, False, 100, attempt=1, error_code="E_BUSY"
    )
    _show("stale round1 failure while round2 ACKED", stale)
    assert stale.accepted and stale.duplicate and stale.attempt == 1
    assert not stale.retried and not stale.terminal

    stored = gateway.store.get_command("d1", cid)
    print(
        f"state after stale echo: status={stored.status.value} "
        f"results={[(r.attempt, r.success, r.error_code) for r in stored.results]}"
    )
    assert stored.status == CommandStatus.ACKED  # round 2 still open
    assert len(stored.results) == 1              # recorded exactly once
    assert stored.results[0].attempt == 1
    assert gateway.flush("d1") == []             # nothing requeued again

    # round 2's genuine verdict is accepted normally and settles the command
    ok = gateway.report_result("d1", "s", cid, True, 90, attempt=2)
    _show("round2 success", ok)
    assert ok.accepted and ok.terminal and ok.attempt == 2
    final = gateway.store.get_command("d1", cid)
    assert final.status == CommandStatus.SUCCEEDED
    assert [r.attempt for r in final.results] == [1, 2]
    assert final.attempt == 2  # no phantom round 3 execution


def test_single_retry_cap_stale_failure_must_not_kill_command_early(gateway):
    """Scenario 2: max_retries=1. After round 2 is redelivered+acked the old
    round-1 failure is retransmitted. Previously it was mistaken for round
    2's failure and the command was wrongly judged RETRIES_EXHAUSTED, after
    which the real round-2 success was rejected. The stale echo must instead
    be a duplicate and the real success must settle SUCCEEDED."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=1)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id

    gateway.ack("d1", "s", cid)
    f1 = gateway.report_result(
        "d1", "s", cid, False, 100, attempt=1, error_code="E_BUSY"
    )
    _show("round1 failure (only retry remains)", f1)
    assert f1.retried

    gateway.disconnect("d1")
    redelivered = gateway.connect("d1")
    assert redelivered[0].attempt == 2
    gateway.ack("d1", "s", cid)

    stale = gateway.report_result(
        "d1", "s", cid, False, 100, attempt=1, error_code="E_BUSY"
    )
    _show("stale round1 echo with cap=1", stale)
    assert stale.accepted and stale.duplicate
    stored = gateway.store.get_command("d1", cid)
    print(f"must NOT be exhausted: status={stored.status.value}")
    assert stored.status == CommandStatus.ACKED  # not RETRIES_EXHAUSTED
    assert len(stored.results) == 1

    # the genuine round-2 result must still be accepted (was rejected before)
    real = gateway.report_result("d1", "s", cid, True, 80, attempt=2)
    _show("real round2 success after stale echo", real)
    assert real.accepted and real.terminal and not real.duplicate
    final = gateway.store.get_command("d1", cid)
    assert final.status == CommandStatus.SUCCEEDED
    assert [r.attempt for r in final.results] == [1, 2]


def test_stale_round_verdict_conflicting_with_recorded_one_changes_nothing(gateway):
    """Scenario 3: the retransmitted old-round result contradicts the verdict
    recorded for that same round (failure on record, echo claims success).
    It is rejected CONFLICTING_RESULT; neither the recorded round-1 failure
    nor the open round 2 is altered, and round 2's own (different) verdict is
    still accepted."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=1)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id

    gateway.ack("d1", "s", cid)
    f1 = gateway.report_result(
        "d1", "s", cid, False, 40, attempt=1, error_code="E_BUSY"
    )
    assert f1.retried
    redelivered = gateway.flush("d1")
    assert redelivered[0].attempt == 2
    gateway.ack("d1", "s", cid)

    # stale round-1 echo that disagrees with round 1's recorded failure
    bad = gateway.report_result("d1", "s", cid, True, 40, attempt=1)
    _show("stale round1 echo claiming success", bad)
    assert not bad.accepted and bad.reason == RejectReason.CONFLICTING_RESULT
    stored = gateway.store.get_command("d1", cid)
    assert stored.status == CommandStatus.ACKED
    assert len(stored.results) == 1 and stored.results[0].success is False

    # round 2 genuinely succeeds with a different verdict -- accepted
    ok = gateway.report_result("d1", "s", cid, True, 70, attempt=2)
    _show("round2 genuine success (different from round1)", ok)
    assert ok.accepted and ok.terminal
    final = gateway.store.get_command("d1", cid)
    assert final.status == CommandStatus.SUCCEEDED
    assert [(r.attempt, r.success) for r in final.results] == [(1, False), (2, True)]


def test_old_result_arrives_before_new_round_is_acked_and_repeatedly(gateway):
    """Boundary: round 2 has been re-delivered but not acked yet (SENT) when
    round 1's failure retransmits -- repeatedly. Every copy is the same
    recorded round-1 verdict: duplicate, never RESULT_BEFORE_ACK, never a new
    requeue. Once round 2 acks and fails for its own reason, that is the one
    new record and the final verdict is stable."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=1)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id

    gateway.ack("d1", "s", cid)
    gateway.report_result(
        "d1", "s", cid, False, 10, attempt=1, error_code="E_AGAIN"
    )
    redelivered = gateway.flush("d1")
    assert redelivered[0].attempt == 2  # SENT, not yet acked

    for i in range(3):
        echo = gateway.report_result(
            "d1", "s", cid, False, 10, attempt=1, error_code="E_AGAIN"
        )
        _show(f"stale round1 echo #{i + 1} while round2 SENT", echo)
        assert echo.accepted and echo.duplicate and echo.attempt == 1
        assert not echo.retried
    stored = gateway.store.get_command("d1", cid)
    assert stored.status == CommandStatus.SENT
    assert len(stored.results) == 1

    gateway.ack("d1", "s", cid)
    # round 2 fails too; with cap 1 this settles the command once and for all
    final_failure = gateway.report_result(
        "d1", "s", cid, False, 20, attempt=2, error_code="E_DEAD"
    )
    _show("round2 own failure", final_failure)
    assert final_failure.accepted and final_failure.terminal
    final = gateway.store.get_command("d1", cid)
    assert final.status == CommandStatus.RETRIES_EXHAUSTED
    assert [r.attempt for r in final.results] == [1, 2]

    # any later echo of either round can no longer move the command
    echo1 = gateway.report_result(
        "d1", "s", cid, False, 10, attempt=1, error_code="E_AGAIN"
    )
    echo2 = gateway.report_result(
        "d1", "s", cid, False, 20, attempt=2, error_code="E_DEAD"
    )
    _show("post-terminal round1 echo", echo1)
    _show("post-terminal round2 echo", echo2)
    assert echo1.duplicate and echo2.duplicate
    assert gateway.store.get_command("d1", cid).status == CommandStatus.RETRIES_EXHAUSTED
    assert [r.attempt for r in gateway.store.get_command("d1", cid).results] == [1, 2]


def test_report_for_never_delivered_round_rejected(gateway):
    """Boundary: a report naming a round that does not exist yet (or round 0,
    which is invalid) can never be accepted -- it must not be confused with a
    current-round before-ack result, and nothing is recorded."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    gateway.ack("d1", "s", cid)
    gateway.report_result(
        "d1", "s", cid, False, 5, attempt=1, error_code="E_X"
    )
    gateway.flush("d1")  # round 2 SENT
    future = gateway.report_result("d1", "s", cid, True, 5, attempt=9)
    _show("result claiming round 9 while round 2 in flight", future)
    assert not future.accepted
    assert future.reason == RejectReason.UNKNOWN_RESULT_ATTEMPT
    zero = gateway.report_result("d1", "s", cid, True, 5, attempt=0)
    _show("result claiming round 0", zero)
    assert not zero.accepted and zero.reason == RejectReason.INVALID_STATE
    stored = gateway.store.get_command("d1", cid)
    assert len(stored.results) == 1  # only the round-1 failure


def test_superseded_round_without_recorded_verdict_is_stale(gateway):
    """Defensive boundary for a retry whose prior round never produced a
    recorded verdict: a late report naming that closed round is rejected
    STALE_RESULT_ATTEMPT and cannot affect the current round. Such state is
    only reachable through internal recovery paths (failure-driven retries
    always record the failure first), so it is built directly here."""
    gateway.register("d1", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d1")
    cmd, _ = gateway.dispatch("d1", "reboot")
    cid = cmd.command_id
    # fabricate: round 1 delivered, closed without a result, round 2 now ACKED
    cmd.attempt = 2
    cmd.status = CommandStatus.ACKED
    stale = gateway.report_result(
        "d1", "s", cid, False, 5, attempt=1, error_code="E_LATE"
    )
    _show("late verdict for closed unrecorded round 1", stale)
    assert not stale.accepted
    assert stale.reason == RejectReason.STALE_RESULT_ATTEMPT
    stored = gateway.store.get_command("d1", cid)
    assert stored.status == CommandStatus.ACKED and stored.results == []
    # current round 2 still accepts its real verdict normally
    ok = gateway.report_result("d1", "s", cid, True, 8, attempt=2)
    assert ok.accepted and ok.terminal
    assert gateway.store.get_command("d1", cid).status == CommandStatus.SUCCEEDED
