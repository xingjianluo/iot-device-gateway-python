"""Execution result reporting: settle once, idempotent, interleave-safe.

Covers the requirement that ack / result / cancel / timeout may arrive in
any order over a flaky network, yet one command always settles into exactly
one final state with a distinguishable reject reason for every ordering.
"""

from concurrent.futures import ThreadPoolExecutor

from gateway.models import CommandStatus, RejectReason


def _v2_online(gateway):
    gateway.register("d2", "s", protocol_version="2.0")
    gateway.connect("d2")


def test_successful_result_settles_succeeded(gateway):
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    assert gateway.ack("d2", "s", cmd.command_id).accepted
    r = gateway.report_result("d2", "s", cmd.command_id, success=True, duration=2.5)
    print(
        f"result success={True} duration=2.5 -> accepted={r.accepted} "
        f"status={r.command_status}"
    )
    assert r.accepted and r.command_status == CommandStatus.SUCCEEDED
    stored = gateway.store.get_command("d2", cmd.command_id)
    assert stored.result is not None and stored.result.success
    assert stored.result.duration == 2.5


def test_failed_result_requires_error_code(gateway):
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    gateway.ack("d2", "s", cmd.command_id)
    r = gateway.report_result("d2", "s", cmd.command_id, success=False, duration=1.0)
    print(f"failure without error_code -> reason={r.reason}")
    assert not r.accepted and r.reason == RejectReason.INVALID_RESULT
    r2 = gateway.report_result(
        "d2",
        "s",
        cmd.command_id,
        success=False,
        duration=-1.0,
        error_code="E1",
    )
    print(f"negative duration -> reason={r2.reason}")
    assert r2.reason == RejectReason.INVALID_RESULT
    # a malformed report must not settle anything
    assert gateway.store.get_command("d2", cmd.command_id).status == CommandStatus.ACKED


def test_result_before_ack_rejected_result_not_acked(gateway):
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    # device is still SENT (no ack observed)
    r = gateway.report_result("d2", "s", cmd.command_id, success=True, duration=1.0)
    print(f"result while SENT -> accepted={r.accepted} reason={r.reason}")
    assert r.reason == RejectReason.RESULT_NOT_ACKED

    # queued (never delivered) command: offline device reports a result
    gateway.register("off", "s", protocol_version="2.0")
    qcmd, _ = gateway.dispatch("off", "reboot")
    rq = gateway.report_result("off", "s", qcmd.command_id, success=True, duration=1.0)
    print(f"result while QUEUED -> reason={rq.reason}")
    assert rq.reason == RejectReason.RESULT_NOT_ACKED
    assert (
        gateway.store.get_command("off", qcmd.command_id).status == CommandStatus.QUEUED
    )


def test_result_after_timeout_rejected_with_distinct_reason(gateway, clock):
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    clock.advance(31.0)
    expired = gateway.expire()
    print(f"expire before result: {[c.command_id for c in expired]}")
    r = gateway.report_result("d2", "s", cmd.command_id, success=True, duration=1.0)
    print(f"result after TIMED_OUT -> accepted={r.accepted} reason={r.reason}")
    assert not r.accepted
    assert r.reason == RejectReason.RESULT_AFTER_TIMEOUT
    assert (
        gateway.store.get_command("d2", cmd.command_id).status
        == CommandStatus.TIMED_OUT
    )


def test_result_after_cancel_rejected_with_distinct_reason(gateway):
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")  # SENT
    ok, _ = gateway.cancel("d2", cmd.command_id)
    print(f"cancel SENT: ok={ok}")
    r = gateway.report_result("d2", "s", cmd.command_id, success=True, duration=1.0)
    print(f"result after CANCELLED -> accepted={r.accepted} reason={r.reason}")
    assert not r.accepted
    assert r.reason == RejectReason.RESULT_AFTER_CANCELLED

    # the three "too late" reasons must be pairwise distinguishable
    reasons = {
        RejectReason.RESULT_NOT_ACKED,
        RejectReason.RESULT_AFTER_TIMEOUT,
        RejectReason.RESULT_AFTER_CANCELLED,
    }
    assert len(reasons) == 3


def test_duplicate_result_is_idempotent_and_never_changes_conclusion(gateway):
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    gateway.ack("d2", "s", cmd.command_id)
    payload = {"success": False, "duration": 0.5, "error_code": "EIO", "attempt": 1}
    r1 = gateway.report_result("d2", "s", cmd.command_id, **payload)
    # first failure triggers a retry (under cap), command goes back to SENT
    print(f"first failure: retried={r1.retried} status={r1.command_status}")
    assert r1.retried and r1.command_status == CommandStatus.SENT
    # the device retransmits the same attempt-1 result after the retry
    # delivery already happened: must be a duplicate, not a second failure
    gateway.ack("d2", "s", cmd.command_id)
    r2 = gateway.report_result("d2", "s", cmd.command_id, **payload)
    print(
        f"retransmitted attempt-1 result -> accepted={r2.accepted} "
        f"duplicate={r2.duplicate} retried={r2.retried}"
    )
    assert r2.accepted and r2.duplicate and not r2.retried
    stored = gateway.store.get_command("d2", cmd.command_id)
    assert stored.attempt == 2 and stored.failed_attempts == [1]

    # a genuinely different outcome contradicts the record: rejected,
    # first conclusion handling wins
    r3 = gateway.report_result(
        "d2", "s", cmd.command_id, success=True, duration=0.5, attempt=1
    )
    print(f"contradictory attempt-1 result -> reason={r3.reason}")
    assert r3.reason == RejectReason.INVALID_STATE


def test_result_for_unknown_command_and_bad_secret(gateway):
    _v2_online(gateway)
    r_unknown = gateway.report_result("d2", "s", "cmd-404", success=True, duration=1.0)
    r_badauth = gateway.report_result(
        "d2", "nope", "cmd-404", success=True, duration=1.0
    )
    print(f"unknown command -> {r_unknown.reason}; bad secret -> {r_badauth.reason}")
    assert r_unknown.reason == RejectReason.UNKNOWN_COMMAND
    assert r_badauth.reason == RejectReason.INVALID_CREDENTIALS


def test_result_attempt_mismatch_rejected(gateway):
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    gateway.ack("d2", "s", cmd.command_id)
    r_future = gateway.report_result(
        "d2", "s", cmd.command_id, success=True, duration=1.0, attempt=9
    )
    print(f"attempt 9 while executing attempt 1 -> reason={r_future.reason}")
    assert r_future.reason == RejectReason.INVALID_STATE
    # rejected report must not touch the command
    assert gateway.store.get_command("d2", cmd.command_id).status == CommandStatus.ACKED


def test_concurrent_result_cancel_timeout_settle_exactly_once(gateway, clock):
    """32 results race 32 cancels and an expire pass: the command ends in
    exactly one valid final state, never two contradictory conclusions."""
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    gateway.ack("d2", "s", cmd.command_id)

    def report(_):
        return gateway.report_result(
            "d2", "s", cmd.command_id, success=True, duration=1.0, attempt=1
        )

    def cancel(_):
        return gateway.cancel("d2", cmd.command_id)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(report, range(32)))
        cancels = list(pool.map(cancel, range(32)))
    gateway.expire()  # cannot move an already-settled command

    final = gateway.store.get_command("d2", cmd.command_id)
    accepted = [r for r in results if r.accepted and not r.duplicate]
    print(
        f"32 results vs 32 cancels: accepted={len(accepted)} final={final.status.value}"
    )
    assert final.status in (CommandStatus.SUCCEEDED, CommandStatus.CANCELLED)
    if final.status == CommandStatus.SUCCEEDED:
        assert len(accepted) == 1
        assert all(not ok for ok, _ in cancels)
    else:
        assert accepted == []
        assert sum(1 for ok, _ in cancels if ok) == 1


def test_invalid_result_rejects_type_and_value_errors_alike(gateway):
    """Both malformed payload kinds (wrong type / bad value) must be
    rejected INVALID_RESULT and leave the command untouched."""
    _v2_online(gateway)
    cmd, _ = gateway.dispatch("d2", "reboot")
    gateway.ack("d2", "s", cmd.command_id)
    bad_type = gateway.report_result(
        "d2", "s", cmd.command_id, success="yes", duration=1.0
    )
    bad_value = gateway.report_result(
        "d2",
        "s",
        cmd.command_id,
        success=False,
        duration=-3,
        error_code="E1",
    )
    print(
        f"wrong-type success -> {bad_type.reason}; "
        f"negative duration -> {bad_value.reason}"
    )
    assert bad_type.reason == RejectReason.INVALID_RESULT
    assert bad_value.reason == RejectReason.INVALID_RESULT
    assert gateway.store.get_command("d2", cmd.command_id).status == CommandStatus.ACKED


def test_all_interleavings_settle_once_with_distinct_reasons(gateway, clock):
    """Enumerate every ordering of ack / cancel / expire / result.

    Whichever event lands first, the command finishes in exactly one valid
    final state, a late result never overturns it, and each late/early
    rejection carries the reason specific to what already happened.
    """

    def fresh():
        cid = f"d-{next(_ids)}"
        gateway.register(cid, "s", protocol_version="2.0")
        gateway.connect(cid)
        cmd, _ = gateway.dispatch(cid, "x")
        return cid, cmd

    def do_ack(cid, cmd):
        return gateway.ack(cid, "s", cmd.command_id)

    def do_cancel(cid, cmd):
        ok, _reason = gateway.cancel(cid, cmd.command_id)
        return ok

    def do_expire(_cid, _cmd):
        clock.advance(31.0)
        return gateway.expire()

    def do_result(cid, cmd):
        return gateway.report_result(
            cid, "s", cmd.command_id, success=True, duration=1.0
        )

    from itertools import permutations

    actions = {
        "ack": do_ack,
        "cancel": do_cancel,
        "expire": do_expire,
        "result": do_result,
    }
    observed = set()
    for order in permutations(actions):
        clock.now = 1_000_000.0  # deterministic reset each ordering
        cid, cmd = fresh()
        verdict = None
        for name in order:
            r = actions[name](cid, cmd)
            if name == "result":
                verdict = (r.accepted, r.duplicate, r.reason)
        final = gateway.store.get_command(cid, cmd.command_id).status
        observed.add((order, final, verdict))
        print(f"order={order} -> final={final.value} result_verdict={verdict}")

        # exactly one valid final state per ordering. ACKED only survives
        # when the result reached the gateway BEFORE the ack (rejected
        # RESULT_NOT_ACKED) and nothing else could settle afterwards.
        assert final in (
            CommandStatus.SUCCEEDED,
            CommandStatus.CANCELLED,
            CommandStatus.TIMED_OUT,
            CommandStatus.ACKED,
        )
        if final == CommandStatus.ACKED:
            assert order.index("result") < order.index("ack")
            assert verdict[2] == RejectReason.RESULT_NOT_ACKED
        if final == CommandStatus.SUCCEEDED:
            # result could only settle when ack preceded it and won
            assert order.index("ack") < order.index("result")
            assert verdict[0] is True
        elif final == CommandStatus.TIMED_OUT:
            assert verdict is None or verdict[2] in (
                RejectReason.RESULT_AFTER_TIMEOUT,
                RejectReason.RESULT_NOT_ACKED,
                RejectReason.INVALID_STATE,
            )
        elif final == CommandStatus.CANCELLED:
            assert verdict is None or verdict[2] in (
                RejectReason.RESULT_AFTER_CANCELLED,
                RejectReason.RESULT_NOT_ACKED,
                RejectReason.INVALID_STATE,
            )

    # the pairwise-distinct late reasons really both occur
    late_reasons = {v[2] for (_o, _f, v) in observed if v is not None and not v[0]}
    print(
        f"distinct rejection reasons observed: {sorted(r.value for r in late_reasons)}"
    )
    assert RejectReason.RESULT_AFTER_TIMEOUT in late_reasons
    assert RejectReason.RESULT_AFTER_CANCELLED in late_reasons
    assert RejectReason.RESULT_NOT_ACKED in late_reasons


_ids = iter(range(1000))


def test_concurrent_ack_result_cancel_expire_under_contention(gateway, clock):
    """Many threads fire ack/result/cancel while expire runs concurrently.
    Under the per-device lock exactly one conclusion survives."""
    gateway.register("d2", "s", protocol_version="2.0")
    gateway.connect("d2")
    cmd, _ = gateway.dispatch("d2", "reboot")

    def ack(_):
        gateway.ack("d2", "s", cmd.command_id)

    def result(_):
        return gateway.report_result(
            "d2", "s", cmd.command_id, success=True, duration=1.0
        )

    def cancel(_):
        gateway.cancel("d2", cmd.command_id)

    def expire(_):
        clock.advance(31.0)
        gateway.expire()

    with ThreadPoolExecutor(max_workers=16) as pool:
        pool.map(ack, range(64))
        results = list(pool.map(result, range(64)))
        pool.map(cancel, range(64))
        pool.map(expire, range(8))

    final = gateway.store.get_command("d2", cmd.command_id)
    settling = [r for r in results if r.accepted and not r.duplicate and not r.retried]
    print(
        f"64 acks/results/cancels + 8 expires: "
        f"settling_results={len(settling)} final={final.status.value}"
    )
    assert final.status in (
        CommandStatus.SUCCEEDED,
        CommandStatus.CANCELLED,
        CommandStatus.TIMED_OUT,
        CommandStatus.ACKED,
    )
    if final.status == CommandStatus.SUCCEEDED:
        assert len(settling) == 1
    else:
        assert settling == []
