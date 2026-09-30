"""Automatic retry of failed v2 commands: cap, ordering, no dup/no loss,
and the v1 default semantics (no result reporting -> no retry)."""

from gateway.models import CommandStatus, RejectReason


def _fail(gateway, cmd_id, attempt, error_code="EIO", duration=0.5):
    return gateway.report_result(
        "d2",
        "s",
        cmd_id,
        success=False,
        duration=duration,
        error_code=error_code,
        attempt=attempt,
    )


def test_failures_retry_up_to_cap_then_failed(gateway):
    gateway.register("d2", "s", protocol_version="2.0", max_retries=2)
    gateway.connect("d2")
    cmd, _ = gateway.dispatch("d2", "ota")
    print(
        "input: max_retries=2 -> initial attempt + 2 retries = 3 executions; "
        "each failure is reported with its explicit attempt number"
    )
    gateway.ack("d2", "s", cmd.command_id)

    r1 = _fail(gateway, cmd.command_id, 1)
    c1 = gateway.store.get_command("d2", cmd.command_id)
    print(
        f"failure #1: retried={r1.retried} status={c1.status.value} attempt={c1.attempt}"
    )
    assert r1.retried and c1.attempt == 2 and c1.failed_attempts == [1]
    gateway.ack("d2", "s", cmd.command_id)

    r2 = _fail(gateway, cmd.command_id, 2)
    c2 = gateway.store.get_command("d2", cmd.command_id)
    print(
        f"failure #2: retried={r2.retried} status={c2.status.value} attempt={c2.attempt}"
    )
    assert r2.retried and c2.attempt == 3 and c2.failed_attempts == [1, 2]
    gateway.ack("d2", "s", cmd.command_id)

    r3 = _fail(gateway, cmd.command_id, 3, error_code="EDEAD")
    c3 = gateway.store.get_command("d2", cmd.command_id)
    print(
        f"failure #3: retried={r3.retried} status={c3.status.value} "
        f"failed_attempts={c3.failed_attempts} final_error={c3.result.error_code}"
    )
    assert not r3.retried
    assert c3.status == CommandStatus.FAILED
    assert c3.failed_attempts == [1, 2, 3]
    assert c3.result.error_code == "EDEAD"  # stable, distinguishable conclusion

    # FAILED is final: a later success cannot overturn it
    late_ok = gateway.report_result(
        "d2", "s", cmd.command_id, success=True, duration=0.5, attempt=3
    )
    print(f"success after FAILED -> reason={late_ok.reason}")
    assert late_ok.reason == RejectReason.INVALID_STATE


def test_successful_retry_stops_retrying(gateway):
    gateway.register("d2", "s", protocol_version="2.0", max_retries=5)
    gateway.connect("d2")
    cmd, _ = gateway.dispatch("d2", "ota")
    gateway.ack("d2", "s", cmd.command_id)
    assert _fail(gateway, cmd.command_id, 1).retried
    gateway.ack("d2", "s", cmd.command_id)
    r = gateway.report_result(
        "d2", "s", cmd.command_id, success=True, duration=1.0, attempt=2
    )
    c = gateway.store.get_command("d2", cmd.command_id)
    print(
        f"attempt 2 succeeds -> status={c.status.value} result.attempt={c.result.attempt}"
    )
    assert r.command_status == CommandStatus.SUCCEEDED
    assert c.result.attempt == 2
    assert c.failed_attempts == [1]


def test_zero_retries_settles_failed_on_first_failure(gateway):
    gateway.register("d2", "s", protocol_version="2.0", max_retries=0)
    gateway.connect("d2")
    cmd, _ = gateway.dispatch("d2", "ota")
    gateway.ack("d2", "s", cmd.command_id)
    r = _fail(gateway, cmd.command_id, 1)
    c = gateway.store.get_command("d2", cmd.command_id)
    print(
        f"max_retries=0, first failure -> retried={r.retried} status={c.status.value}"
    )
    assert not r.retried and c.status == CommandStatus.FAILED


def test_retry_is_requeued_at_tail_and_redelivered_once_in_order(gateway):
    gateway.register("d2", "s", protocol_version="2.0", max_retries=3)
    gateway.connect("d2")
    retry_cmd, _ = gateway.dispatch("d2", "first")
    gateway.ack("d2", "s", retry_cmd.command_id)

    # device goes offline; two fresh commands queue behind the in-flight one
    gateway.disconnect("d2")
    w1, _ = gateway.dispatch("d2", "wait-1")
    w2, _ = gateway.dispatch("d2", "wait-2")
    r = _fail(gateway, retry_cmd.command_id, 1)
    print(
        "failure reported while offline: retry is requeued at tail, "
        f"retried={r.retried} status={r.command_status.value}; "
        "existing queued commands are untouched"
    )
    assert r.retried
    assert [c.command_id for c in gateway.store.queued_commands("d2")] == [
        w1.command_id,
        w2.command_id,
        retry_cmd.command_id,
    ]
    qs = gateway.queue_status("d2")
    print(f"queue before reconnect: queued={qs.queued} in_flight={qs.in_flight}")
    assert qs.queued == 3 and qs.in_flight == 0

    delivered = gateway.connect("d2")
    print(
        "redelivery order: "
        + ", ".join(f"{c.name}#attempt{c.attempt}" for c in delivered)
    )
    assert [c.command_id for c in delivered] == [
        w1.command_id,
        w2.command_id,
        retry_cmd.command_id,
    ]
    assert delivered[2].attempt == 2  # retry distinguishable from attempt 1
    # each command delivered exactly once in this flush
    assert len({id(c) for c in delivered}) == 3

    # reconnect again delivers nothing (no duplicate execution)
    gateway.disconnect("d2")
    assert gateway.connect("d2") == []


def test_retry_requeue_is_not_capacity_rejected(gateway):
    """Capacity bounds new dispatches only: an internal retry requeue must
    never be dropped even when the queue is already full."""
    gateway.register("d2", "s", protocol_version="2.0", max_queue_size=2, max_retries=1)
    gateway.connect("d2")
    retry_cmd, _ = gateway.dispatch("d2", "first")
    gateway.ack("d2", "s", retry_cmd.command_id)
    gateway.disconnect("d2")
    gateway.dispatch("d2", "w1")
    gateway.dispatch("d2", "w2")
    _, reason = gateway.dispatch("d2", "w3")  # full -> new work rejected
    print(f"queue full, new dispatch reason={reason}; retrying in-flight cmd")
    assert reason == RejectReason.QUEUE_FULL
    r = _fail(gateway, retry_cmd.command_id, 1)
    assert r.retried  # retry still enqueued: nothing silently lost
    names = [c.name for c in gateway.store.queued_commands("d2")]
    print(f"queue after requeue: {names}")
    assert names == ["w1", "w2", "first"]


def test_v1_client_ack_is_terminal_no_result_no_retry(gateway):
    """Old clients (v1.0) cannot report execution results. Deterministic
    default: ACKED is their final conclusion; results are refused and no
    retry is ever scheduled."""
    gateway.register("old", "s", protocol_version="1.0", max_retries=3)
    gateway.connect("old")
    cmd, _ = gateway.dispatch("old", "reboot")
    assert gateway.ack("old", "s", cmd.command_id).accepted
    r = gateway.report_result(
        "old",
        "s",
        cmd.command_id,
        success=False,
        duration=1.0,
        error_code="EIO",
    )
    c = gateway.store.get_command("old", cmd.command_id)
    print(
        f"v1 result report -> accepted={r.accepted} reason={r.reason}; "
        f"command stays {c.status.value}, failed_attempts={c.failed_attempts}"
    )
    assert r.reason == RejectReason.UNSUPPORTED_VERSION
    assert c.status == CommandStatus.ACKED
    assert c.failed_attempts == [] and c.result is None

    # mixed deployment: a neighboring v2 device still gets full new semantics
    gateway.register("new", "s", protocol_version="2.0", max_retries=1)
    gateway.connect("new")
    cmd2, _ = gateway.dispatch("new", "reboot")
    gateway.ack("new", "s", cmd2.command_id)
    r2 = gateway.report_result(
        "new",
        "s",
        cmd2.command_id,
        success=False,
        duration=1.0,
        error_code="EIO",
        attempt=1,
    )
    print(f"v2 neighbor still retries: retried={r2.retried}")
    assert r2.retried


def test_retry_backoff_waits_then_delivers_in_fifo(gateway, clock):
    """With retry_backoff, a requeued retry is parked until not_before; an
    immediate flush skips it, a later flush (after backoff) delivers it
    after everything already waiting — order stays explainable."""
    gateway.register(
        "d2", "s", protocol_version="2.0", max_retries=2, retry_backoff=10.0
    )
    gateway.connect("d2")
    retry_cmd, _ = gateway.dispatch("d2", "retry-me")
    gateway.ack("d2", "s", retry_cmd.command_id)
    gateway.disconnect("d2")
    waiting, _ = gateway.dispatch("d2", "waiting")

    r = gateway.report_result(
        "d2",
        "s",
        retry_cmd.command_id,
        success=False,
        duration=0.5,
        error_code="EIO",
        attempt=1,
    )
    print(
        f"failure with backoff=10s: retried={r.retried} status={r.command_status.value}"
    )
    assert r.retried and r.command_status == CommandStatus.QUEUED

    # reconnect immediately: parked retry must not be delivered; the
    # waiting command in front goes first
    delivered_now = gateway.connect("d2")
    print(f"flush at t={clock.now}: {[c.name for c in delivered_now]}")
    assert [c.command_id for c in delivered_now] == [waiting.command_id]
    assert (
        gateway.store.get_command("d2", retry_cmd.command_id).status
        == CommandStatus.QUEUED
    )

    # before backoff elapses, a flush still skips the parked retry
    gateway.disconnect("d2")
    clock.advance(5.0)
    assert gateway.connect("d2") == []
    print(f"flush at t={clock.now}: parked retry still held")

    # after backoff the retry is delivered (attempt 2, same command_id)
    gateway.disconnect("d2")
    clock.advance(6.0)
    delivered_later = gateway.connect("d2")
    print(f"flush at t={clock.now}: {[(c.name, c.attempt) for c in delivered_later]}")
    assert [c.command_id for c in delivered_later] == [retry_cmd.command_id]
    assert delivered_later[0].attempt == 2
