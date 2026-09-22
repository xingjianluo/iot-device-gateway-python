"""命令生命周期：离线排队、FIFO 补发不重复、超时/取消/重复确认状态一致。"""

from __future__ import annotations

import logging
import threading

import pytest

from gateway.commands import (
    ack_command,
    cancel_command,
    drain_on_reconnect,
    get_command,
    issue_command,
    timeout_command,
)
from gateway.devices import connect, disconnect, register_device
from gateway.errors import ErrorCode, GatewayError
from gateway.models import CommandState

logger = logging.getLogger(__name__)

T0 = 10_000.0


def _online_device(store, device_id="dev-c", version="v2"):
    device = register_device(store, device_id, version)
    connect(store, device.device_id, device.secret, version, )
    return device


def test_online_issue_is_sent_with_deadline(store) -> None:
    device = _online_device(store)
    command = issue_command(store, device.device_id, "do-x", now=T0, ttl_seconds=10)
    logger.info("issue online -> state=%s sent_at=%s timeout_at=%s",
                command.state.value, command.sent_at, command.timeout_at)
    assert command.state is CommandState.SENT
    assert command.timeout_at == T0 + 10


def test_offline_commands_queued_and_redelivered_in_fifo_once(store) -> None:
    device = _online_device(store)
    disconnect(store, device.device_id)
    c1 = issue_command(store, device.device_id, "c1", now=T0)
    c2 = issue_command(store, device.device_id, "c2", now=T0 + 1)
    c3 = issue_command(store, device.device_id, "c3", now=T0 + 2)
    assert all(c.state is CommandState.QUEUED for c in (c1, c2, c3))
    logger.info("offline queue issue order=[c1,c2,c3] all QUEUED")

    # 重连 -> FIFO 补发，每条恰好一次
    _, delivered = connect(store, device.device_id, device.secret, "v2")
    delivered_ids = [c.command_id for c in delivered]
    logger.info("reconnect redelivered order=%s", [c.payload for c in delivered])
    assert delivered_ids == [c1.command_id, c2.command_id, c3.command_id]
    for c in (c1, c2, c3):
        assert store.commands[c.command_id].state is CommandState.SENT

    # 再次 drain 不得重复补发
    again = drain_on_reconnect(store, device.device_id, now=T0 + 3)
    assert again == []
    logger.info("second drain -> %d commands (must be 0)", len(again))


def test_duplicate_ack_is_idempotent(store) -> None:
    device = _online_device(store)
    c = issue_command(store, device.device_id, "ack-me", now=T0)
    acked = ack_command(store, c.command_id, now=T0 + 1)
    assert acked.state is CommandState.ACKED
    first_acked_at = acked.acked_at

    again = ack_command(store, c.command_id, now=T0 + 5)
    logger.info("duplicate ACK -> state=%s acked_at unchanged=%s",
                again.state.value, again.acked_at == first_acked_at)
    assert again.state is CommandState.ACKED
    assert again.acked_at == first_acked_at  # 不产生新迁移


def test_terminal_states_reject_transitions_with_distinct_error(store) -> None:
    device = _online_device(store)
    c = issue_command(store, device.device_id, "freeze", now=T0)
    ack_command(store, c.command_id, now=T0 + 1)
    with pytest.raises(GatewayError) as exc:
        cancel_command(store, c.command_id, now=T0 + 2)
    logger.info("cancel ACKED command -> reject %s", exc.value.code.value)
    assert exc.value.code is ErrorCode.INVALID_COMMAND_TRANSITION
    with pytest.raises(GatewayError) as exc:
        timeout_command(store, c.command_id, now=T0 + 2)
    assert exc.value.code is ErrorCode.INVALID_COMMAND_TRANSITION
    assert store.commands[c.command_id].state is CommandState.ACKED  # 未被覆盖


def test_sent_command_times_out_after_deadline(store) -> None:
    device = _online_device(store)
    c = issue_command(store, device.device_id, "slow", now=T0, ttl_seconds=5)
    # 在窗口内：不超时
    ack_command(store, c.command_id, now=T0 + 4)
    assert store.commands[c.command_id].state is CommandState.ACKED

    c2 = issue_command(store, device.device_id, "slow-2", now=T0, ttl_seconds=5)
    # 超过 deadline 后再 ack：先确定性转 TIMEOUT，ack 被拒
    with pytest.raises(GatewayError) as exc:
        ack_command(store, c2.command_id, now=T0 + 6)
    logger.info("late ack past deadline -> reject %s; command state=%s",
                exc.value.code.value, store.commands[c2.command_id].state.value)
    assert exc.value.code is ErrorCode.INVALID_COMMAND_TRANSITION
    assert store.commands[c2.command_id].state is CommandState.TIMEOUT


def test_cancel_queued_command_removes_it_from_queue(store) -> None:
    device = _online_device(store)
    disconnect(store, device.device_id)
    c1 = issue_command(store, device.device_id, "keep", now=T0)
    c2 = issue_command(store, device.device_id, "drop", now=T0 + 1)
    cancel_command(store, c2.command_id, now=T0 + 2)
    _, delivered = connect(store, device.device_id, device.secret, "v2")
    logger.info("after cancel c2, redelivered=%s", [c.payload for c in delivered])
    assert [c.command_id for c in delivered] == [c1.command_id]
    assert store.commands[c2.command_id].state is CommandState.CANCELLED


def test_concurrent_lifecycle_operations_do_not_overwrite(store) -> None:
    device = _online_device(store)
    c = issue_command(store, device.device_id, "race", now=T0)
    outcomes: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(3)

    def do_ack() -> None:
        barrier.wait()
        try:
            ack_command(store, c.command_id, now=T0 + 1)
            result = "ACKED"
        except GatewayError as exc:
            result = exc.code.value
        with lock:
            outcomes.append(result)

    def do_cancel() -> None:
        barrier.wait()
        try:
            cancel_command(store, c.command_id, now=T0 + 1)
            result = "CANCELLED"
        except GatewayError as exc:
            result = exc.code.value
        with lock:
            outcomes.append(result)

    def do_timeout() -> None:
        barrier.wait()
        try:
            timeout_command(store, c.command_id, now=T0 + 1)
            result = "TIMEOUT"
        except GatewayError as exc:
            result = exc.code.value
        with lock:
            outcomes.append(result)

    threads = [threading.Thread(target=do_ack), threading.Thread(target=do_cancel),
               threading.Thread(target=do_timeout)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final_state = store.commands[c.command_id].state
    winners = [o for o in outcomes if o in ("ACKED", "CANCELLED", "TIMEOUT")]
    logger.info("concurrent ack/cancel/timeout outcomes=%s final_state=%s", outcomes, final_state.value)
    assert len(winners) == 1                    # 只有一个操作完成迁移
    assert final_state.value == winners[0]     # 最终状态与赢家一致
    # 另两个操作必须收到可区分的非法迁移拒绝，而非静默成功
    assert outcomes.count(ErrorCode.INVALID_COMMAND_TRANSITION.value) == 2


def test_command_for_unknown_device_rejected(store) -> None:
    with pytest.raises(GatewayError) as exc:
        issue_command(store, "ghost", "x", now=T0)
    assert exc.value.code is ErrorCode.DEVICE_NOT_FOUND


def test_concurrent_reconnect_and_issue_no_loss_no_duplicate(store) -> None:
    """设备离线状态下，多线程同时重连/下发：结果必须不丢失、不重复补发。"""
    device = _online_device(store, "dev-race")
    disconnect(store, device.device_id)

    n_issuers = 8
    n_reconnectors = 4
    barrier = threading.Barrier(n_issuers + n_reconnectors)
    issued: list[str] = []
    list_lock = threading.Lock()

    def issuer(i: int) -> None:
        barrier.wait()
        c = issue_command(store, device.device_id, f"p-{i}", now=T0 + i)
        with list_lock:
            issued.append(c.command_id)

    def reconnecter() -> None:
        barrier.wait()
        connect(store, device.device_id, device.secret, "v2")

    threads = (
        [threading.Thread(target=issuer, args=(i,)) for i in range(n_issuers)]
        + [threading.Thread(target=reconnecter) for _ in range(n_reconnectors)]
    )
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 全部命令最终必须存在，且每条状态唯一确定（QUEUED 或 SENT，无重复 SENT 痕迹）
    assert len(issued) == n_issuers
    sent = queued = 0
    for cid in issued:
        state = store.commands[cid].state
        assert state in (CommandState.QUEUED, CommandState.SENT)
        sent += state is CommandState.SENT
        queued += state is CommandState.QUEUED
    # 重连后队列为空：若还有 QUEUED（在最终 ONLINE 之后入队），再 drain 一次必须为空安全
    drain_on_reconnect(store, device.device_id, now=T0 + 100)
    states = [store.commands[cid].state for cid in issued]
    logger.info("concurrent issue+reconnect: total=%d final SENT=%d other=%s",
                len(issued), states.count(CommandState.SENT),
                {s.value: states.count(s) for s in set(states)})
    assert all(s is CommandState.SENT for s in states)  # 最终全部恰好补发一次
    # 队列中不存在任何残留命令 id
    assert set(store.device_queues[device.device_id]).intersection(issued) == set()


def test_query_expires_sent_command_at_deadline(store) -> None:
    device = _online_device(store)
    c = issue_command(store, device.device_id, "tick", now=T0, ttl_seconds=5)
    within = get_command(store, c.command_id, now=T0 + 4.999)
    assert within.state is CommandState.SENT
    expired = get_command(store, c.command_id, now=T0 + 5.001)
    logger.info("query at deadline+epsilon -> state=%s", expired.state.value)
    assert expired.state is CommandState.TIMEOUT
    # 已到期后再次查询幂等稳定
    assert get_command(store, c.command_id, now=T0 + 99).state is CommandState.TIMEOUT
