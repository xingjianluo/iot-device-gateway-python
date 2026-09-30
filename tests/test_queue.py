"""Per-device offline-queue capacity: admission boundary and occupancy view."""
from gateway.models import RejectReason


def test_capacity_boundary_offline_rejects_only_overflow(gateway, clock):
    gateway.register("d1", "s", queue_capacity=2)
    c1, r1 = gateway.dispatch("d1", "a")
    c2, r2 = gateway.dispatch("d1", "b")
    c3, r3 = gateway.dispatch("d1", "c")  # over capacity
    print(f"cap=2 admits: {c1 is not None},{c2 is not None}; third -> {r3}")
    assert c1 is not None and c2 is not None and r1 is None and r2 is None
    assert c3 is None and r3 == RejectReason.QUEUE_FULL
    st = gateway.queue_status("d1")
    print(f"occupancy: queued={st.queued} cap={st.capacity} full={st.full}")
    assert st.queued == 2 and st.capacity == 2 and st.full
    # queued commands were not dropped and keep FIFO order
    ids = [c.command_id for c in gateway.store.queued_commands("d1")]
    assert ids == [c1.command_id, c2.command_id]


def test_capacity_zero_rejects_all_offline_but_allows_online(gateway, clock):
    gateway.register("d1", "s", queue_capacity=0)
    st0 = gateway.queue_status("d1")
    assert st0.full and st0.queued == 0
    c, reason = gateway.dispatch("d1", "a")  # offline -> no waiting allowed
    print(f"cap=0 offline dispatch -> {reason}")
    assert c is None and reason == RejectReason.QUEUE_FULL
    gateway.connect("d1")
    online, reason = gateway.dispatch("d1", "a")  # delivered immediately
    print(f"cap=0 online dispatch -> {online.status.value if online else reason}")
    assert online is not None and reason is None


def test_unlimited_capacity_default(gateway, clock):
    gateway.register("d1", "s")
    for i in range(50):
        cmd, reason = gateway.dispatch("d1", f"op{i}")
        assert cmd is not None and reason is None
    st = gateway.queue_status("d1")
    print(f"unlimited: queued={st.queued} cap={st.capacity} full={st.full}")
    assert st.queued == 50 and st.capacity is None and not st.full


def test_capacity_drains_on_flush_and_refills_independently(gateway, clock):
    gateway.register("d1", "s", queue_capacity=2)
    gateway.dispatch("d1", "a")
    gateway.dispatch("d1", "b")
    assert gateway.queue_status("d1").full
    delivered = gateway.connect("d1")  # SENT: no longer occupying the queue
    assert len(delivered) == 2
    st = gateway.queue_status("d1")
    print(f"after flush: queued={st.queued} full={st.full}")
    assert st.queued == 0 and not st.full
    # disconnect and refill up to the cap again
    gateway.disconnect("d1")
    gateway.dispatch("d1", "c")
    gateway.dispatch("d1", "d")
    over, reason = gateway.dispatch("d1", "e")
    assert over is None and reason == RejectReason.QUEUE_FULL
    assert gateway.queue_status("d1").queued == 2


def test_capacity_is_per_device(gateway, clock):
    gateway.register("d1", "s", queue_capacity=1)
    gateway.register("d2", "s", queue_capacity=1)
    gateway.dispatch("d1", "a")  # d1 now full
    c2, reason = gateway.dispatch("d2", "a")  # d2 has its own independent slot
    print(f"d1 full does not affect d2: admitted={c2 is not None} reason={reason}")
    assert c2 is not None and reason is None
    # both full independently; d2 overflow is still rejected on its own cap
    over, over_reason = gateway.dispatch("d2", "b")
    assert over is None and over_reason == RejectReason.QUEUE_FULL
    assert gateway.queue_status("d1").full
    assert gateway.queue_status("d2").full


def test_queue_status_unknown_device(gateway, clock):
    assert gateway.queue_status("ghost") is None


def test_shrink_capacity_below_occupancy_does_not_drop(gateway, clock):
    gateway.register("d1", "s", queue_capacity=5)
    ids = []
    for i in range(3):
        cmd, _ = gateway.dispatch("d1", f"op{i}")
        ids.append(cmd.command_id)
    gateway.configure_policy("d1", queue_capacity=1)  # shrink below occupancy
    st = gateway.queue_status("d1")
    print(f"shrunk cap: queued={st.queued} cap={st.capacity} full={st.full}")
    assert st.queued == 3 and st.capacity == 1 and st.full
    # nothing dropped; and even though 3 > 1, ordering on flush is intact
    delivered = gateway.connect("d1")
    assert [c.command_id for c in delivered] == ids
