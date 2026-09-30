"""Telemetry ingestion: auth, dedupe, ordering, timestamp window, versions."""

from concurrent.futures import ThreadPoolExecutor

from gateway.models import RejectReason


def test_unknown_device_rejected(gateway, clock):
    r = gateway.ingest(
        "ghost", "s", "1.0", {"message_id": "m1", "timestamp": clock.now}
    )
    print(f"input: unknown device 'ghost' -> reason={r.reason}")
    assert not r.accepted and r.reason == RejectReason.UNKNOWN_DEVICE


def test_invalid_credentials_rejected(gateway, clock):
    gateway.register("d1", "right")
    r = gateway.ingest(
        "d1", "wrong", "1.0", {"message_id": "m1", "timestamp": clock.now}
    )
    print(f"input: bad secret -> reason={r.reason}")
    assert not r.accepted and r.reason == RejectReason.INVALID_CREDENTIALS


def test_timestamp_out_of_range_rejected(gateway, clock):
    gateway.register("d1", "s")
    for label, ts in [
        ("too old", clock.now - 25 * 3600),
        ("too far future", clock.now + 10 * 60),
    ]:
        r = gateway.ingest("d1", "s", "1.0", {"message_id": label, "timestamp": ts})
        print(f"input: {label} ts={ts} now={clock.now} -> reason={r.reason}")
        assert not r.accepted and r.reason == RejectReason.TIMESTAMP_OUT_OF_RANGE


def test_duplicate_message_id_not_double_counted(gateway, clock):
    gateway.register("d1", "s")
    msg = {"message_id": "m1", "timestamp": clock.now}
    r1 = gateway.ingest("d1", "s", "1.0", msg)
    r2 = gateway.ingest("d1", "s", "1.0", msg)
    print(f"first={r1.accepted}, second: duplicate={r2.duplicate} reason={r2.reason}")
    assert r1.accepted
    assert r2.duplicate and r2.reason == RejectReason.DUPLICATE_TELEMETRY
    assert gateway.store.telemetry_count("d1") == 1


def test_concurrent_ingest_same_message_counted_once(gateway, clock):
    gateway.register("d1", "s")
    msg = {"message_id": "m1", "timestamp": clock.now}
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(
            pool.map(lambda _: gateway.ingest("d1", "s", "1.0", dict(msg)), range(64))
        )
    accepted = sum(1 for r in results if r.accepted)
    duplicates = sum(1 for r in results if r.duplicate)
    print(f"64 concurrent same-id ingests: accepted={accepted} duplicates={duplicates}")
    assert accepted == 1 and duplicates == 63
    assert gateway.store.telemetry_count("d1") == 1


def test_concurrent_ingest_distinct_messages_all_counted(gateway, clock):
    gateway.register("d1", "s")
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(
            pool.map(
                lambda i: gateway.ingest(
                    "d1", "s", "1.0", {"message_id": f"m{i}", "timestamp": clock.now}
                ),
                range(100),
            )
        )
    print(
        f"100 concurrent distinct ingests: accepted={sum(r.accepted for r in results)}"
    )
    assert all(r.accepted for r in results)
    assert gateway.store.telemetry_count("d1") == 100


def test_out_of_order_timestamps_accepted(gateway, clock):
    gateway.register("d1", "s")
    base = clock.now
    # arrive out of order: seq 2 (newer ts) before seq 1 (older ts)
    r_new = gateway.ingest(
        "d1", "s", "2.0", {"message_id": "b", "timestamp": base, "seq": 2}
    )
    r_old = gateway.ingest(
        "d1", "s", "2.0", {"message_id": "a", "timestamp": base - 60, "seq": 1}
    )
    print(
        f"out-of-order: newer accepted={r_new.accepted}, older accepted={r_old.accepted}"
    )
    assert r_new.accepted and r_old.accepted
    assert gateway.store.telemetry_count("d1") == 2
