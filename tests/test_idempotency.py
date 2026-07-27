"""The dedupe store: TTL expiry and the size bound."""

import pytest

from eventsvc import InMemoryIdempotencyStore, ManualClock, NullIdempotencyStore


def test_mark_then_seen():
    store = InMemoryIdempotencyStore(clock=ManualClock())
    assert not store.seen("a")
    store.mark("a")
    assert store.seen("a")
    assert len(store) == 1


def test_entries_expire_after_ttl():
    clock = ManualClock()
    store = InMemoryIdempotencyStore(ttl=60.0, clock=clock)
    store.mark("a")
    clock.advance(59.9)
    assert store.seen("a")
    clock.advance(0.2)
    assert not store.seen("a")
    assert len(store) == 0


def test_size_bound_evicts_least_recently_used():
    store = InMemoryIdempotencyStore(max_size=3, clock=ManualClock())
    for event_id in ("a", "b", "c"):
        store.mark(event_id)
    store.seen("a")  # refresh "a" so "b" is now the coldest
    store.mark("d")  # exceeds the bound
    assert store.seen("a")
    assert not store.seen("b")
    assert store.seen("c")
    assert store.seen("d")


def test_expired_entries_are_swept_on_mark():
    clock = ManualClock()
    store = InMemoryIdempotencyStore(ttl=10.0, clock=clock)
    store.mark("old")
    clock.advance(11)
    store.mark("new")
    assert len(store) == 1


def test_null_store_never_remembers():
    store = NullIdempotencyStore()
    store.mark("a")
    assert not store.seen("a")
    assert len(store) == 0


@pytest.mark.parametrize("kwargs", [{"max_size": 0}, {"ttl": 0}])
def test_invalid_bounds_rejected(kwargs):
    with pytest.raises(ValueError):
        InMemoryIdempotencyStore(**kwargs)
