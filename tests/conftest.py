import pytest

from gateway.service import Gateway


class FakeClock:
    """Deterministic, manually advanced clock."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def gateway(clock) -> Gateway:
    return Gateway(clock=clock)
