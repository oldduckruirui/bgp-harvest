from __future__ import annotations

import datetime as dt
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from bgp_harvest.models import RouteRecord, ValidationState
from bgp_harvest.pipeline import HTTPROVValidator, RouteAnnotator


class FakeValidator:
    def __init__(self) -> None:
        self.calls: list[set[tuple[str, int]]] = []

    def bulk_validate(self, pairs: set[tuple[str, int]]) -> dict[tuple[str, int], ValidationState]:
        self.calls.append(pairs)
        return {(prefix, origin): ValidationState.VALID for prefix, origin in pairs}


def test_validation_service_deduplicates_prefix_origin_pairs() -> None:
    validator = FakeValidator()
    service = RouteAnnotator(validator)
    ts = dt.datetime(2026, 3, 14, tzinfo=dt.timezone.utc)
    routes = [
        RouteRecord.from_as_path("2001:db8::/32", [64512, 64496], "rv1", ts),
        RouteRecord.from_as_path("2001:db8::/32", [64512, 64496], "rv2", ts),
    ]

    annotated = service.annotate(routes)

    assert len(validator.calls) == 1
    assert validator.calls[0] == {("2001:db8::/32", 64496)}
    assert all(route.rpki_status == ValidationState.VALID for route in annotated)


def test_validation_service_reuses_cache_across_batches() -> None:
    validator = FakeValidator()
    service = RouteAnnotator(validator)
    ts = dt.datetime(2026, 3, 14, tzinfo=dt.timezone.utc)

    first_batch = [RouteRecord.from_as_path("2001:db8::/32", [64512, 64496], "rv1", ts)]
    second_batch = [RouteRecord.from_as_path("2001:db8::/32", [64512, 64496], "rv2", ts)]

    service.annotate(first_batch)
    annotated = service.annotate(second_batch)

    assert len(validator.calls) == 1
    assert annotated[0].rpki_status == ValidationState.VALID


class SlowValidator:
    def __init__(self) -> None:
        self.calls = 0
        self._lock = threading.Lock()

    def bulk_validate(self, pairs: set[tuple[str, int]]) -> dict[tuple[str, int], ValidationState]:
        with self._lock:
            self.calls += 1
        time.sleep(0.05)
        return {(prefix, origin): ValidationState.VALID for prefix, origin in pairs}


def test_validation_service_deduplicates_inflight_pairs_across_threads() -> None:
    validator = SlowValidator()
    service = RouteAnnotator(validator)
    ts = dt.datetime(2026, 3, 14, tzinfo=dt.timezone.utc)

    first_batch = [RouteRecord.from_as_path("2001:db8::/32", [64512, 64496], "rv1", ts)]
    second_batch = [RouteRecord.from_as_path("2001:db8::/32", [64512, 64496], "rv2", ts)]

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(service.annotate, first_batch)
        second_future = executor.submit(service.annotate, second_batch)
        first_annotated = first_future.result()
        second_annotated = second_future.result()

    assert validator.calls == 1
    assert first_annotated[0].rpki_status == ValidationState.VALID
    assert second_annotated[0].rpki_status == ValidationState.VALID


class BatchValidatorProbe(HTTPROVValidator):
    def __init__(self) -> None:
        super().__init__(
            endpoint="http://127.0.0.1:8323/validity",
            timeout_seconds=1,
            max_workers=2,
            request_batch_size=2,
        )
        self.seen_batches: list[list[tuple[str, int]]] = []

    def _validate_batch_post(self, pairs: list[tuple[str, int]]) -> dict[tuple[str, int], ValidationState]:
        self.seen_batches.append(list(pairs))
        return {pair: ValidationState.VALID for pair in pairs}


def test_http_rov_validator_splits_pairs_into_post_batches() -> None:
    validator = BatchValidatorProbe()
    pairs = [
        ("2001:db8::/32", 64496),
        ("2001:db8:1::/48", 64497),
        ("2001:db8:2::/48", 64498),
        ("2001:db8:3::/48", 64499),
        ("2001:db8:4::/48", 64500),
    ]

    results = validator.bulk_validate(pairs)

    assert results == {pair: ValidationState.VALID for pair in pairs}
    assert {tuple(batch) for batch in validator.seen_batches} == {
        (("2001:db8::/32", 64496), ("2001:db8:1::/48", 64497)),
        (("2001:db8:2::/48", 64498), ("2001:db8:3::/48", 64499)),
        (("2001:db8:4::/48", 64500),),
    }
