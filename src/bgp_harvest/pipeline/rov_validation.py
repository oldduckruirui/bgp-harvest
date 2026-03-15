"""Route Origin Validation helpers and the default HTTP validator client."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, TypeVar

import requests
from requests.adapters import HTTPAdapter

from ..models.types import RouteRecord, ValidationState, normalize_validation_state
from ..models.interfaces import RouteValidator

LOG = logging.getLogger(__name__)
T = TypeVar("T")


class HTTPROVValidator:
    """Call a remote ROV HTTP API for prefix/origin validation results."""

    def __init__(
        self,
        endpoint: str,
        timeout_seconds: int = 30,
        max_workers: int = 10,
        request_batch_size: int = 64,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_workers = max(1, max_workers)
        self.request_batch_size = max(1, request_batch_size)
        self._thread_local = threading.local()
        self._shared_session = self._build_session()

    def validate_pair(self, pair: tuple[str, int]) -> ValidationState:
        """Validate one pair with a single HTTP request."""

        prefix, origin = pair
        return self._validate_pair(prefix, int(origin))

    def bulk_validate(
        self,
        pairs: Iterable[tuple[str, int]],
    ) -> dict[tuple[str, int], ValidationState]:
        """Validate unique prefix/origin pairs via concurrent bulk POST requests."""

        pair_list = list(pairs)
        if not pair_list:
            return {}

        results: dict[tuple[str, int], ValidationState] = {}
        batches = list(_chunked(pair_list, self.request_batch_size))
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batches))) as executor:
            future_map = {
                executor.submit(self._validate_batch, batch): batch
                for batch in batches
            }
            for future in as_completed(future_map):
                try:
                    results.update(future.result())
                except Exception:
                    batch = future_map[future]
                    LOG.warning(
                        "ROV batch validation failed batch_pairs=%s first_pair=%s",
                        len(batch),
                        batch[0] if batch else None,
                        exc_info=True,
                    )
                    results.update({pair: ValidationState.UNKNOWN for pair in batch})
        return results

    def _validate_batch(
        self,
        pairs: list[tuple[str, int]],
    ) -> dict[tuple[str, int], ValidationState]:
        """Validate one batch through POST, falling back to GET when required."""

        try:
            return self._validate_batch_post(pairs)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in {404, 405, 415, 501}:
                raise
            LOG.info(
                "ROV POST batch not supported or rejected; falling back to GET batch_pairs=%s status=%s",
                len(pairs),
                status_code,
            )
            return {pair: self.validate_pair(pair) for pair in pairs}

    def _validate_pair(self, prefix: str, origin: int) -> ValidationState:
        """Validate a single prefix/origin pair against the configured endpoint."""

        session = self._session()
        response = session.get(
            self.endpoint,
            params={"asn": origin, "prefix": prefix},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        raw_state = payload["validated_route"]["validity"]["state"]
        return normalize_validation_state(raw_state)

    def _validate_batch_post(
        self,
        pairs: list[tuple[str, int]],
    ) -> dict[tuple[str, int], ValidationState]:
        """Validate one batch through Routinator's POST /validity API."""

        session = self._session()
        response = session.post(
            self.endpoint,
            json={
                "routes": [
                    {
                        "asn": f"AS{origin}",
                        "prefix": prefix,
                    }
                    for prefix, origin in pairs
                ]
            },
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        validated_routes = payload.get("validated_routes")
        if not isinstance(validated_routes, list):
            raise ValueError("Unexpected POST response payload")

        results: dict[tuple[str, int], ValidationState] = {}
        for entry in validated_routes:
            route_data = entry.get("route", {})
            validity = entry.get("validity", {})
            raw_asn = str(route_data.get("origin_asn", ""))
            prefix = str(route_data.get("prefix", ""))
            if not raw_asn.startswith("AS"):
                raise ValueError("Unexpected origin_asn in POST response payload")
            results[(prefix, int(raw_asn[2:]))] = normalize_validation_state(validity.get("state", "unknown"))

        if len(results) != len(pairs):
            raise ValueError("POST response did not contain all requested routes")
        return results

    def _session(self) -> requests.Session:
        """Return a thread-local session to avoid connection-pool contention."""

        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._shared_session if self.max_workers == 1 else self._build_session()
            self._thread_local.session = session
        return session

    def _build_session(self) -> requests.Session:
        """Build a requests session configured for retryable HTTP calls."""

        session = requests.Session()
        session.trust_env = False
        adapter = HTTPAdapter(
            max_retries=3,
            pool_connections=self.max_workers,
            pool_maxsize=self.max_workers,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session


class RouteAnnotator:
    """Annotate parsed routes with validation states using a bulk validator."""

    def __init__(self, validator: RouteValidator | None) -> None:
        self.validator = validator
        self._validation_cache: dict[tuple[str, int], ValidationState] = {}
        self._inflight_pairs: set[tuple[str, int]] = set()
        self._condition = threading.Condition()

    def reset(self) -> None:
        """Drop cached validation results before a new harvest run starts."""

        with self._condition:
            self._validation_cache.clear()
            self._inflight_pairs.clear()

    def cached_state(self, pair: tuple[str, int]) -> ValidationState | None:
        """Return a cached validation state for one pair when available."""

        with self._condition:
            return self._validation_cache.get(pair)

    def reserve_pair(self, pair: tuple[str, int]) -> bool:
        """Mark one pair as in-flight when it is not cached or already pending."""

        with self._condition:
            if pair in self._validation_cache or pair in self._inflight_pairs:
                return False
            self._inflight_pairs.add(pair)
            return True

    def resolve_pairs(
        self,
        requested_pairs: Iterable[tuple[str, int]],
        results: dict[tuple[str, int], ValidationState],
    ) -> dict[tuple[str, int], ValidationState]:
        """Store validation results for one batch of requested pairs."""

        requested_list = list(requested_pairs)
        resolved = {
            pair: results.get(pair, ValidationState.UNKNOWN)
            for pair in requested_list
        }
        with self._condition:
            self._validation_cache.update(resolved)
            self._inflight_pairs.difference_update(requested_list)
            self._condition.notify_all()
        return resolved

    def parallelism_hint(self) -> int:
        """Return the desired number of concurrent HTTP validation requests."""

        if self.validator is None:
            return 1
        validator_workers = getattr(self.validator, "max_workers", 1)
        return max(1, int(validator_workers))

    def annotate(self, routes: Iterable[RouteRecord]) -> list[RouteRecord]:
        """Validate unique pairs once and apply the results back to every route."""

        route_list = list(routes)
        if not route_list or self.validator is None:
            if not route_list:
                LOG.info("Skipping ROV validation because no routes were parsed")
            else:
                LOG.info("Skipping ROV validation because validation is disabled")
            return route_list

        validation_started_at = time.perf_counter()
        unique_pairs = {(route.prefix, route.origin) for route in route_list}
        missing_pairs: set[tuple[str, int]] = set()
        waiting_pairs: set[tuple[str, int]] = set()
        with self._condition:
            for pair in unique_pairs:
                if pair in self._validation_cache:
                    continue
                if pair in self._inflight_pairs:
                    waiting_pairs.add(pair)
                    continue
                missing_pairs.add(pair)
                self._inflight_pairs.add(pair)

        LOG.info(
            "Starting ROV validation routes=%s unique_pairs=%s cache_miss=%s cache_wait=%s cache_hit=%s",
            len(route_list),
            len(unique_pairs),
            len(missing_pairs),
            len(waiting_pairs),
            len(unique_pairs) - len(missing_pairs) - len(waiting_pairs),
        )

        new_results: dict[tuple[str, int], ValidationState] = {}
        try:
            if missing_pairs:
                new_results = dict(self.validator.bulk_validate(missing_pairs))
        finally:
            self.resolve_pairs(missing_pairs, new_results)
            with self._condition:
                while waiting_pairs and any(pair not in self._validation_cache for pair in waiting_pairs):
                    self._condition.wait()

                resolved_states = {
                    pair: self._validation_cache.get(pair, ValidationState.UNKNOWN)
                    for pair in unique_pairs
                }

        status_counts: dict[ValidationState, int] = {
            ValidationState.VALID: 0,
            ValidationState.INVALID: 0,
            ValidationState.NOT_FOUND: 0,
            ValidationState.UNKNOWN: 0,
        }
        for route in route_list:
            route.rpki_status = resolved_states.get(
                (route.prefix, route.origin),
                ValidationState.UNKNOWN,
            )
            status_counts[route.rpki_status] += 1
        LOG.info(
            "Finished ROV validation valid=%s invalid=%s not_found=%s unknown=%s elapsed_seconds=%.3f",
            status_counts[ValidationState.VALID],
            status_counts[ValidationState.INVALID],
            status_counts[ValidationState.NOT_FOUND],
            status_counts[ValidationState.UNKNOWN],
            time.perf_counter() - validation_started_at,
        )
        return route_list


def _chunked(items: list[T], size: int) -> Iterable[list[T]]:
    """Yield fixed-size chunks from a list."""

    for index in range(0, len(items), size):
        yield items[index : index + size]
