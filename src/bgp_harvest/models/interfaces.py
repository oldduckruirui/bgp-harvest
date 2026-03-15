"""Protocol definitions for the main moving parts of the harvest workflow."""

from __future__ import annotations

import datetime as dt
from typing import Iterable, Mapping, Protocol, Sequence

from .types import MaterializedRibResource, RibResource, RouteRecord, ValidationState


class RibDiscovery(Protocol):
    """Discover RIB resources for a given time window."""
    def discover(
        self,
        start: dt.datetime,
        end: dt.datetime,
        collector: str | None = None,
    ) -> Sequence[RibResource]: ...


class RibFetcher(Protocol):
    """Download a discovered resource into a local directory."""
    def fetch(self, resource: RibResource, directory: str) -> MaterializedRibResource: ...


class RibParser(Protocol):
    """Parse a local RIB file into normalized route records."""
    def parse(self, resource: MaterializedRibResource) -> Iterable[RouteRecord]: ...


class RouteValidator(Protocol):
    """Validate unique ``(prefix, origin)`` pairs."""
    def bulk_validate(
        self,
        pairs: Iterable[tuple[str, int]],
    ) -> Mapping[tuple[str, int], ValidationState]: ...


class RouteRepository(Protocol):
    """Persist routes and route metadata to the storage backend."""
    def connect(self) -> None: ...

    def disconnect(self) -> None: ...

    def ensure_schema(self) -> None: ...

    def ingest_routes(self, routes: Iterable[RouteRecord]) -> None: ...

    def update_route_metadata(self, start_time: dt.datetime, end_time: dt.datetime) -> None: ...
