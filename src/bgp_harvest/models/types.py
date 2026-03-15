"""Core records exchanged between sources, processing steps, and storage."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Iterable


class ValidationState(StrEnum):
    """Normalized validation states used throughout the project."""
    VALID = "valid"
    INVALID = "invalid"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


def ensure_utc(value: dt.datetime) -> dt.datetime:
    """Return a timezone-aware UTC timestamp."""
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def normalize_validation_state(value: str | ValidationState | None) -> ValidationState:
    """Normalize raw validator output into the local ``ValidationState`` enum."""
    if isinstance(value, ValidationState):
        return value
    if value is None:
        return ValidationState.UNKNOWN

    normalized = value.replace("-", "_").lower()
    try:
        return ValidationState(normalized)
    except ValueError:
        return ValidationState.UNKNOWN


@dataclass(frozen=True, slots=True)
class RibResource:
    """Metadata for a RIB file discovered from an upstream source."""
    url: str
    collector_id: str
    ts_start: dt.datetime
    ts_end: dt.datetime
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts_start", ensure_utc(self.ts_start))
        object.__setattr__(self, "ts_end", ensure_utc(self.ts_end))

    def with_local_path(self, path: Path) -> "MaterializedRibResource":
        """Return a materialized copy that points at a downloaded local file."""
        return MaterializedRibResource(
            url=self.url,
            collector_id=self.collector_id,
            ts_start=self.ts_start,
            ts_end=self.ts_end,
            size_bytes=self.size_bytes,
            local_path=path,
        )


@dataclass(frozen=True, slots=True)
class MaterializedRibResource:
    """A RIB resource that has already been downloaded to local storage."""
    url: str
    collector_id: str
    ts_start: dt.datetime
    ts_end: dt.datetime
    local_path: Path
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts_start", ensure_utc(self.ts_start))
        object.__setattr__(self, "ts_end", ensure_utc(self.ts_end))


@dataclass(slots=True)
class RouteRecord:
    """A normalized route record ready for validation and storage."""
    prefix: str
    as_path: list[int]
    origin: int
    collector: str
    timestamp: dt.datetime
    rpki_status: ValidationState = ValidationState.UNKNOWN
    path_id: int | None = None

    def __post_init__(self) -> None:
        """Normalize field types once at construction time."""
        self.as_path = [int(item) for item in self.as_path]
        if not self.as_path:
            raise ValueError("as_path must not be empty")
        self.origin = int(self.origin)
        self.timestamp = ensure_utc(self.timestamp)
        self.rpki_status = normalize_validation_state(self.rpki_status)

    @classmethod
    def from_as_path(
        cls,
        prefix: str,
        as_path: Iterable[int],
        collector: str,
        timestamp: dt.datetime,
    ) -> "RouteRecord":
        """Build a route record from a parsed AS path sequence."""
        path = [int(item) for item in as_path]
        if not path:
            raise ValueError("as_path must not be empty")
        return cls(
            prefix=prefix,
            as_path=path,
            origin=path[-1],
            collector=collector,
            timestamp=timestamp,
        )
