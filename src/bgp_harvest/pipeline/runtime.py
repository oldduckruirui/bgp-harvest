"""Common runtime helpers used by the CLI and harvest code."""

from __future__ import annotations

import datetime as dt
from typing import Iterable, Iterator, TypeVar

T = TypeVar("T")


def default_window(hours: int = 2) -> tuple[dt.datetime, dt.datetime]:
    """Return the trailing UTC time window used when no explicit range is provided."""

    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=hours)
    return start, end


def batch_iterable(iterable: Iterable[T], batch_size: int) -> Iterator[list[T]]:
    """Yield lists of at most ``batch_size`` items while preserving input order."""

    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
