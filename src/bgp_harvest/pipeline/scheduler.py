"""Scheduling primitives for periodic harvest execution."""

from __future__ import annotations

import datetime as dt
import logging
import time
from collections.abc import Callable

LOG = logging.getLogger(__name__)


def next_scheduled_time(now: dt.datetime, interval_hours: int, minute: int) -> dt.datetime:
    """Compute the next UTC run time aligned to the configured hour interval and minute."""

    now = now.astimezone(dt.timezone.utc)
    candidate = now.replace(minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += dt.timedelta(hours=1)
    while candidate.hour % interval_hours != 0:
        candidate += dt.timedelta(hours=1)
    return candidate


def scheduled_window(run_time: dt.datetime, interval_hours: int) -> tuple[dt.datetime, dt.datetime]:
    """Return the time window covered by one scheduled run."""

    end = run_time.astimezone(dt.timezone.utc)
    start = end - dt.timedelta(hours=interval_hours)
    return start, end


class Scheduler:
    """Run a callback forever using the configured UTC schedule policy."""

    def __init__(self, interval_hours: int, minute: int) -> None:
        self.interval_hours = interval_hours
        self.minute = minute

    def run_forever(self, callback: Callable[[dt.datetime, dt.datetime], bool]) -> None:
        """Sleep until the next scheduled slot, then execute the callback for that window."""

        next_run = next_scheduled_time(
            dt.datetime.now(dt.timezone.utc),
            interval_hours=self.interval_hours,
            minute=self.minute,
        )
        LOG.info("First scheduled run at %s", next_run)

        while True:
            now = dt.datetime.now(dt.timezone.utc)
            sleep_seconds = max(0.0, (next_run - now).total_seconds())
            if sleep_seconds > 0:
                LOG.info("Sleeping %.0f seconds until %s", sleep_seconds, next_run)
                time.sleep(sleep_seconds)

            start, end = scheduled_window(next_run, self.interval_hours)
            LOG.info("Launching scheduled run start=%s end=%s", start, end)
            success = callback(start, end)
            if not success:
                LOG.warning("Scheduled run failed start=%s end=%s", start, end)
            next_run += dt.timedelta(hours=self.interval_hours)
