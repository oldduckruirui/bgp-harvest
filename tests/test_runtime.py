from __future__ import annotations

import datetime as dt

from bgp_harvest.pipeline import compute_path_id, next_scheduled_time, scheduled_window


def test_next_scheduled_time_advances_to_even_hour_boundary() -> None:
    now = dt.datetime(2026, 3, 14, 1, 45, tzinfo=dt.timezone.utc)
    scheduled = next_scheduled_time(now, interval_hours=2, minute=30)
    assert scheduled == dt.datetime(2026, 3, 14, 2, 30, tzinfo=dt.timezone.utc)


def test_scheduled_window_matches_interval() -> None:
    run_time = dt.datetime(2026, 3, 14, 4, 30, tzinfo=dt.timezone.utc)
    start, end = scheduled_window(run_time, interval_hours=2)
    assert start == dt.datetime(2026, 3, 14, 2, 30, tzinfo=dt.timezone.utc)
    assert end == run_time


def test_compute_path_id_is_stable() -> None:
    assert compute_path_id([64512, 64496]) == compute_path_id([64512, 64496])
