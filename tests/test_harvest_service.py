from __future__ import annotations

import datetime as dt
from pathlib import Path

from bgp_harvest.models import RibResource, RouteRecord
from bgp_harvest.pipeline import HarvestJob, RouteAnnotator


class FakeDiscovery:
    def __init__(self, resources: list[RibResource]) -> None:
        self.resources = resources

    def discover(self, start: dt.datetime, end: dt.datetime, collector: str | None = None) -> list[RibResource]:
        return self.resources


class FakeFetcher:
    def fetch(self, resource: RibResource, directory: str):
        path = Path(directory) / f"{resource.collector_id}.mrt.bz2"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture")
        return resource.with_local_path(path)


class FakeParser:
    def parse(self, resource):
        yield RouteRecord.from_as_path(
            prefix="2001:db8::/32",
            as_path=[64512, 64496],
            collector=resource.collector_id,
            timestamp=resource.ts_start,
        )


class FakeRepository:
    def __init__(self) -> None:
        self.connected = False
        self.schema_ready = False
        self.ingested: list[RouteRecord] = []
        self.metadata_updates: list[tuple[dt.datetime, dt.datetime]] = []

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def ensure_schema(self) -> None:
        self.schema_ready = True

    def ingest_routes(self, routes) -> None:
        self.ingested.extend(list(routes))

    def update_route_metadata(self, start_time: dt.datetime, end_time: dt.datetime) -> None:
        self.metadata_updates.append((start_time, end_time))


def test_harvest_window_service_runs_end_to_end(tmp_path: Path) -> None:
    ts = dt.datetime(2026, 3, 14, tzinfo=dt.timezone.utc)
    resource = RibResource(
        url="https://example.test/rib.bz2",
        collector_id="rv1",
        ts_start=ts,
        ts_end=ts,
    )
    repository = FakeRepository()
    service = HarvestJob(
        discovery=FakeDiscovery([resource]),
        fetcher=FakeFetcher(),
        parser=FakeParser(),
        validation_service=RouteAnnotator(None),
        repository=repository,
        temporary_dir=tmp_path,
        max_download_workers=1,
        max_parse_workers=1,
    )

    summary = service.run(ts, ts + dt.timedelta(hours=2))

    assert summary.resources_discovered == 1
    assert summary.resources_downloaded == 1
    assert summary.routes_harvested == 1
    assert repository.schema_ready is True
    assert len(repository.ingested) == 1
    assert len(repository.metadata_updates) == 1
