from __future__ import annotations

import datetime as dt
from pathlib import Path

from bgp_harvest.models import RibResource, RouteRecord, ValidationState, VrpObject, VrpSnapshot
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
        self.vrp_snapshots: list[VrpSnapshot] = []
        self.metadata_updates: list[tuple[dt.datetime, dt.datetime, int | None, dt.datetime | None]] = []

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def ensure_schema(self) -> None:
        self.schema_ready = True

    def ingest_routes(self, routes) -> None:
        self.ingested.extend(list(routes))

    def store_vrp_snapshot(self, snapshot: VrpSnapshot) -> None:
        self.vrp_snapshots.append(snapshot)

    def update_route_metadata(
        self,
        start_time: dt.datetime,
        end_time: dt.datetime,
        vrp_snapshot_id: int | None = None,
        vrp_generated_at: dt.datetime | None = None,
    ) -> None:
        self.metadata_updates.append((start_time, end_time, vrp_snapshot_id, vrp_generated_at))


class FakeSnapshotValidator:
    def __init__(self) -> None:
        self.reset_calls = 0
        generated_at = dt.datetime(2026, 3, 14, 6, 0, tzinfo=dt.timezone.utc)
        self.snapshot = VrpSnapshot(
            snapshot_id=42,
            source_endpoint="http://127.0.0.1:8323/json",
            generated_at=generated_at,
            objects=(
                VrpObject(
                    vrp_id=7,
                    prefix="2001:db8::/32",
                    prefix_length=32,
                    max_length=48,
                    asn=64496,
                    ta="test",
                    ip_version=6,
                ),
            ),
        )

    def reset(self) -> None:
        self.reset_calls += 1

    def current_snapshot(self) -> VrpSnapshot:
        return self.snapshot

    def bulk_validate(self, pairs: set[tuple[str, int]]) -> dict[tuple[str, int], ValidationState]:
        return {(prefix, origin): ValidationState.VALID for prefix, origin in pairs}


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
    assert repository.vrp_snapshots == []
    assert repository.metadata_updates[0][2] is None


def test_harvest_window_service_persists_loaded_vrp_snapshot(tmp_path: Path) -> None:
    ts = dt.datetime(2026, 3, 14, tzinfo=dt.timezone.utc)
    resource = RibResource(
        url="https://example.test/rib.bz2",
        collector_id="rv1",
        ts_start=ts,
        ts_end=ts,
    )
    repository = FakeRepository()
    validator = FakeSnapshotValidator()
    service = HarvestJob(
        discovery=FakeDiscovery([resource]),
        fetcher=FakeFetcher(),
        parser=FakeParser(),
        validation_service=RouteAnnotator(validator),
        repository=repository,
        temporary_dir=tmp_path,
        max_download_workers=1,
        max_parse_workers=1,
    )

    service.run(ts, ts + dt.timedelta(hours=2))

    assert validator.reset_calls == 1
    assert len(repository.vrp_snapshots) == 1
    assert repository.vrp_snapshots[0].snapshot_id == 42
    assert repository.metadata_updates[0][2] == 42
    assert repository.metadata_updates[0][3] == validator.snapshot.generated_at
