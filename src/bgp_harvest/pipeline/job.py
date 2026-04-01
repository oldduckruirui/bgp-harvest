"""End-to-end workflow for harvesting one explicit time window."""

from __future__ import annotations

import datetime as dt
import logging
import os
import time
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Sequence

from ..models.types import MaterializedRibResource, RibResource, RouteRecord
from ..models.interfaces import RibDiscovery, RibFetcher, RibParser, RouteRepository
from .rov_validation import RouteAnnotator

LOG = logging.getLogger(__name__)
ParsedRouteRow = tuple[str, tuple[int, ...], float]


@dataclass(frozen=True, slots=True)
class HarvestSummary:
    """Result summary emitted after one harvest window completes."""

    resources_discovered: int
    resources_downloaded: int
    routes_harvested: int
    min_timestamp: dt.datetime | None
    max_timestamp: dt.datetime | None


@dataclass(frozen=True, slots=True)
class _ParsedFileResult:
    """Parse result for one downloaded RIB file."""

    resource: MaterializedRibResource
    rows: list[ParsedRouteRow]
    parse_seconds: float


class HarvestJob:
    """Coordinate discovery, download, parse, validate, and storage for one window."""

    def __init__(
        self,
        discovery: RibDiscovery,
        fetcher: RibFetcher,
        parser: RibParser,
        validation_service: RouteAnnotator,
        repository: RouteRepository,
        temporary_dir: Path,
        insert_batch_size: int = 100_000,
        max_download_workers: int = 4,
        max_parse_workers: int = 4,
    ) -> None:
        self.discovery = discovery
        self.fetcher = fetcher
        self.parser = parser
        self.validation_service = validation_service
        self.repository = repository
        self.temporary_dir = temporary_dir
        self.insert_batch_size = max(1, insert_batch_size)
        self.max_download_workers = max(1, max_download_workers)
        self.max_parse_workers = max(1, max_parse_workers)

    def run(
        self,
        start: dt.datetime,
        end: dt.datetime,
        collector: str | None = None,
    ) -> HarvestSummary:
        """Execute one complete harvest window."""

        start_utc = start.astimezone(dt.timezone.utc)
        end_utc = end.astimezone(dt.timezone.utc)
        LOG.info("Starting harvest start=%s end=%s collector=%s", start_utc, end_utc, collector)

        self.repository.connect()
        self.repository.ensure_schema()
        self.validation_service.reset()

        materialized: list[MaterializedRibResource] = []
        harvest_started_at = time.perf_counter()
        try:
            resources = list(self.discovery.discover(start_utc, end_utc, collector=collector))
            if not resources:
                LOG.info("No RIB resources found start=%s end=%s collector=%s", start_utc, end_utc, collector)
                return HarvestSummary(0, 0, 0, None, None)

            LOG.info("Discovered %s RIB resources; starting download stage", len(resources))
            materialized = self._download_resources(resources)
            LOG.info("Download stage completed; starting parse/validate/store pipeline files=%s", len(materialized))

            summary = self._process_downloaded_resources(materialized)
            vrp_snapshot = self.validation_service.current_vrp_snapshot()
            if summary.min_timestamp and summary.max_timestamp:
                self.repository.update_route_metadata(
                    summary.min_timestamp,
                    summary.max_timestamp,
                    vrp_snapshot_id=vrp_snapshot.snapshot_id if vrp_snapshot is not None else None,
                    vrp_generated_at=vrp_snapshot.generated_at if vrp_snapshot is not None else None,
                )
                LOG.info(
                    "Recorded harvest metadata start=%s end=%s vrp_snapshot_id=%s",
                    summary.min_timestamp,
                    summary.max_timestamp,
                    vrp_snapshot.snapshot_id if vrp_snapshot is not None else None,
                )

            LOG.info(
                "Harvest completed resources=%s downloaded=%s routes=%s elapsed_seconds=%.3f",
                len(resources),
                len(materialized),
                summary.routes_harvested,
                time.perf_counter() - harvest_started_at,
            )
            return HarvestSummary(
                resources_discovered=len(resources),
                resources_downloaded=len(materialized),
                routes_harvested=summary.routes_harvested,
                min_timestamp=summary.min_timestamp,
                max_timestamp=summary.max_timestamp,
            )
        finally:
            self._cleanup(materialized)
            self.repository.disconnect()

    def _download_resources(self, resources: Sequence[RibResource]) -> list[MaterializedRibResource]:
        """Download all discovered resources into the configured temporary directory."""

        self.temporary_dir.mkdir(parents=True, exist_ok=True)
        downloaded: list[MaterializedRibResource] = []
        total = len(resources)
        with ThreadPoolExecutor(max_workers=min(len(resources), self.max_download_workers)) as executor:
            future_map = {
                executor.submit(self.fetcher.fetch, resource, str(self.temporary_dir)): resource
                for resource in resources
            }
            try:
                for future in as_completed(future_map):
                    resource = future_map[future]
                    try:
                        downloaded_resource = future.result()
                        downloaded.append(downloaded_resource)
                        LOG.info(
                            "Downloaded %s/%s collector=%s path=%s",
                            len(downloaded),
                            total,
                            downloaded_resource.collector_id,
                            downloaded_resource.local_path,
                        )
                    except Exception:
                        LOG.error(
                            "Download failed collector=%s ts_start=%s",
                            resource.collector_id,
                            resource.ts_start,
                            exc_info=True,
                        )
                        raise
            except BaseException:
                cleanup_temporary_directory(self.temporary_dir)
                raise
        return downloaded

    def _process_downloaded_resources(self, resources: Sequence[MaterializedRibResource]) -> HarvestSummary:
        """Run full-collection parse, validation, and storage in explicit stages."""

        if not resources:
            return HarvestSummary(0, 0, 0, None, None)

        parse_started_at = time.perf_counter()
        parsed_routes = self._parse_all_resources(resources)
        LOG.info(
            "Parse stage completed files=%s routes=%s elapsed_seconds=%.3f",
            len(resources),
            len(parsed_routes),
            time.perf_counter() - parse_started_at,
        )

        validation_started_at = time.perf_counter()
        validated_routes = self.validation_service.annotate(parsed_routes)
        LOG.info(
            "Validation stage completed routes=%s elapsed_seconds=%.3f",
            len(validated_routes),
            time.perf_counter() - validation_started_at,
        )

        vrp_snapshot = self.validation_service.current_vrp_snapshot()
        if vrp_snapshot is not None:
            self.repository.store_vrp_snapshot(vrp_snapshot)
            LOG.info(
                "Persisted VRP snapshot snapshot_id=%s objects=%s",
                vrp_snapshot.snapshot_id,
                len(vrp_snapshot.objects),
            )

        storage_started_at = time.perf_counter()
        self.repository.ingest_routes(validated_routes)
        LOG.info(
            "Storage stage completed routes=%s elapsed_seconds=%.3f",
            len(validated_routes),
            time.perf_counter() - storage_started_at,
        )

        min_timestamp, max_timestamp = self._route_time_bounds(validated_routes)
        return HarvestSummary(
            resources_discovered=len(resources),
            resources_downloaded=len(resources),
            routes_harvested=len(validated_routes),
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
        )

    def _parse_all_resources(
        self,
        resources: Sequence[MaterializedRibResource],
    ) -> list[RouteRecord]:
        """Parse every downloaded RIB file and return one in-memory route list."""

        parsed_routes: list[RouteRecord] = []
        if self.max_parse_workers <= 1:
            LOG.info("Using in-process parse loop workers=%s", self.max_parse_workers)
            total_files = len(resources)
            for file_index, resource in enumerate(resources, start=1):
                parsed_result = _parse_resource(self.parser, resource)
                file_routes = _iter_routes(parsed_result)
                parsed_routes.extend(file_routes)
                LOG.info(
                    "Completed parse file=%s/%s collector=%s routes=%s elapsed_seconds=%.3f",
                    file_index,
                    total_files,
                    parsed_result.resource.collector_id,
                    len(file_routes),
                    parsed_result.parse_seconds,
                )
            return parsed_routes

        worker_count = min(len(resources), self.max_parse_workers)
        LOG.info("Starting parse worker pool workers=%s", worker_count)
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_map: dict[Future[_ParsedFileResult], MaterializedRibResource] = {
                executor.submit(_parse_resource, self.parser, resource): resource
                for resource in resources
            }
            completed_files = 0
            total_files = len(resources)
            for future in as_completed(future_map):
                resource = future_map[future]
                try:
                    parsed_result = future.result()
                except Exception:
                    LOG.error(
                        "Parse worker failed collector=%s path=%s",
                        resource.collector_id,
                        resource.local_path,
                        exc_info=True,
                    )
                    raise

                completed_files += 1
                file_routes = _iter_routes(parsed_result)
                parsed_routes.extend(file_routes)
                LOG.info(
                    "Completed parse file=%s/%s collector=%s routes=%s elapsed_seconds=%.3f",
                    completed_files,
                    total_files,
                    parsed_result.resource.collector_id,
                    len(file_routes),
                    parsed_result.parse_seconds,
                )

        return parsed_routes

    @staticmethod
    def _route_time_bounds(routes: Sequence[RouteRecord]) -> tuple[dt.datetime | None, dt.datetime | None]:
        """Compute the inclusive timestamp bounds for one route collection."""

        if not routes:
            return None, None

        min_timestamp = routes[0].timestamp
        max_timestamp = routes[0].timestamp
        for route in routes[1:]:
            if route.timestamp < min_timestamp:
                min_timestamp = route.timestamp
            if route.timestamp > max_timestamp:
                max_timestamp = route.timestamp
        return min_timestamp, max_timestamp

    @staticmethod
    def _cleanup(resources: Sequence[MaterializedRibResource]) -> None:
        """Best-effort cleanup of downloaded temporary files."""

        removed = 0
        for resource in resources:
            try:
                if resource.local_path.exists():
                    os.remove(resource.local_path)
                    removed += 1
            except OSError:
                LOG.warning("Failed to remove %s", resource.local_path, exc_info=True)
        if removed:
            LOG.info("Cleaned up %s temporary RIB files", removed)


def _parse_resource(parser: RibParser, resource: MaterializedRibResource) -> _ParsedFileResult:
    """Parse one downloaded RIB file inside a worker process."""

    parse_started_at = time.perf_counter()
    rows = [
        (
            route.prefix,
            tuple(route.as_path),
            route.timestamp.timestamp(),
        )
        for route in parser.parse(resource)
    ]
    return _ParsedFileResult(
        resource=replace(resource, local_path=resource.local_path.resolve()),
        rows=rows,
        parse_seconds=time.perf_counter() - parse_started_at,
    )


def _iter_routes(parsed_result: _ParsedFileResult) -> Sequence[RouteRecord]:
    """Rebuild route records from a lighter parsed-file payload."""

    collector = parsed_result.resource.collector_id
    return [
        RouteRecord.from_as_path(
            prefix=prefix,
            as_path=as_path,
            collector=collector,
            timestamp=dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc),
        )
        for prefix, as_path, timestamp in parsed_result.rows
    ]


def cleanup_temporary_directory(directory: Path) -> None:
    """Best-effort cleanup for the configured temporary download directory."""

    if not directory.exists():
        return

    for path in sorted(directory.rglob("*"), reverse=True):
        try:
            if path.is_file() or path.is_symlink():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        except OSError:
            LOG.warning("Failed to remove temporary path %s", path, exc_info=True)
