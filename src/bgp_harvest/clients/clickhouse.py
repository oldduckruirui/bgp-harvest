"""ClickHouse repository for AS-path deduplication and route persistence."""

from __future__ import annotations

import datetime as dt
import logging
import time
from pathlib import Path
from typing import Any
from typing import Iterable, Sequence

from ..config.settings import ClickHouseSettings
from ..models.types import RouteRecord, VrpObject, VrpSnapshot
from ..pipeline.path_id import compute_path_id
from ..pipeline.runtime import batch_iterable

LOG = logging.getLogger(__name__)

try:
    from clickhouse_driver import Client as DriverClient
except ImportError:  # pragma: no cover
    DriverClient = None

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover
    clickhouse_connect = None


class ClickHouseRouteRepository:
    """Persist harvested routes into ClickHouse using bulk inserts."""

    _LOOKUP_BATCH_SIZE = 2_000

    def __init__(self, settings: ClickHouseSettings, batch_size: int) -> None:
        self.settings = settings
        self.batch_size = batch_size
        self._client = None
        self._seen_paths: set[int] = set()
        self._paths_cache_loaded = False
        self._insert_settings = (
            {"async_insert": 1, "wait_for_async_insert": 0} if settings.async_inserts else None
        )

    def connect(self) -> None:
        """Open the ClickHouse client and preload known AS-path identifiers."""

        if self._client is not None:
            return
        connect_started_at = time.perf_counter()
        LOG.info(
            "Connecting to ClickHouse host=%s port=%s database=%s user=%s http_driver=%s",
            self.settings.host,
            self.settings.port,
            self.settings.database,
            self.settings.user,
            self.settings.use_http_driver,
        )

        if self.settings.use_http_driver:
            if clickhouse_connect is None:
                raise RuntimeError("clickhouse-connect is not installed")
            self._client = clickhouse_connect.get_client(
                host=self.settings.host,
                port=self.settings.port,
                database=self.settings.database,
                username=self.settings.user,
                password=self.settings.password or None,
            )
        else:
            if DriverClient is None:
                raise RuntimeError("clickhouse-driver is not installed")
            self._client = DriverClient(
                host=self.settings.host,
                port=self.settings.port,
                database=self.settings.database,
                user=self.settings.user,
                password=self.settings.password,
            )
        self._ensure_paths_cache()
        LOG.info(
            "Connected to ClickHouse database=%s elapsed_seconds=%.3f",
            self.settings.database,
            time.perf_counter() - connect_started_at,
        )

    def disconnect(self) -> None:
        """Drop the active client reference."""

        self._client = None

    def ensure_schema(self) -> None:
        """Create required ClickHouse tables from the bundled schema file."""

        schema_path = Path(__file__).resolve().parent / "sql" / "schema.sql"
        sql = schema_path.read_text(encoding="utf-8")
        schema_started_at = time.perf_counter()
        LOG.info("Ensuring ClickHouse schema from %s", schema_path)
        for statement in sql.split(";"):
            stmt = statement.strip()
            if not stmt:
                continue
            if hasattr(self.client, "execute"):
                self.client.execute(stmt)
            else:
                self.client.command(stmt)
        LOG.info("ClickHouse schema is ready elapsed_seconds=%.3f", time.perf_counter() - schema_started_at)

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Repository is not connected")
        return self._client

    def ingest_routes(self, routes: Iterable[RouteRecord]) -> None:
        """Persist route batches, creating missing AS-path rows on demand."""

        ingest_started_at = time.perf_counter()
        total_routes = 0
        batch_count = 0
        for batch in batch_iterable(routes, self.batch_size):
            batch_count += 1
            total_routes += len(batch)
            self._ingest_batch(batch, batch_number=batch_count, total_routes=total_routes)
        LOG.info(
            "Storage stage completed batches=%s routes=%s elapsed_seconds=%.3f",
            batch_count,
            total_routes,
            time.perf_counter() - ingest_started_at,
        )

    def store_vrp_snapshot(self, snapshot: VrpSnapshot) -> None:
        """Persist a deduplicated VRP snapshot and its object references."""

        if self._snapshot_exists(snapshot.snapshot_id):
            LOG.info("Skipping existing VRP snapshot snapshot_id=%s", snapshot.snapshot_id)
            return

        objects_by_id = {item.vrp_id: item for item in snapshot.objects}
        object_rows = list(objects_by_id.values())
        new_object_rows = self._filter_new_vrp_objects(object_rows)
        snapshot_started_at = time.perf_counter()
        if new_object_rows:
            self._insert_vrp_objects(new_object_rows)
        self._insert_vrp_snapshot(
            snapshot_id=snapshot.snapshot_id,
            generated_at=snapshot.generated_at,
            source_endpoint=snapshot.source_endpoint,
            object_count=len(object_rows),
        )
        if object_rows:
            self._insert_vrp_snapshot_entries(snapshot.snapshot_id, list(objects_by_id))
        LOG.info(
            "Stored VRP snapshot snapshot_id=%s objects=%s new_objects=%s elapsed_seconds=%.3f",
            snapshot.snapshot_id,
            len(object_rows),
            len(new_object_rows),
            time.perf_counter() - snapshot_started_at,
        )

    def update_route_metadata(
        self,
        start_time: dt.datetime,
        end_time: dt.datetime,
        vrp_snapshot_id: int | None = None,
        vrp_generated_at: dt.datetime | None = None,
    ) -> None:
        """Record the time range covered by a successful harvest run."""

        payload = [
            (
                self._normalize_ts(start_time),
                self._normalize_ts(end_time),
                vrp_snapshot_id,
                self._normalize_optional_ts(vrp_generated_at),
            )
        ]
        if hasattr(self.client, "execute"):
            kwargs: dict[str, Any] = {"types_check": False}
            if self._insert_settings:
                kwargs["settings"] = self._insert_settings
            self.client.execute(
                (
                    "INSERT INTO route_metadata "
                    "(start_time, end_time, vrp_snapshot_id, vrp_generated_at) VALUES"
                ),
                payload,
                **kwargs,
            )
            return

        self.client.insert(
            "route_metadata",
            payload,
            column_names=["start_time", "end_time", "vrp_snapshot_id", "vrp_generated_at"],
            settings=self._insert_settings,
        )

    def _ingest_batch(
        self,
        routes: Sequence[RouteRecord],
        batch_number: int,
        total_routes: int,
    ) -> None:
        """Build AS-path and route insert rows for one batch."""

        batch_started_at = time.perf_counter()
        self._ensure_paths_cache()
        path_rows: dict[int, Sequence[int]] = {}
        prefixes: list[str] = []
        path_ids: list[int] = []
        origins: list[int] = []
        collectors: list[str] = []
        timestamps: list[dt.datetime] = []
        statuses: list[str] = []

        for route in routes:
            path_id = compute_path_id(route.as_path)
            route.path_id = path_id
            if path_id not in self._seen_paths and path_id not in path_rows:
                path_rows[path_id] = route.as_path

            prefixes.append(route.prefix)
            path_ids.append(path_id)
            origins.append(route.origin)
            collectors.append(route.collector)
            timestamps.append(self._normalize_ts(route.timestamp))
            statuses.append(route.rpki_status.value)

        if path_rows:
            self._insert_paths(list(path_rows.items()))
        if prefixes:
            self._insert_routes(prefixes, path_ids, origins, collectors, timestamps, statuses)
        LOG.info(
            "Inserted ClickHouse batch=%s batch_routes=%s cumulative_routes=%s new_paths=%s elapsed_seconds=%.3f",
            batch_number,
            len(prefixes),
            total_routes,
            len(path_rows),
            time.perf_counter() - batch_started_at,
        )

    def _insert_paths(self, rows: Sequence[tuple[int, Sequence[int]]]) -> None:
        """Insert new AS-path rows."""

        row_payload = [(path_id, list(as_path)) for path_id, as_path in rows]
        if hasattr(self.client, "execute"):
            kwargs: dict[str, Any] = {"types_check": False}
            if self._insert_settings:
                kwargs["settings"] = self._insert_settings
            try:
                self.client.execute(
                    "INSERT INTO as_paths (path_id, as_path) VALUES",
                    [
                        [path_id for path_id, _ in rows],
                        [list(as_path) for _, as_path in rows],
                    ],
                    columnar=True,
                    **kwargs,
                )
            except TypeError:
                self.client.execute(
                    "INSERT INTO as_paths (path_id, as_path) VALUES",
                    row_payload,
                    **kwargs,
                )
        else:
            try:
                self.client.insert(
                    "as_paths",
                    [
                        [path_id for path_id, _ in rows],
                        [list(as_path) for _, as_path in rows],
                    ],
                    column_names=["path_id", "as_path"],
                    settings=self._insert_settings,
                    column_oriented=True,
                )
            except TypeError:
                self.client.insert(
                    "as_paths",
                    row_payload,
                    column_names=["path_id", "as_path"],
                    settings=self._insert_settings,
                )
        self._seen_paths.update(path_id for path_id, _ in rows)

    def _insert_vrp_objects(self, objects: Sequence[VrpObject]) -> None:
        """Insert or upsert unique VRP object rows."""

        row_payload = [
            (
                int(item.vrp_id),
                item.prefix,
                int(item.prefix_length),
                int(item.max_length),
                int(item.asn),
                item.ta,
                int(item.ip_version),
            )
            for item in objects
        ]
        if hasattr(self.client, "execute"):
            kwargs: dict[str, Any] = {"types_check": False}
            if self._insert_settings:
                kwargs["settings"] = self._insert_settings
            self.client.execute(
                (
                    "INSERT INTO vrp_objects "
                    "(vrp_id, prefix, prefix_length, max_length, asn, ta, ip_version) VALUES"
                ),
                row_payload,
                **kwargs,
            )
            return

        self.client.insert(
            "vrp_objects",
            row_payload,
            column_names=["vrp_id", "prefix", "prefix_length", "max_length", "asn", "ta", "ip_version"],
            settings=self._insert_settings,
        )

    def _insert_vrp_snapshot(
        self,
        snapshot_id: int,
        generated_at: dt.datetime | None,
        source_endpoint: str,
        object_count: int,
    ) -> None:
        """Insert one VRP snapshot metadata row."""

        row_payload = [
            (
                int(snapshot_id),
                self._normalize_optional_ts(generated_at),
                source_endpoint,
                int(object_count),
            )
        ]
        if hasattr(self.client, "execute"):
            kwargs: dict[str, Any] = {"types_check": False}
            if self._insert_settings:
                kwargs["settings"] = self._insert_settings
            self.client.execute(
                "INSERT INTO vrp_snapshots (snapshot_id, generated_at, source_endpoint, object_count) VALUES",
                row_payload,
                **kwargs,
            )
            return

        self.client.insert(
            "vrp_snapshots",
            row_payload,
            column_names=["snapshot_id", "generated_at", "source_endpoint", "object_count"],
            settings=self._insert_settings,
        )

    def _insert_vrp_snapshot_entries(self, snapshot_id: int, vrp_ids: Sequence[int]) -> None:
        """Insert snapshot-to-object reference rows."""

        row_payload = [(int(snapshot_id), int(vrp_id)) for vrp_id in vrp_ids]
        if hasattr(self.client, "execute"):
            kwargs: dict[str, Any] = {"types_check": False}
            if self._insert_settings:
                kwargs["settings"] = self._insert_settings
            self.client.execute(
                "INSERT INTO vrp_snapshot_entries (snapshot_id, vrp_id) VALUES",
                row_payload,
                **kwargs,
            )
            return

        self.client.insert(
            "vrp_snapshot_entries",
            row_payload,
            column_names=["snapshot_id", "vrp_id"],
            settings=self._insert_settings,
        )

    def _filter_new_vrp_objects(self, objects: Sequence[VrpObject]) -> list[VrpObject]:
        """Return only VRP objects that are not already present in ClickHouse."""

        if not objects:
            return []
        existing_ids = self._fetch_existing_vrp_object_ids([item.vrp_id for item in objects])
        return [item for item in objects if item.vrp_id not in existing_ids]

    def _insert_routes(
        self,
        prefixes: Sequence[str],
        path_ids: Sequence[int],
        origins: Sequence[int],
        collectors: Sequence[str],
        timestamps: Sequence[dt.datetime],
        statuses: Sequence[str],
    ) -> None:
        """Insert route rows for one batch."""

        row_payload = list(zip(prefixes, path_ids, origins, collectors, timestamps, statuses, strict=False))
        if hasattr(self.client, "execute"):
            kwargs: dict[str, Any] = {"types_check": False}
            if self._insert_settings:
                kwargs["settings"] = self._insert_settings
            try:
                self.client.execute(
                    "INSERT INTO bgp_routes (prefix, path_id, origin, collector, timestamp, rpki_status) VALUES",
                    [list(prefixes), list(path_ids), list(origins), list(collectors), list(timestamps), list(statuses)],
                    columnar=True,
                    **kwargs,
                )
            except TypeError:
                self.client.execute(
                    "INSERT INTO bgp_routes (prefix, path_id, origin, collector, timestamp, rpki_status) VALUES",
                    row_payload,
                    **kwargs,
                )
            return

        try:
            self.client.insert(
                "bgp_routes",
                [list(prefixes), list(path_ids), list(origins), list(collectors), list(timestamps), list(statuses)],
                column_names=["prefix", "path_id", "origin", "collector", "timestamp", "rpki_status"],
                settings=self._insert_settings,
                column_oriented=True,
            )
        except TypeError:
            self.client.insert(
                "bgp_routes",
                row_payload,
                column_names=["prefix", "path_id", "origin", "collector", "timestamp", "rpki_status"],
                settings=self._insert_settings,
            )

    def _ensure_paths_cache(self) -> None:
        """Load existing path identifiers once per repository instance."""
        if self._paths_cache_loaded:
            return
        try:
            self._seen_paths.update(self._fetch_all_path_ids())
            self._paths_cache_loaded = True
            LOG.info("Loaded %s existing AS path ids from ClickHouse", len(self._seen_paths))
        except Exception:
            LOG.debug("Failed to preload path ids", exc_info=True)

    def _fetch_all_path_ids(self) -> set[int]:
        """Fetch all known path identifiers from ClickHouse."""

        query = "SELECT path_id FROM as_paths"
        return self._query_first_column(query)

    def _fetch_existing_vrp_object_ids(self, vrp_ids: Sequence[int]) -> set[int]:
        """Fetch the subset of VRP object ids that already exist."""

        existing_ids: set[int] = set()
        lookup_batch_size = min(self.batch_size, self._LOOKUP_BATCH_SIZE)
        for batch in batch_iterable(vrp_ids, lookup_batch_size):
            id_list = ", ".join(str(int(vrp_id)) for vrp_id in batch)
            query = f"SELECT vrp_id FROM vrp_objects WHERE vrp_id IN ({id_list})"
            existing_ids.update(self._query_first_column(query))
        return existing_ids

    def _snapshot_exists(self, snapshot_id: int) -> bool:
        """Return whether one snapshot id is already present."""

        query = f"SELECT snapshot_id FROM vrp_snapshots WHERE snapshot_id = {int(snapshot_id)} LIMIT 1"
        return bool(self._query_first_column(query))

    def _query_first_column(self, query: str) -> set[int]:
        """Execute a query and return the first column as integers."""

        if hasattr(self.client, "execute"):
            return {int(row[0]) for row in self.client.execute(query)}
        if hasattr(self.client, "query_column"):
            return {int(item) for item in self.client.query_column(query)}
        result = self.client.query(query)
        return {int(item) for item in result.first_column()}

    @staticmethod
    def _normalize_ts(value: dt.datetime) -> dt.datetime:
        """Normalize timestamps before inserting them into ClickHouse."""

        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)

    @classmethod
    def _normalize_optional_ts(cls, value: dt.datetime | None) -> dt.datetime | None:
        """Normalize nullable timestamps before inserting them into ClickHouse."""

        if value is None:
            return None
        return cls._normalize_ts(value)
