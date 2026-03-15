"""Typed settings models and the config merge pipeline.

The project accepts configuration from YAML, environment variables, and CLI
overrides. This module normalizes those inputs into a single ``Settings``
object consumed by the CLI and runtime services.
"""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def parse_datetime(value: str | None) -> dt.datetime | None:
    """Parse an ISO8601 timestamp and normalize it to UTC."""
    if value is None or value == "":
        return None
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _parse_bool(value: str) -> bool:
    """Parse common truthy string values used in environment variables."""
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``extra`` into ``base`` without mutating either input."""
    merged = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        elif value is not None:
            merged[key] = value
    return merged


@dataclass(frozen=True, slots=True)
class HarvestSettings:
    """Configuration for one harvest run or the scheduled harvest loop."""
    collector: str | None
    temporary_dir: Path
    batch_size: int
    ip_version: str
    max_download_workers: int
    max_parse_workers: int
    discovery_page_size: int
    project: str
    start: dt.datetime | None = None
    end: dt.datetime | None = None


@dataclass(frozen=True, slots=True)
class ROVSettings:
    """Settings for the Route Origin Validation client."""

    enabled: bool
    endpoint: str
    timeout_seconds: int
    max_workers: int
    request_batch_size: int


@dataclass(frozen=True, slots=True)
class ClickHouseSettings:
    """Settings for the ClickHouse storage backend."""
    host: str
    port: int
    database: str
    user: str
    password: str
    use_http_driver: bool
    async_inserts: bool


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Settings that control scheduling, timeouts, and logging."""
    timeout_seconds: int
    log_level: str
    schedule_interval_hours: int
    schedule_minute: int


@dataclass(frozen=True, slots=True)
class Settings:
    """Top-level application settings assembled from all configuration sources."""
    harvest: HarvestSettings
    rov: ROVSettings
    clickhouse: ClickHouseSettings
    runtime: RuntimeSettings


DEFAULTS: dict[str, Any] = {
    "harvest": {
        "collector": None,
        "temporary_dir": "./.bgp_tmp",
        "batch_size": 100_000,
        "ip_version": "6",
        "max_download_workers": 4,
        "max_parse_workers": 4,
        "discovery_page_size": 500,
        "project": "routeviews",
        "start": None,
        "end": None,
    },
    "rov": {
        "enabled": True,
        "endpoint": "http://127.0.0.1:8323/validity",
        "timeout_seconds": 30,
        "max_workers": 10,
        "request_batch_size": 64,
    },
    "clickhouse": {
        "host": "localhost",
        "port": 9000,
        "database": "bgp",
        "user": "default",
        "password": "",
        "use_http_driver": False,
        "async_inserts": True,
    },
    "runtime": {
        "timeout_seconds": 3600,
        "log_level": "INFO",
        "schedule_interval_hours": 2,
        "schedule_minute": 30,
    },
}


ENV_MAP: dict[str, tuple[str, str, Any]] = {
    "BGP_HARVEST_COLLECTOR": ("harvest", "collector", str),
    "BGP_HARVEST_TEMPORARY_DIR": ("harvest", "temporary_dir", str),
    "BGP_HARVEST_BATCH_SIZE": ("harvest", "batch_size", int),
    "BGP_HARVEST_IP_VERSION": ("harvest", "ip_version", str),
    "BGP_HARVEST_MAX_DOWNLOAD_WORKERS": ("harvest", "max_download_workers", int),
    "BGP_HARVEST_MAX_PARSE_WORKERS": ("harvest", "max_parse_workers", int),
    "BGP_HARVEST_DISCOVERY_PAGE_SIZE": ("harvest", "discovery_page_size", int),
    "BGP_HARVEST_PROJECT": ("harvest", "project", str),
    "BGP_HARVEST_START": ("harvest", "start", str),
    "BGP_HARVEST_END": ("harvest", "end", str),
    "BGP_HARVEST_ROV_ENABLED": ("rov", "enabled", _parse_bool),
    "BGP_HARVEST_ROV_ENDPOINT": ("rov", "endpoint", str),
    "BGP_HARVEST_ROV_TIMEOUT_SECONDS": ("rov", "timeout_seconds", int),
    "BGP_HARVEST_ROV_MAX_WORKERS": ("rov", "max_workers", int),
    "BGP_HARVEST_ROV_REQUEST_BATCH_SIZE": ("rov", "request_batch_size", int),
    "BGP_HARVEST_CLICKHOUSE_HOST": ("clickhouse", "host", str),
    "BGP_HARVEST_CLICKHOUSE_PORT": ("clickhouse", "port", int),
    "BGP_HARVEST_CLICKHOUSE_DATABASE": ("clickhouse", "database", str),
    "BGP_HARVEST_CLICKHOUSE_USER": ("clickhouse", "user", str),
    "BGP_HARVEST_CLICKHOUSE_PASSWORD": ("clickhouse", "password", str),
    "BGP_HARVEST_CLICKHOUSE_USE_HTTP_DRIVER": ("clickhouse", "use_http_driver", _parse_bool),
    "BGP_HARVEST_CLICKHOUSE_ASYNC_INSERTS": ("clickhouse", "async_inserts", _parse_bool),
    "BGP_HARVEST_RUNTIME_TIMEOUT_SECONDS": ("runtime", "timeout_seconds", int),
    "BGP_HARVEST_RUNTIME_LOG_LEVEL": ("runtime", "log_level", str),
    "BGP_HARVEST_RUNTIME_SCHEDULE_INTERVAL_HOURS": ("runtime", "schedule_interval_hours", int),
    "BGP_HARVEST_RUNTIME_SCHEDULE_MINUTE": ("runtime", "schedule_minute", int),
}


def _load_yaml(path: Path | None) -> dict[str, Any]:
    """Load a YAML config file, returning an empty mapping when no file is provided."""
    if path is None or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError("Config file must contain a mapping at the top level")
    return payload


def _env_overrides() -> dict[str, Any]:
    """Read supported environment variables into the nested settings structure."""
    payload: dict[str, Any] = {}
    for env_name, (section, key, parser) in ENV_MAP.items():
        raw = os.getenv(env_name)
        if raw is None:
            continue
        payload.setdefault(section, {})[key] = parser(raw)
    return payload


def _build_dataclasses(payload: dict[str, Any]) -> Settings:
    """Convert the merged raw mapping into typed settings objects."""
    harvest_data = payload["harvest"]
    rov_data = payload["rov"]
    clickhouse_data = payload["clickhouse"]
    runtime_data = payload["runtime"]

    return Settings(
        harvest=HarvestSettings(
            collector=harvest_data.get("collector"),
            temporary_dir=Path(harvest_data["temporary_dir"]),
            batch_size=int(harvest_data["batch_size"]),
            ip_version=str(harvest_data["ip_version"]),
            max_download_workers=int(harvest_data["max_download_workers"]),
            max_parse_workers=int(harvest_data["max_parse_workers"]),
            discovery_page_size=int(harvest_data["discovery_page_size"]),
            project=str(harvest_data["project"]),
            start=parse_datetime(harvest_data.get("start")),
            end=parse_datetime(harvest_data.get("end")),
        ),
        rov=ROVSettings(
            enabled=bool(rov_data["enabled"]),
            endpoint=str(rov_data["endpoint"]),
            timeout_seconds=int(rov_data["timeout_seconds"]),
            max_workers=int(rov_data["max_workers"]),
            request_batch_size=int(rov_data["request_batch_size"]),
        ),
        clickhouse=ClickHouseSettings(
            host=str(clickhouse_data["host"]),
            port=int(clickhouse_data["port"]),
            database=str(clickhouse_data["database"]),
            user=str(clickhouse_data["user"]),
            password=str(clickhouse_data["password"]),
            use_http_driver=bool(clickhouse_data["use_http_driver"]),
            async_inserts=bool(clickhouse_data["async_inserts"]),
        ),
        runtime=RuntimeSettings(
            timeout_seconds=int(runtime_data["timeout_seconds"]),
            log_level=str(runtime_data["log_level"]).upper(),
            schedule_interval_hours=int(runtime_data["schedule_interval_hours"]),
            schedule_minute=int(runtime_data["schedule_minute"]),
        ),
    )


def build_settings(
    config_path: Path | None = None,
    overrides: dict[str, Any] | None = None,
) -> Settings:
    """Merge defaults, YAML, environment, and CLI overrides into ``Settings``."""
    merged = DEFAULTS
    merged = _deep_merge(merged, _load_yaml(config_path))
    merged = _deep_merge(merged, _env_overrides())
    if overrides:
        merged = _deep_merge(merged, overrides)
    return _build_dataclasses(merged)
