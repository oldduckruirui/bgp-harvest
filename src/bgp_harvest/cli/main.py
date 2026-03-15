"""CLI assembly for single-run and scheduled harvest commands."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from multiprocessing import Process
from pathlib import Path
from typing import Any

from ..clients import BGPKitRibClient, ClickHouseRouteRepository
from ..config import Settings, build_settings, parse_datetime
from ..pipeline import (
    HTTPROVValidator,
    HarvestJob,
    PyBGPStreamRibParser,
    RouteAnnotator,
    Scheduler,
    cleanup_temporary_directory,
    default_window,
)

LOG = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser and subcommands."""
    parser = argparse.ArgumentParser(prog="bgp-harvest", description="BGP route harvest CLI")
    parser.add_argument("--config", type=Path, help="Path to YAML config file")

    top_level = parser.add_subparsers(dest="command", required=True)
    harvest = top_level.add_parser("harvest", help="Harvest commands")
    harvest_subcommands = harvest.add_subparsers(dest="harvest_command", required=True)

    run_parser = harvest_subcommands.add_parser("run", help="Run one harvest window")
    schedule_parser = harvest_subcommands.add_parser("schedule", help="Run harvest on a schedule")

    _add_common_options(run_parser)
    _add_common_options(schedule_parser)
    run_parser.add_argument("--start", type=str, help="UTC ISO8601 start timestamp")
    run_parser.add_argument("--end", type=str, help="UTC ISO8601 end timestamp")

    return parser


def main(argv: list[str] | None = None) -> None:
    """Parse CLI arguments, build runtime settings, and execute the requested command."""
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = build_settings(config_path=args.config, overrides=_build_overrides(args))
    configure_logging(settings.runtime.log_level)

    if args.command != "harvest":
        parser.error("Unsupported command")

    if args.harvest_command == "run":
        start = parse_datetime(args.start) or settings.harvest.start
        end = parse_datetime(args.end) or settings.harvest.end
        if start is None or end is None:
            start, end = default_window(settings.runtime.schedule_interval_hours)
        if end <= start:
            raise SystemExit("end must be after start")

        try:
            success = _run_with_timeout(settings, start, end, settings.harvest.collector)
        except KeyboardInterrupt:
            LOG.info("Harvest interrupted")
            raise SystemExit(130)
        if not success:
            raise SystemExit(1)
        return

    scheduler = Scheduler(
        interval_hours=settings.runtime.schedule_interval_hours,
        minute=settings.runtime.schedule_minute,
    )
    try:
        scheduler.run_forever(
            lambda start, end: _run_with_timeout(settings, start, end, settings.harvest.collector)
        )
    except KeyboardInterrupt:
        LOG.info("Scheduler interrupted")


def configure_logging(level: str) -> None:
    """Initialize process-wide logging once per process."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="[ %(asctime)s | %(levelname)s ] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _build_service(settings: Settings) -> HarvestJob:
    """Wire concrete source, parser, validator, and storage implementations together."""
    bgp_client = BGPKitRibClient(
        timeout_seconds=60,
        page_size=settings.harvest.discovery_page_size,
        project=settings.harvest.project,
    )
    parser = PyBGPStreamRibParser(ip_version=settings.harvest.ip_version)
    validator = None
    if settings.rov.enabled:
        validator = HTTPROVValidator(
            endpoint=settings.rov.endpoint,
            timeout_seconds=settings.rov.timeout_seconds,
            max_workers=settings.rov.max_workers,
            request_batch_size=settings.rov.request_batch_size,
        )
    repository = ClickHouseRouteRepository(settings.clickhouse, batch_size=settings.harvest.batch_size)

    return HarvestJob(
        discovery=bgp_client,
        fetcher=bgp_client,
        parser=parser,
        validation_service=RouteAnnotator(validator),
        repository=repository,
        temporary_dir=settings.harvest.temporary_dir,
        insert_batch_size=settings.harvest.batch_size,
        max_download_workers=settings.harvest.max_download_workers,
        max_parse_workers=settings.harvest.max_parse_workers,
    )


def _run_with_timeout(
    settings: Settings,
    start: dt.datetime,
    end: dt.datetime,
    collector: str | None,
) -> bool:
    """Run one harvest window in a child process and enforce the configured timeout."""
    process = Process(target=_harvest_worker, args=(settings, start, end, collector), daemon=False)
    process.start()
    try:
        process.join(settings.runtime.timeout_seconds)
    except KeyboardInterrupt:
        LOG.warning("Interrupt received; terminating active harvest worker")
        if process.is_alive():
            process.terminate()
            process.join()
        cleanup_temporary_directory(settings.harvest.temporary_dir)
        raise
    if process.is_alive():
        LOG.error("Harvest timed out after %s seconds", settings.runtime.timeout_seconds)
        process.terminate()
        process.join()
        cleanup_temporary_directory(settings.harvest.temporary_dir)
        return False
    return process.exitcode == 0


def _harvest_worker(
    settings: Settings,
    start: dt.datetime,
    end: dt.datetime,
    collector: str | None,
) -> None:
    """Execute one harvest window inside the worker process."""
    configure_logging(settings.runtime.log_level)
    service = _build_service(settings)
    summary = service.run(start=start, end=end, collector=collector)
    LOG.info(
        "Completed harvest resources=%s downloaded=%s routes=%s",
        summary.resources_discovered,
        summary.resources_downloaded,
        summary.routes_harvested,
    )


def _build_overrides(args: argparse.Namespace) -> dict[str, Any]:
    """Translate CLI arguments into the nested settings override structure."""
    overrides: dict[str, Any] = {
        "harvest": {},
        "rov": {},
        "clickhouse": {},
        "runtime": {},
    }

    def set_if_present(section: str, key: str, value: Any) -> None:
        if value is not None:
            overrides[section][key] = value

    set_if_present("harvest", "collector", getattr(args, "collector", None))
    set_if_present("harvest", "temporary_dir", _stringify_path(getattr(args, "temporary_dir", None)))
    set_if_present("harvest", "batch_size", getattr(args, "batch_size", None))
    set_if_present("harvest", "ip_version", getattr(args, "ip_version", None))
    set_if_present("harvest", "max_download_workers", getattr(args, "max_download_workers", None))
    set_if_present("harvest", "max_parse_workers", getattr(args, "max_parse_workers", None))
    set_if_present("harvest", "discovery_page_size", getattr(args, "discovery_page_size", None))
    set_if_present("harvest", "project", getattr(args, "project", None))
    set_if_present("rov", "endpoint", getattr(args, "rov_endpoint", None))
    set_if_present("rov", "timeout_seconds", getattr(args, "rov_timeout_seconds", None))
    set_if_present("rov", "max_workers", getattr(args, "rov_max_workers", None))
    set_if_present("rov", "request_batch_size", getattr(args, "rov_request_batch_size", None))
    set_if_present("clickhouse", "host", getattr(args, "clickhouse_host", None))
    set_if_present("clickhouse", "port", getattr(args, "clickhouse_port", None))
    set_if_present("clickhouse", "database", getattr(args, "clickhouse_database", None))
    set_if_present("clickhouse", "user", getattr(args, "clickhouse_user", None))
    set_if_present("clickhouse", "password", getattr(args, "clickhouse_password", None))
    set_if_present("runtime", "timeout_seconds", getattr(args, "timeout_seconds", None))
    set_if_present("runtime", "log_level", getattr(args, "log_level", None))
    set_if_present(
        "runtime",
        "schedule_interval_hours",
        getattr(args, "schedule_interval_hours", None),
    )
    set_if_present("runtime", "schedule_minute", getattr(args, "schedule_minute", None))

    if getattr(args, "skip_validation", False):
        overrides["rov"]["enabled"] = False
    if getattr(args, "clickhouse_use_http_driver", False):
        overrides["clickhouse"]["use_http_driver"] = True
    if getattr(args, "disable_clickhouse_async_inserts", False):
        overrides["clickhouse"]["async_inserts"] = False

    return {section: values for section, values in overrides.items() if values}


def _stringify_path(path: Path | None) -> str | None:
    return None if path is None else str(path)


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    """Register options shared by ``harvest run`` and ``harvest schedule``."""
    parser.add_argument("--collector", type=str)
    parser.add_argument("--temporary-dir", type=Path)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--ip-version", type=str)
    parser.add_argument("--max-download-workers", type=int)
    parser.add_argument("--max-parse-workers", type=int)
    parser.add_argument("--discovery-page-size", type=int)
    parser.add_argument("--project", type=str)
    parser.add_argument("--rov-endpoint", type=str)
    parser.add_argument("--rov-timeout-seconds", type=int)
    parser.add_argument("--rov-max-workers", type=int)
    parser.add_argument("--rov-request-batch-size", type=int)
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--clickhouse-host", type=str)
    parser.add_argument("--clickhouse-port", type=int)
    parser.add_argument("--clickhouse-database", type=str)
    parser.add_argument("--clickhouse-user", type=str)
    parser.add_argument("--clickhouse-password", type=str)
    parser.add_argument("--clickhouse-use-http-driver", action="store_true")
    parser.add_argument("--disable-clickhouse-async-inserts", action="store_true")
    parser.add_argument("--timeout-seconds", type=int)
    parser.add_argument("--log-level", type=str)
    parser.add_argument("--schedule-interval-hours", type=int)
    parser.add_argument("--schedule-minute", type=int)
