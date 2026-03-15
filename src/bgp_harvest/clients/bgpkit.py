"""BGPKit-based discovery and download client for RIB files."""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Sequence

import requests
import urllib3
from requests.adapters import HTTPAdapter

from ..models.types import MaterializedRibResource, RibResource

LOG = logging.getLogger(__name__)

BGPKIT_API = "https://api.bgpkit.com/v3/broker"


class BGPKitRibClient:
    """Discover and download RouteViews RIB files via the BGPKit Broker API."""

    def __init__(
        self,
        timeout_seconds: int = 60,
        page_size: int = 500,
        project: str = "routeviews",
        session: requests.Session | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.page_size = max(1, min(page_size, 1000))
        self.project = project
        self.session = session or self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        """Build a requests session with retryable HTTP behaviour."""

        session = requests.Session()
        session.trust_env = False
        retries = urllib3.Retry(total=3, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504))
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def discover(
        self,
        start: dt.datetime,
        end: dt.datetime,
        collector: str | None = None,
    ) -> Sequence[RibResource]:
        """List matching RIB resources for the given UTC time window."""

        resources: list[RibResource] = []
        LOG.info(
            "Querying BGPKit project=%s start=%s end=%s collector=%s page_size=%s",
            self.project,
            start,
            end,
            collector,
            self.page_size,
        )
        params = {
            "project": self.project,
            "data_type": "rib",
            "ts_start": start.astimezone(dt.timezone.utc).isoformat(),
            "ts_end": end.astimezone(dt.timezone.utc).isoformat(),
            "page_size": self.page_size,
        }
        if collector:
            params["collector_id"] = collector

        page = 1
        while True:
            LOG.info("Requesting BGPKit page=%s", page)
            response = self.session.get(
                f"{BGPKIT_API}/search",
                params={**params, "page": page},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json().get("data", [])
            LOG.info("Received %s resources from BGPKit page=%s", len(data), page)

            for item in data:
                resources.append(
                    RibResource(
                        url=item["url"],
                        collector_id=item["collector_id"],
                        ts_start=_parse_ts(item["ts_start"]),
                        ts_end=_parse_ts(item.get("ts_end", item["ts_start"])),
                        size_bytes=item.get("exact_size") or item.get("rough_size"),
                    )
                )

            if len(data) < self.page_size:
                break
            page += 1

        LOG.info("Discovered %s resources between %s and %s", len(resources), start, end)
        return resources

    def fetch(self, resource: RibResource, directory: str) -> MaterializedRibResource:
        """Download a single RIB resource into ``directory`` if needed."""

        target_dir = Path(directory)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{resource.collector_id}_{resource.ts_start.strftime('%Y%m%d%H%M')}.mrt.bz2"
        partial_path = Path(f"{target_path}.part")

        if not target_path.exists():
            LOG.info(
                "Downloading RIB collector=%s ts_start=%s url=%s",
                resource.collector_id,
                resource.ts_start,
                resource.url,
            )
            response = None
            try:
                if partial_path.exists():
                    partial_path.unlink()
                response = self.session.get(resource.url, stream=True, timeout=self.timeout_seconds)
                response.raise_for_status()
                with partial_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
                partial_path.replace(target_path)
            except BaseException:
                if partial_path.exists():
                    partial_path.unlink()
                raise
            finally:
                if response is not None:
                    response.close()
            LOG.info(
                "Finished download collector=%s path=%s size_bytes=%s",
                resource.collector_id,
                target_path,
                resource.size_bytes,
            )
        else:
            LOG.info("Reusing cached RIB collector=%s path=%s", resource.collector_id, target_path)

        return resource.with_local_path(target_path)


def _parse_ts(value: str) -> dt.datetime:
    """Parse a BGPKit timestamp and normalize it to UTC."""

    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)
