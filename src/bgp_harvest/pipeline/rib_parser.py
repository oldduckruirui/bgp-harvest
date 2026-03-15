"""Parse downloaded RIB files into normalized route records."""

from __future__ import annotations

import datetime as dt
from typing import Iterable

from ..models.types import MaterializedRibResource, RouteRecord


class PyBGPStreamRibParser:
    """RIB parser backed by ``pybgpstream``."""

    def __init__(self, ip_version: str = "6") -> None:
        self.ip_version = ip_version

    def parse(self, resource: MaterializedRibResource) -> Iterable[RouteRecord]:
        """Yield route records extracted from a downloaded RIB file."""

        import pybgpstream

        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "rib-file", str(resource.local_path))
        stream._maybe_add_filter("ipversion", self.ip_version, None)

        for elem in stream:
            prefix = elem.fields.get("prefix")
            if not prefix or prefix == "::/0":
                continue

            raw_path = elem.fields.get("as-path")
            if not raw_path:
                continue

            segments = raw_path.split()
            if not segments or not all(segment.isdigit() for segment in segments):
                continue

            yield RouteRecord.from_as_path(
                prefix=prefix,
                as_path=[int(segment) for segment in segments],
                collector=resource.collector_id,
                timestamp=dt.datetime.fromtimestamp(elem.time, tz=dt.timezone.utc),
            )
