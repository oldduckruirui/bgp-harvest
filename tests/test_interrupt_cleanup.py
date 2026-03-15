from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from bgp_harvest.clients import BGPKitRibClient
from bgp_harvest.models import RibResource
from bgp_harvest.pipeline import cleanup_temporary_directory


class FailingResponse:
    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        yield b"partial"
        raise RuntimeError("interrupted download")

    def close(self) -> None:
        return None


class FailingSession:
    def get(self, url: str, stream: bool, timeout: int) -> FailingResponse:
        return FailingResponse()


def test_bgpkit_fetch_removes_partial_file_on_failure(tmp_path: Path) -> None:
    client = BGPKitRibClient(session=FailingSession())  # type: ignore[arg-type]
    ts = dt.datetime(2026, 3, 14, tzinfo=dt.timezone.utc)
    resource = RibResource(
        url="https://example.test/rib.bz2",
        collector_id="rv1",
        ts_start=ts,
        ts_end=ts,
    )

    with pytest.raises(RuntimeError, match="interrupted download"):
        client.fetch(resource, str(tmp_path))

    assert list(tmp_path.iterdir()) == []


def test_cleanup_temporary_directory_removes_downloaded_files(tmp_path: Path) -> None:
    download_dir = tmp_path / ".bgp_tmp"
    download_dir.mkdir()
    finished = download_dir / "rv1_202603140000.mrt.bz2"
    partial = download_dir / "rv2_202603140000.mrt.bz2.part"
    nested_dir = download_dir / "nested"
    nested_dir.mkdir()
    nested_file = nested_dir / "extra.tmp"

    finished.write_bytes(b"done")
    partial.write_bytes(b"part")
    nested_file.write_bytes(b"nested")

    cleanup_temporary_directory(download_dir)

    assert not finished.exists()
    assert not partial.exists()
    assert not nested_file.exists()
