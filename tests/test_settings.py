from __future__ import annotations
from pathlib import Path

from bgp_harvest.config.settings import build_settings


def test_build_settings_prefers_overrides_over_file(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
clickhouse:
  host: from-file
runtime:
  log_level: warning
""".strip(),
        encoding="utf-8",
    )

    settings = build_settings(
        config_path=config_path,
        overrides={"clickhouse": {"host": "from-cli"}, "runtime": {"log_level": "DEBUG"}},
    )

    assert settings.clickhouse.host == "from-cli"
    assert settings.runtime.log_level == "DEBUG"


def test_build_settings_parses_datetime_fields() -> None:
    settings = build_settings(
        overrides={
            "harvest": {
                "start": "2026-03-14T00:00:00Z",
                "end": "2026-03-14T02:00:00Z",
            }
        }
    )

    assert settings.harvest.start is not None
    assert settings.harvest.end is not None
    assert settings.harvest.start.isoformat() == "2026-03-14T00:00:00+00:00"
    assert settings.harvest.end.isoformat() == "2026-03-14T02:00:00+00:00"


def test_build_settings_defaults_to_snapshot_rov_mode() -> None:
    settings = build_settings()

    assert settings.rov.mode == "snapshot"
    assert settings.rov.snapshot_endpoint == "http://127.0.0.1:8323/json"


def test_build_settings_reads_snapshot_rov_env(monkeypatch) -> None:
    monkeypatch.setenv("BGP_HARVEST_ROV_MODE", "http")
    monkeypatch.setenv("BGP_HARVEST_ROV_SNAPSHOT_ENDPOINT", "http://127.0.0.1:9323/json")

    settings = build_settings()

    assert settings.rov.mode == "http"
    assert settings.rov.snapshot_endpoint == "http://127.0.0.1:9323/json"
