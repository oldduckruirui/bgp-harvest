# bgp-harvest

`bgp-harvest` is a BGP RIB ingestion pipeline. It discovers RIB snapshots, downloads and parses them, validates `(prefix, origin)` pairs through an ROV endpoint, and stores the resulting route data in ClickHouse.

## Features

- Discover RIB resources from the BGPKit Broker API for an explicit UTC time window
- Download and parse RIB files with `pybgpstream`
- Validate unique `(prefix, origin)` pairs through Routinator-compatible HTTP endpoints
- Persist routes and AS paths into ClickHouse
- Run once or on a recurring schedule

## Project Layout

- `bgp_harvest.cli`: CLI entry points
- `bgp_harvest.config`: configuration loading and precedence handling
- `bgp_harvest.models`: shared data types and protocol contracts
- `bgp_harvest.pipeline`: harvest workflow, parsing, validation, scheduling, and runtime helpers
- `bgp_harvest.clients`: external clients such as BGPKit and ClickHouse

## Installation

`bgp-harvest` supports two run modes:

1. Run directly from the repository without installing the package
2. Install the package and use the `bgp-harvest` CLI

If you are working inside this repository, installation is optional.

Install runtime dependencies only:

```bash
pip install -e .
```

Install runtime and development dependencies:

```bash
pip install -e .[dev]
```

If you want an isolated environment, create and activate a virtual environment first.

## Requirements

At minimum, make sure the following services or dependencies are available:

- Python 3.11 or newer
- `pybgpstream` and its native dependencies
- a reachable ClickHouse instance
- a reachable ROV HTTP endpoint, such as Routinator

Adjust [config.example.yaml](./config.example.yaml) for your environment, or override defaults with environment variables. At minimum, validate:

- ClickHouse connection settings
- ROV API endpoint
- temporary download directory permissions
- scheduling timeout and default window settings

## Quick Start

### Recommended: run directly from the repository

This is the simplest option if you are operating the project from the checked-out source tree. It does not require package installation as long as the dependencies are already installed.

Start the scheduled harvest loop:

```bash
./run.sh
```

By default, this uses [config.example.yaml](./config.example.yaml). To use a different configuration file:

```bash
BGP_HARVEST_CONFIG=./config.example.yaml ./run.sh
```

To run a single harvest window without installing the package:

```bash
PYTHONPATH=src python -m bgp_harvest --config config.example.yaml harvest run --start 2026-03-14T00:00:00Z --end 2026-03-14T02:00:00Z
```

### Optional: install and use the CLI

If you want the `bgp-harvest` command in your environment, install the package and then use the CLI directly.

## Usage

After installation, you can use the CLI directly:

```bash
bgp-harvest --help
```

Run a single harvest window:

```bash
bgp-harvest harvest run --start 2026-03-14T00:00:00Z --end 2026-03-14T02:00:00Z
```

Run on a schedule:

```bash
bgp-harvest harvest schedule
```

Use a config file explicitly:

```bash
bgp-harvest harvest run --config config.example.yaml
```

Configuration precedence is fixed as `CLI > ENV > YAML > defaults`.

## Configuration

The configuration is split into four sections:

- `harvest`: collector, temporary directory, batch size, IP version, discovery paging, and concurrency settings
  - `batch_size`: internal batch size used when writing to ClickHouse
- `rov`: validation enablement, endpoint, timeout, and concurrency settings
  - `max_workers`: number of concurrent ROV HTTP request batches
  - `request_batch_size`: number of `(prefix, origin)` pairs included in each POST `/validity` request
- `clickhouse`: storage connection settings, driver selection, and async insert toggle
- `runtime`: log level, per-run timeout, schedule interval, and schedule minute

All environment variables use the `BGP_HARVEST_` prefix, for example:

```bash
export BGP_HARVEST_CLICKHOUSE_HOST=127.0.0.1
export BGP_HARVEST_ROV_ENDPOINT=http://127.0.0.1:8323/validity
```

## Docker

The ClickHouse Docker Compose example is available at [docker/clickhouse/docker-compose.yaml](./docker/clickhouse/docker-compose.yaml).

The Routinator Docker Compose example is available at [docker/routinator/docker-compose.yaml](./docker/routinator/docker-compose.yaml).

## Development

Run the test suite:

```bash
pytest
```

Run linting:

```bash
ruff check .
```

Run type checks:

```bash
PYTHONPATH=src mypy src
```
