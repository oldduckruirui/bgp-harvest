CREATE TABLE IF NOT EXISTS as_paths (
    path_id UInt64,
    as_path Array(UInt32)
)
ENGINE = ReplacingMergeTree()
ORDER BY path_id;

CREATE TABLE IF NOT EXISTS bgp_routes (
    prefix String,
    path_id UInt64,
    origin UInt32,
    collector String,
    timestamp DateTime64(3, 'UTC'),
    rpki_status LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (prefix, timestamp)
TTL timestamp + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS vrp_objects (
    vrp_id UInt64,
    prefix String,
    prefix_length UInt8,
    max_length UInt8,
    asn UInt32,
    ta LowCardinality(String),
    ip_version UInt8
)
ENGINE = ReplacingMergeTree()
ORDER BY vrp_id;

CREATE TABLE IF NOT EXISTS vrp_snapshots (
    snapshot_id UInt64,
    generated_at Nullable(DateTime64(3, 'UTC')),
    source_endpoint String,
    object_count UInt32,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree()
ORDER BY snapshot_id;

CREATE TABLE IF NOT EXISTS vrp_snapshot_entries (
    snapshot_id UInt64,
    vrp_id UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (snapshot_id, vrp_id);

CREATE TABLE IF NOT EXISTS route_metadata (
    start_time DateTime64(3, 'UTC'),
    end_time DateTime64(3, 'UTC'),
    vrp_snapshot_id Nullable(UInt64),
    vrp_generated_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree()
ORDER BY start_time;

ALTER TABLE route_metadata
ADD COLUMN IF NOT EXISTS vrp_snapshot_id Nullable(UInt64);

ALTER TABLE route_metadata
ADD COLUMN IF NOT EXISTS vrp_generated_at Nullable(DateTime64(3, 'UTC'));
