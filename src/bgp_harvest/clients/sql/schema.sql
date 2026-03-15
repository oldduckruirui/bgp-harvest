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

CREATE TABLE IF NOT EXISTS route_metadata (
    start_time DateTime64(3, 'UTC'),
    end_time DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree()
ORDER BY start_time;
