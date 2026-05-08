CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}
AND durable_writes = true;

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
    product_id TEXT,
    zone_id TEXT,
    available_quantity BIGINT,
    reserved_quantity BIGINT,
    last_event_timestamp TIMESTAMP,
    PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
    zone_id TEXT,
    product_id TEXT,
    available_quantity BIGINT,
    reserved_quantity BIGINT,
    last_event_timestamp TIMESTAMP,
    PRIMARY KEY ((zone_id), product_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_totals (
    product_id TEXT PRIMARY KEY,
    total_available BIGINT,
    total_reserved BIGINT,
    last_event_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TIMESTAMP,
    event_type TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.event_history (
    product_id TEXT,
    event_timestamp TIMESTAMP,
    event_id TEXT,
    event_type TEXT,
    payload TEXT,
    PRIMARY KEY ((product_id), event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC);

CREATE TABLE IF NOT EXISTS warehouse.orders (
    order_id TEXT PRIMARY KEY,
    status TEXT,
    created_at TIMESTAMP,
    completed_at TIMESTAMP,
    items TEXT,
    last_event_timestamp TIMESTAMP
);
