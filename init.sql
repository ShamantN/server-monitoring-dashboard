create extension if not exists timescaledb;

create table if not exists timescaleGrafanaServerMetrics(
    server_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    cpu_pct DOUBLE PRECISION,
    mem_pct DOUBLE PRECISION,
    net_in DOUBLE PRECISION,
    net_out DOUBLE PRECISION,
    disk_io DOUBLE PRECISION
);

select create_hypertable('timescaleGrafanaServerMetrics', 'ts', chunk_time_interval => INTERVAL '10 minutes', if_not_exists => TRUE);

alter table timescaleGrafanaServerMetrics set (timescaledb.compress, timescaledb.compress_segmentby='server_id', timescaledb.compress_orderby='ts DESC');

select add_compression_policy('timescaleGrafanaServerMetrics', INTERVAL '1 hour');

select add_retention_policy('timescaleGrafanaServerMetrics', INTERVAL '2 days');