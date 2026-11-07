-- BRONZE: raw, append-only copy of every Debezium change event. Nothing is cleaned or
-- deduplicated here; bronze is the replayable source of truth for silver.
-- One table per source table; {table} is filled in by Python.
CREATE TABLE IF NOT EXISTS lake.bronze.{table} (
    kafka_topic      VARCHAR,
    kafka_partition  INTEGER,
    kafka_offset     BIGINT,       -- with partition, uniquely identifies the event
    kafka_timestamp  TIMESTAMPTZ,
    record_key       VARCHAR,      -- Kafka key JSON, e.g. {"id":1}
    op               VARCHAR,      -- c=create, u=update, d=delete, r=snapshot read
    before           VARCHAR,      -- row JSON before the change (null for c/r)
    after            VARCHAR,      -- row JSON after the change (null for d)
    source_lsn       BIGINT,       -- Postgres WAL position
    source_ts        TIMESTAMPTZ,  -- when the change happened in Postgres
    ingested_at      TIMESTAMPTZ   -- when the ingestor wrote it to the lake
);
