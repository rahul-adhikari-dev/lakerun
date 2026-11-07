-- SILVER users: one clean, typed row per user = current state of the Postgres table.
-- Pattern (same for every silver table):
--   1. read only NEW bronze events: transform.py fills in {new_events}, a per-Kafka-partition
--      offset range (watermark < offset <= high mark), so each run costs O(new data),
--      not O(all history). The watermark is advanced in the same transaction as the MERGE.
--   2. parse the JSON into typed columns
--   3. keep only the LATEST event per primary key, ordered by source_lsn (the Postgres WAL
--      position). LSN is unique per change and increases for a given row, so it stays correct
--      even if a key moves to another Kafka partition after the topic is repartitioned.
--   4. MERGE (upsert) into silver: a delete removes the row, a newer change updates it,
--      a new key is inserted. Re-running is safe (idempotent).

CREATE TABLE IF NOT EXISTS lake.silver.users (
    user_id        BIGINT,
    email          VARCHAR,
    name           VARCHAR,
    created_at     TIMESTAMPTZ,
    _source_lsn    BIGINT,
    _source_ts     TIMESTAMPTZ,
    _merged_at     TIMESTAMPTZ
);

-- ==== MERGE ====
MERGE INTO lake.silver.users AS t
USING (
    SELECT
        CAST(coalesce(after::JSON ->> 'id', before::JSON ->> 'id') AS BIGINT) AS user_id,
        op,
        after::JSON ->> 'email'                     AS email,
        after::JSON ->> 'name'                      AS name,
        CAST(after::JSON ->> 'created_at' AS TIMESTAMPTZ) AS created_at,
        source_lsn,
        source_ts
    FROM lake.bronze.users
    WHERE {new_events}
    QUALIFY row_number() OVER (PARTITION BY user_id ORDER BY source_lsn DESC, kafka_offset DESC) = 1
) AS s
ON t.user_id = s.user_id
WHEN MATCHED AND s.op = 'd' AND s.source_lsn >= t._source_lsn THEN DELETE
WHEN MATCHED AND s.op <> 'd' AND s.source_lsn > t._source_lsn THEN UPDATE SET
    email = s.email, name = s.name, created_at = s.created_at,
    _source_lsn = s.source_lsn, _source_ts = s.source_ts, _merged_at = now()
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
    (user_id, email, name, created_at, _source_lsn, _source_ts, _merged_at)
    VALUES (s.user_id, s.email, s.name, s.created_at, s.source_lsn, s.source_ts, now());
