-- SILVER orders: current state of each order (see users.sql for the incremental pattern).
-- `total` goes through dbz_decimal() because older events carry base64-encoded decimals.

CREATE TABLE IF NOT EXISTS lake.silver.orders (
    order_id       BIGINT,
    user_id        BIGINT,
    status         VARCHAR,
    total          DECIMAL(12, 2),
    updated_at     TIMESTAMPTZ,
    _source_lsn    BIGINT,
    _source_ts     TIMESTAMPTZ,
    _merged_at     TIMESTAMPTZ
);

-- ==== MERGE ====
MERGE INTO lake.silver.orders AS t
USING (
    SELECT
        CAST(coalesce(after::JSON ->> 'id', before::JSON ->> 'id') AS BIGINT) AS order_id,
        op,
        CAST(after::JSON ->> 'user_id' AS BIGINT)          AS user_id,
        lower(after::JSON ->> 'status')                    AS status,
        dbz_decimal(after::JSON ->> 'total', 2)            AS total,
        CAST(after::JSON ->> 'updated_at' AS TIMESTAMPTZ)  AS updated_at,
        source_lsn,
        source_ts
    FROM lake.bronze.orders
    WHERE {new_events}
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY source_lsn DESC, kafka_offset DESC) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'd' AND s.source_lsn >= t._source_lsn THEN DELETE
WHEN MATCHED AND s.op <> 'd' AND s.source_lsn > t._source_lsn THEN UPDATE SET
    user_id = s.user_id, status = s.status, total = s.total, updated_at = s.updated_at,
    _source_lsn = s.source_lsn, _source_ts = s.source_ts, _merged_at = now()
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
    (order_id, user_id, status, total, updated_at, _source_lsn, _source_ts, _merged_at)
    VALUES (s.order_id, s.user_id, s.status, s.total, s.updated_at,
            s.source_lsn, s.source_ts, now());
