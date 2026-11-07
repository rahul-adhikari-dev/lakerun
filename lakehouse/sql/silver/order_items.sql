-- SILVER order_items: current state of each order line (see users.sql for the pattern).
-- order_items uses the default REPLICA IDENTITY, so delete events only carry the primary
-- key in `before`. That's all a delete needs.

CREATE TABLE IF NOT EXISTS lake.silver.order_items (
    order_item_id  BIGINT,
    order_id       BIGINT,
    sku            VARCHAR,
    quantity       INTEGER,
    price          DECIMAL(12, 2),
    line_total     DECIMAL(14, 2),
    _source_lsn    BIGINT,
    _source_ts     TIMESTAMPTZ,
    _merged_at     TIMESTAMPTZ
);

-- ==== MERGE ====
MERGE INTO lake.silver.order_items AS t
USING (
    SELECT
        CAST(coalesce(after::JSON ->> 'id', before::JSON ->> 'id') AS BIGINT) AS order_item_id,
        op,
        CAST(after::JSON ->> 'order_id' AS BIGINT)  AS order_id,
        upper(after::JSON ->> 'sku')                AS sku,
        CAST(after::JSON ->> 'quantity' AS INTEGER) AS quantity,
        dbz_decimal(after::JSON ->> 'price', 2)     AS price,
        source_lsn,
        source_ts
    FROM lake.bronze.order_items
    WHERE {new_events}
    QUALIFY row_number() OVER (PARTITION BY order_item_id ORDER BY source_lsn DESC, kafka_offset DESC) = 1
) AS s
ON t.order_item_id = s.order_item_id
WHEN MATCHED AND s.op = 'd' AND s.source_lsn >= t._source_lsn THEN DELETE
WHEN MATCHED AND s.op <> 'd' AND s.source_lsn > t._source_lsn THEN UPDATE SET
    order_id = s.order_id, sku = s.sku, quantity = s.quantity, price = s.price,
    line_total = s.quantity * s.price,
    _source_lsn = s.source_lsn, _source_ts = s.source_ts, _merged_at = now()
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
    (order_item_id, order_id, sku, quantity, price, line_total,
     _source_lsn, _source_ts, _merged_at)
    VALUES (s.order_item_id, s.order_id, s.sku, s.quantity, s.price, s.quantity * s.price,
            s.source_lsn, s.source_ts, now());
