-- GOLD order_status_funnel: how many orders sit in each lifecycle stage right now.

DROP TABLE IF EXISTS lake.gold.order_status_funnel;

CREATE TABLE lake.gold.order_status_funnel AS
SELECT
    status,
    count(*)                                              AS orders,
    sum(total)                                            AS order_value,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)    AS pct_of_orders,
    now()                                                 AS _built_at
FROM lake.silver.orders
GROUP BY status
ORDER BY CASE status
    WHEN 'pending' THEN 1 WHEN 'paid' THEN 2 WHEN 'shipped' THEN 3
    WHEN 'delivered' THEN 4 WHEN 'cancelled' THEN 5 ELSE 6 END;
