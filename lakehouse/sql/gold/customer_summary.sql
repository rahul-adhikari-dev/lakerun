-- GOLD customer_summary: one row per customer (a small "customer 360").
-- Answers: who are our best customers, and who hasn't ordered recently?

DROP TABLE IF EXISTS lake.gold.customer_summary;

CREATE TABLE lake.gold.customer_summary AS
WITH paid AS (
    SELECT * FROM lake.silver.orders WHERE status IN ('paid', 'shipped', 'delivered')
)
SELECT
    u.user_id,
    u.name,
    u.email,
    u.created_at                                  AS signed_up_at,
    count(o.order_id)                             AS total_orders,
    count(p.order_id)                             AS paid_orders,
    coalesce(sum(p.total), 0)                     AS lifetime_value,
    round(avg(p.total), 2)                        AS avg_order_value,
    min(o.updated_at)                             AS first_order_at,
    max(o.updated_at)                             AS last_order_at,
    CASE
        WHEN coalesce(sum(p.total), 0) >= 3000 THEN 'vip'
        WHEN count(p.order_id) >= 1           THEN 'active'
        ELSE 'prospect'
    END                                           AS segment,
    now()                                         AS _built_at
FROM lake.silver.users u
LEFT JOIN lake.silver.orders o ON o.user_id = u.user_id
LEFT JOIN paid p              ON p.order_id = o.order_id
GROUP BY u.user_id, u.name, u.email, u.created_at
ORDER BY lifetime_value DESC;
