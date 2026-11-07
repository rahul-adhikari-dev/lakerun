-- GOLD daily_revenue: one row per day, ready for a dashboard line chart.
-- Gold tables are small aggregates, so they're rebuilt from silver on every run
-- (DuckDB-Iceberg has no CREATE OR REPLACE, hence DROP + CREATE).
-- "Revenue" counts orders that were paid (paid/shipped/delivered), not pending/cancelled.
-- NOTE: the source `orders` table has no created_at, so updated_at stands in as the order date.

DROP TABLE IF EXISTS lake.gold.daily_revenue;

CREATE TABLE lake.gold.daily_revenue AS
SELECT
    CAST(updated_at AS DATE)                                         AS order_date,
    count(*)                                                         AS orders,
    count(*) FILTER (WHERE status IN ('paid', 'shipped', 'delivered')) AS paid_orders,
    count(*) FILTER (WHERE status = 'cancelled')                     AS cancelled_orders,
    coalesce(sum(total) FILTER (WHERE status IN ('paid', 'shipped', 'delivered')), 0) AS revenue,
    round(avg(total) FILTER (WHERE status IN ('paid', 'shipped', 'delivered')), 2)    AS avg_order_value,
    count(DISTINCT user_id)                                          AS active_customers,
    now()                                                            AS _built_at
FROM lake.silver.orders
GROUP BY ALL
ORDER BY order_date;
