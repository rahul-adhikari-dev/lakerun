-- GOLD product_sales: one row per SKU. Which products sell, and to how many people?
-- Joins line items to orders so cancelled and pending orders are left out.

DROP TABLE IF EXISTS lake.gold.product_sales;

CREATE TABLE lake.gold.product_sales AS
SELECT
    i.sku,
    sum(i.quantity)              AS units_sold,
    sum(i.line_total)            AS revenue,
    count(DISTINCT i.order_id)   AS orders,
    count(DISTINCT o.user_id)    AS customers,
    round(avg(i.price), 2)       AS avg_price,
    now()                        AS _built_at
FROM lake.silver.order_items i
JOIN lake.silver.orders o ON o.order_id = i.order_id
WHERE o.status IN ('paid', 'shipped', 'delivered')
GROUP BY i.sku
ORDER BY revenue DESC;
