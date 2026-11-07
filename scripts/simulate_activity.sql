-- Fake shop activity, so the CDC pipeline has inserts, updates AND deletes to carry.
-- Run it with:  python main.py simulate      (each run adds a fresh round of activity)

-- 1) A few new customers (INSERT -> op 'c')
INSERT INTO users (email, name)
SELECT 'customer' || g || '.' || substr(md5(random()::text), 1, 6) || '@example.com',
       (ARRAY['Asha','Ravi','Meera','Kabir','Zoya','Arjun','Isha','Dev'])[1 + floor(random() * 8)::int]
FROM generate_series(1, 3) g;

-- 2) New orders, each placed by a random customer (total filled in at step 4)
INSERT INTO orders (user_id, status, total)
SELECT u.id, 'pending', 0
FROM generate_series(1, 10) g
CROSS JOIN LATERAL (SELECT id FROM users WHERE g > 0 ORDER BY random() LIMIT 1) u;

-- 3) 1-3 line items per new order, from a small product catalog
INSERT INTO order_items (order_id, sku, quantity, price)
SELECT o.id, p.sku, 1 + floor(random() * 3)::int, p.price
FROM orders o
CROSS JOIN LATERAL generate_series(1, 1 + floor(random() * 3)::int) n
CROSS JOIN LATERAL (
    SELECT * FROM (VALUES ('SKU-100', 499.00), ('SKU-200', 649.75), ('SKU-300', 129.99),
                          ('SKU-400', 1999.00), ('SKU-500', 89.50)) AS c(sku, price)
    WHERE n > 0 AND o.id > 0 ORDER BY random() LIMIT 1
) p
WHERE o.total = 0;

-- 4) Order total = sum of its lines (UPDATE -> op 'u')
UPDATE orders o
SET total = s.total, updated_at = now()
FROM (SELECT order_id, sum(quantity * price) AS total FROM order_items GROUP BY order_id) s
WHERE o.id = s.order_id AND o.total <> s.total;

-- 5) Move some orders forward in their lifecycle, and cancel a few pending ones
UPDATE orders
SET status = CASE status WHEN 'pending' THEN 'paid' WHEN 'paid' THEN 'shipped' ELSE 'delivered' END,
    updated_at = now()
WHERE status IN ('pending', 'paid', 'shipped') AND random() < 0.5;

UPDATE orders SET status = 'cancelled', updated_at = now()
WHERE status = 'pending' AND random() < 0.2;

-- 6) Cancelled orders lose their line items (DELETE -> op 'd')
DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE status = 'cancelled');

SELECT status, count(*), sum(total) FROM orders GROUP BY status ORDER BY status;
