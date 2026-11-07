-- Runs once, the first time the Postgres container starts.

CREATE TABLE users (
    id         BIGSERIAL PRIMARY KEY,
    email      TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES users(id),
    status     TEXT NOT NULL DEFAULT 'pending',
    total      NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT NOT NULL REFERENCES orders(id),
    sku        TEXT NOT NULL,
    quantity   INT NOT NULL,
    price      NUMERIC(12,2) NOT NULL
);

-- Optional: include the full old row in update/delete events for orders
ALTER TABLE orders REPLICA IDENTITY FULL;

-- A dedicated user for Debezium (needs REPLICATION and read access)
CREATE ROLE cdc WITH LOGIN REPLICATION PASSWORD 'cdc_pass';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc;
GRANT USAGE ON SCHEMA public TO cdc;

-- Which tables are published to Debezium
CREATE PUBLICATION lakesync_pub FOR TABLE users, orders, order_items;

-- A little starting data
INSERT INTO users (email, name) VALUES
    ('asha@example.com', 'Asha'),
    ('ravi@example.com', 'Ravi');

INSERT INTO orders (user_id, status, total) VALUES
    (1, 'pending', 499.00),
    (2, 'paid', 1299.50);

INSERT INTO order_items (order_id, sku, quantity, price) VALUES
    (1, 'SKU-100', 1, 499.00),
    (2, 'SKU-200', 2, 649.75);