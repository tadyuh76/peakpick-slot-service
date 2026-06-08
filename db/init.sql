CREATE TABLE IF NOT EXISTS stores (
    store_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS identity_users (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'store_manager', 'customer')),
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    display_name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS event_log (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    correlation_id UUID NOT NULL,
    source TEXT NOT NULL,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS event_log
    ADD COLUMN IF NOT EXISTS store_id TEXT NOT NULL DEFAULT 'store-ueh';

CREATE INDEX IF NOT EXISTS idx_event_log_correlation_id
    ON event_log (correlation_id);

CREATE INDEX IF NOT EXISTS idx_event_log_event_type
    ON event_log (event_type);

CREATE INDEX IF NOT EXISTS idx_event_log_store_event
    ON event_log (store_id, event_type);

CREATE TABLE IF NOT EXISTS carts (
    cart_id TEXT PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    customer_name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS carts
    ADD COLUMN IF NOT EXISTS store_id TEXT NOT NULL DEFAULT 'store-ueh';

CREATE TABLE IF NOT EXISTS cart_items (
    cart_id TEXT NOT NULL REFERENCES carts(cart_id) ON DELETE CASCADE,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (cart_id, sku)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    customer_name TEXT NOT NULL,
    pickup_window TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    order_status TEXT NOT NULL,
    paid_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS orders
    ADD COLUMN IF NOT EXISTS store_id TEXT NOT NULL DEFAULT 'store-ueh';

CREATE INDEX IF NOT EXISTS idx_orders_store_created
    ON orders (store_id, created_at DESC);

CREATE TABLE IF NOT EXISTS order_items (
    order_id TEXT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE IF NOT EXISTS pickup_windows (
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    pickup_window TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    active BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (store_id, pickup_window)
);

ALTER TABLE IF EXISTS pickup_windows
    ADD COLUMN IF NOT EXISTS store_id TEXT NOT NULL DEFAULT 'store-ueh';

CREATE UNIQUE INDEX IF NOT EXISTS idx_pickup_windows_store_window
    ON pickup_windows (store_id, pickup_window);

CREATE TABLE IF NOT EXISTS pickup_slots (
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    slot_id TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (store_id, slot_id)
);

ALTER TABLE IF EXISTS pickup_slots
    ADD COLUMN IF NOT EXISTS store_id TEXT NOT NULL DEFAULT 'store-ueh';

CREATE UNIQUE INDEX IF NOT EXISTS idx_pickup_slots_store_slot
    ON pickup_slots (store_id, slot_id);

CREATE TABLE IF NOT EXISTS slot_reservations (
    order_id TEXT PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    slot_id TEXT NOT NULL,
    pickup_window TEXT NOT NULL,
    status TEXT NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ,
    FOREIGN KEY (store_id, slot_id) REFERENCES pickup_slots(store_id, slot_id),
    FOREIGN KEY (store_id, pickup_window) REFERENCES pickup_windows(store_id, pickup_window)
);

ALTER TABLE IF EXISTS slot_reservations
    ADD COLUMN IF NOT EXISTS store_id TEXT NOT NULL DEFAULT 'store-ueh';

CREATE INDEX IF NOT EXISTS idx_slot_reservations_store_window_status
    ON slot_reservations (store_id, pickup_window, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_slot_reservations_active_slot_window
    ON slot_reservations (store_id, pickup_window, slot_id)
    WHERE status <> 'Available';

CREATE TABLE IF NOT EXISTS slot_reservation_blocks (
    order_id TEXT PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS slot_reservation_blocks
    ADD COLUMN IF NOT EXISTS store_id TEXT NOT NULL DEFAULT 'store-ueh';

INSERT INTO stores (store_id, name)
VALUES
    ('store-ueh', 'UEH Campus Store'),
    ('store-d1', 'District 1 Store')
ON CONFLICT (store_id) DO UPDATE
    SET name = EXCLUDED.name,
        active = true;

INSERT INTO identity_users (username, password_hash, role, store_id, display_name)
VALUES
    ('admin@peakpick.local', '11cf072926003cc1ff6dc60064170469c01f39fba8a2319d58e99a842d08b292', 'admin', 'store-ueh', 'PeakPick Admin'),
    ('manager.ueh@peakpick.local', '7bd7e62467b6d51191887630a664ea3ba3f46364068ddfc1b80392e73878054a', 'store_manager', 'store-ueh', 'UEH Store Manager'),
    ('manager.d1@peakpick.local', '7bd7e62467b6d51191887630a664ea3ba3f46364068ddfc1b80392e73878054a', 'store_manager', 'store-d1', 'District 1 Store Manager')
ON CONFLICT (username) DO UPDATE
    SET password_hash = EXCLUDED.password_hash,
        role = EXCLUDED.role,
        store_id = EXCLUDED.store_id,
        display_name = EXCLUDED.display_name,
        active = true;

INSERT INTO pickup_windows (store_id, pickup_window, capacity)
SELECT store_id, pickup_window, 32
FROM stores
CROSS JOIN (
    VALUES
        ('09:30-09:35'),
        ('12:00-12:15'),
        ('17:30-17:45')
) AS windows(pickup_window)
ON CONFLICT (store_id, pickup_window) DO NOTHING;

INSERT INTO pickup_slots (store_id, slot_id)
SELECT stores.store_id, 'P-' || lpad(value::text, 2, '0')
FROM stores
CROSS JOIN generate_series(1, 32) AS value
ON CONFLICT (store_id, slot_id) DO NOTHING;
