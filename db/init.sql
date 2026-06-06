CREATE TABLE IF NOT EXISTS event_log (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    correlation_id UUID NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_log_correlation_id
    ON event_log (correlation_id);

CREATE INDEX IF NOT EXISTS idx_event_log_event_type
    ON event_log (event_type);

CREATE TABLE IF NOT EXISTS carts (
    cart_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cart_items (
    cart_id TEXT NOT NULL REFERENCES carts(cart_id) ON DELETE CASCADE,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (cart_id, sku)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    pickup_window TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    order_status TEXT NOT NULL,
    paid_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id TEXT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE IF NOT EXISTS pickup_windows (
    pickup_window TEXT PRIMARY KEY,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS pickup_slots (
    slot_id TEXT PRIMARY KEY,
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS slot_reservations (
    order_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL REFERENCES pickup_slots(slot_id),
    pickup_window TEXT NOT NULL REFERENCES pickup_windows(pickup_window),
    status TEXT NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_slot_reservations_pickup_window_status
    ON slot_reservations (pickup_window, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_slot_reservations_active_slot_window
    ON slot_reservations (pickup_window, slot_id)
    WHERE status <> 'Available';

CREATE TABLE IF NOT EXISTS slot_reservation_blocks (
    order_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO pickup_windows (pickup_window, capacity)
VALUES
    ('09:30-09:35', 32),
    ('12:00-12:15', 32),
    ('17:30-17:45', 32)
ON CONFLICT (pickup_window) DO NOTHING;

INSERT INTO pickup_slots (slot_id)
SELECT 'P-' || lpad(value::text, 2, '0')
FROM generate_series(1, 32) AS value
ON CONFLICT (slot_id) DO NOTHING;
