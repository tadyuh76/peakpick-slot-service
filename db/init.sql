CREATE TABLE IF NOT EXISTS stores (
    store_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_event_log_correlation_id
    ON event_log (correlation_id);

CREATE INDEX IF NOT EXISTS idx_event_log_event_type
    ON event_log (event_type);

CREATE INDEX IF NOT EXISTS idx_event_log_store_event
    ON event_log (store_id, event_type);

CREATE TABLE IF NOT EXISTS pickup_windows (
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    pickup_window TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    active BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (store_id, pickup_window)
);

CREATE TABLE IF NOT EXISTS pickup_slots (
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    slot_id TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (store_id, slot_id)
);

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

INSERT INTO stores (store_id, name)
VALUES
    ('store-ueh', 'UEH Campus Store'),
    ('store-d1', 'District 1 Store')
ON CONFLICT (store_id) DO UPDATE
    SET name = EXCLUDED.name,
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
