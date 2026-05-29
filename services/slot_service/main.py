from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI, Request

from shared.event_bus import InMemoryEventBus, RabbitMQEventBus, build_event_bus
from shared.events import EventEnvelope, EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("slot-service")
logger = configure_logging(settings.service_name)
reservations: dict[str, dict[str, object]] = {}
SLOT_COUNT = 8
DEFAULT_PICKUP_WINDOWS = [
    {"pickup_window": "09:30-09:35", "capacity": SLOT_COUNT, "active": True},
    {"pickup_window": "12:00-12:15", "capacity": SLOT_COUNT, "active": True},
    {"pickup_window": "17:30-17:45", "capacity": SLOT_COUNT, "active": True},
]


def _database_enabled() -> bool:
    return bool(settings.database_url)


def _pick_available_slot(order_id: str, used_slots: set[str]) -> str | None:
    start = int(hashlib.sha1(order_id.encode("utf-8")).hexdigest(), 16) % SLOT_COUNT
    for offset in range(SLOT_COUNT):
        slot_id = f"P-{(start + offset) % SLOT_COUNT + 1:02d}"
        if slot_id not in used_slots:
            return slot_id
    return None


def assign_slot(order_id: str, pickup_window: str, state: dict[str, dict[str, object]]) -> str | None:
    used_slots = {
        str(reservation["slot_id"])
        for reservation in state.values()
        if reservation["pickup_window"] == pickup_window and reservation["status"] != "Available"
    }
    return _pick_available_slot(order_id, used_slots)


async def _get_reservation(order_id: str) -> dict[str, object] | None:
    if not _database_enabled():
        return reservations.get(order_id)
    return await asyncio.to_thread(_get_reservation_sync, order_id)


def _get_reservation_sync(order_id: str) -> dict[str, object] | None:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        row = conn.execute(
            """
            SELECT order_id, slot_id, pickup_window, status, reserved_at, released_at
            FROM slot_reservations
            WHERE order_id = %s
            """,
            (order_id,),
        ).fetchone()
        return dict(row) if row else None


async def _used_slots_for_window(pickup_window: str) -> set[str]:
    if not _database_enabled():
        return {
            str(reservation["slot_id"])
            for reservation in reservations.values()
            if reservation["pickup_window"] == pickup_window and reservation["status"] != "Available"
        }
    return await asyncio.to_thread(_used_slots_for_window_sync, pickup_window)


def _used_slots_for_window_sync(pickup_window: str) -> set[str]:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            """
            SELECT slot_id
            FROM slot_reservations
            WHERE pickup_window = %s
              AND status <> 'Available'
            """,
            (pickup_window,),
        ).fetchall()
        return {str(row[0]) for row in rows}


async def _save_reservation(reservation: dict[str, object]) -> None:
    if not _database_enabled():
        reservations[str(reservation["order_id"])] = reservation
        return
    await asyncio.to_thread(_save_reservation_sync, reservation)


def _save_reservation_sync(reservation: dict[str, object]) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO pickup_windows (pickup_window, capacity)
                VALUES (%s, %s)
                ON CONFLICT (pickup_window) DO NOTHING
                """,
                (reservation["pickup_window"], SLOT_COUNT),
            )
            conn.execute(
                """
                INSERT INTO pickup_slots (slot_id)
                VALUES (%s)
                ON CONFLICT (slot_id) DO NOTHING
                """,
                (reservation["slot_id"],),
            )
            conn.execute(
                """
                INSERT INTO slot_reservations (
                    order_id, slot_id, pickup_window, status, reserved_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    reservation["order_id"],
                    reservation["slot_id"],
                    reservation["pickup_window"],
                    reservation["status"],
                    reservation["reserved_at"],
                ),
            )


async def _reserve_slot_for_order(order_id: str, pickup_window: str) -> dict[str, object] | None:
    return await asyncio.to_thread(_reserve_slot_for_order_sync, order_id, pickup_window)


def _reserve_slot_for_order_sync(order_id: str, pickup_window: str) -> dict[str, object] | None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            # Same-window reservations must be serialized so two paid orders
            # cannot choose the same physical slot before either insert commits.
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (pickup_window,))
            existing = conn.execute(
                """
                SELECT order_id
                FROM slot_reservations
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()
            if existing:
                return None

            used_rows = conn.execute(
                """
                SELECT slot_id
                FROM slot_reservations
                WHERE pickup_window = %s
                  AND status <> 'Available'
                """,
                (pickup_window,),
            ).fetchall()
            slot_id = _pick_available_slot(order_id, {str(row[0]) for row in used_rows})
            if slot_id is None:
                return None

            reservation = {
                "order_id": order_id,
                "slot_id": slot_id,
                "pickup_window": pickup_window,
                "status": "Reserved",
                "reserved_at": datetime.now(UTC).isoformat(),
            }
            conn.execute(
                """
                INSERT INTO pickup_windows (pickup_window, capacity)
                VALUES (%s, %s)
                ON CONFLICT (pickup_window) DO NOTHING
                """,
                (pickup_window, SLOT_COUNT),
            )
            conn.execute(
                """
                INSERT INTO pickup_slots (slot_id)
                VALUES (%s)
                ON CONFLICT (slot_id) DO NOTHING
                """,
                (slot_id,),
            )
            conn.execute(
                """
                INSERT INTO slot_reservations (
                    order_id, slot_id, pickup_window, status, reserved_at
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    reservation["order_id"],
                    reservation["slot_id"],
                    reservation["pickup_window"],
                    reservation["status"],
                    reservation["reserved_at"],
                ),
            )
            return reservation


async def _update_reservation_status(order_id: str, status: str, released_at: str | None = None) -> None:
    if not _database_enabled():
        reservation = reservations.get(order_id)
        if reservation:
            reservation["status"] = status
            if released_at:
                reservation["released_at"] = released_at
        return
    await asyncio.to_thread(_update_reservation_status_sync, order_id, status, released_at)


def _update_reservation_status_sync(order_id: str, status: str, released_at: str | None = None) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        conn.execute(
            """
            UPDATE slot_reservations
            SET status = %s,
                released_at = COALESCE(%s, released_at)
            WHERE order_id = %s
            """,
            (status, released_at, order_id),
        )


async def _list_reservations() -> list[dict[str, object]]:
    if not _database_enabled():
        return list(reservations.values())
    return await asyncio.to_thread(_list_reservations_sync)


def _list_reservations_sync() -> list[dict[str, object]]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT order_id, slot_id, pickup_window, status, reserved_at, released_at
            FROM slot_reservations
            ORDER BY reserved_at DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]


async def _list_pickup_windows() -> list[dict[str, object]]:
    if not _database_enabled():
        return DEFAULT_PICKUP_WINDOWS
    return await asyncio.to_thread(_list_pickup_windows_sync)


def _list_pickup_windows_sync() -> list[dict[str, object]]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT pickup_window, capacity, active
            FROM pickup_windows
            ORDER BY pickup_window
            """
        ).fetchall()
        return [dict(row) for row in rows]


async def _list_slots() -> list[dict[str, object]]:
    if not _database_enabled():
        return [{"slot_id": f"P-{index:02d}", "active": True} for index in range(1, SLOT_COUNT + 1)]
    return await asyncio.to_thread(_list_slots_sync)


def _list_slots_sync() -> list[dict[str, object]]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT slot_id, active
            FROM pickup_slots
            ORDER BY slot_id
            """
        ).fetchall()
        return [dict(row) for row in rows]


async def handle_order_paid(
    event: EventEnvelope,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    state: dict[str, dict[str, object]] = reservations,
) -> None:
    if _database_enabled():
        if await _get_reservation(event.aggregate_id):
            return
        pickup_window = event.payload["pickup_window"]
        reservation = await _reserve_slot_for_order(event.aggregate_id, str(pickup_window))
        slot_id = reservation["slot_id"] if reservation else None
    elif event.aggregate_id in state:
        return
    else:
        pickup_window = event.payload["pickup_window"]
        slot_id = assign_slot(event.aggregate_id, str(pickup_window), state)

    if slot_id is None:
        await event_bus.publish(
            new_event(
                EventType.PICKUP_SLOT_FULL,
                aggregate_id=event.aggregate_id,
                source=settings.service_name,
                payload={"order_id": event.aggregate_id, "pickup_window": pickup_window},
                correlation_id=event.correlation_id,
            )
        )
        log_event(logger, settings.service_name, "pickup slot full", order_id=event.aggregate_id)
        return

    if not _database_enabled():
        reservation = {
            "order_id": event.aggregate_id,
            "slot_id": slot_id,
            "pickup_window": pickup_window,
            "status": "Reserved",
            "reserved_at": datetime.now(UTC).isoformat(),
        }
        state[event.aggregate_id] = reservation
    await event_bus.publish(
        new_event(
            EventType.PICKUP_SLOT_RESERVED,
            aggregate_id=event.aggregate_id,
            source=settings.service_name,
            payload=reservation,
            correlation_id=event.correlation_id,
        )
    )
    log_event(logger, settings.service_name, "slot reserved", **reservation, correlation_id=event.correlation_id)


async def handle_status_event(
    event: EventEnvelope,
    state: dict[str, dict[str, object]] = reservations,
) -> None:
    reservation = await _get_reservation(event.aggregate_id) if _database_enabled() else state.get(event.aggregate_id)
    if not reservation:
        return
    event_type = EventType(event.event_type)
    status_by_event = {
        EventType.ORDER_PREPARING: "Preparing",
        EventType.ORDER_PLACED_IN_SLOT: "PlacedInSlot",
        EventType.ORDER_READY: "Ready",
        EventType.ORDER_PICKED_UP: "Available",
        EventType.ORDER_EXPIRED: "Available",
    }
    status = status_by_event.get(event_type, str(reservation["status"]))
    released_at = None
    if event_type in {EventType.ORDER_PICKED_UP, EventType.ORDER_EXPIRED}:
        released_at = datetime.now(UTC).isoformat()
    if _database_enabled():
        await _update_reservation_status(event.aggregate_id, status, released_at)
    else:
        reservation["status"] = status
        if released_at:
            reservation["released_at"] = released_at


@asynccontextmanager
async def lifespan(app: FastAPI):
    event_bus = build_event_bus(settings)
    await event_bus.connect()
    await event_bus.subscribe(
        EventType.ORDER_PAID,
        lambda event: handle_order_paid(event, event_bus),
        queue_name=f"{settings.service_name}.order-paid",
    )
    for event_type in (
        EventType.ORDER_PREPARING,
        EventType.ORDER_PLACED_IN_SLOT,
        EventType.ORDER_READY,
        EventType.ORDER_PICKED_UP,
        EventType.ORDER_EXPIRED,
    ):
        await event_bus.subscribe(
            event_type,
            handle_status_event,
            queue_name=f"{settings.service_name}.{event_type}",
        )
    app.state.event_bus = event_bus
    log_event(logger, settings.service_name, "event subscriptions ready", bus=settings.event_bus)
    try:
        yield
    finally:
        await event_bus.close()


app = FastAPI(
    title="PeakPick Slot Service",
    version="0.1.0",
    description="Pickup window capacity and slot assignment.",
    lifespan=lifespan,
)


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {
        "status": "ok",
        "service": settings.service_name,
        "event_bus_connected": request.app.state.event_bus.is_connected,
    }


@app.get("/reservations")
async def list_reservations() -> list[dict[str, object]]:
    return await _list_reservations()


@app.get("/pickup-windows")
async def list_pickup_windows() -> list[dict[str, object]]:
    return await _list_pickup_windows()


@app.get("/slots")
async def list_slots() -> list[dict[str, object]]:
    return await _list_slots()
