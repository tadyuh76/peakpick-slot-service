from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from shared.event_bus import InMemoryEventBus, RabbitMQEventBus, build_event_bus
from shared.events import EventEnvelope, EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("slot-service")
logger = configure_logging(settings.service_name)
reservations: dict[str, dict[str, object]] = {}
blocked_orders: set[str] = set()
pickup_window_capacity_overrides: dict[str, int] = {}
SLOT_COUNT = 32
SLOT_STATUS_RANK = {
    "Reserved": 0,
    "Preparing": 1,
    "PlacedInSlot": 2,
    "Ready": 3,
    "Available": 4,
}
DEFAULT_PICKUP_WINDOWS = [
    {"pickup_window": "09:30-09:35", "capacity": SLOT_COUNT, "active": True},
    {"pickup_window": "12:00-12:15", "capacity": SLOT_COUNT, "active": True},
    {"pickup_window": "17:30-17:45", "capacity": SLOT_COUNT, "active": True},
]


class PickupWindowCapacityUpdate(BaseModel):
    capacity: int = Field(ge=1, le=99)


def _database_enabled() -> bool:
    return bool(settings.database_url)


def _slot_number(slot_id: str) -> int:
    try:
        return int(slot_id.replace("P-", "", 1))
    except ValueError:
        return 0


def _pick_available_slot(order_id: str, used_slots: set[str], capacity: int = SLOT_COUNT) -> str | None:
    start = int(hashlib.sha1(order_id.encode("utf-8")).hexdigest(), 16) % capacity
    for offset in range(capacity):
        slot_id = f"P-{(start + offset) % capacity + 1:02d}"
        if slot_id not in used_slots:
            return slot_id
    return None


def _pickup_window_capacity(pickup_window: str) -> int:
    if pickup_window in pickup_window_capacity_overrides:
        return pickup_window_capacity_overrides[pickup_window]
    default = next(
        (item["capacity"] for item in DEFAULT_PICKUP_WINDOWS if item["pickup_window"] == pickup_window),
        SLOT_COUNT,
    )
    return int(default)


def assign_slot(
    order_id: str,
    pickup_window: str,
    state: dict[str, dict[str, object]],
    capacity: int | None = None,
) -> str | None:
    used_slots = {
        str(reservation["slot_id"])
        for reservation in state.values()
        if reservation["pickup_window"] == pickup_window and reservation["status"] != "Available"
    }
    return _pick_available_slot(order_id, used_slots, capacity or _pickup_window_capacity(pickup_window))


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


async def _is_order_blocked(order_id: str) -> bool:
    if not _database_enabled():
        return order_id in blocked_orders
    return await asyncio.to_thread(_is_order_blocked_sync, order_id)


def _is_order_blocked_sync(order_id: str) -> bool:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute(
            """
            SELECT order_id
            FROM slot_reservation_blocks
            WHERE order_id = %s
            """,
            (order_id,),
        ).fetchone()
        return row is not None


async def _block_order_reservation(order_id: str, reason: str) -> None:
    if not _database_enabled():
        blocked_orders.add(order_id)
        return
    await asyncio.to_thread(_block_order_reservation_sync, order_id, reason)


def _block_order_reservation_sync(order_id: str, reason: str) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        conn.execute(
            """
            INSERT INTO slot_reservation_blocks (order_id, reason)
            VALUES (%s, %s)
            ON CONFLICT (order_id) DO UPDATE
                SET reason = EXCLUDED.reason
            """,
            (order_id, reason),
        )


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


def _ensure_pickup_slots_sync(capacity: int) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        _ensure_pickup_slots_in_transaction(conn, capacity)


def _ensure_pickup_slots_in_transaction(conn, capacity: int) -> None:
    for index in range(1, capacity + 1):
        conn.execute(
            """
            INSERT INTO pickup_slots (slot_id)
            VALUES (%s)
            ON CONFLICT (slot_id) DO NOTHING
            """,
            (f"P-{index:02d}",),
        )


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


async def _reserve_slot_for_order(order_id: str, pickup_window: str) -> tuple[dict[str, object] | None, bool]:
    return await asyncio.to_thread(_reserve_slot_for_order_sync, order_id, pickup_window)


def _reserve_slot_for_order_sync(order_id: str, pickup_window: str) -> tuple[dict[str, object] | None, bool]:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            # Same-window reservations must be serialized so two paid orders
            # cannot choose the same physical slot before either insert commits.
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (pickup_window,))
            conn.execute(
                """
                INSERT INTO pickup_windows (pickup_window, capacity)
                VALUES (%s, %s)
                ON CONFLICT (pickup_window) DO NOTHING
                """,
                (pickup_window, SLOT_COUNT),
            )
            capacity_row = conn.execute(
                """
                SELECT capacity
                FROM pickup_windows
                WHERE pickup_window = %s
                FOR UPDATE
                """,
                (pickup_window,),
            ).fetchone()
            capacity = int(capacity_row[0]) if capacity_row else SLOT_COUNT
            blocked = conn.execute(
                """
                SELECT order_id
                FROM slot_reservation_blocks
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()
            if blocked:
                return None, True

            existing = conn.execute(
                """
                SELECT order_id
                FROM slot_reservations
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()
            if existing:
                return None, True

            used_rows = conn.execute(
                """
                SELECT slot_id
                FROM slot_reservations
                WHERE pickup_window = %s
                  AND status <> 'Available'
                """,
                (pickup_window,),
            ).fetchall()
            slot_id = _pick_available_slot(order_id, {str(row[0]) for row in used_rows}, capacity)
            if slot_id is None:
                return None, False

            _ensure_pickup_slots_in_transaction(conn, capacity)
            reservation = {
                "order_id": order_id,
                "slot_id": slot_id,
                "pickup_window": pickup_window,
                "status": "Reserved",
                "reserved_at": datetime.now(UTC).isoformat(),
            }
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
            return reservation, False


async def _update_reservation_status(order_id: str, status: str, released_at: str | None = None) -> None:
    if not _database_enabled():
        reservation = reservations.get(order_id)
        if reservation:
            current_status = str(reservation.get("status", ""))
            if not _can_transition_slot_status(current_status, status):
                return
            reservation["status"] = status
            if released_at:
                reservation["released_at"] = released_at
        return
    await asyncio.to_thread(_update_reservation_status_sync, order_id, status, released_at)


def _can_transition_slot_status(current_status: str, next_status: str) -> bool:
    if current_status == next_status:
        return True
    if current_status == "Available":
        return False
    current_rank = SLOT_STATUS_RANK.get(current_status)
    next_rank = SLOT_STATUS_RANK.get(next_status)
    if current_rank is None or next_rank is None:
        return True
    return next_rank >= current_rank


def _update_reservation_status_sync(order_id: str, status: str, released_at: str | None = None) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            row = conn.execute(
                """
                SELECT status
                FROM slot_reservations
                WHERE order_id = %s
                FOR UPDATE
                """,
                (order_id,),
            ).fetchone()
            if row is None or not _can_transition_slot_status(str(row[0]), status):
                return
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
        windows = [
            {
                **window,
                "capacity": pickup_window_capacity_overrides.get(
                    str(window["pickup_window"]),
                    int(window["capacity"]),
                ),
            }
            for window in DEFAULT_PICKUP_WINDOWS
        ]
        for pickup_window, capacity in pickup_window_capacity_overrides.items():
            if any(window["pickup_window"] == pickup_window for window in windows):
                continue
            windows.append({"pickup_window": pickup_window, "capacity": capacity, "active": True})
        return windows
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


async def _update_pickup_window_capacity(pickup_window: str, capacity: int) -> dict[str, object]:
    if not _database_enabled():
        active_slot_numbers = [
            _slot_number(str(reservation["slot_id"]))
            for reservation in reservations.values()
            if reservation["pickup_window"] == pickup_window and reservation["status"] != "Available"
        ]
        minimum_capacity = max(active_slot_numbers, default=1)
        if capacity < minimum_capacity:
            raise HTTPException(
                status_code=409,
                detail=f"Capacity must be at least {minimum_capacity} while active reservations exist",
            )
        pickup_window_capacity_overrides[pickup_window] = capacity
        default_window = next(
            (item for item in DEFAULT_PICKUP_WINDOWS if item["pickup_window"] == pickup_window),
            None,
        )
        return {
            "pickup_window": pickup_window,
            "capacity": capacity,
            "active": bool(default_window["active"]) if default_window else True,
        }
    try:
        return await asyncio.to_thread(_update_pickup_window_capacity_sync, pickup_window, capacity)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


def _update_pickup_window_capacity_sync(pickup_window: str, capacity: int) -> dict[str, object]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.transaction():
            rows = conn.execute(
                """
                SELECT slot_id
                FROM slot_reservations
                WHERE pickup_window = %s
                  AND status <> 'Available'
                """,
                (pickup_window,),
            ).fetchall()
            minimum_capacity = max((_slot_number(str(row["slot_id"])) for row in rows), default=1)
            if capacity < minimum_capacity:
                raise ValueError(f"Capacity must be at least {minimum_capacity} while active reservations exist")

            _ensure_pickup_slots_in_transaction(conn, capacity)
            row = conn.execute(
                """
                INSERT INTO pickup_windows (pickup_window, capacity, active)
                VALUES (%s, %s, true)
                ON CONFLICT (pickup_window) DO UPDATE
                    SET capacity = EXCLUDED.capacity
                RETURNING pickup_window, capacity, active
                """,
                (pickup_window, capacity),
            ).fetchone()
            return dict(row)


async def _list_slots() -> list[dict[str, object]]:
    if not _database_enabled():
        max_capacity = max(
            [int(item["capacity"]) for item in DEFAULT_PICKUP_WINDOWS] + list(pickup_window_capacity_overrides.values())
        )
        return [{"slot_id": f"P-{index:02d}", "active": True} for index in range(1, max_capacity + 1)]
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
        if await _is_order_blocked(event.aggregate_id):
            return
        pickup_window = event.payload["pickup_window"]
        reservation, already_handled = await _reserve_slot_for_order(event.aggregate_id, str(pickup_window))
        if already_handled:
            return
        slot_id = reservation["slot_id"] if reservation else None
    elif event.aggregate_id in state:
        return
    elif event.aggregate_id in blocked_orders:
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
    event_type = EventType(event.event_type)
    if event_type == EventType.INVENTORY_SHORTAGE_DETECTED:
        await _block_order_reservation(event.aggregate_id, "InventoryShortageDetected")
        if not reservation:
            return
    elif not reservation:
        return

    status_by_event = {
        EventType.ORDER_PREPARING: "Preparing",
        EventType.ORDER_PLACED_IN_SLOT: "PlacedInSlot",
        EventType.ORDER_READY: "Ready",
        EventType.ORDER_PICKED_UP: "Available",
        EventType.ORDER_EXPIRED: "Available",
        EventType.INVENTORY_SHORTAGE_DETECTED: "Available",
    }
    status = status_by_event.get(event_type, str(reservation["status"]))
    released_at = None
    if event_type in {
        EventType.ORDER_PICKED_UP,
        EventType.ORDER_EXPIRED,
        EventType.INVENTORY_SHORTAGE_DETECTED,
    }:
        released_at = datetime.now(UTC).isoformat()
    if _database_enabled():
        await _update_reservation_status(event.aggregate_id, status, released_at)
    else:
        if not _can_transition_slot_status(str(reservation.get("status", "")), status):
            return
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
        EventType.INVENTORY_SHORTAGE_DETECTED,
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


@app.patch("/pickup-windows/{pickup_window}")
async def update_pickup_window_capacity(
    pickup_window: str,
    payload: PickupWindowCapacityUpdate,
) -> dict[str, object]:
    updated = await _update_pickup_window_capacity(pickup_window, payload.capacity)
    log_event(
        logger,
        settings.service_name,
        "pickup window capacity updated",
        pickup_window=pickup_window,
        capacity=payload.capacity,
    )
    return updated


@app.get("/slots")
async def list_slots() -> list[dict[str, object]]:
    return await _list_slots()
