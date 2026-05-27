from __future__ import annotations

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


def assign_slot(order_id: str, pickup_window: str, state: dict[str, dict[str, object]]) -> str | None:
    used_slots = {
        str(reservation["slot_id"])
        for reservation in state.values()
        if reservation["pickup_window"] == pickup_window and reservation["status"] != "Available"
    }
    start = int(hashlib.sha1(order_id.encode("utf-8")).hexdigest(), 16) % SLOT_COUNT
    for offset in range(SLOT_COUNT):
        slot_id = f"P-{(start + offset) % SLOT_COUNT + 1:02d}"
        if slot_id not in used_slots:
            return slot_id
    return None


async def handle_order_paid(
    event: EventEnvelope,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    state: dict[str, dict[str, object]] = reservations,
) -> None:
    if event.aggregate_id in state:
        return

    pickup_window = event.payload["pickup_window"]
    slot_id = assign_slot(event.aggregate_id, pickup_window, state)
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
    reservation = state.get(event.aggregate_id)
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
    reservation["status"] = status_by_event.get(event_type, reservation["status"])
    if event_type in {EventType.ORDER_PICKED_UP, EventType.ORDER_EXPIRED}:
        reservation["released_at"] = datetime.now(UTC).isoformat()


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
    return list(reservations.values())
