from datetime import UTC, datetime

import pytest
from fastapi import HTTPException

from services.analytics_service.main import event_counts, handle_any_event, recent_events
from services.inventory_service.main import handle_order_paid as reserve_inventory
from services.notification_service.main import _notification_from_event_row
from services.notification_service.main import handle_inventory_shortage, handle_notification_requested
from services.slot_service.main import handle_order_paid as reserve_slot
from services.slot_service.main import handle_status_event
from services.store_ops_service.main import (
    _apply_board_event,
    handle_pickup_slot_reserved,
    mark_preparing,
    mark_ready,
    verify_pickup,
)
from shared.event_bus import InMemoryEventBus
from shared.events import EventType, new_event


@pytest.mark.asyncio
async def test_order_paid_drives_pickup_flow_until_slot_is_released() -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    event_counts.clear()
    recent_events.clear()

    slot_state: dict[str, dict[str, object]] = {}
    inventory_stock = {"coffee": 3, "water": 5}
    inventory_reservations: dict[str, dict[str, object]] = {}
    staff_board: dict[str, dict[str, object]] = {}
    notifications: list[dict[str, object]] = []

    await bus.subscribe(
        EventType.ORDER_PAID,
        lambda event: reserve_inventory(event, bus, inventory_stock, inventory_reservations),
    )
    await bus.subscribe(EventType.ORDER_PAID, lambda event: reserve_slot(event, bus, slot_state))
    await bus.subscribe(
        EventType.PICKUP_SLOT_RESERVED,
        lambda event: handle_pickup_slot_reserved(event, staff_board),
    )
    for event_type in (
        EventType.ORDER_PREPARING,
        EventType.ORDER_PLACED_IN_SLOT,
        EventType.ORDER_READY,
        EventType.ORDER_PICKED_UP,
    ):
        await bus.subscribe(event_type, lambda event: handle_status_event(event, slot_state))
    await bus.subscribe(
        EventType.NOTIFICATION_REQUESTED,
        lambda event: handle_notification_requested(event, notifications),
    )
    await bus.subscribe("*", handle_any_event)

    order_paid = new_event(
        EventType.ORDER_PAID,
        aggregate_id="order-100",
        source="order-service",
        payload={
            "order_id": "order-100",
            "customer_name": "Huy",
            "items": [{"sku": "coffee", "quantity": 2}],
            "pickup_window": "12:00-12:15",
            "payment_status": "Paid",
            "order_status": "Paid",
        },
        correlation_id="22222222-2222-2222-2222-222222222222",
    )

    await bus.publish(order_paid)

    assert inventory_stock["coffee"] == 1
    assert inventory_reservations["order-100"]["items"] == [{"sku": "coffee", "quantity": 2}]
    assert inventory_reservations["order-100"]["status"] == "Reserved"
    assert slot_state["order-100"]["status"] == "Reserved"
    assert staff_board["order-100"]["status"] == "SlotAssigned"
    assert staff_board["order-100"]["correlation_id"] == order_paid.correlation_id

    preparing = await mark_preparing("order-100", bus, staff_board)
    assert preparing["status"] == "Preparing"
    assert slot_state["order-100"]["status"] == "Preparing"

    ready = await mark_ready("order-100", bus, staff_board)
    assert ready["status"] == "ReadyForPickup"
    assert ready["token"]
    assert slot_state["order-100"]["status"] == "Ready"
    assert notifications[0]["order_id"] == "order-100"
    assert "Token:" in notifications[0]["message"]

    completed = await verify_pickup("order-100", str(ready["token"]), bus, staff_board)

    assert completed["status"] == "Completed"
    assert slot_state["order-100"]["status"] == "Available"
    assert event_counts["OrderPaid"] == 1
    assert event_counts["InventoryReserved"] == 1
    assert event_counts["PickupSlotReserved"] == 1
    assert event_counts["OrderPreparing"] == 1
    assert event_counts["OrderPlacedInSlot"] == 1
    assert event_counts["OrderReady"] == 1
    assert event_counts["NotificationRequested"] == 1
    assert event_counts["OrderPickedUp"] == 1


@pytest.mark.asyncio
async def test_inventory_shortage_publishes_staff_notification() -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    notifications: list[dict[str, object]] = []

    await bus.subscribe(
        EventType.ORDER_PAID,
        lambda event: reserve_inventory(event, bus, {"coffee": 0}, {}),
    )
    await bus.subscribe(
        EventType.INVENTORY_SHORTAGE_DETECTED,
        lambda event: handle_inventory_shortage(event, notifications),
    )

    await bus.publish(
        new_event(
            EventType.ORDER_PAID,
            aggregate_id="order-shortage",
            source="order-service",
            payload={
                "order_id": "order-shortage",
                "customer_name": "Huy",
                "items": [{"sku": "coffee", "quantity": 1}],
                "pickup_window": "12:00-12:15",
            },
        )
    )

    assert notifications[0]["channel"] == "staff"
    assert notifications[0]["details"][0]["available"] == 0


@pytest.mark.asyncio
async def test_pickup_verification_rejects_wrong_token() -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    staff_board = {
        "order-100": {
            "order_id": "order-100",
            "slot_id": "P-01",
            "pickup_window": "12:00-12:15",
            "status": "ReadyForPickup",
            "token": "PK-123456",
            "correlation_id": "33333333-3333-3333-3333-333333333333",
        }
    }

    with pytest.raises(HTTPException) as exc_info:
        await verify_pickup("order-100", "bad-token", bus, staff_board)

    assert exc_info.value.status_code == 400


def test_store_board_projection_can_rebuild_from_event_rows() -> None:
    state: dict[str, dict[str, object]] = {}
    occurred_at = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)

    _apply_board_event(
        state,
        "PickupSlotReserved",
        "order-100",
        "33333333-3333-3333-3333-333333333333",
        {"slot_id": "P-01", "pickup_window": "12:00-12:15"},
        occurred_at,
    )
    _apply_board_event(
        state,
        "OrderReady",
        "order-100",
        "33333333-3333-3333-3333-333333333333",
        {"slot_id": "P-01", "pickup_window": "12:00-12:15", "token": "PK-123456"},
        occurred_at,
    )

    assert state["order-100"]["status"] == "ReadyForPickup"
    assert state["order-100"]["slot_id"] == "P-01"
    assert state["order-100"]["token"] == "PK-123456"


def test_notification_projection_can_rebuild_ready_message_from_event_row() -> None:
    notification = _notification_from_event_row(
        {
            "event_type": "NotificationRequested",
            "aggregate_id": "order-100",
            "payload": {
                "message": "Order order-100 is ready at slot P-01. Token: PK-123456",
                "channel": "demo",
            },
            "occurred_at": datetime(2026, 6, 9, 12, 0, tzinfo=UTC),
        }
    )

    assert notification == {
        "order_id": "order-100",
        "message": "Order order-100 is ready at slot P-01. Token: PK-123456",
        "channel": "demo",
        "status": "Sent",
        "sent_at": "2026-06-09T12:00:00+00:00",
    }
