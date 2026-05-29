import pytest
from fastapi.testclient import TestClient

from services.order_service.main import app as order_app
from services.order_service.main import carts, handle_order_lifecycle_event, orders
from services.slot_service.main import SLOT_COUNT
from services.slot_service.main import app as slot_app
from services.slot_service.main import assign_slot
from services.slot_service.main import handle_order_paid, reservations
from shared.event_bus import InMemoryEventBus
from shared.events import EventType, new_event


def test_order_service_creates_cart_and_paid_order() -> None:
    carts.clear()
    orders.clear()

    with TestClient(order_app) as client:
        cart_response = client.post(
            "/carts",
            json={
                "customer_name": "Huy",
                "items": [{"sku": "coffee", "quantity": 1}],
            },
        )
        assert cart_response.status_code == 201
        cart = cart_response.json()["cart"]
        assert cart["status"] == "CartCreated"

        checkout_response = client.post(
            "/checkout",
            json={
                "customer_name": "Huy",
                "pickup_window": "12:00-12:15",
                "items": [{"sku": "coffee", "quantity": 2}],
            },
        )
        assert checkout_response.status_code == 201
        order = checkout_response.json()["order"]
        assert order["payment_status"] == "Paid"
        assert order["order_status"] == "Paid"

        fetched_response = client.get(f"/orders/{order['order_id']}")
        assert fetched_response.status_code == 200
        assert fetched_response.json()["items"] == [{"sku": "coffee", "quantity": 2}]


@pytest.mark.asyncio
async def test_order_service_reflects_slot_and_pickup_lifecycle_events() -> None:
    state = {
        "order-200": {
            "order_id": "order-200",
            "customer_name": "Huy",
            "items": [{"sku": "water", "quantity": 1}],
            "pickup_window": "12:00-12:15",
            "payment_status": "Paid",
            "order_status": "Paid",
            "paid_at": "2026-06-09T12:00:00+00:00",
        }
    }

    await handle_order_lifecycle_event(
        new_event(
            EventType.PICKUP_SLOT_RESERVED,
            aggregate_id="order-200",
            source="slot-service",
            payload={"slot_id": "P-01"},
        ),
        state,
    )
    assert state["order-200"]["order_status"] == "SlotAssigned"

    await handle_order_lifecycle_event(
        new_event(
            EventType.ORDER_PICKED_UP,
            aggregate_id="order-200",
            source="store-ops-service",
            payload={"slot_id": "P-01"},
        ),
        state,
    )
    assert state["order-200"]["order_status"] == "Completed"


@pytest.mark.asyncio
async def test_slot_service_publishes_slot_full_when_window_has_no_capacity() -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    slot_state = {
        f"order-{index}": {
            "order_id": f"order-{index}",
            "slot_id": f"P-{index + 1:02d}",
            "pickup_window": "12:00-12:15",
            "status": "Reserved",
            "reserved_at": "2026-06-09T12:00:00+00:00",
        }
        for index in range(SLOT_COUNT)
    }

    await handle_order_paid(
        new_event(
            EventType.ORDER_PAID,
            aggregate_id="order-full",
            source="order-service",
            payload={
                "order_id": "order-full",
                "customer_name": "Huy",
                "pickup_window": "12:00-12:15",
                "items": [{"sku": "coffee", "quantity": 1}],
            },
        ),
        bus,
        slot_state,
    )

    assert bus.history[-1].event_type == EventType.PICKUP_SLOT_FULL
    assert "order-full" not in slot_state


def test_slot_service_exposes_demo_capacity_metadata() -> None:
    reservations.clear()

    with TestClient(slot_app) as client:
        windows_response = client.get("/pickup-windows")
        slots_response = client.get("/slots")

    assert windows_response.status_code == 200
    assert {"pickup_window": "12:00-12:15", "capacity": 8, "active": True} in windows_response.json()
    assert len(slots_response.json()) == 8


def test_slot_capacity_is_scoped_to_each_pickup_window() -> None:
    state = {
        f"morning-{index}": {
            "order_id": f"morning-{index}",
            "slot_id": f"P-{index + 1:02d}",
            "pickup_window": "09:30-09:35",
            "status": "Reserved",
        }
        for index in range(SLOT_COUNT)
    }

    assert assign_slot("lunch-order", "12:00-12:15", state) is not None


def test_schema_prevents_active_double_booking_for_same_slot_window() -> None:
    schema_sql = open("db/init.sql", encoding="utf-8").read()

    assert "idx_slot_reservations_active_slot_window" in schema_sql
    assert "WHERE status <> 'Available'" in schema_sql
