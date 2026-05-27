from shared.events import EventEnvelope, EventType, new_event


def test_event_contract_contains_required_peakpick_events() -> None:
    required_events = {
        "CartCreated",
        "OrderPaid",
        "PickupSlotReserved",
        "PickupSlotFull",
        "InventoryReserved",
        "InventoryShortageDetected",
        "OrderPreparing",
        "OrderPlacedInSlot",
        "OrderReady",
        "OrderPickedUp",
        "OrderExpired",
        "NotificationRequested",
        "AnalyticsUpdated",
    }

    assert required_events.issubset({event_type.value for event_type in EventType})


def test_event_envelope_round_trips_as_json() -> None:
    event = new_event(
        EventType.ORDER_PAID,
        aggregate_id="order-1",
        source="order-service",
        payload={"pickup_window": "12:00-12:15", "items": [{"sku": "coffee", "quantity": 1}]},
        correlation_id="11111111-1111-1111-1111-111111111111",
    )

    restored = EventEnvelope.model_validate_json(event.model_dump_json())

    assert restored.event_type == EventType.ORDER_PAID
    assert restored.aggregate_id == "order-1"
    assert restored.correlation_id == "11111111-1111-1111-1111-111111111111"
    assert restored.payload["items"][0]["sku"] == "coffee"
