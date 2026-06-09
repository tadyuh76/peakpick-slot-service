from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

from shared.event_store import PostgresEventStore
from shared.events import EventEnvelope, EventType
from shared.settings import Settings

EventHandler = Callable[[EventEnvelope], Awaitable[None]]


class InMemoryEventBus:
    def __init__(self, event_store: PostgresEventStore | None = None):
        self.event_store = event_store
        self.handlers: dict[str, list[EventHandler]] = defaultdict(list)
        self.history: list[EventEnvelope] = []
        self.is_connected = False

    async def connect(self) -> None:
        self.is_connected = True

    async def close(self) -> None:
        self.is_connected = False

    async def publish(self, event: EventEnvelope) -> None:
        if self.event_store:
            await self.event_store.append(event)
        self.history.append(event)
        handlers = [*self.handlers[event.event_type], *self.handlers["*"]]
        for handler in handlers:
            await handler(event)

    async def subscribe(self, event_type: EventType | str, handler: EventHandler, queue_name: str = "") -> None:
        self.handlers[str(event_type)].append(handler)


class RabbitMQEventBus:
    def __init__(
        self,
        rabbitmq_url: str,
        exchange_name: str,
        event_store: PostgresEventStore | None = None,
    ):
        self.rabbitmq_url = rabbitmq_url
        self.exchange_name = exchange_name
        self.event_store = event_store
        self.connection: Any = None
        self.channel: Any = None
        self.exchange: Any = None
        self.dead_letter_exchange: Any = None
        self.consumer_tags: list[str] = []
        self.is_connected = False

    async def connect(self) -> None:
        import aio_pika

        last_error: Exception | None = None
        for attempt in range(1, 11):
            try:
                self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
                break
            except Exception as exc:
                last_error = exc
                if attempt == 10:
                    raise
                await asyncio.sleep(min(attempt, 5))
        if self.connection is None and last_error:
            raise last_error
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        self.dead_letter_exchange = await self.channel.declare_exchange(
            f"{self.exchange_name}.dlx",
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        self.is_connected = True

    async def close(self) -> None:
        if self.connection:
            await self.connection.close()
        self.is_connected = False

    async def publish(self, event: EventEnvelope) -> None:
        import aio_pika

        if self.event_store:
            await self.event_store.append(event)
        body = event.model_dump_json().encode("utf-8")
        message = aio_pika.Message(
            body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            correlation_id=event.correlation_id,
            message_id=event.event_id,
        )
        await self.exchange.publish(message, routing_key=event.event_type)

    async def subscribe(self, event_type: EventType | str, handler: EventHandler, queue_name: str = "") -> None:
        queue_name = queue_name or f"{event_type}.queue"
        queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments={"x-dead-letter-exchange": f"{self.exchange_name}.dlx"},
        )
        await queue.bind(self.exchange, routing_key=str(event_type))
        dead_letter_queue = await self.channel.declare_queue(f"{queue_name}.dlq", durable=True)
        await dead_letter_queue.bind(self.dead_letter_exchange, routing_key=str(event_type))

        async def on_message(message: Any) -> None:
            async with message.process(requeue=False):
                event = EventEnvelope.model_validate_json(message.body)
                if self.event_store:
                    await self.event_store.append(event)
                await handler(event)

        tag = await queue.consume(on_message)
        self.consumer_tags.append(tag)


def build_event_bus(settings: Settings) -> InMemoryEventBus | RabbitMQEventBus:
    event_store = PostgresEventStore(settings.database_url) if settings.database_url else None
    if settings.event_bus.lower() == "rabbitmq":
        return RabbitMQEventBus(
            rabbitmq_url=settings.rabbitmq_url,
            exchange_name=settings.event_exchange,
            event_store=event_store,
        )
    return InMemoryEventBus(event_store=event_store)


async def wait_for_events() -> None:
    await asyncio.sleep(0)
