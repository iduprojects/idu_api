"""All fixtures for broker integration tests are defined here."""

import uuid
from unittest.mock import AsyncMock

import pytest_asyncio
import structlog
from otteroad import BaseMessageHandler, KafkaConsumerService, KafkaConsumerSettings
from pydantic import BaseModel

from idu_api.urban_api.config import UrbanAPIConfig

__all__ = ["kafka_consumer"]


def mock_handler(event_type: type[BaseModel]):
    class MessageHandler(BaseMessageHandler[event_type]):
        def __init__(self, logger=None):
            super().__init__(logger)
            self.received_events = []

        async def on_startup(self):
            pass

        async def on_shutdown(self):
            pass

        async def handle(self, event, ctx):
            self.received_events.append(event)

    return MessageHandler()


@pytest_asyncio.fixture
async def kafka_consumer(config: UrbanAPIConfig) -> KafkaConsumerService:
    """Fixture for Kafka consumer service."""
    settings = KafkaConsumerSettings(
        bootstrap_servers=config.broker.bootstrap_servers,
        group_id=f"test-group-{uuid.uuid4().hex}",
        auto_offset_reset="latest",
        schema_registry_url=config.broker.schema_registry_url,
        enable_auto_commit=False,
    )

    # Initialize the consumer service and add a worker for topics
    service = KafkaConsumerService(settings, logger=structlog.getLogger("test-consumer"))
    service.add_worker(["urban.events", "scenario.events", "indicator.events"])
    await service.start()
    yield service
    await service.stop()
