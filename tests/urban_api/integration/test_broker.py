"""Integration tests for broker handlers are defined here."""

import asyncio

import httpx
import pytest
from otteroad import KafkaConsumerService
from otteroad.models import (
    FunctionalZonesUpdated,
    RegionalScenarioCreated,
    ScenarioObjectsUpdated,
    ScenarioZonesUpdated,
    TerritoriesDeleted,
    TerritoriesUpdated,
    UrbanObjectsUpdated,
)

from idu_api.urban_api.schemas import OkResponse
from tests.urban_api.helpers.broker import mock_handler
from tests.urban_api.helpers.utils import assert_response


@pytest.mark.asyncio
async def test_urban_objects_update(urban_api_host: str, kafka_consumer: KafkaConsumerService):
    """Test POST /broker/urban_events/urban_objects_updated method."""

    # Arrange
    new_handler = mock_handler(UrbanObjectsUpdated)
    kafka_consumer.register_handler(new_handler)
    event = UrbanObjectsUpdated(territory_id=1, service_types=[1], physical_object_types=[1])

    # Act
    await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api") as client:
        response = await client.post("/broker/urban_events/urban_objects_updated", json=event.model_dump())
    await asyncio.sleep(5)

    # Assert
    assert_response(response, 200, OkResponse, None)
    assert len(new_handler.received_events) == 1, "No one event was received"
    received = new_handler.received_events[0]
    assert isinstance(received, UrbanObjectsUpdated), "Received event is not UrbanObjectsUpdated"
    assert received == event, "Event data does not match"


@pytest.mark.asyncio
async def test_functional_zones_update(urban_api_host: str, kafka_consumer: KafkaConsumerService):
    """Test POST /broker/urban_events/zones_updated method."""

    # Arrange
    new_handler = mock_handler(FunctionalZonesUpdated)
    kafka_consumer.register_handler(new_handler)
    event = FunctionalZonesUpdated(territory_id=1)

    # Act
    await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api") as client:
        response = await client.post("/broker/urban_events/zones_updated", json=event.model_dump())
    await asyncio.sleep(5)

    # Assert
    assert_response(response, 200, OkResponse, None)
    assert len(new_handler.received_events) == 1, "No one event was received"
    received = new_handler.received_events[0]
    assert isinstance(received, FunctionalZonesUpdated), "Received event is not FunctionalZonesUpdated"
    assert received == event, "Event data does not match"


@pytest.mark.asyncio
async def test_territories_update(urban_api_host: str, kafka_consumer: KafkaConsumerService):
    """Test POST /broker/urban_events/territories_updated method."""

    # Arrange
    new_handler = mock_handler(TerritoriesUpdated)
    kafka_consumer.register_handler(new_handler)
    event = TerritoriesUpdated(territory_ids=[1])

    # Act
    await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api") as client:
        response = await client.post("/broker/urban_events/territories_updated", json=event.model_dump())
    await asyncio.sleep(5)

    # Assert
    assert_response(response, 200, OkResponse, None)
    assert len(new_handler.received_events) == 1, "No one event was received"
    received = new_handler.received_events[0]
    assert isinstance(received, TerritoriesUpdated), "Received event is not TerritoriesUpdated"
    assert received == event, "Event data does not match"


@pytest.mark.asyncio
async def test_territories_delete(urban_api_host: str, kafka_consumer: KafkaConsumerService):
    """Test POST /broker/urban_events/territories_deleted method."""

    # Arrange
    new_handler = mock_handler(TerritoriesDeleted)
    kafka_consumer.register_handler(new_handler)
    event = TerritoriesDeleted(parent_ids=[1])

    # Act
    await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api") as client:
        response = await client.post("/broker/urban_events/territories_deleted", json=event.model_dump())
    await asyncio.sleep(5)

    # Assert
    assert_response(response, 200, OkResponse, None)
    assert len(new_handler.received_events) == 1, "No one event was received"
    received = new_handler.received_events[0]
    assert isinstance(received, TerritoriesDeleted), "Received event is not TerritoriesDeleted"
    assert received == event, "Event data does not match"


@pytest.mark.asyncio
async def test_regional_scenario_create(urban_api_host: str, kafka_consumer: KafkaConsumerService):
    """Test POST /broker/urban_events/regional_scenario_created method."""

    # Arrange
    new_handler = mock_handler(RegionalScenarioCreated)
    kafka_consumer.register_handler(new_handler)
    event = RegionalScenarioCreated(scenario_id=1, territory_id=1)

    # Act
    await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api") as client:
        response = await client.post("/broker/scenario_events/regional_scenario_created", json=event.model_dump())
    await asyncio.sleep(5)

    # Assert
    assert_response(response, 200, OkResponse, None)
    assert len(new_handler.received_events) == 1, "No one event was received"
    received = new_handler.received_events[0]
    assert isinstance(received, RegionalScenarioCreated), "Received event is not RegionalScenarioCreated"
    assert received == event, "Event data does not match"


@pytest.mark.asyncio
async def test_scenario_objects_update(urban_api_host: str, kafka_consumer: KafkaConsumerService):
    """Test POST /broker/urban_events/scenario_objects_updated method."""

    # Arrange
    new_handler = mock_handler(ScenarioObjectsUpdated)
    kafka_consumer.register_handler(new_handler)
    event = ScenarioObjectsUpdated(project_id=1, scenario_id=1, service_types=[1], physical_object_types=[1])

    # Act
    await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api") as client:
        response = await client.post("/broker/scenario_events/scenario_objects_updated", json=event.model_dump())
    await asyncio.sleep(5)

    # Assert
    assert_response(response, 200, OkResponse, None)
    assert len(new_handler.received_events) == 1, "No one event was received"
    received = new_handler.received_events[0]
    assert isinstance(received, ScenarioObjectsUpdated), "Received event is not ScenarioObjectsUpdated"
    assert received == event, "Event data does not match"


@pytest.mark.asyncio
async def test_scenario_zones_update(urban_api_host: str, kafka_consumer: KafkaConsumerService):
    """Test POST /broker/urban_events/scenario_zones_updated method."""

    # Arrange
    new_handler = mock_handler(ScenarioZonesUpdated)
    kafka_consumer.register_handler(new_handler)
    event = ScenarioZonesUpdated(project_id=1, scenario_id=1)

    # Act
    await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api") as client:
        response = await client.post("/broker/scenario_events/scenario_zones_updated", json=event.model_dump())
    await asyncio.sleep(5)

    # Assert
    assert_response(response, 200, OkResponse, None)
    assert len(new_handler.received_events) == 1, "No one event was received"
    received = new_handler.received_events[0]
    assert isinstance(received, ScenarioZonesUpdated), "Received event is not ScenarioZonesUpdated"
    assert received == event, "Event data does not match"
