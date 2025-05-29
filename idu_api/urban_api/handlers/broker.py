"""Endpoints for sending messages to Kafka broker are defined here."""

from fastapi import Depends
from otteroad import KafkaProducerClient
from otteroad.models import (
    FunctionalZonesUpdated,
    RegionalScenarioCreated,
    ScenarioObjectsUpdated,
    ScenarioZonesUpdated,
    TerritoriesDeleted,
    TerritoriesUpdated,
    UrbanObjectsUpdated,
)
from starlette import status

from idu_api.urban_api.schemas import OkResponse
from idu_api.urban_api.utils.broker import get_kafka_producer

from .routers import broker_router


@broker_router.post(
    "/broker/urban_events/urban_objects_updated",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def urban_objects_update(
    message: UrbanObjectsUpdated,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> OkResponse:
    """
    ## Push message to Kafka broker (topic `urban.events`) indicates that urban objects have been updated for territory.

    ### Parameters:
    - **message** (UrbanObjectsUpdated, Body): Model for message value includes territory identifier
    and two lists of identifiers for service types and physical objects types that has been updated.

    ### Returns:
    - **OkResponse**: A confirmation message of the success.
    """
    await kafka_producer.send(message)
    return OkResponse()


@broker_router.post(
    "/broker/urban_events/zones_updated",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def functional_zones_update(
    message: FunctionalZonesUpdated,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> OkResponse:
    """
    ## Push message to Kafka broker (topic `urban.events`) indicates that functional zones have been updated for territory.

    ### Parameters:
    - **message** (FunctionalZonesUpdated, Body): Model for message value includes territory identifier.

    ### Returns:
    - **OkResponse**: A confirmation message of the success.
    """
    await kafka_producer.send(message)
    return OkResponse()


@broker_router.post(
    "/broker/urban_events/territories_updated",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def territories_update(
    message: TerritoriesUpdated,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> OkResponse:
    """
    ## Push message to Kafka broker (topic `urban.events`) indicates that territories have been created or updated.

    ### Parameters:
    - **message** (TerritoriesUpdated, Body): Model for message value includes list of updated territories identifiers.

    ### Returns:
    - **OkResponse**: A confirmation message of the success.
    """
    await kafka_producer.send(message)
    return OkResponse()


@broker_router.post(
    "/broker/urban_events/territories_deleted",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def territories_delete(
    message: TerritoriesDeleted,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> OkResponse:
    """
    ## Push message to Kafka broker (topic `urban.events`) indicates that territories have been deleted.

    ### Parameters:
    - **message** (TerritoriesUpdated, Body): Model for message value includes list of regional territories identifiers
    for which children territories have been deleted.

    ### Returns:
    - **OkResponse**: A confirmation message of the success.
    """
    await kafka_producer.send(message)
    return OkResponse()


@broker_router.post(
    "/broker/scenario_events/regional_scenario_created",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def regional_scenario_create(
    message: RegionalScenarioCreated,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> OkResponse:
    """
    ## Push message to Kafka broker (topic `scenario.events`) indicates that regional scenario has been created.

    ### Parameters:
    - **message** (RegionalScenarioCreated, Body): Model for message value includes regional scenario
    identifier and territory (region) identifier.

    ### Returns:
    - **OkResponse**: A confirmation message of the success.
    """
    await kafka_producer.send(message)
    return OkResponse()


@broker_router.post(
    "/broker/scenario_events/scenario_objects_updated",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def scenario_objects_update(
    message: ScenarioObjectsUpdated,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> OkResponse:
    """
    ## Push message to Kafka broker (topic `urban.events`) indicates that new urban objects have been uploaded for **PROJECT** scenario.

    ### Parameters:
    - **message** (RegionalScenarioCreated, Body): Model for message value includes project and scenario identifiers,
    and also two lists of identifiers for service types and physical objects types that has been updated.

    ### Returns:
    - **OkResponse**: A confirmation message of the success.
    """
    await kafka_producer.send(message)
    return OkResponse()


@broker_router.post(
    "/broker/scenario_events/scenario_zones_updated",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def scenario_zones_update(
    message: ScenarioZonesUpdated,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> OkResponse:
    """
    ## Push message to Kafka broker (topic `urban.events`) indicates that new functional zones have been uploaded for **PROJECT** scenario.

    ### Parameters:
    - **message** (RegionalScenarioCreated, Body): Model for message value includes project and scenario identifiers.

    ### Returns:
    - **OkResponse**: A confirmation message of the success.
    """
    await kafka_producer.send(message)
    return OkResponse()
