"""
Kafka producer dependency for FastAPI applications.

This module provides a dependency function for injecting an initialized
`KafkaProducerClient` instance from the FastAPI application state. It is
intended to be used with FastAPI's dependency injection system in route
handlers and background tasks.

Example usage:

    from fastapi import Depends

    @app.post("/events/")
    async def publish_event(
        event: MyEventModel,
        producer: KafkaProducerClient = Depends(get_kafka_producer),
    ):
        await producer.send(event)

The producer instance should be initialized during FastAPI startup and attached
to `app.state.kafka_producer`.
"""

from fastapi import Request
from otteroad import KafkaProducerClient


def get_kafka_producer(request: Request) -> KafkaProducerClient:
    return request.app.state.kafka_producer
