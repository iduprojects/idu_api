"""
Services endpoints are defined here.
"""

from typing import List, Optional

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.services import add_service_to_db
from urban_api.schemas import ServicesData, ServicesDataPost

from .routers import services_router


@services_router.post(
    "/services",
    response_model=ServicesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_service(service: ServicesDataPost, connection: AsyncConnection = Depends(get_connection)) -> ServicesData:
    """
    Summary:
        Add service

    Description:
        Add a service
    """

    service_dto = await add_service_to_db(service, connection)

    return ServicesData.from_dto(service_dto)
