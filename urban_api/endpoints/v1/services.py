"""
Services endpoints are defined here.
"""

from fastapi import Depends, Path
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.services import add_service_to_db, patch_service_to_db, put_service_to_db
from urban_api.schemas import ServicesData, ServicesDataPatch, ServicesDataPost, ServicesDataPut

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


@services_router.put(
    "/services/{service_id}",
    response_model=ServicesData,
    status_code=status.HTTP_201_CREATED,
)
async def put_service(
    service: ServicesDataPut,
    service_id: int = Path(..., description="Service id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> ServicesData:
    """
    Summary:
        Put service

    Description:
        Put a service
    """

    service_dto = await put_service_to_db(service, service_id, connection)

    return ServicesData.from_dto(service_dto)


@services_router.patch(
    "/services/{service_id}",
    response_model=ServicesData,
    status_code=status.HTTP_201_CREATED,
)
async def patch_service(
    service: ServicesDataPatch,
    service_id: int = Path(..., description="Service id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> ServicesData:
    """
    Summary:
        Patch service

    Description:
        Patch a service
    """

    service_dto = await patch_service_to_db(service, service_id, connection)

    return ServicesData.from_dto(service_dto)
