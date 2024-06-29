"""Services handlers are defined here."""

from fastapi import Path, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.logic.services import add_service_to_db, patch_service_to_db, put_service_to_db
from urban_api.schemas import ServicesData, ServicesDataPatch, ServicesDataPost, ServicesDataPut

from .routers import services_router


@services_router.post(
    "/services",
    response_model=ServicesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_service(request: Request, service: ServicesDataPost) -> ServicesData:
    """Add a service to a given physical object."""
    conn: AsyncConnection = request.state.conn

    service_dto = await add_service_to_db(conn, service)

    return ServicesData.from_dto(service_dto)


@services_router.put(
    "/services/{service_id}",
    response_model=ServicesData,
    status_code=status.HTTP_201_CREATED,
)
async def put_service(
    request: Request,
    service: ServicesDataPut,
    service_id: int = Path(..., description="Service id", gt=0),
) -> ServicesData:
    """Update the given service - all attributes."""
    conn: AsyncConnection = request.state.conn

    service_dto = await put_service_to_db(conn, service, service_id)

    return ServicesData.from_dto(service_dto)


@services_router.patch(
    "/services/{service_id}",
    response_model=ServicesData,
    status_code=status.HTTP_201_CREATED,
)
async def patch_service(
    request: Request,
    service: ServicesDataPatch,
    service_id: int = Path(..., description="Service id", gt=0),
) -> ServicesData:
    """Update the given service - only given attributes."""
    conn: AsyncConnection = request.state.conn

    service_dto = await patch_service_to_db(conn, service, service_id)

    return ServicesData.from_dto(service_dto)
