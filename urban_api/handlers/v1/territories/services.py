"""Services territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from fastapi_pagination import paginate
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import ServicesData, ServicesDataWithGeometry
from urban_api.schemas.pages import Page

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/services",
    response_model=Page[ServicesData],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_territory_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="Service type id", gt=0),
    name: str | None = Query(None, description="Filter services by name substring (case-insensitive)"),
) -> Page[ServicesData]:
    """Get services for territory by id.

    service type and name could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    services = await territories_service.get_services_by_territory_id(territory_id, service_type_id, name)
    services = [ServicesData.from_dto(service) for service in services]

    return paginate(services)


@territories_router.get(
    "/territory/{territory_id}/services_with_geometry",
    response_model=Page[ServicesDataWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="Service type id", gt=0),
    name: str | None = Query(None, description="Filter services by name substring (case-insensitive)"),
) -> Page[ServicesDataWithGeometry]:
    """Get services for territory by id.

    service type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    services = await territories_service.get_services_with_geometry_by_territory_id(territory_id, service_type_id, name)
    services = [ServicesDataWithGeometry.from_dto(service) for service in services]

    return paginate(services)


@territories_router.get(
    "/territory/{territory_id}/services_capacity",
    response_model=int | None,
    status_code=status.HTTP_200_OK,
)
async def get_total_services_capacity_by_territory_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="Service type id", gt=0),
) -> int | None:
    """Get aggregated capacity of services for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    capacity = await territories_service.get_services_capacity_by_territory_id(
        territory_id, service_type_id=service_type_id
    )

    return capacity
