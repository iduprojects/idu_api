"""Services territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import ServicesCountCapacity, ServicesData, ServicesDataWithGeometry, ServiceTypes
from idu_api.urban_api.schemas.enums import Ordering
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.pages import Page
from idu_api.urban_api.schemas.services import ServicesOrderByField
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/service_types",
    response_model=list[ServiceTypes],
    status_code=status.HTTP_200_OK,
)
async def get_service_types_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[ServiceTypes]:
    """Get service types for territory by territory identifier."""
    territories_service: TerritoriesService = request.state.territories_service

    service_types = await territories_service.get_service_types_by_territory_id(territory_id)

    return [ServiceTypes.from_dto(service_type) for service_type in service_types]


@territories_router.get(
    "/territory/{territory_id}/services",
    response_model=Page[ServicesData],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
    urban_function_id: int | None = Query(None, description="urban function identifier", gt=0),
    name: str | None = Query(None, description="filter services by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only cities or not"),
    order_by: ServicesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> Page[ServicesData]:
    """Get services for territory by id.

    service type, name and is_city could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_by_territory_id(
        territory_id,
        service_type_id,
        urban_function_id,
        name,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [ServicesData.from_dto(item) for item in x],
    )


@territories_router.get(
    "/territory/{territory_id}/services_with_geometry",
    response_model=Page[ServicesDataWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="Service type id", gt=0),
    urban_function_id: int | None = Query(None, description="urban function identifier", gt=0),
    name: str | None = Query(None, description="Filter services by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only cities or not"),
    order_by: ServicesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> Page[ServicesDataWithGeometry]:
    """Get services for territory by id.

    service type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_with_geometry_by_territory_id(
        territory_id,
        service_type_id,
        urban_function_id,
        name,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [ServicesDataWithGeometry.from_dto(item) for item in x],
    )


@territories_router.get(
    "/territory/{territory_id}/services_geojson",
    response_model=GeoJSONResponse[Feature[Geometry, ServicesData]],
    status_code=status.HTTP_200_OK,
)
async def get_services_geojson_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="Service type id", gt=0),
    urban_function_id: int | None = Query(None, description="urban function identifier", gt=0),
    name: str | None = Query(None, description="Filter services by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only cities or not"),
    centers_only: bool = Query(False, description="to get only center points of geometries"),
) -> GeoJSONResponse[Feature[Geometry, ServicesData]]:
    """Get FeatureCollection with geometries of service objects for given territory.

    Service type and name of services could be specified in parameters.
    Set centers_only = true to get only center points of geometries.
    """
    territories_service: TerritoriesService = request.state.territories_service

    services = await territories_service.get_services_with_geometry_by_territory_id(
        territory_id, service_type_id, urban_function_id, name, cities_only, None, None
    )

    return await GeoJSONResponse.from_list([service.to_geojson_dict() for service in services], centers_only)


@territories_router.get(
    "/territory/{territory_id}/services_capacity",
    response_model=list[ServicesCountCapacity],
    status_code=status.HTTP_200_OK,
)
async def get_total_services_capacity_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    level: int = Query(..., description="territory level", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
) -> list[ServicesCountCapacity]:
    """Get aggregated count and capacity of services for territories at the given level.

    Could be specified by service type in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    services = await territories_service.get_services_capacity_by_territory_id(territory_id, level, service_type_id)

    return [ServicesCountCapacity.from_dto(s) for s in services]
