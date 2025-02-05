"""Services territories-related handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import Service, ServicesCountCapacity, ServiceType, ServiceWithGeometry
from idu_api.urban_api.schemas.enums import Ordering
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.pages import Page
from idu_api.urban_api.schemas.services import ServicesOrderByField
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/service_types",
    response_model=list[ServiceType],
    status_code=status.HTTP_200_OK,
)
async def get_service_types_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> list[ServiceType]:
    """
    ## Get service types for a given territory.

    **WARNING:** Set `cities_only = True` only if you want to get entities from child territories.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **include_child_territories** (bool, Query): If True, includes data from child territories.
    - **cities_only** (bool, Query): If True, retrieves data only for cities.

    ### Returns:
    - **list[ServiceType]**: A list of service types for the given territory.

    ### Errors:
    - **400 Bad Request**: If you set cities_only = true and include_child_territories = false.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    service_types = await territories_service.get_service_types_by_territory_id(
        territory_id, include_child_territories, cities_only
    )

    return [ServiceType.from_dto(service_type) for service_type in service_types]


@territories_router.get(
    "/territory/{territory_id}/services",
    response_model=Page[Service],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
    urban_function_id: int | None = Query(None, description="urban function identifier", gt=0),
    name: str | None = Query(None, description="filter services by name substring (case-insensitive)"),
    include_child_territories: bool = Query(
        True, description="to get from child territories (unsafe for high level territories)"
    ),
    cities_only: bool = Query(False, description="to get only cities or not"),
    order_by: ServicesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> Page[Service]:
    """
    ## Get services for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by service type or urban function.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **urban_function_id** (int | None, Query): Filters results by urban function.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **order_by** (TerritoriesOrderByField, Query): Defines the sorting attribute - service_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **page** (int, Query): Specifies the page number for retrieving services (default: 1).
    - **page_size** (int, Query): Defines the number of services per page (default: 10).

    ### Returns:
    - **Page[Service]**: A paginated list of services.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `service_type_id` and `urban_function_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either service_type_id or urban_function_id",
        )

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_by_territory_id(
        territory_id,
        service_type_id,
        urban_function_id,
        name,
        include_child_territories,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [Service.from_dto(item) for item in x],
    )


@territories_router.get(
    "/territory/{territory_id}/services_with_geometry",
    response_model=Page[ServiceWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    name: str | None = Query(None, description="to filter services by name substring (case-insensitive)"),
    include_child_territories: bool = Query(
        True, description="to get from child territories (unsafe for high level territories)"
    ),
    cities_only: bool = Query(False, description="to get only for cities"),
    order_by: ServicesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> Page[ServiceWithGeometry]:
    """
    ## Get services with geometry for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by service type or urban function.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **urban_function_id** (int | None, Query): Filters results by urban function.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **order_by** (TerritoriesOrderByField, Query): Defines the sorting attribute - service_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **page** (int, Query): Specifies the page number for retrieving services (default: 1).
    - **page_size** (int, Query): Defines the number of services per page (default: 10).

    ### Returns:
    - **Page[ServiceWithGeometry]**: A paginated list of services with geometry.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `service_type_id` and `urban_function_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either service_type_id or urban_function_id",
        )

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_with_geometry_by_territory_id(
        territory_id,
        service_type_id,
        urban_function_id,
        name,
        include_child_territories,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [ServiceWithGeometry.from_dto(item) for item in x],
    )


@territories_router.get(
    "/territory/{territory_id}/services_geojson",
    response_model=GeoJSONResponse[Feature[Geometry, Service]],
    status_code=status.HTTP_200_OK,
)
async def get_services_geojson_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    name: str | None = Query(None, description="to filter services by name substring (case-insensitive)"),
    include_child_territories: bool = Query(
        True, description="to get from child territories (unsafe for high level territories)"
    ),
    cities_only: bool = Query(False, description="to get only for cities"),
    centers_only: bool = Query(False, description="to get only center points of geometries"),
) -> GeoJSONResponse[Feature[Geometry, Service]]:
    """
    ## Get services in GeoJSON format for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by service type or urban function.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **urban_function_id** (int | None, Query): Filters results by urban function.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **centers_only** (bool, Query): If True, returns only center points of geometries (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, Service]]**: A GeoJSON response containing services and their geometries.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `service_type_id` and `urban_function_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either service_type_id or urban_function_id",
        )

    services = await territories_service.get_services_with_geometry_by_territory_id(
        territory_id,
        service_type_id,
        urban_function_id,
        name,
        include_child_territories,
        cities_only,
        None,
        "asc",
        paginate=False,
    )

    return await GeoJSONResponse.from_list([service.to_geojson_dict() for service in services], centers_only)


@territories_router.get(
    "/territory/{territory_id}/services_capacity",
    response_model=list[ServicesCountCapacity],
    status_code=status.HTTP_200_OK,
)
async def get_total_services_capacity_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    level: int = Query(..., description="territory level", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
) -> list[ServicesCountCapacity]:
    """
    ## Get aggregated count and capacity of services for territories at the given level.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **level** (int, Query): Level of the territory hierarchy to retrieve data for.
    - **service_type_id** (int | None, Query): Filters results by service type. If not provided, returns data for all service types.

    ### Returns:
    - **list[ServicesCountCapacity]**: A list of aggregated service counts and capacities for the specified territory and level.

    ### Errors:
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    services = await territories_service.get_services_capacity_by_territory_id(territory_id, level, service_type_id)

    return [ServicesCountCapacity.from_dto(s) for s in services]
