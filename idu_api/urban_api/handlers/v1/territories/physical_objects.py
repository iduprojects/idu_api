"""Physical objects territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import PhysicalObjectsData, PhysicalObjectsTypes, PhysicalObjectWithGeometry
from idu_api.urban_api.schemas.enums import Ordering
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.pages import Page
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectsOrderByField
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/physical_object_types",
    response_model=list[PhysicalObjectsTypes],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[PhysicalObjectsTypes]:
    """Get physical object types for territory by territory identifier."""
    territories_service: TerritoriesService = request.state.territories_service

    physical_object_types = await territories_service.get_physical_object_types_by_territory_id(territory_id)

    return [PhysicalObjectsTypes.from_dto(service_type) for service_type in physical_object_types]


@territories_router.get(
    "/territory/{territory_id}/physical_objects",
    response_model=Page[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
    physical_object_function_id: int | None = Query(None, description="Physical object function id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only cities or not"),
    order_by: PhysicalObjectsOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> Page[PhysicalObjectsData]:
    """Get physical objects for territory.

    physical object type, cities only and physical object function could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    physical_objects = await territories_service.get_physical_objects_by_territory_id(
        territory_id,
        physical_object_type_id,
        physical_object_function_id,
        name,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        physical_objects.items,
        physical_objects.total,
        transformer=lambda x: [PhysicalObjectsData.from_dto(item) for item in x],
    )


@territories_router.get(
    "/territory/{territory_id}/physical_objects_with_geometry",
    response_model=Page[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
    physical_object_function_id: int | None = Query(None, description="Physical object function id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only cities or not"),
    order_by: PhysicalObjectsOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> Page[PhysicalObjectWithGeometry]:
    """Get physical objects with geometry for territory.

    physical object type, cities only and physical object function could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    physical_objects = await territories_service.get_physical_objects_with_geometry_by_territory_id(
        territory_id,
        physical_object_type_id,
        physical_object_function_id,
        name,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        physical_objects.items,
        physical_objects.total,
        transformer=lambda x: [PhysicalObjectWithGeometry.from_dto(item) for item in x],
    )


@territories_router.get(
    "/territory/{territory_id}/physical_objects_geojson",
    response_model=GeoJSONResponse[Feature[Geometry, PhysicalObjectsData]],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_geojson_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
    physical_object_function_id: int | None = Query(None, description="Physical object function id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only cities or not"),
    centers_only: bool = Query(False, description="to get only center points of geometries"),
) -> GeoJSONResponse[Feature[Geometry, PhysicalObjectsData]]:
    """Get FeatureCollection with geometries of physical objects for given territory.

    Physical object type, name and physical object function could be specified in parameters.
    Set centers_only = true to get only center points of geometries.
    """
    territories_service: TerritoriesService = request.state.territories_service

    physical_objects = await territories_service.get_physical_objects_with_geometry_by_territory_id(
        territory_id, physical_object_type_id, physical_object_function_id, name, cities_only, None, None
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in physical_objects], centers_only)
