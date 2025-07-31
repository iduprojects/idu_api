"""Buffers territories-related handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import BufferAttributes
from idu_api.urban_api.schemas.geometries import GeoJSONResponse

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/buffers_geojson",
    response_model=GeoJSONResponse[Feature[Geometry, BufferAttributes]],
    status_code=status.HTTP_200_OK,
)
async def get_buffers_geojson_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    buffer_type_id: int | None = Query(None, description="to filter by buffer type", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    include_child_territories: bool = Query(
        True, description="to get from child territories (unsafe for high level territories)"
    ),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> GeoJSONResponse[Feature[Geometry, BufferAttributes]]:
    """
    ## Get buffers in GeoJSON format for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by physical object type or service_type.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **buffer_type_id** (int | None, Query): Filters results by buffer type.
    - **physical_object_type_id** (int | None, Query): Filters results by physical object type.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **centers_only** (bool, Query): If True, returns only center points of geometries (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, BufferAttributes]]**: A GeoJSON response containing buffers and their geometries.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `physical_object_type_id` and `service_type_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if physical_object_type_id is not None and service_type_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either physical_object_type_id or service_type_id",
        )

    buffers = await territories_service.get_buffers_by_territory_id(
        territory_id,
        include_child_territories,
        cities_only,
        buffer_type_id,
        physical_object_type_id,
        service_type_id,
    )

    return await GeoJSONResponse.from_list((buffer.to_geojson_dict() for buffer in buffers))
