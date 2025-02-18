"""Buildings territories-related handlers (v2) are defined here."""

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import BuildingWithGeometry
from idu_api.urban_api.schemas.pages import CursorPage
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/living_buildings_with_geometry",
    response_model=CursorPage[BuildingWithGeometry],
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def get_living_buildings_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> CursorPage[BuildingWithGeometry]:
    """
    ## Get living buildings with geometry for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to retrieve entities exclusively for cities.

    **WARNING 2:** This method has been deprecated since version 0.33.1 and will be removed in version 1.0.
    Instead, use the method **GET /territories/{territory_id}/physical_objects_with_geometry**
    with the parameter `physical_object_type_id = 4`.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory. Must be greater than 0.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: False).
    - **cursor** (str, Query): Cursor (encrypted living_building_id) for the next page.
    - **page_size** (int, Query): Defines the number of physical objects per page (default: 10).

    ### Returns:
    - **CursorPage[BuildingWithGeometry]**: A paginated list of living buildings with geometry, including cursor-based pagination data.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False.
    - **404 Not Found**: If the specified territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    buildings = await territories_service.get_living_buildings_with_geometry_by_territory_id(
        territory_id, include_child_territories, cities_only
    )

    return paginate(
        buildings.items,
        buildings.total,
        transformer=lambda x: [BuildingWithGeometry.from_dto(item) for item in x],
        additional_data=buildings.cursor_data,
    )
