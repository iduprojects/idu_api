"""Buildings territories-related handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import BuildingWithGeometry
from idu_api.urban_api.schemas.pages import Page
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/living_buildings_with_geometry",
    response_model=Page[BuildingWithGeometry],
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def get_buildings_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    include_child_territories: bool = Query(
        True, description="to get from child territories (unsafe for high level territories)"
    ),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> Page[BuildingWithGeometry]:
    """
    ## Get living buildings with geometry for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** This method has been deprecated since version 0.33.1 and will be removed in version 1.0.
    Instead, use method **GET /territories/{territory_id}/physical_objects_with_geometry**
    with parameter `physical_object_type_id = 4`.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **page** (int, Query): Specifies the page number for retrieving living buildings (default: 1).
    - **page_size** (int, Query): Defines the number of living buildings per page (default: 10).

    ### Returns:
    - **Page[BuildingWithGeometry]**: A paginated list of living buildings with geometry.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    buildings = await territories_service.get_buildings_with_geometry_by_territory_id(
        territory_id, include_child_territories, cities_only
    )

    return paginate(
        buildings.items,
        buildings.total,
        transformer=lambda x: [BuildingWithGeometry.from_dto(item) for item in x],
    )
