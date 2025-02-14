"""Normatives territories-related handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import OkResponse, TerritoryWithNormatives
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.normatives import Normative, NormativeDelete, NormativePatch, NormativePost

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/normatives",
    response_model=list[Normative],
    status_code=status.HTTP_200_OK,
)
async def get_territory_normatives(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    year: int | None = Query(None, description="optional filter by year (default: None)"),
    last_only: bool = Query(False, description="to get last normatives"),
    include_child_territories: bool = Query(False, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> list[Normative]:
    """
    ## Get normatives for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** Set `last_only = True` only if you don't specify `year`.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **year** (int | None, Query): Optional filter by the specified year (default: None).
    - **last_only** (bool, Query): If True, retrieves only the most recent normatives (default: true).
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: false).
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).

    ### Returns:
    - **list[Normative]**: A list of normatives associated with the territory.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False
    or `last_only` is set to True and `year` is set.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if year is not None and last_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either specific year or last_only",
        )

    normatives = await territories_service.get_normatives_by_territory_id(
        territory_id, year, last_only, include_child_territories, cities_only
    )

    return [Normative.from_dto(normative) for normative in normatives]


@territories_router.post(
    "/territory/{territory_id}/normatives",
    response_model=list[Normative],
    status_code=status.HTTP_200_OK,
)
async def post_territory_normatives(
    request: Request,
    normatives: list[NormativePost],
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> list[Normative]:
    """
    ## Create a batch of normatives for a given territory.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **normatives** (list[NormativePost], Body): List of normatives to be created.

    ### Returns:
    - **list[Normative]**: A list of created normatives.

    ### Errors:
    - **400 Bad Request**: If at least one normative with the specified attributes already exists.
    - **404 Not Found**: If the territory (or related entities) does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    normative_dtos = await territories_service.add_normatives_to_territory(territory_id, normatives)

    return [Normative.from_dto(normative) for normative in normative_dtos]


@territories_router.put(
    "/territory/{territory_id}/normatives",
    response_model=list[Normative],
    status_code=status.HTTP_200_OK,
)
async def put_territory_normatives(
    request: Request,
    normatives: list[NormativePost],
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> list[Normative]:
    """
    ## Update a batch of normatives for a given territory.

    **NOTE**: If a normative with the specified attributes already exists, it will be updated.
    Otherwise, a new normative will be created.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **normatives** (list[NormativePost], Body): List of normatives to be updated.

    ### Returns:
    - **list[Normative]**: A list of updated normatives.

    ### Errors:
    - **404 Not Found**: If the territory (or related entities) does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    normative_dtos = await territories_service.put_normatives_by_territory_id(territory_id, normatives)

    return [Normative.from_dto(normative) for normative in normative_dtos]


@territories_router.patch(
    "/territory/{territory_id}/normatives",
    response_model=list[Normative],
    status_code=status.HTTP_200_OK,
)
async def patch_territory_normatives(
    request: Request,
    normatives: list[NormativePatch],
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> list[Normative]:
    """
    ## Partially update a batch of normatives for a given territory.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **normatives** (list[NormativePatch], Body): List of normatives to be partially updated.

    ### Returns:
    - **list[Normative]**: A list of updated normatives.

    ### Errors:
    - **400 Bad Request**: If at least one normative with the specified attributes already exists.
    - **404 Not Found**: If the territory (or related entities) does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    normative_dtos = await territories_service.patch_normatives_by_territory_id(territory_id, normatives)

    return [Normative.from_dto(normative) for normative in normative_dtos]


@territories_router.delete(
    "/territory/{territory_id}/normatives",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_territory_normatives(
    request: Request,
    normatives: list[NormativeDelete],
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> OkResponse:
    """
    ## Delete a batch of normatives for a given territory.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **normatives** (list[NormativeDelete], Body): List of normatives to be deleted.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If at least one of the normatives does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    await territories_service.delete_normatives_by_territory_id(territory_id, normatives)

    return OkResponse()


@territories_router.get(
    "/territory/normatives_values",
    response_model=GeoJSONResponse[Feature[Geometry, TerritoryWithNormatives]],
    response_model_exclude_none=True,
    status_code=status.HTTP_200_OK,
)
async def get_normatives_values_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent territory identifier, should be skipped to get top level territories", gt=0
    ),
    year: int | None = Query(None, description="optional filter by year (default: None)"),
    last_only: bool = Query(False, description="to get last normatives"),
    centers_only: bool = Query(False, description="display only centers"),
) -> GeoJSONResponse[Feature[Geometry, TerritoryWithNormatives]]:
    """
    ## Get normatives for child territories (only given territory's level + 1) in GeoJSON format.

    **WARNING:** Set `last_only = True` only if you don't specify `year`.

    ### Parameters:
    - **parent_id** (int | None, Query): Unique identifier of the parent territory. If skipped, returns the highest level territories.
    - **year** (int | None, Query): Optional filter by the specified year (default: None).
    - **last_only** (bool, Query): If True, retrieves only the most recent normatives (default: true).
    - **centers_only** (bool, Query): If True, returns only center points of geometries (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, TerritoryWithNormatives]]**: A GeoJSON response containing territories and their normatives.

    ### Errors:
    - **400 Bad Request**: If `last_only` is set to True and `year` is set.
    - **404 Not Found**: If the parent territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if year is not None and last_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either specific year or last_only",
        )

    territories = await territories_service.get_normatives_values_by_parent_id(parent_id, year, last_only)

    return await GeoJSONResponse.from_list([territory.to_geojson_dict() for territory in territories], centers_only)
