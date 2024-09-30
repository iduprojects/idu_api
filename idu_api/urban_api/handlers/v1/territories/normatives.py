"""Normatives territories-related handlers are defined here."""

from datetime import date

from fastapi import Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import TerritoryWithNormatives
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
    territory_id: int = Path(..., description="territory id", gt=0),
    year: int = Query(date.today().year, description="to filter by year, current year is used by default"),
) -> list[Normative]:
    """Get territory normatives."""
    territories_service: TerritoriesService = request.state.territories_service

    normatives = await territories_service.get_normatives_by_territory_id(territory_id, year)

    return [Normative.from_dto(normative) for normative in normatives]


@territories_router.post(
    "/territory/{territory_id}/normatives",
    response_model=list[Normative],
    status_code=status.HTTP_200_OK,
)
async def post_territory_normatives(
    request: Request,
    normatives: list[NormativePost],
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[Normative]:
    """Post batch of territory normatives.

    If at least one normative already exists, 400 error is returned and none are added.
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
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[Normative]:
    """Post batch of territory normatives.

    If at least one of normatives does not exist, 404 error is returned and no normatives are updated.
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
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[Normative]:
    """Patch batch of territory normatives.

    If at least one of normatives does not exist, 404 error is returned and no normatives are updated.
    """
    territories_service: TerritoriesService = request.state.territories_service

    normative_dtos = await territories_service.patch_normatives_by_territory_id(territory_id, normatives)

    return [Normative.from_dto(normative) for normative in normative_dtos]


@territories_router.delete(
    "/territory/{territory_id}/normatives",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_territory_normatives(
    request: Request,
    normatives: list[NormativeDelete],
    territory_id: int = Path(..., description="territory id", gt=0),
) -> dict:
    """Delete batch of territory normatives.

    If at least one of normatives does not exist, 404 error is returned and no normatives are deleted.
    """
    territories_service: TerritoriesService = request.state.territories_service

    return await territories_service.delete_normatives_by_territory_id(territory_id, normatives)


@territories_router.get(
    "/territory/normatives_values",
    response_model=GeoJSONResponse[Feature[Geometry, TerritoryWithNormatives]],
    response_model_exclude_none=True,
    status_code=status.HTTP_200_OK,
)
async def get_normatives_values_by_parent_id(
    request: Request,
    parent_id: int | None = Query(None, description="parent territory id", gt=0),
    year: int = Query(date.today().year, description="to filter by year, current year is used by default"),
) -> GeoJSONResponse[Feature[Geometry, TerritoryWithNormatives]]:
    """Get FeatureCollection with child territories and list of normatives with values in properties.

    parent id should be null or skipped for high-level territories.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territories = await territories_service.get_normatives_values_by_parent_id(parent_id, year)

    return await GeoJSONResponse.from_list([territory.to_geojson_dict() for territory in territories])
