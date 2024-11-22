"""Object geometries projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    GeometryAttributes,
    ObjectGeometriesPatch,
    ObjectGeometriesPut,
    ScenarioAllObjects,
    ScenarioGeometryAttributes,
    ScenarioObjectGeometry,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.object_geometries import AllObjects
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/geometries",
    response_model=GeoJSONResponse[Feature[Geometry, ScenarioGeometryAttributes]],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_geometries_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_id: int | None = Query(None, description="to filter by physical object"),
    service_id: int | None = Query(None, description="to filter by service"),
    centers_only: bool = Query(False, description="to get only center points"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ScenarioGeometryAttributes]]:
    """Get all geometries for given scenario in geojson format.

    It could be specified by physical object and service."""
    user_project_service: UserProjectService = request.state.user_project_service

    geometries = await user_project_service.get_geometries_by_scenario_id(
        scenario_id,
        user.id,
        physical_object_id,
        service_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in geometries], centers_only)


@projects_router.get(
    "/scenarios/{scenario_id}/geometries_with_all_objects",
    response_model=GeoJSONResponse[Feature[Geometry, ScenarioAllObjects]],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_geometries_with_all_objects_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type"),
    service_type_id: int | None = Query(None, description="to filter by service type"),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function"),
    urban_function_id: int | None = Query(None, description="to filter by urban function"),
    centers_only: bool = Query(False, description="to get only center points"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ScenarioAllObjects]]:
    """Get all geometries with lists of services and physical objects for given scenario in geojson format.

    It could be specified by physical object type and service type."""
    user_project_service: UserProjectService = request.state.user_project_service

    geometries = await user_project_service.get_geometries_with_all_objects_by_scenario_id(
        scenario_id,
        user.id,
        physical_object_type_id,
        service_type_id,
        physical_object_function_id,
        urban_function_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in geometries], centers_only)


@projects_router.get(
    "/scenarios/{scenario_id}/context/geometries",
    response_model=GeoJSONResponse[Feature[Geometry, GeometryAttributes]],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_context_geometries_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_id: int | None = Query(None, description="to filter by physical object"),
    service_id: int | None = Query(None, description="to filter by service"),
    centers_only: bool = Query(False, description="to get only center points"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, GeometryAttributes]]:
    """Get all geometries for context of the project territory for given scenario in geojson format.

    It could be specified by physical object and service."""
    user_project_service: UserProjectService = request.state.user_project_service

    geometries = await user_project_service.get_context_geometries_by_scenario_id(
        scenario_id,
        user.id,
        physical_object_id,
        service_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in geometries], centers_only)


@projects_router.get(
    "/scenarios/{scenario_id}/context/geometries_with_all_objects",
    response_model=GeoJSONResponse[Feature[Geometry, AllObjects]],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_context_geometries_with_all_objects_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type"),
    service_type_id: int | None = Query(None, description="to filter by service type"),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function"),
    urban_function_id: int | None = Query(None, description="to filter by urban function"),
    centers_only: bool = Query(False, description="to get only center points"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, AllObjects]]:
    """Get all geometries with lists of services and physical objects for context of the project territory
     for given scenario in geojson format.

    It could be specified by physical object type and service type."""
    user_project_service: UserProjectService = request.state.user_project_service

    geometries = await user_project_service.get_context_geometries_with_all_objects_by_scenario_id(
        scenario_id,
        user.id,
        physical_object_type_id,
        service_type_id,
        physical_object_function_id,
        urban_function_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in geometries], centers_only)


@projects_router.put(
    "/scenarios/{scenario_id}/geometries/{object_geometry_id}",
    response_model=ScenarioObjectGeometry,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_object_geometry(
    request: Request,
    object_geometry: ObjectGeometriesPut,
    scenario_id: int = Path(..., description="scenario identifier"),
    object_geometry_id: int = Path(..., description="object geometry identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioObjectGeometry:
    """Update scenario object geometry - all attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    object_geometry_dto = await user_project_service.put_object_geometry(
        object_geometry,
        scenario_id,
        object_geometry_id,
        is_scenario_object,
        user.id,
    )

    return ScenarioObjectGeometry.from_dto(object_geometry_dto)


@projects_router.patch(
    "/scenarios/{scenario_id}/geometries/{object_geometry_id}",
    response_model=ScenarioObjectGeometry,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_object_geometry(
    request: Request,
    object_geometry: ObjectGeometriesPatch,
    scenario_id: int = Path(..., description="scenario identifier"),
    object_geometry_id: int = Path(..., description="object geometry identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioObjectGeometry:
    """Update scenario object geometry - only given fields."""
    user_project_service: UserProjectService = request.state.user_project_service

    object_geometry_dto = await user_project_service.patch_object_geometry(
        object_geometry,
        scenario_id,
        object_geometry_id,
        is_scenario_object,
        user.id,
    )

    return ScenarioObjectGeometry.from_dto(object_geometry_dto)


@projects_router.delete(
    "/scenarios/{scenario_id}/geometries/{object_geometry_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_object_geometry(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    object_geometry_id: int = Path(..., description="object geometry identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete scenario object geometry by given id."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_object_geometry(
        scenario_id, object_geometry_id, is_scenario_object, user.id
    )
