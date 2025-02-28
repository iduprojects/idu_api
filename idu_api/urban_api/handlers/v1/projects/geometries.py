"""Object geometries projects-related endpoints are defined here."""

from fastapi import Depends, HTTPException, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    GeometryAttributes,
    ObjectGeometryPatch,
    ObjectGeometryPut,
    OkResponse,
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
)
async def get_geometries_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_id: int | None = Query(None, description="to filter by physical object", gt=0),
    service_id: int | None = Query(None, description="to filter by service", gt=0),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ScenarioGeometryAttributes]]:
    """
    ## Get geometries for a given scenario in GeoJSON format.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_id** (int | None, Query): Optional filter by physical object identifier.
    - **service_id** (int | None, Query): Optional filter by service identifier.
    - **centers_only** (bool, Query): If True, returns only center points of geometries (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, ScenarioGeometryAttributes]]**: A GeoJSON response containing the geometries.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    geometries = await user_project_service.get_geometries_by_scenario_id(
        scenario_id,
        user,
        physical_object_id,
        service_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in geometries], centers_only)


@projects_router.get(
    "/scenarios/{scenario_id}/geometries_with_all_objects",
    response_model=GeoJSONResponse[Feature[Geometry, ScenarioAllObjects]],
    status_code=status.HTTP_200_OK,
)
async def get_geometries_with_all_objects_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ScenarioAllObjects]]:
    """
    ## Get geometries with associated services and physical objects for a given scenario in GeoJSON format.

    **WARNING:** You can only filter by physical object type or physical object function (and only by service type or urban function).

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_type_id** (int | None, Query): Optional filter by physical object type identifier.
    - **service_type_id** (int | None, Query): Optional filter by service type identifier.
    - **physical_object_function_id** (int | None, Query): Optional filter by physical object function identifier.
    - **urban_function_id** (int | None, Query): Optional filter by urban function identifier.
    - **centers_only** (bool, Query): If True, returns only center points of geometries (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, ScenarioAllObjects]]**: A GeoJSON response containing the geometries with associated objects in properties.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `physical_object_function_id` (or `service_type_id` and `urban_function_id`).
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if physical_object_type_id is not None and physical_object_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either physical_object_type_id or physical_object_function_id",
        )

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Please, choose either service_type_id or urban_function_id"
        )

    geometries = await user_project_service.get_geometries_with_all_objects_by_scenario_id(
        scenario_id,
        user,
        physical_object_type_id,
        service_type_id,
        physical_object_function_id,
        urban_function_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in geometries], centers_only)


@projects_router.get(
    "/projects/{project_id}/context/geometries",
    response_model=GeoJSONResponse[Feature[Geometry, GeometryAttributes]],
    status_code=status.HTTP_200_OK,
)
async def get_context_geometries(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    physical_object_id: int | None = Query(None, description="to filter by physical object", gt=0),
    service_id: int | None = Query(None, description="to filter by service", gt=0),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, GeometryAttributes]]:
    """
    ## Get geometries for the context of a project territory in GeoJSON format.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **physical_object_id** (int | None, Query): Optional filter by physical object identifier.
    - **service_id** (int | None, Query): Optional filter by service identifier.
    - **centers_only** (bool, Query): If True, returns only center points of geometries (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, GeometryAttributes]]**: A GeoJSON response containing the geometries.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    geometries = await user_project_service.get_context_geometries(
        project_id,
        user,
        physical_object_id,
        service_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in geometries], centers_only)


@projects_router.get(
    "/projects/{project_id}/context/geometries_with_all_objects",
    response_model=GeoJSONResponse[Feature[Geometry, AllObjects]],
    status_code=status.HTTP_200_OK,
)
async def get_context_geometries_with_all_objects(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, AllObjects]]:
    """
    ## Get geometries with associated services and physical objects for the context of a project territory in GeoJSON format.

    **WARNING:** You can only filter by physical object type or physical object function (and only by service type or urban function).

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **physical_object_type_id** (int | None, Query): Optional filter by physical object type identifier.
    - **service_type_id** (int | None, Query): Optional filter by service type identifier.
    - **physical_object_function_id** (int | None, Query): Optional filter by physical object function identifier.
    - **urban_function_id** (int | None, Query): Optional filter by urban function identifier.
    - **centers_only** (bool, Query): If True, returns only center points of geometries (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, AllObjects]]**: A GeoJSON response containing the geometries with associated objects.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `physical_object_function_id` (or `service_type_id` and `urban_function_id`).
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if physical_object_type_id is not None and physical_object_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either physical_object_type_id or physical_object_function_id",
        )

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Please, choose either service_type_id or urban_function_id"
        )

    geometries = await user_project_service.get_context_geometries_with_all_objects(
        project_id,
        user,
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
    object_geometry: ObjectGeometryPut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioObjectGeometry:
    """
    ## Update all attributes of a scenario object geometry.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.
    - **object_geometry** (ObjectGeometryPut, Body): The updated object geometry data.

    ### Returns:
    - **ScenarioObjectGeometry**: The updated scenario object geometry.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or object geometry (or related entity) does not exist.
    - **409 Conflict**: If you try to update non-scenario object geometry that has been already updated
    (then it is a scenario object).

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    object_geometry_dto = await user_project_service.put_object_geometry(
        object_geometry,
        scenario_id,
        object_geometry_id,
        is_scenario_object,
        user,
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
    object_geometry: ObjectGeometryPatch,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioObjectGeometry:
    """
    ## Update specific fields of a scenario object geometry.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.
    - **object_geometry** (ObjectGeometryPatch, Body): The partial object geometry data to update.

    ### Returns:
    - **ScenarioObjectGeometry**: The updated scenario object geometry.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or object geometry (or related entity) does not exist.
    - **409 Conflict**: If you try to update non-scenario object geometry that has been already updated
    (then it is a scenario object).

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    object_geometry_dto = await user_project_service.patch_object_geometry(
        object_geometry,
        scenario_id,
        object_geometry_id,
        is_scenario_object,
        user,
    )

    return ScenarioObjectGeometry.from_dto(object_geometry_dto)


@projects_router.delete(
    "/scenarios/{scenario_id}/geometries/{object_geometry_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_object_geometry(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a scenario object geometry by its identifier.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or object geometry does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_object_geometry(scenario_id, object_geometry_id, is_scenario_object, user)

    return OkResponse()
