"""Physical objects projects-related endpoints are defined here."""

from fastapi import Depends, HTTPException, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    OkResponse,
    PhysicalObject,
    PhysicalObjectPatch,
    PhysicalObjectPut,
    PhysicalObjectWithGeometryPost,
    ScenarioBuildingPatch,
    ScenarioBuildingPost,
    ScenarioBuildingPut,
    ScenarioPhysicalObject,
    ScenarioPhysicalObjectWithGeometryAttributes,
    ScenarioUrbanObject,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/physical_objects",
    response_model=list[ScenarioPhysicalObject],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioPhysicalObject]:
    """
    ## Get a list of physical objects for a given scenario.

    **WARNING:** You can only filter by physical object type or physical object function.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_type_id** (int | None, Query): Optional filter by physical object type identifier.
    - **physical_object_function_id** (int | None, Query): Optional filter by physical object function identifier.

    ### Returns:
    - **list[ScenarioPhysicalObject]**: A list of physical objects.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `physical_object_function_id`.
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

    physical_objects = await user_project_service.get_physical_objects_by_scenario_id(
        scenario_id,
        user,
        physical_object_type_id,
        physical_object_function_id,
    )

    return [ScenarioPhysicalObject.from_dto(phys_obj) for phys_obj in physical_objects]


@projects_router.get(
    "/scenarios/{scenario_id}/physical_objects_with_geometry",
    response_model=GeoJSONResponse[Feature[Geometry, ScenarioPhysicalObjectWithGeometryAttributes]],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_with_geometry_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ScenarioPhysicalObjectWithGeometryAttributes]]:
    """
    ## Get a list of physical objects with geometry for a given scenario.

    **WARNING:** You can only filter by physical object type or physical object function.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_type_id** (int | None, Query): Optional filter by physical object type identifier.
    - **physical_object_function_id** (int | None, Query): Optional filter by physical object function identifier.

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, ScenarioPhysicalObjectWithGeometryAttributes]]**: A GeoJSON response containing the physical objects.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `physical_object_function_id`.
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

    physical_objects = await user_project_service.get_physical_objects_with_geometry_by_scenario_id(
        scenario_id,
        user,
        physical_object_type_id,
        physical_object_function_id,
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in physical_objects], centers_only)


@projects_router.get(
    "/projects/{project_id}/context/physical_objects",
    response_model=list[PhysicalObject],
    status_code=status.HTTP_200_OK,
)
async def get_context_physical_objects(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[PhysicalObject]:
    """
    ## Get a list of physical objects for the context of a project territory.

    **WARNING:** You can only filter by physical object type or physical object function.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **physical_object_type_id** (int | None, Query): Optional filter by physical object type identifier.
    - **physical_object_function_id** (int | None, Query): Optional filter by physical object function identifier.

    ### Returns:
    - **list[PhysicalObject]**: A list of physical objects.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `physical_object_function_id`.
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

    physical_objects = await user_project_service.get_context_physical_objects(
        project_id, user, physical_object_type_id, physical_object_function_id
    )

    return [PhysicalObject.from_dto(phys_obj) for phys_obj in physical_objects]


@projects_router.get(
    "/projects/{project_id}/context/physical_objects_with_geometry",
    response_model=GeoJSONResponse[Feature[Geometry, PhysicalObject]],
    status_code=status.HTTP_200_OK,
)
async def get_context_physical_objects_with_geometry(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, PhysicalObject]]:
    """
    ## Get a list of physical objects for the context of a project territory.

    **WARNING:** You can only filter by physical object type or physical object function.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **physical_object_type_id** (int | None, Query): Optional filter by physical object type identifier.
    - **physical_object_function_id** (int | None, Query): Optional filter by physical object function identifier.

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, PhysicalObject]]**: A GeoJSON response containing the physical objects.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `physical_object_function_id`.
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

    physical_objects = await user_project_service.get_context_physical_objects_with_geometry(
        project_id, user, physical_object_type_id, physical_object_function_id
    )

    return await GeoJSONResponse.from_list([obj.to_geojson_dict() for obj in physical_objects], centers_only)


@projects_router.post(
    "/scenarios/{scenario_id}/physical_objects",
    response_model=ScenarioUrbanObject,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def add_physical_object_with_geometry(
    request: Request,
    physical_object: PhysicalObjectWithGeometryPost,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioUrbanObject:
    """
    ## Create a new physical object with geometry for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object** (PhysicalObjectWithGeometryPost, Body): The physical object data including geometry.

    ### Returns:
    - **ScenarioUrbanObject**: The created urban object (physical object + geometry).

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_physical_object_with_geometry(
        physical_object,
        scenario_id,
        user,
    )

    return ScenarioUrbanObject.from_dto(urban_object)


@projects_router.post(
    "/scenarios/{scenario_id}/all_physical_objects",
    response_model=list[ScenarioUrbanObject],
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def update_physical_objects_by_function_id(
    request: Request,
    physical_object: list[PhysicalObjectWithGeometryPost],
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_function_id: int = Query(..., description="physical object function identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioUrbanObject]:
    """
    ## Update physical objects by function identifier for a given scenario.

    **NOTE:** This operation deletes all physical objects with the specified function identifier
    for a given scenario and uploads new objects with the same function.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_function_id** (int, Query): Unique identifier of the physical object function.
    - **physical_object** (list[PhysicalObjectWithGeometryPost], Body): List of physical objects to be added.

    ### Returns:
    - **list[ScenarioUrbanObject]**: A list of updated urban objects (physical objects + geometry + service).

    ### Errors:
    - **400 Bad Request**: If a list of physical objects contains physical objects with another function.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or physical object function (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    try:
        urban_objects = await user_project_service.update_physical_objects_by_function_id(
            physical_object,
            scenario_id,
            user,
            physical_object_function_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return [ScenarioUrbanObject.from_dto(urban_object) for urban_object in urban_objects]


@projects_router.put(
    "/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
    response_model=ScenarioPhysicalObject,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_physical_object(
    request: Request,
    physical_object: PhysicalObjectPut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioPhysicalObject:
    """
    ## Update all attributes of a physical object for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.
    - **physical_object** (PhysicalObjectPut, Body): The updated physical object data.

    ### Returns:
    - **ScenarioPhysicalObject**: The updated physical object.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or physical object (or related entity) does not exist.
    - **409 Conflict**: If you try to update non-scenario physical object that has been already updated
    (then it is scenario object).

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    physical_object_dto = await user_project_service.put_physical_object(
        physical_object,
        scenario_id,
        physical_object_id,
        is_scenario_object,
        user,
    )

    return ScenarioPhysicalObject.from_dto(physical_object_dto)


@projects_router.patch(
    "/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
    response_model=ScenarioPhysicalObject,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_physical_object(
    request: Request,
    physical_object: PhysicalObjectPatch,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioPhysicalObject:
    """
    ## Update specific fields of a physical object for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.
    - **physical_object** (PhysicalObjectPatch, Body): The partial physical object data to update.

    ### Returns:
    - **ScenarioPhysicalObject**: The updated physical object.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or physical object (or related entity) does not exist.
    - **409 Conflict**: If you try to update non-scenario physical object that has been already updated
    (then it is scenario object).

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    physical_object_dto = await user_project_service.patch_physical_object(
        physical_object,
        scenario_id,
        physical_object_id,
        is_scenario_object,
        user,
    )

    return ScenarioPhysicalObject.from_dto(physical_object_dto)


@projects_router.delete(
    "/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_physical_object(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a physical object by its identifier for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or physical object does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_physical_object(scenario_id, physical_object_id, is_scenario_object, user)

    return OkResponse()


@projects_router.post(
    "/scenarios/{scenario_id}/buildings",
    response_model=ScenarioPhysicalObject,
    status_code=status.HTTP_201_CREATED,
)
async def add_building(
    request: Request,
    building: ScenarioBuildingPost,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioPhysicalObject:
    """
    ## Create a new building for given scenario.

    **WARNING:** There can only be one building per physical object.

    ### Parameters:
    - **building** (ScenarioBuildingPost, Body): Data for the new building.
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **ScenarioPhysicalObject**: Physical object with the new building information.

    ## Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the physical object does not exist.
    - **409 Conflict**: If a building already exists for this physical object.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    physical_object = await user_project_service.add_building(building, scenario_id, user)

    return ScenarioPhysicalObject.from_dto(physical_object)


@projects_router.put(
    "/scenarios/{scenario_id}/buildings",
    response_model=PhysicalObject,
    status_code=status.HTTP_200_OK,
)
async def put_building(
    request: Request,
    building: ScenarioBuildingPut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioPhysicalObject:
    """
    ## Create or update a building for given scenario.

    **NOTE:** If a building for given physical object already exists, it will be updated.
    Otherwise, a new building will be created.

    ### Parameters:
    - **building** (BuildingPut, Body): Data for updating or creating a building.
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **ScenarioPhysicalObject**: Physical object with the new building information.

    ## Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the physical object does not exist.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    physical_object = await user_project_service.put_building(building, scenario_id, user)

    return ScenarioPhysicalObject.from_dto(physical_object)


@projects_router.patch(
    "/scenarios/{scenario_id}/buildings/{building_id}",
    response_model=PhysicalObject,
    status_code=status.HTTP_200_OK,
)
async def patch_building(
    request: Request,
    building: ScenarioBuildingPatch,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    building_id: int = Path(..., description="building identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioPhysicalObject:
    """
    ## Partially update a building for given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **building_id** (int, Path): Unique identifier of the building.
    - **building** (BuildingPatch, Body): Fields to update in the building.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.

    ### Returns:
    - **ScenarioPhysicalObject**: Physical object with the new building information.

    ## Errors:
    - **404 Not Found**: If the building (or related entity) does not exist.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    physical_object = await user_project_service.patch_building(
        building, scenario_id, building_id, is_scenario_object, user
    )

    return ScenarioPhysicalObject.from_dto(physical_object)


@projects_router.delete(
    "/scenarios/{scenario_id}/buildings/{building_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_building(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    building_id: int = Path(..., description="building identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a building by its identifier for given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **building_id** (int, Path): Unique identifier of the building.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ## Errors:
    - **404 Not Found**: If the building does not exist.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_building(scenario_id, building_id, is_scenario_object, user)

    return OkResponse()
