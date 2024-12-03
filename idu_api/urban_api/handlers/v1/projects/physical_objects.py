"""Physical objects projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    PhysicalObjectsData,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPut,
    PhysicalObjectWithGeometryPost,
    ScenarioPhysicalObject,
)
from idu_api.urban_api.schemas.urban_objects import ScenarioUrbanObject
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/physical_objects",
    response_model=list[ScenarioPhysicalObject],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_physical_objects_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type"),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function"),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioPhysicalObject]:
    """Get list of physical objects for given scenario.

    It could be specified by physical object type and physical object function."""
    user_project_service: UserProjectService = request.state.user_project_service

    physical_objects = await user_project_service.get_physical_objects_by_scenario_id(
        scenario_id,
        user.id,
        physical_object_type_id,
        physical_object_function_id,
    )

    return [ScenarioPhysicalObject.from_dto(phys_obj) for phys_obj in physical_objects]


@projects_router.get(
    "/scenarios/{scenario_id}/context/physical_objects",
    response_model=list[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_context_physical_objects_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type"),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function"),
    user: UserDTO = Depends(get_user),
) -> list[PhysicalObjectsData]:
    """Get list of physical objects for context of the project territory for given scenario.

    It could be specified by physical object type and physical object function."""
    user_project_service: UserProjectService = request.state.user_project_service

    physical_objects = await user_project_service.get_context_physical_objects_by_scenario_id(
        scenario_id,
        user.id,
        physical_object_type_id,
        physical_object_function_id,
    )

    return [PhysicalObjectsData.from_dto(phys_obj) for phys_obj in physical_objects]


@projects_router.post(
    "/scenarios/{scenario_id}/physical_objects",
    response_model=ScenarioUrbanObject,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def add_physical_object_with_geometry(
    request: Request,
    physical_object: PhysicalObjectWithGeometryPost,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenarioUrbanObject:
    """Create new physical object and geometry for given scenario."""
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_physical_object_with_geometry(
        physical_object,
        scenario_id,
        user.id,
    )

    return ScenarioUrbanObject.from_dto(urban_object)


@projects_router.post(
    "/scenarios/{scenario_id}/all_physical_objects",
    response_model=list[ScenarioUrbanObject],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def update_physical_objects_by_function_id(
    request: Request,
    physical_object: list[PhysicalObjectWithGeometryPost],
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_function_id: int = Query(..., description="physical object function identifier"),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioUrbanObject]:
    """Delete all physical objects by physical object function identifier
    and upload new objects with the same function for given scenario."""
    user_project_service: UserProjectService = request.state.user_project_service

    urban_objects = await user_project_service.update_physical_objects_by_function_id(
        physical_object,
        scenario_id,
        user.id,
        physical_object_function_id,
    )

    return [ScenarioUrbanObject.from_dto(urban_object) for urban_object in urban_objects]


@projects_router.put(
    "/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
    response_model=ScenarioPhysicalObject,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_physical_object(
    request: Request,
    physical_object: PhysicalObjectsDataPut,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_id: int = Path(..., description="physical object identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioPhysicalObject:
    """Update scenario physical object - all attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    physical_object_dto = await user_project_service.put_physical_object(
        physical_object,
        scenario_id,
        physical_object_id,
        is_scenario_object,
        user.id,
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
    physical_object: PhysicalObjectsDataPatch,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_id: int = Path(..., description="physical object identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioPhysicalObject:
    """Update scenario physical object - only given fields."""
    user_project_service: UserProjectService = request.state.user_project_service

    physical_object_dto = await user_project_service.patch_physical_object(
        physical_object,
        scenario_id,
        physical_object_id,
        is_scenario_object,
        user.id,
    )

    return ScenarioPhysicalObject.from_dto(physical_object_dto)


@projects_router.delete(
    "/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_physical_object(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    physical_object_id: int = Path(..., description="physical object identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete scenario physical object by given id."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_physical_object(
        scenario_id, physical_object_id, is_scenario_object, user.id
    )
