"""Physical objects projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import ScenarioPhysicalObject
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/physical_objects",
    response_model=list[ScenarioPhysicalObject],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_physical_objects_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="project identifier"),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type"),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function"),
    for_context: bool = Query(False, description="to get objects for context of territory"),
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
        for_context,
    )

    return [ScenarioPhysicalObject.from_dto(phys_obj) for phys_obj in physical_objects]
