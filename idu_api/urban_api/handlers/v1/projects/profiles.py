"""
Profiles endpoints are defined here.
"""

from fastapi import Request
from starlette import status

from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import TargetProfilesData, TargetProfilesPost


@projects_router.get(
    "/target_profiles",
    response_model=list[TargetProfilesData],
    status_code=status.HTTP_200_OK,
)
async def get_target_profiles(request: Request) -> list[TargetProfilesData]:
    """Get a list of target profiles."""
    user_project_service: UserProjectService = request.state.user_project_service

    target_profiles = await user_project_service.get_target_profiles()

    return [TargetProfilesData.from_dto(target_profile) for target_profile in target_profiles]


@projects_router.post(
    "/target_profiles",
    response_model=TargetProfilesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_target_profile(request: Request, target_profile: TargetProfilesPost) -> TargetProfilesData:
    """Add a new target profile."""
    user_project_service: UserProjectService = request.state.user_project_service

    new_target_profile = await user_project_service.add_target_profile(target_profile)

    return TargetProfilesData.from_dto(new_target_profile)
