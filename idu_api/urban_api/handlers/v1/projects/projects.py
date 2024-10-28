"""Projects endpoints are defined here."""

from fastapi import Depends, File, HTTPException, Path, Request, Security, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import Project, ProjectPatch, ProjectPost, ProjectPut, ProjectTerritory
from idu_api.urban_api.utils.auth_client import get_user
from idu_api.urban_api.utils.minio_client import AsyncMinioClient, get_minio_client


@projects_router.get(
    "/projects",
    response_model=list[Project],
    status_code=status.HTTP_200_OK,
)
async def get_all_projects(request: Request, user: UserDTO = Depends(get_user)) -> list[Project]:
    """Get all public projects and projects that are owned by the user."""
    user_project_service: UserProjectService = request.state.user_project_service

    projects = await user_project_service.get_all_available_projects(user.id if user is not None else None)

    return [Project.from_dto(project) for project in projects]


@projects_router.get(
    "/projects/previews",
    status_code=status.HTTP_200_OK,
)
async def get_all_preview_project_images(
    request: Request,
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> StreamingResponse:
    """Get preview images for all public and user's projects as a zip archive."""
    user_project_service: UserProjectService = request.state.user_project_service

    zip_buffer = await user_project_service.get_all_preview_projects_images(
        minio_client, user.id if user is not None else None
    )

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=project_previews.zip"},
    )


@projects_router.get(
    "/user_projects",
    response_model=list[Project],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_user_projects(request: Request, user: UserDTO = Depends(get_user)) -> list[Project]:
    """Get all user's projects."""
    user_project_service: UserProjectService = request.state.user_project_service

    projects = await user_project_service.get_user_projects(user.id)

    return [Project.from_dto(project) for project in projects]


@projects_router.get(
    "/user_projects/previews",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_user_preview_project_images(
    request: Request,
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> StreamingResponse:
    """Get preview images for all user's projects as a zip archive."""
    user_project_service: UserProjectService = request.state.user_project_service

    zip_buffer = await user_project_service.get_all_preview_projects_images(minio_client, user.id)

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=project_previews.zip"},
    )


@projects_router.get(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_project_by_id(
    request: Request,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
) -> Project:
    """Get a project by id."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.get_project_by_id(project_id, user.id)

    return Project.from_dto(project_dto)


@projects_router.get(
    "/projects/{project_id}/territory_info",
    response_model=ProjectTerritory,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_projects_territory_info(
    request: Request,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectTerritory:
    """Get territory info of a project by id."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_territory_dto = await user_project_service.get_project_territory_by_id(project_id, user.id)

    return ProjectTerritory.from_dto(project_territory_dto)


@projects_router.post(
    "/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def post_project(request: Request, project: ProjectPost, user: UserDTO = Depends(get_user)) -> Project:
    """Add a new project."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.add_project(project, user.id)

    return Project.from_dto(project_dto)


@projects_router.put(
    "/projects/{project_id}/image",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def upload_project_image(
    request: Request,
    project_id: int = Path(..., description="project identifier"),
    file: UploadFile = File(...),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> dict:
    """Upload project image to minio."""
    user_project_service: UserProjectService = request.state.user_project_service

    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Uploaded file is not an image.")

    return await user_project_service.upload_project_image(minio_client, project_id, user.id, await file.read())


@projects_router.get(
    "/projects/{project_id}/image",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_full_project_image(
    request: Request,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> StreamingResponse:
    """Get full image for given project."""
    user_project_service: UserProjectService = request.state.user_project_service

    image_stream = await user_project_service.get_full_project_image(minio_client, project_id, user.id)

    return StreamingResponse(image_stream, media_type="image/jpeg")


@projects_router.get(
    "/projects/{project_id}/preview",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_preview_project_image(
    request: Request,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> StreamingResponse:
    """Get preview image for given project."""
    user_project_service: UserProjectService = request.state.user_project_service

    image_stream = await user_project_service.get_preview_project_image(minio_client, project_id, user.id)

    return StreamingResponse(image_stream, media_type="image/png")


@projects_router.put(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_project(
    request: Request,
    project: ProjectPut,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
) -> Project:
    """Update a project by setting all of its attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.put_project(project, project_id, user.id)

    return Project.from_dto(project_dto)


@projects_router.patch(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_project(
    request: Request,
    project: ProjectPatch,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
) -> Project:
    """Update a project by setting given attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.patch_project(project, project_id, user.id)

    return Project.from_dto(project_dto)


@projects_router.delete(
    "/projects/{project_id}",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_project(
    request: Request,
    project_id: int = Path(..., description="project identifier"),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete a project."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_project(project_id, minio_client, user.id)
