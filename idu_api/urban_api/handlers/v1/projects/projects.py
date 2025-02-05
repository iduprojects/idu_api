"""Projects endpoints are defined here."""

from datetime import date

from fastapi import Depends, File, HTTPException, Path, Query, Request, Security, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    MinioImagesURL,
    MinioImageURL,
    OkResponse,
    Page,
    Project,
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ProjectTerritory,
    Scenario,
)
from idu_api.urban_api.schemas.enums import Ordering
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.territories import TerritoriesOrderByField
from idu_api.urban_api.utils.auth_client import get_user
from idu_api.urban_api.utils.minio_client import AsyncMinioClient, get_minio_client
from idu_api.urban_api.utils.pagination import paginate


@projects_router.get(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def get_project_by_id(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Project:
    """
    ## Get the project by given identifier.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **Project**: A project model with related base scenario and region short information.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.get_project_by_id(project_id, user.id if user is not None else None)

    return Project.from_dto(project_dto)


@projects_router.get(
    "/projects/{project_id}/territory",
    response_model=ProjectTerritory,
    status_code=status.HTTP_200_OK,
)
async def get_project_territory_by_project_id(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ProjectTerritory:
    """
    ## Get the territory of a given project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **ProjectTerritory**: A project territory model containing its geometry and properties.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    project_territory_dto = await user_project_service.get_project_territory_by_id(
        project_id, user.id if user is not None else None
    )

    return ProjectTerritory.from_dto(project_territory_dto)


@projects_router.get(
    "/projects/{project_id}/scenarios",
    response_model=list[Scenario],
    status_code=status.HTTP_200_OK,
)
async def get_scenarios_by_project_id(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[Scenario]:
    """
    ## Get a list of scenarios for a given project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **list[Scenario]**: A list of scenarios with related project short information.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenarios = await user_project_service.get_scenarios_by_project_id(
        project_id, user.id if user is not None else None
    )

    return [Scenario.from_dto(scenario) for scenario in scenarios]


@projects_router.get(
    "/projects",
    response_model=Page[Project],
    status_code=status.HTTP_200_OK,
)
async def get_projects(
    request: Request,
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    territory_id: int | None = Query(None, description="to filter by territory"),
    name: str | None = Query(None, description="to filter projects by name substring (case-insensitive)"),
    created_at: date | None = Query(None, description="to get projects created after created_at date"),
    order_by: TerritoriesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
    user: UserDTO = Depends(get_user),
) -> Page[Project]:
    """
    ## Get a list of projects.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only images for the user's projects (default: false).
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **name** (str | None, Query): Filters projects by a case-insensitive substring match.
    - **created_at** (date | None, Query): Returns projects created after the specified date.
    - **order_by** (TerritoriesOrderByField, Query): Defines the sorting attribute - project_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **Page[Project]**: A paginated list of projects.

    ### Errors:
    - **401 Unauthorized**: If authentication is required to view user-specific projects.

    ### Constraints:
    - The user must be authenticated to retrieve their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own projects",
        )

    order_by_value = order_by.value if order_by is not None else None

    projects = await user_project_service.get_projects(
        user.id if user is not None else None,
        only_own,
        is_regional,
        territory_id,
        name,
        created_at,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        projects.items,
        projects.total,
        transformer=lambda x: [Project.from_dto(item) for item in x],
    )


@projects_router.get(
    "/projects_territories",
    response_model=GeoJSONResponse[Feature[Geometry, Project]],
    status_code=status.HTTP_200_OK,
)
async def get_projects_territories(
    request: Request,
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    territory_id: int | None = Query(None, description="to filter by territory"),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, Project]]:
    """
    ## Get project territories in GeoJSON format.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only territories for the user's projects (default: false).
    - **territory_id** (int | None, Query): Filters results by a specific territory.
    - **centers_only** (bool, Query): If True, retrieves only center points of project territories (default: true).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, Project]]**: A GeoJSON-formatted response containing project territories.

    ### Errors:
    - **401 Unauthorized**: If authentication is required to view user-specific projects.

    ### Constraints:
    - The user must be authenticated to retrieve their own project territories.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own projects",
        )

    projects = await user_project_service.get_projects_territories(
        user.id if user is not None else None, only_own, territory_id
    )

    return await GeoJSONResponse.from_list([p.to_geojson_dict() for p in projects], centers_only=centers_only)


@projects_router.get(
    "/projects_preview",
    status_code=status.HTTP_200_OK,
)
async def get_preview_project_images(
    request: Request,
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    name: str | None = Query(None, description="to filter projects by name substring (case-insensitive)"),
    created_at: date | None = Query(None, description="to get projects created after created_at date"),
    order_by: TerritoriesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
    page: int = Query(1, gt=0, description="to get images for projects from the current page"),
    page_size: int = Query(10, gt=0, description="the number of projects images"),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> StreamingResponse:
    """
    ## Get preview images for projects as a ZIP archive.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only images for the user's projects (default: false).
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **name** (str | None, Query): Filters projects by a case-insensitive substring match.
    - **created_at** (date | None, Query): Returns projects created after the specified date.
    - **order_by** (TerritoriesOrderByField, Query): Defines the sorting attribute - project_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **StreamingResponse**: A ZIP archive containing preview images of projects.

    ### Errors:
    - **401 Unauthorized**: If authentication is required to view user-specific project images.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be authenticated to retrieve images of their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own projects",
        )

    order_by_value = order_by.value if order_by is not None else None

    zip_buffer = await user_project_service.get_preview_projects_images(
        minio_client,
        user.id if user is not None else None,
        only_own,
        is_regional,
        territory_id,
        name,
        created_at,
        order_by_value,
        ordering.value,
        page,
        page_size,
    )

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=project_previews.zip"},
    )


@projects_router.get(
    "/projects_preview_url",
    response_model=list[MinioImageURL],
    status_code=status.HTTP_200_OK,
)
async def get_preview_project_images_url(
    request: Request,
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    name: str | None = Query(None, description="to filter projects by name substring (case-insensitive)"),
    created_at: date | None = Query(None, description="to get projects created after created_at date"),
    order_by: TerritoriesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
    page: int = Query(1, gt=0, description="to get images for projects from the current page"),
    page_size: int = Query(10, gt=0, description="the number of projects images"),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> list[MinioImageURL]:
    """
    ## Get URLs for preview images of projects.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only images for the user's projects (default: false).
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **name** (str | None, Query): Filters projects by a case-insensitive substring match.
    - **created_at** (date | None, Query): Returns projects created after the specified date.
    - **order_by** (TerritoriesOrderByField, Query): Defines the sorting attribute - project_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **list[MinioImageURL]**: A list of URLs for preview images of projects.

    ### Errors:
    - **401 Unauthorized**: If authentication is required to view user-specific project images.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be authenticated to retrieve image URLs of their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own projects",
        )

    order_by_value = order_by.value if order_by is not None else None

    images = await user_project_service.get_preview_projects_images_url(
        minio_client,
        user.id if user is not None else None,
        only_own,
        is_regional,
        territory_id,
        name,
        created_at,
        order_by_value,
        ordering.value,
        page,
        page_size,
    )

    return [MinioImageURL(**img) for img in images]


@projects_router.post(
    "/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def add_project(request: Request, project: ProjectPost, user: UserDTO = Depends(get_user)) -> Project:
    """
    ## Create a new project with its territory and base scenario.

    ### Parameters:
    - **project** (ProjectPost, Body): The project data including geometry.

    ### Returns:
    - **Project**: The created project with related base scenario and region short information.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the related entity does not exist.

    ### Constraints:
    - The user must be authorized to create a new project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.add_project(project, user.id)

    return Project.from_dto(project_dto)


@projects_router.put(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def put_project(
    request: Request,
    project: ProjectPut,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Project:
    """
    ## Update all attributes of the given project.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use PATCH method.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **project** (ProjectPut, Body): The updated project data.

    ### Returns:
    - **Project**: The updated project with related base scenario and region short information.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
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
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Project:
    """
    ## Update specific fields of the given project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **project** (ProjectPatch, Body): The partial project data to update.

    ### Returns:
    - **Project**: The updated project with related base scenario and region short information.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
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
    project_id: int = Path(..., description="project identifier", gt=0),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a project by its identifier. It also deletes all related objects from db and image from minio.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_project(project_id, minio_client, user.id)

    return OkResponse()


@projects_router.put(
    "/projects/{project_id}/image",
    response_model=MinioImagesURL,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def upload_project_image(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    file: UploadFile = File(...),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> MinioImagesURL:
    """
    ## Upload an image for a project to MinIO file server.

    **NOTE:** This method also creates preview image (300x300) and uploads it to the MinIO file server.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **file** (UploadFile, File): Image file to be uploaded (JPEG/PNG).

    ### Returns:
    - **MinioImagesURL**: A response containing the URL of the uploaded images (original and preview).

    ### Errors:
    - **400 Bad Request**: If the uploaded file is not an image or image is invalid.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file is not an image")

    images_url = await user_project_service.upload_project_image(minio_client, project_id, user.id, await file.read())

    return MinioImagesURL(**images_url)


@projects_router.get(
    "/projects/{project_id}/image",
    status_code=status.HTTP_200_OK,
)
async def get_full_project_image(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> StreamingResponse:
    """
    ## Get the full image of a project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **StreamingResponse**: The full-sized image of the project in JPEG format (bytes).

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    image_stream = await user_project_service.get_full_project_image(
        minio_client, project_id, user.id if user is not None else None
    )

    return StreamingResponse(image_stream, media_type="image/jpeg")


@projects_router.get(
    "/projects/{project_id}/preview",
    status_code=status.HTTP_200_OK,
)
async def get_preview_project_image(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> StreamingResponse:
    """
    ## Get a preview image of a project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **StreamingResponse**: A preview version of the project image in PNG format (bytes).

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    image_stream = await user_project_service.get_preview_project_image(
        minio_client, project_id, user.id if user is not None else None
    )

    return StreamingResponse(image_stream, media_type="image/png")


@projects_router.get(
    "/projects/{project_id}/image_url",
    response_model=str,
    status_code=status.HTTP_200_OK,
)
async def get_full_project_image_url(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> str:
    """
    ## Get the URL for the full image of a project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **str**: A URL pointing to the full project image.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.get_full_project_image_url(
        minio_client, project_id, user.id if user is not None else None
    )


@projects_router.get(
    "/user_projects",
    response_model=Page[Project],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def get_user_projects(
    request: Request,
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Page[Project]:
    """
    ## Get a list of user's projects.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use method **GET /projects** with parameter `only_own = True`.

    ### Parameters:
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **Page[Project]**: A paginated list of projects.

    ### Constraints:
    - The user must be authenticated to retrieve their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    projects = await user_project_service.get_user_projects(user.id, is_regional, territory_id)

    return paginate(
        projects.items,
        projects.total,
        transformer=lambda x: [Project.from_dto(item) for item in x],
    )


@projects_router.get(
    "/user_projects_preview",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def get_user_preview_project_images(
    request: Request,
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    page: int = Query(1, gt=0, description="to get images for projects from the current page"),
    page_size: int = Query(10, gt=0, description="the number of projects images"),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> StreamingResponse:
    """
    ## Get preview images for user's projects as a ZIP archive.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use method **GET /projects_preview** with parameter `only_own = True`.

    ### Parameters:
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **StreamingResponse**: A ZIP archive containing preview images of projects.

    ### Errors:
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be authenticated to retrieve images of their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    zip_buffer = await user_project_service.get_user_preview_projects_images(
        minio_client, user.id, is_regional, territory_id, page, page_size
    )

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=project_previews.zip"},
    )


@projects_router.get(
    "/user_projects_preview_url",
    response_model=list[MinioImageURL],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def get_user_preview_project_images_url(
    request: Request,
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    page: int = Query(1, gt=0, description="to get images for projects from the current page"),
    page_size: int = Query(10, gt=0, description="the number of projects images"),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
    user: UserDTO = Depends(get_user),
) -> list[MinioImageURL]:
    """
    ## Get URLs for preview images of user's projects.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use method **GET /projects_preview_url** with parameter `only_own = True`.

    ### Parameters:
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **list[MinioImageURL]**: A list of URLs for preview images of projects.

    ### Errors:
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be authenticated to retrieve image URLs of their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    images = await user_project_service.get_user_preview_projects_images_url(
        minio_client, user.id, is_regional, territory_id, page, page_size
    )

    return [MinioImageURL(**img) for img in images]
