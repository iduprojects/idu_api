"""Projects endpoints are defined here."""

from datetime import date

from fastapi import Depends, File, HTTPException, Path, Query, Request, Security, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from otteroad import KafkaProducerClient
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
from idu_api.urban_api.schemas.enums import OrderByField, Ordering, ProjectType
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.utils.auth_client import get_user
from idu_api.urban_api.utils.broker import get_kafka_producer
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

    project_dto = await user_project_service.get_project_by_id(project_id, user)

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

    project_territory_dto = await user_project_service.get_project_territory_by_id(project_id, user)

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

    scenarios = await user_project_service.get_scenarios_by_project_id(project_id, user)

    return [Scenario.from_dto(scenario) for scenario in scenarios]


@projects_router.get(
    "/projects",
    response_model=Page[Project],
    status_code=status.HTTP_200_OK,
)
async def get_projects(
    request: Request,
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    is_regional: bool = Query(False, description="to get regional projects"),
    project_type: ProjectType = Query(  # should be Optional, but swagger is generated wrongly then
        None,
        description="to get only certain project types, should be skipped to get all projects",
    ),
    territory_id: int | None = Query(None, description="to filter by region"),
    name: str | None = Query(None, description="to filter projects by name substring (case-insensitive)"),
    created_at: date | None = Query(None, description="to get projects created after created_at date"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
    user: UserDTO = Depends(get_user),
) -> Page[Project]:
    """
    ## Get a list of projects.

    **WARNING:** You cannot set both `project_type` and `is_regional = True` at the same time.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only the user's projects (default: false).
    - **is_regional** (bool, Query): If True, returns regional projects, else returns only common projects (default: false).
    - **project_type** (ProjectType | None, Query): If "city", returns cities projects, else if "common" returns only common projects (default: None).
      NOTE: Skip to get all projects (non-regional).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **name** (str | None, Query): Filters projects by a case-insensitive substring match.
    - **created_at** (date | None, Query): Returns projects created after the specified date.
    - **order_by** (OrderByField, Query): Defines the sorting attribute - project_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **Page[Project]**: A paginated list of projects.

    ### Errors:
    - **400 Bad Request**: If `project_type` is set and `is_regional` is set to True.
    - **401 Unauthorized**: If authentication is required to view user-specific projects.

    ### Constraints:
    - The user must be authenticated to retrieve their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if project_type is not None and is_regional:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either regional projects or certain project type",
        )

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own projects",
        )

    project_type_value = project_type.value if project_type is not None else None
    order_by_value = order_by.value if order_by is not None else None

    projects = await user_project_service.get_projects(
        user,
        only_own,
        is_regional,
        project_type_value,
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
    project_type: ProjectType = Query(  # should be Optional, but swagger is generated wrongly then
        None,
        description="to get only certain project types, should be skipped to get all projects",
    ),
    territory_id: int | None = Query(None, description="to filter by region"),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, Project]]:
    """
    ## Get project territories in GeoJSON format.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only territories for the user's projects (default: false).
    - **project_type** (ProjectType | None, Query): If "city", returns cities projects, else if "common" returns only common projects (default: None).
      NOTE: Skip to get all projects (non-regional).
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

    project_type_value = project_type.value if project_type is not None else None

    projects = await user_project_service.get_projects_territories(user, only_own, project_type_value, territory_id)

    return await GeoJSONResponse.from_list([p.to_geojson_dict() for p in projects], centers_only=centers_only)


@projects_router.get(
    "/projects_preview",
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def get_preview_project_images(
    request: Request,
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    project_type: ProjectType = Query(  # should be Optional, but swagger is generated wrongly then
        None,
        description="to get only certain project types, should be skipped to get all projects",
    ),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    name: str | None = Query(None, description="to filter projects by name substring (case-insensitive)"),
    created_at: date | None = Query(None, description="to get projects created after created_at date"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
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

    **WARNING 1:** You cannot set both `project_type` and `is_regional = True` at the same time.

    **WARNING 2:** This method has been deprecated since version 0.37.1 and will be removed in version 1.0.
    This is due to big sizes of image previews.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only images for the user's projects (default: false).
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **project_type** (ProjectType | None, Query): If "city", returns cities projects, else if "common" returns only common projects (default: None).
      NOTE: Skip to get all projects (non-regional).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **name** (str | None, Query): Filters projects by a case-insensitive substring match.
    - **created_at** (date | None, Query): Returns projects created after the specified date.
    - **order_by** (OrderByField, Query): Defines the sorting attribute - project_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **page** (int, Query): Specifies the page number for retrieving images (default: 1).
    - **page_size** (int, Query): Defines the number of project images per page (default: 10).

    ### Returns:
    - **StreamingResponse**: A ZIP archive containing preview images of projects.

    ### Errors:
    - **400 Bad Request**: If `project_type` is set and `is_regional` is set to True.
    - **401 Unauthorized**: If authentication is required to view user-specific project images.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be authenticated to retrieve images of their own projects.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if project_type is not None and is_regional:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either regional projects or certain project type.",
        )

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own projects",
        )

    project_type_value = project_type.value if project_type is not None else None
    order_by_value = order_by.value if order_by is not None else None

    zip_buffer = await user_project_service.get_preview_projects_images(
        minio_client,
        user,
        only_own,
        is_regional,
        project_type_value,
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
async def get_project_previews_url(
    request: Request,
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    is_regional: bool = Query(False, description="filter to get only regional projects or not"),
    project_type: ProjectType = Query(  # should be Optional, but swagger is generated wrongly then
        None,
        description="to get only certain project types, should be skipped to get all projects",
    ),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    name: str | None = Query(None, description="to filter projects by name substring (case-insensitive)"),
    created_at: date | None = Query(None, description="to get projects created after created_at date"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
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
    ## Get URLs for images of projects.

    **WARNING:** You cannot set both `project_type` and `is_regional = True` at the same time.

    ### Parameters:
    - **only_own** (bool, Query): If True, returns only images url for the user's projects (default: false).
    - **is_regional** (bool, Query): If True, filters results to include only regional projects (default: false).
    - **project_type** (ProjectType | None, Query): If "city", returns cities projects, else if "common" returns only common projects (default: None).
      NOTE: Skip to get all projects (non-regional).
    - **territory_id** (int | None, Query): Filters projects by a specific territory.
    - **name** (str | None, Query): Filters projects by a case-insensitive substring match.
    - **created_at** (date | None, Query): Returns projects created after the specified date.
    - **order_by** (OrderByField, Query): Defines the sorting attribute - project_id (default), created_at or updated_at.
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

    if project_type is not None and is_regional:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either regional projects or certain project type.",
        )

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own projects",
        )

    project_type_value = project_type.value if project_type is not None else None
    order_by_value = order_by.value if order_by is not None else None

    images = await user_project_service.get_preview_projects_images_url(
        minio_client,
        user,
        only_own,
        is_regional,
        project_type_value,
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
async def add_project(
    request: Request,
    project: ProjectPost,
    user: UserDTO = Depends(get_user),
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> Project:
    """
    ## Create a new project with its territory and base scenario.

    **NOTE:** After the project is created, a corresponding message will be sent to the Kafka broker.

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

    project_dto = await user_project_service.add_project(project, user, kafka_producer)

    return Project.from_dto(project_dto)


@projects_router.post(
    "/projects/{project_id}/base_scenario/{scenario_id}",
    response_model=Scenario,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def create_base_scenario(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    scenario_id: int = Path(..., description="regional scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> Scenario:
    """
    ## Create a new base scenario for given project from specified regional scenario.

    **NOTE:** After the base scenario is created, a corresponding message will be sent to the Kafka broker.

    **WARNING:** This is an auxiliary method for third-party APIs. Only a superuser can use it.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **scenario_id** (int, Path): Unique identifier of the regional scenario.

    ### Returns:
    - **Scenario**: The created base scenario.

    ### Errors:
    - **400 Bad Request**: If the user has provided identifier of a non-regional scenario.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the related entity does not exist.

    ### Constraints:
    - The user must be authorized to create a new base scenario.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if not user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must be a superuser to create a new base scenario.",
        )

    try:
        scenario = await user_project_service.create_base_scenario(project_id, scenario_id, kafka_producer)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return Scenario.from_dto(scenario)


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

    project_dto = await user_project_service.put_project(project, project_id, user)

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

    project_dto = await user_project_service.patch_project(project, project_id, user)

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

    await user_project_service.delete_project(project_id, minio_client, user)

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
) -> MinioImageURL:
    """
    ## Upload an image for a project to MinIO file server.

    **NOTE:** This method also creates preview image (resizing main image to max 1600px on the larger side)
    and uploads it to the MinIO file server.

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

    image_url = await user_project_service.upload_project_image(minio_client, project_id, user, await file.read())

    return MinioImagesURL(**image_url)


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
    ## Get the original image of a project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **StreamingResponse**: The original full-sized image of the project in JPEG format (bytes).

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    image_stream = await user_project_service.get_project_image(minio_client, project_id, user, image_type="origin")

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
    - **StreamingResponse**: A preview version of the project image in JPEG format (bytes).

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    image_stream = await user_project_service.get_project_image(minio_client, project_id, user, image_type="preview")

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
    ## Get the URL for the original image of a project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **str**: A URL pointing to the original project image.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.get_project_image_url(minio_client, project_id, user, image_type="origin")


@projects_router.get(
    "/projects/{project_id}/preview_url",
    response_model=str,
    status_code=status.HTTP_200_OK,
)
async def get_preview_project_image_url(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    user: UserDTO = Depends(get_user),
    minio_client: AsyncMinioClient = Depends(get_minio_client),
) -> str:
    """
    ## Get the URL for the preview image of a project.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.

    ### Returns:
    - **str**: A URL pointing to the preview project image.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.
    - **503 Service Unavailable**: If it was not possible to connect to the MinIO file server.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.get_project_image_url(minio_client, project_id, user, image_type="preview")


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

    projects = await user_project_service.get_user_projects(user, is_regional, territory_id)

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
        minio_client, user, is_regional, territory_id, page, page_size
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
    Instead, use method **GET /projects_images_url** with parameter `only_own = True`.

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
        minio_client, user, is_regional, territory_id, page, page_size
    )

    return [MinioImageURL(**img) for img in images]
