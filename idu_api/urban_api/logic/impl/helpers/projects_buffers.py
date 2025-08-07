"""Projects buffers internal logic is defined here."""

from geoalchemy2.functions import ST_AsEWKB, ST_Intersection, ST_Intersects
from sqlalchemy import delete, union_all
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.elements import literal, or_
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import coalesce

from idu_api.common.db.entities import (
    buffer_types_dict,
    buffers_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    projects_buffers_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import ScenarioBufferDTO, UserDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntityAlreadyEdited,
    EntityNotFoundById,
    EntityNotFoundByParams,
)
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario
from idu_api.urban_api.logic.impl.helpers.utils import (
    check_existence,
    extract_values_from_model,
    get_context_territories_geometry,
)
from idu_api.urban_api.schemas import ScenarioBufferDelete, ScenarioBufferPut
from idu_api.urban_api.utils.query_filters import EqFilter, apply_filters


async def get_buffers_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    buffer_type_id: int | None,
    physical_object_type_id: int | None,
    service_type_id: int | None,
    user: UserDTO | None,
) -> list[ScenarioBufferDTO]:
    """Get list of buffer objects by scenario identifier."""

    scenario = await check_scenario(conn, scenario_id, user, allow_regional=False, return_value=True)

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == scenario.project_id)
    ).scalar_subquery()

    # Step 1: Get all the public_urbi_object_id for a given scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    # Step 2: Collect all records from public.buffers_data without collected public_urban_object_id
    public_buffers_query = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            urban_objects_data.c.urban_object_id,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(ST_Intersection(buffers_data.c.geometry, project_geometry)).label("geometry"),
            buffers_data.c.is_custom,
            literal(False).label("is_scenario_object"),
            (~ST_Intersects(object_geometries_data.c.geometry, project_geometry)).label("is_locked"),
        )
        .select_from(
            buffers_data.join(buffer_types_dict, buffer_types_dict.c.buffer_type_id == buffers_data.c.buffer_type_id)
            .join(urban_objects_data, urban_objects_data.c.urban_object_id == buffers_data.c.urban_object_id)
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Intersects(buffers_data.c.geometry, project_geometry),
        )
    )

    # Step 3: Collect locked buffers from parent regional scenario
    locked_regional_scenario_buffers_query = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(ST_Intersection(projects_buffers_data.c.geometry, project_geometry)).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(True).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario.parent_id,
            ST_Intersects(projects_buffers_data.c.geometry, project_geometry),
            ~ST_Intersects(object_geometries_data.c.geometry, project_geometry),
            ~ST_Intersects(projects_object_geometries_data.c.geometry, project_geometry),
        )
    )

    # Step 4: Collect buffers from given scenario
    scenario_buffers_query = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(projects_buffers_data.c.geometry).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(False).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
    )

    union_query = union_all(
        public_buffers_query,
        locked_regional_scenario_buffers_query,
        scenario_buffers_query,
    ).cte(name="union_query")
    statement = select(union_query)

    # Apply optional filters
    statement = apply_filters(
        statement,
        EqFilter(union_query, "buffer_type_id", buffer_type_id),
        EqFilter(union_query, "physical_object_type_id", physical_object_type_id),
        EqFilter(union_query, "service_type_id", service_type_id),
    )

    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioBufferDTO(**row) for row in result]


async def get_context_buffers_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    buffer_type_id: int | None,
    physical_object_type_id: int | None,
    service_type_id: int | None,
    user: UserDTO | None,
) -> list[ScenarioBufferDTO]:
    """Get list of buffer objects for `context` of project territory."""

    parent_id, context_geom, context_ids = await get_context_territories_geometry(conn, scenario_id, user)

    # Step 1: Get all the public_urban_object_id for a given scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == parent_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    # Step 2: Find all intersecting object geometries from public (except object from previous step)
    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            object_geometries_data.c.territory_id.in_(context_ids)
            | ST_Intersects(object_geometries_data.c.geometry, context_geom),
        )
        .cte(name="objects_intersecting")
    )

    # Step 3: Collect all buffers from `public` intersecting context geometry
    public_buffers_query = select(
        buffer_types_dict.c.buffer_type_id,
        buffer_types_dict.c.name.label("buffer_type_name"),
        urban_objects_data.c.urban_object_id,
        physical_objects_data.c.physical_object_id,
        physical_objects_data.c.name.label("physical_object_name"),
        physical_object_types_dict.c.physical_object_type_id,
        physical_object_types_dict.c.name.label("physical_object_type_name"),
        object_geometries_data.c.object_geometry_id,
        territories_data.c.territory_id,
        territories_data.c.name.label("territory_name"),
        services_data.c.service_id,
        services_data.c.name.label("service_name"),
        service_types_dict.c.service_type_id,
        service_types_dict.c.name.label("service_type_name"),
        ST_AsEWKB(ST_Intersection(buffers_data.c.geometry, context_geom)).label("geometry"),
        buffers_data.c.is_custom,
        literal(False).label("is_scenario_object"),
        literal(True).label("is_locked"),
    ).select_from(
        buffers_data.join(buffer_types_dict, buffer_types_dict.c.buffer_type_id == buffers_data.c.buffer_type_id)
        .join(urban_objects_data, urban_objects_data.c.urban_object_id == buffers_data.c.urban_object_id)
        .join(
            physical_objects_data,
            physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
        )
        .join(
            physical_object_types_dict,
            physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
        )
        .join(
            object_geometries_data,
            object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
        )
        .join(
            objects_intersecting,
            objects_intersecting.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
        )
        .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
        .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
        .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
    )

    # Step 4: Collect all buffers from regional scenario intersecting context geometry
    regional_scenario_buffers_query = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(ST_Intersection(projects_buffers_data.c.geometry, context_geom)).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(True).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == parent_id,
            ST_Intersects(projects_buffers_data.c.geometry, context_geom),
        )
    )

    union_query = union_all(
        public_buffers_query,
        regional_scenario_buffers_query,
    ).cte(name="union_query")
    statement = select(union_query)

    # Apply optional filters
    statement = apply_filters(
        statement,
        EqFilter(union_query, "buffer_type_id", buffer_type_id),
        EqFilter(union_query, "physical_object_type_id", physical_object_type_id),
        EqFilter(union_query, "service_type_id", service_type_id),
    )

    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioBufferDTO(**row) for row in result]


async def get_buffer_from_db(conn: AsyncConnection, buffer_type_id: int, urban_object_id: int) -> ScenarioBufferDTO:
    """Get buffer object by identifier."""

    statement = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(projects_buffers_data.c.geometry).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(False).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(
            projects_buffers_data.c.buffer_type_id == buffer_type_id,
            projects_buffers_data.c.urban_object_id == urban_object_id,
        )
    )

    result = (await conn.execute(statement)).mappings().one()

    return ScenarioBufferDTO(**result)


async def put_buffer_to_db(
    conn: AsyncConnection,
    buffer: ScenarioBufferPut,
    scenario_id: int,
    user: UserDTO,
) -> ScenarioBufferDTO:
    """Create or update a new buffer object."""

    await check_scenario(conn, scenario_id, user, to_edit=True, allow_regional=False)

    is_from_public = False
    physical_object_column = (
        projects_urban_objects_data.c.physical_object_id
        if buffer.is_scenario_physical_object
        else projects_urban_objects_data.c.public_physical_object_id
    )
    geometry_column = (
        projects_urban_objects_data.c.object_geometry_id
        if buffer.is_scenario_geometry
        else projects_urban_objects_data.c.public_object_geometry_id
    )
    service_column = (
        projects_urban_objects_data.c.service_id
        if buffer.is_scenario_service
        else projects_urban_objects_data.c.public_service_id
    )
    statement = (
        select(projects_urban_objects_data.c.urban_object_id)
        .where(
            physical_object_column == buffer.physical_object_id,
            geometry_column == buffer.object_geometry_id,
            service_column == buffer.service_id if buffer.service_id is not None else service_column.is_(None),
            projects_urban_objects_data.c.scenario_id == scenario_id,
        )
        .limit(1)
    )
    urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    if urban_object_id is None:
        is_from_public = True
        scenario_params = [
            buffer.is_scenario_physical_object,
            buffer.is_scenario_geometry,
            buffer.is_scenario_service if buffer.service_id is not None else False,
        ]
        if not any(scenario_params):
            statement = (
                select(urban_objects_data.c.urban_object_id)
                .where(
                    urban_objects_data.c.physical_object_id == buffer.physical_object_id,
                    urban_objects_data.c.object_geometry_id == buffer.object_geometry_id,
                    (
                        urban_objects_data.c.service_id == buffer.service_id
                        if buffer.service_id is not None
                        else urban_objects_data.c.service_id.is_(None)
                    ),
                )
                .limit(1)
            )
            urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
        if not urban_object_id:
            raise EntityNotFoundByParams(
                "urban object",
                buffer.physical_object_id,
                buffer.object_geometry_id,
                buffer.service_id,
                scenario_id,
            )

    if is_from_public and await check_existence(
        conn,
        projects_urban_objects_data,
        conditions={"public_urban_object_id": urban_object_id, "scenario_id": scenario_id},
    ):
        raise EntityAlreadyEdited("buffer", scenario_id)

    if not await check_existence(conn, buffer_types_dict, conditions={"buffer_type_id": buffer.buffer_type_id}):
        raise EntityNotFoundById(buffer.buffer_type_id, "buffer type")

    values = extract_values_from_model(buffer, exclude_unset=True, allow_null_geometry=True)

    if is_from_public:
        statement = insert(projects_urban_objects_data).values(
            scenario_id=scenario_id,
            public_urban_object_id=urban_object_id,
        )
        await conn.execute(statement)

        statement = (
            insert(projects_physical_objects_data)
            .from_select(
                [
                    "public_physical_object_id",
                    "physical_object_type_id",
                    "name",
                    "properties",
                    "created_at",
                    "updated_at",
                ],
                select(physical_objects_data).where(
                    physical_objects_data.c.physical_object_id == buffer.physical_object_id
                ),
            )
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        new_physical_object_id = (await conn.execute(statement)).scalar_one()

        statement = (
            insert(projects_object_geometries_data)
            .from_select(
                [
                    "public_object_geometry_id",
                    "territory_id",
                    "geometry",
                    "centre_point",
                    "address",
                    "osm_id",
                    "created_at",
                    "updated_at",
                ],
                select(object_geometries_data).where(
                    object_geometries_data.c.object_geometry_id == buffer.object_geometry_id
                ),
            )
            .returning(projects_object_geometries_data.c.object_geometry_id)
        )
        new_object_geometry_id = (await conn.execute(statement)).scalar_one()

        new_service_id = None
        if buffer.service_id is not None:
            statement = (
                insert(projects_services_data)
                .from_select(
                    [
                        "public_service_id",
                        "service_type_id",
                        "territory_type_id",
                        "name",
                        "capacity",
                        "is_capacity_real",
                        "properties",
                        "created_at",
                        "updated_at",
                    ],
                    select(services_data).where(services_data.c.service_id == buffer.service_id),
                )
                .returning(projects_services_data.c.service_id)
            )
            new_service_id = (await conn.execute(statement)).scalar_one()

        statement = (
            insert(projects_urban_objects_data)
            .values(
                scenario_id=scenario_id,
                physical_object_id=new_physical_object_id,
                object_geometry_id=new_object_geometry_id,
                service_id=new_service_id,
            )
            .returning(projects_urban_objects_data.c.urban_object_id)
        )
        urban_object_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(projects_buffers_data)
        .values(
            buffer_type_id=values["buffer_type_id"],
            urban_object_id=urban_object_id,
            geometry=values["geometry"],
            is_custom=buffer.geometry is not None,
        )
        .on_conflict_do_update(
            index_elements=["urban_object_id", "buffer_type_id"],
            set_={
                "geometry": values["geometry"],
                "is_custom": buffer.geometry is not None,
            },
        )
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_buffer_from_db(conn, buffer.buffer_type_id, urban_object_id)


async def delete_buffer_from_db(
    conn: AsyncConnection, buffer: ScenarioBufferDelete, scenario_id: int, user: UserDTO
) -> dict:
    """Delete buffer object."""

    await check_scenario(conn, scenario_id, user, to_edit=True, allow_regional=False)

    is_from_public = False
    physical_object_column = (
        projects_urban_objects_data.c.physical_object_id
        if buffer.is_scenario_physical_object
        else projects_urban_objects_data.c.public_physical_object_id
    )
    geometry_column = (
        projects_urban_objects_data.c.object_geometry_id
        if buffer.is_scenario_geometry
        else projects_urban_objects_data.c.public_object_geometry_id
    )
    service_column = (
        projects_urban_objects_data.c.service_id
        if buffer.is_scenario_service
        else projects_urban_objects_data.c.public_service_id
    )
    statement = (
        select(projects_urban_objects_data.c.urban_object_id)
        .where(
            physical_object_column == buffer.physical_object_id,
            geometry_column == buffer.object_geometry_id,
            service_column == buffer.service_id if buffer.service_id is not None else service_column.is_(None),
            projects_urban_objects_data.c.scenario_id == scenario_id,
        )
        .limit(1)
    )
    urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    if urban_object_id is None:
        is_from_public = True
        scenario_params = [
            buffer.is_scenario_physical_object,
            buffer.is_scenario_geometry,
            buffer.is_scenario_service if buffer.service_id is not None else False,
        ]
        if not any(scenario_params):
            statement = (
                select(urban_objects_data.c.urban_object_id)
                .where(
                    urban_objects_data.c.physical_object_id == buffer.physical_object_id,
                    urban_objects_data.c.object_geometry_id == buffer.object_geometry_id,
                    (
                        urban_objects_data.c.service_id == buffer.service_id
                        if buffer.service_id is not None
                        else urban_objects_data.c.service_id.is_(None)
                    ),
                )
                .limit(1)
            )
            urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
        if not urban_object_id:
            raise EntityNotFoundByParams(
                "urban object",
                buffer.physical_object_id,
                buffer.object_geometry_id,
                buffer.service_id,
                scenario_id,
            )

    if is_from_public and await check_existence(
        conn,
        projects_urban_objects_data,
        conditions={"public_urban_object_id": urban_object_id, "scenario_id": scenario_id},
    ):
        raise EntityAlreadyEdited("buffer", scenario_id)

    if not await check_existence(conn, buffer_types_dict, conditions={"buffer_type_id": buffer.buffer_type_id}):
        raise EntityNotFoundById(buffer.buffer_type_id, "buffer type")

    if not await check_existence(
        conn,
        buffers_data if is_from_public else projects_buffers_data,
        conditions={"buffer_type_id": buffer.buffer_type_id, "urban_object_id": urban_object_id},
    ):
        raise EntityNotFoundByParams("buffer", buffer.buffer_type_id, urban_object_id)

    if is_from_public:
        statement = insert(projects_urban_objects_data).values(
            scenario_id=scenario_id,
            public_urban_object_id=urban_object_id,
        )
        await conn.execute(statement)

        statement = insert(projects_urban_objects_data).values(
            scenario_id=scenario_id,
            public_service_id=buffer.service_id,
            public_physical_object_id=buffer.physical_object_id,
            public_object_geometry_id=buffer.object_geometry_id,
        )
        await conn.execute(statement)

    else:
        statement = delete(projects_buffers_data).where(
            projects_buffers_data.c.buffer_type_id == buffer.buffer_type_id,
            projects_buffers_data.c.urban_object_id == urban_object_id,
        )

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
