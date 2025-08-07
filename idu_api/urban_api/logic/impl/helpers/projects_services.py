"""Projects services internal logic is defined here."""

from collections import defaultdict

from geoalchemy2.functions import ST_AsEWKB, ST_Centroid, ST_Intersection, ST_Intersects, ST_IsEmpty, ST_Within
from sqlalchemy import delete, insert, literal, or_, select, union_all, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.functions import coalesce

from idu_api.common.db.entities import (
    object_geometries_data,
    projects_object_geometries_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import (
    ScenarioServiceDTO,
    ScenarioServiceWithGeometryDTO,
    ScenarioUrbanObjectDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntityAlreadyEdited,
    EntityNotFoundById,
    EntityNotFoundByParams,
)
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario
from idu_api.urban_api.logic.impl.helpers.projects_urban_objects import get_scenario_urban_object_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import (
    check_existence,
    extract_values_from_model,
    get_context_territories_geometry,
    include_child_territories_cte,
)
from idu_api.urban_api.schemas import ScenarioServicePost, ServicePatch, ServicePut
from idu_api.urban_api.utils.query_filters import EqFilter, RecursiveFilter, apply_filters


async def get_services_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioServiceDTO]:
    """Get services by scenario identifier."""

    scenario = await check_scenario(conn, scenario_id, user, return_value=True)

    project_geometry = None
    territories_cte = None
    if not scenario.is_regional:
        project_geometry = (
            select(projects_territory_data.c.geometry).where(
                projects_territory_data.c.project_id == scenario.project_id
            )
        ).scalar_subquery()
    else:
        territories_cte = include_child_territories_cte(scenario.territory_id)

    # Step 1: Get all the public_urban_object_id for a given scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    # Step 2: Collect all services from `public.urban_objects_data`
    public_services_query = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties,
            services_data.c.created_at,
            services_data.c.updated_at,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_object"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            (
                ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery())
                if not scenario.is_regional
                else True
            ),
            (
                object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                if scenario.is_regional
                else True
            ),
        )
    )

    # Step 2: Collect all services from `user_projects.urban_objects_data`
    scenario_services_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_object"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
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
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
            .outerjoin(
                territory_types_dict,
                or_(
                    territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
                    territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
                ),
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
        )
    )

    union_query = union_all(public_services_query, scenario_services_query).cte(name="union_query")
    statement = select(union_query)

    # Apply optional filters
    statement = apply_filters(
        statement,
        EqFilter(union_query, "service_type_id", service_type_id),
        RecursiveFilter(union_query, "urban_function_id", urban_function_id, urban_functions_dict),
    )

    result = (await conn.execute(statement)).mappings().all()

    grouped_objects = defaultdict(lambda: {"territories": []})
    for obj in result:
        service_id = obj["service_id"]
        is_scenario_service = obj["is_scenario_object"]
        key = service_id if not is_scenario_service else f"scenario_{service_id}"

        if key not in grouped_objects:
            grouped_objects[key].update({k: v for k, v in obj.items() if k in ScenarioServiceDTO.fields()})

        territory = {"territory_id": obj["territory_id"], "name": obj["territory_name"]}
        grouped_objects[key]["territories"].append(territory)

    return [ScenarioServiceDTO(**row) for row in grouped_objects.values()]


async def get_services_with_geometry_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioServiceWithGeometryDTO]:
    """Get services with geometry by scenario identifier."""

    scenario = await check_scenario(conn, scenario_id, user, return_value=True)

    project_geometry = None
    territories_cte = None
    if not scenario.is_regional:
        project_geometry = (
            select(projects_territory_data.c.geometry).where(
                projects_territory_data.c.project_id == scenario.project_id
            )
        ).scalar_subquery()
    else:
        territories_cte = include_child_territories_cte(scenario.territory_id)

    # Step 1: Get all the public_urban_object_id for a given scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    # Step 2: Collect all physical objects from `public.urban_objects_data`
    public_services_query = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties,
            services_data.c.created_at,
            services_data.c.updated_at,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_service"),
            literal(False).label("is_scenario_geometry"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            (
                ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery())
                if not scenario.is_regional
                else True
            ),
            (
                object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                if scenario.is_regional
                else True
            ),
        )
    )

    # Step 3: Collect all physical objects from `user_projects.urban_objects_data`
    scenario_services_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id,
                object_geometries_data.c.object_geometry_id,
            ).label("object_geometry_id"),
            coalesce(
                projects_object_geometries_data.c.address,
                object_geometries_data.c.address,
            ).label("address"),
            coalesce(
                projects_object_geometries_data.c.osm_id,
                object_geometries_data.c.osm_id,
            ).label("osm_id"),
            ST_AsEWKB(
                coalesce(
                    projects_object_geometries_data.c.geometry,
                    object_geometries_data.c.geometry,
                ),
            ).label("geometry"),
            ST_AsEWKB(
                coalesce(
                    projects_object_geometries_data.c.centre_point,
                    object_geometries_data.c.centre_point,
                ),
            ).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_service"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
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
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
            .outerjoin(
                territory_types_dict,
                or_(
                    territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
                    territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
                ),
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
        )
    )

    union_query = union_all(public_services_query, scenario_services_query).cte(name="union_query")
    statement = select(union_query)

    # Apply optional filters
    statement = apply_filters(
        statement,
        EqFilter(union_query, "service_type_id", service_type_id),
        RecursiveFilter(union_query, "urban_function_id", urban_function_id, urban_functions_dict),
    )

    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioServiceWithGeometryDTO(**row) for row in result]


async def get_context_services_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioServiceDTO]:
    """Get list of services for 'context' of the project territory."""

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

    # Step 3: Collect all services from `public` intersecting context geometry
    public_services_query = select(
        services_data.c.service_id,
        services_data.c.name,
        services_data.c.capacity,
        services_data.c.is_capacity_real,
        services_data.c.properties,
        services_data.c.created_at,
        services_data.c.updated_at,
        service_types_dict.c.service_type_id,
        service_types_dict.c.urban_function_id,
        urban_functions_dict.c.name.label("urban_function_name"),
        service_types_dict.c.name.label("service_type_name"),
        service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
        service_types_dict.c.code.label("service_type_code"),
        service_types_dict.c.infrastructure_type,
        service_types_dict.c.properties.label("service_type_properties"),
        territory_types_dict.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.territory_id,
        territories_data.c.name.label("territory_name"),
        literal(False).label("is_scenario_object"),
    ).select_from(
        urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
        .join(
            object_geometries_data,
            object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
        )
        .join(
            objects_intersecting,
            objects_intersecting.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
        )
        .join(
            territories_data,
            territories_data.c.territory_id == object_geometries_data.c.territory_id,
        )
        .join(
            service_types_dict,
            service_types_dict.c.service_type_id == services_data.c.service_type_id,
        )
        .outerjoin(
            territory_types_dict,
            territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
        )
        .join(
            urban_functions_dict,
            urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
        )
    )

    # Step 4: Collect all services from parent regional scenario intersecting context geometry
    scenario_services_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_object"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
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
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
            .outerjoin(
                territory_types_dict,
                or_(
                    territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
                    territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
                ),
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == parent_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
        )
    )

    union_query = union_all(public_services_query, scenario_services_query).cte(name="union_query")
    statement = select(union_query)

    # Apply optional filters
    statement = apply_filters(
        statement,
        EqFilter(union_query, "service_type_id", service_type_id),
        RecursiveFilter(union_query, "urban_function_id", urban_function_id, urban_functions_dict),
    )

    result = (await conn.execute(statement)).mappings().all()

    grouped_data = defaultdict(lambda: {"territories": []})
    for row in result:
        service_id = row["service_id"]
        is_scenario_service = row["is_scenario_object"]
        key = service_id if not is_scenario_service else f"scenario_{service_id}"
        if key not in grouped_data:
            grouped_data[key].update({k: v for k, v in row.items() if k in ScenarioServiceDTO.fields()})

        territory = {"territory_id": row.territory_id, "name": row.territory_name}
        grouped_data[key]["territories"].append(territory)

    return [ScenarioServiceDTO(**row) for row in grouped_data.values()]


async def get_context_services_with_geometry_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioServiceWithGeometryDTO]:
    """Get list of services with geometry for 'context' of the project territory."""

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

    # Step 3: Collect all services from `public` intersecting context geometry
    intersected_geom = ST_Intersection(object_geometries_data.c.geometry, context_geom)
    public_services_query = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties,
            services_data.c.created_at,
            services_data.c.updated_at,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(intersected_geom).label("geometry"),
            ST_AsEWKB(ST_Centroid(intersected_geom)).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_service"),
            literal(False).label("is_scenario_geometry"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                objects_intersecting,
                objects_intersecting.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(~ST_IsEmpty(intersected_geom))
        .distinct()
    )

    # Step 4: Collect all services from parent regional scenario intersecting context geometry
    geom_expr = ST_Intersection(
        coalesce(
            projects_object_geometries_data.c.geometry,
            object_geometries_data.c.geometry,
        ),
        context_geom,
    )
    scenario_services_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id,
                object_geometries_data.c.object_geometry_id,
            ).label("object_geometry_id"),
            coalesce(
                projects_object_geometries_data.c.address,
                object_geometries_data.c.address,
            ).label("address"),
            coalesce(
                projects_object_geometries_data.c.osm_id,
                object_geometries_data.c.osm_id,
            ).label("osm_id"),
            ST_AsEWKB(geom_expr).label("geometry"),
            ST_AsEWKB(ST_Centroid(geom_expr)).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_service"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
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
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
            .outerjoin(
                territory_types_dict,
                or_(
                    territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
                    territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
                ),
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == parent_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
            ~ST_IsEmpty(geom_expr),
        )
        .distinct()
    )

    union_query = union_all(public_services_query, scenario_services_query).cte(name="union_query")
    statement = select(union_query)

    # Apply optional filters
    statement = apply_filters(
        statement,
        EqFilter(union_query, "service_type_id", service_type_id),
        RecursiveFilter(union_query, "urban_function_id", urban_function_id, urban_functions_dict),
    )

    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioServiceWithGeometryDTO(**row) for row in result]


async def add_service_to_db(
    conn: AsyncConnection, service: ScenarioServicePost, scenario_id: int, user: UserDTO
) -> ScenarioUrbanObjectDTO:
    """Create scenario service object."""

    await check_scenario(conn, scenario_id, user, to_edit=True)

    physical_object_column = (
        projects_urban_objects_data.c.physical_object_id
        if service.is_scenario_physical_object
        else projects_urban_objects_data.c.public_physical_object_id
    )
    geometry_column = (
        projects_urban_objects_data.c.object_geometry_id
        if service.is_scenario_geometry
        else projects_urban_objects_data.c.public_object_geometry_id
    )
    statement = select(projects_urban_objects_data).where(
        physical_object_column == service.physical_object_id,
        geometry_column == service.object_geometry_id,
        projects_urban_objects_data.c.scenario_id == scenario_id,
    )
    urban_objects = (await conn.execute(statement)).mappings().all()
    is_from_public = False
    if not urban_objects:
        is_from_public = True
        if not service.is_scenario_physical_object and not service.is_scenario_geometry:
            statement = select(urban_objects_data).where(
                urban_objects_data.c.physical_object_id == service.physical_object_id,
                urban_objects_data.c.object_geometry_id == service.object_geometry_id,
            )
            urban_objects = (await conn.execute(statement)).mappings().all()
        if not urban_objects:
            raise EntityNotFoundByParams(
                "urban object", service.physical_object_id, service.object_geometry_id, scenario_id
            )

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service.service_type_id}):
        raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        if not await check_existence(
            conn, territory_types_dict, conditions={"territory_type_id": service.territory_type_id}
        ):
            raise EntityNotFoundById(service.territory_type_id, "territory type")

    statement = (
        insert(projects_services_data)
        .values(
            **service.model_dump(
                exclude={
                    "physical_object_id",
                    "object_geometry_id",
                    "is_scenario_physical_object",
                    "is_scenario_geometry",
                }
            )
        )
        .returning(services_data.c.service_id)
    )
    service_id = (await conn.execute(statement)).scalar_one()

    if is_from_public:
        statement = (
            insert(projects_urban_objects_data)
            .values(
                scenario_id=scenario_id,
                service_id=service_id,
                public_physical_object_id=service.physical_object_id,
                public_object_geometry_id=service.object_geometry_id,
            )
            .returning(projects_urban_objects_data.c.urban_object_id)
        )
    else:
        flag = False
        for urban_object in urban_objects:
            if urban_object.service_id is None:
                statement = (
                    update(projects_urban_objects_data)
                    .where(projects_urban_objects_data.c.urban_object_id == urban_object.urban_object_id)
                    .values(service_id=service_id)
                    .returning(projects_urban_objects_data.c.urban_object_id)
                )
                flag = True
                break

        if not flag:
            statement = (
                insert(projects_urban_objects_data)
                .values(
                    scenario_id=scenario_id,
                    service_id=service_id,
                    physical_object_id=service.physical_object_id if service.is_scenario_physical_object else None,
                    object_geometry_id=service.object_geometry_id if service.is_scenario_geometry else None,
                    public_physical_object_id=(
                        service.physical_object_id if not service.is_scenario_physical_object else None
                    ),
                    public_object_geometry_id=service.object_geometry_id if not service.is_scenario_geometry else None,
                )
                .returning(projects_urban_objects_data.c.urban_object_id)
            )

    urban_object_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return (await get_scenario_urban_object_by_ids_from_db(conn, [urban_object_id]))[0]


async def get_scenario_service_by_id_from_db(conn: AsyncConnection, service_id: int) -> ScenarioServiceDTO:
    """Get scenario service by identifier."""

    statement = (
        select(
            projects_services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
            literal(True).label("is_scenario_object"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_urban_objects_data.join(
                projects_services_data,
                projects_services_data.c.service_id == projects_urban_objects_data.c.service_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(projects_services_data.c.service_id == service_id)
        .distinct()
    )
    result = (await conn.execute(statement)).mappings().all()
    if not result:
        raise EntityNotFoundById(service_id, "scenario service")

    territories = [{"territory_id": row.territory_id, "name": row.territory_name} for row in result]
    service = {k: v for k, v in result[0].items() if k in ScenarioServiceDTO.fields()}

    return ScenarioServiceDTO(**service, territories=territories)


async def put_service_to_db(
    conn: AsyncConnection,
    service: ServicePut,
    scenario_id: int,
    service_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> ScenarioServiceDTO:
    """Update scenario service by all its attributes."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn,
        projects_services_data if is_scenario_object else services_data,
        conditions={"service_id": service_id},
    ):
        raise EntityNotFoundById(service_id, "service")

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service.service_type_id}):
        raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        if not await check_existence(
            conn, territory_types_dict, conditions={"territory_type_id": service.territory_type_id}
        ):
            raise EntityNotFoundById(service.territory_type_id, "territory type")

    if not is_scenario_object:
        statement = (
            select(projects_services_data.c.service_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_services_data,
                    projects_services_data.c.service_id == projects_urban_objects_data.c.service_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_services_data.c.public_service_id == service_id,
            )
            .limit(1)
        )
        public_service = (await conn.execute(statement)).scalar_one_or_none()
        if public_service is not None:
            raise EntityAlreadyEdited("service", scenario_id)

    if is_scenario_object:
        statement = (
            update(projects_services_data)
            .where(projects_services_data.c.service_id == service_id)
            .values(**extract_values_from_model(service, to_update=True))
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_services_data)
            .values(public_service_id=service_id, **service.model_dump())
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()

        project_geometry = None
        territories_cte = None
        if not scenario.is_regional:
            project_geometry = (
                select(projects_territory_data.c.geometry).where(
                    projects_territory_data.c.project_id == scenario.project_id
                )
            ).scalar_subquery()
        else:
            territories_cte = include_child_territories_cte(scenario.territory_id)

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .cte("public_urban_object_ids")
        )

        statement = (
            select(urban_objects_data)
            .select_from(
                urban_objects_data.join(
                    object_geometries_data,
                    object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                urban_objects_data.c.service_id == service_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, project_geometry) if not scenario.is_regional else True,
                (
                    object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                    if scenario.is_regional
                    else True
                ),
            )
        )
        urban_objects = (await conn.execute(statement)).mappings().all()
        if urban_objects:
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "public_urban_object_id": row.urban_object_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "service_id": updated_service_id,
                            "public_physical_object_id": row.physical_object_id,
                            "public_object_geometry_id": row.object_geometry_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
        await conn.execute(
            (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.public_service_id == service_id)
                .values(service_id=updated_service_id, public_service_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_service_by_id_from_db(conn, updated_service_id)


async def patch_service_to_db(
    conn: AsyncConnection,
    service: ServicePatch,
    scenario_id: int,
    service_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> ScenarioServiceDTO:
    """Update scenario service by only given attributes."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if is_scenario_object:
        statement = select(projects_services_data).where(projects_services_data.c.service_id == service_id)
    else:
        statement = select(services_data).where(services_data.c.service_id == service_id)
    requested_service = (await conn.execute(statement)).mappings().one_or_none()
    if requested_service is None:
        raise EntityNotFoundById(service_id, "service")

    if service.service_type_id is not None:
        if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service.service_type_id}):
            raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        if not await check_existence(
            conn, territory_types_dict, conditions={"territory_type_id": service.territory_type_id}
        ):
            raise EntityNotFoundById(service.territory_type_id, "territory type")

    if not is_scenario_object:
        statement = (
            select(projects_services_data.c.service_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_services_data,
                    projects_services_data.c.service_id == projects_urban_objects_data.c.service_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_services_data.c.public_service_id == service_id,
            )
            .limit(1)
        )
        public_service = (await conn.execute(statement)).scalar_one_or_none()
        if public_service is not None:
            raise EntityAlreadyEdited("service", scenario_id)

    values = extract_values_from_model(service, exclude_unset=True, to_update=True)

    if is_scenario_object:
        statement = (
            update(projects_services_data)
            .where(projects_services_data.c.service_id == service_id)
            .values(**values)
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_services_data)
            .values(
                public_service_id=service_id,
                service_type_id=values.get("service_type_id", requested_service.service_type_id),
                territory_type_id=values.get("territory_type_id", requested_service.territory_type_id),
                name=values.get("name", requested_service.name),
                capacity=values.get("capacity", requested_service.capacity),
                is_capacity_real=values.get("is_capacity_real", requested_service.is_capacity_real),
                properties=values.get("properties", requested_service.properties),
            )
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()

        project_geometry = None
        territories_cte = None
        if not scenario.is_regional:
            project_geometry = (
                select(projects_territory_data.c.geometry).where(
                    projects_territory_data.c.project_id == scenario.project_id
                )
            ).scalar_subquery()
        else:
            territories_cte = include_child_territories_cte(scenario.territory_id)

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .cte("public_urban_object_ids")
        )

        statement = (
            select(urban_objects_data)
            .select_from(
                urban_objects_data.join(
                    object_geometries_data,
                    object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                urban_objects_data.c.service_id == service_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, project_geometry) if not scenario.is_regional else True,
                (
                    object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                    if scenario.is_regional
                    else True
                ),
            )
        )
        urban_objects = (await conn.execute(statement)).mappings().all()
        if urban_objects:
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "public_urban_object_id": row.urban_object_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "service_id": updated_service_id,
                            "public_physical_object_id": row.physical_object_id,
                            "public_object_geometry_id": row.object_geometry_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
        await conn.execute(
            (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.public_service_id == service_id)
                .values(service_id=updated_service_id, public_service_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_service_by_id_from_db(conn, updated_service_id)


async def delete_service_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    service_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> dict:
    """Delete scenario service."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn,
        projects_services_data if is_scenario_object else services_data,
        conditions={"service_id": service_id},
    ):
        raise EntityNotFoundById(service_id, "service")

    if not is_scenario_object:
        statement = (
            select(urban_objects_data.c.service_id)
            .select_from(
                projects_urban_objects_data.join(
                    urban_objects_data,
                    urban_objects_data.c.urban_object_id == projects_urban_objects_data.c.public_urban_object_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                urban_objects_data.c.service_id == service_id,
            )
            .limit(1)
        )
        public_urban_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_urban_object is not None:
            statement = (
                select(projects_urban_objects_data.c.public_service_id)
                .where(
                    projects_urban_objects_data.c.scenario_id == scenario_id,
                    projects_urban_objects_data.c.public_service_id == service_id,
                )
                .limit(1)
            )
            public_service = (await conn.execute(statement)).scalar_one_or_none()
            if public_service is None:
                raise EntityAlreadyEdited("service", scenario_id)

    if is_scenario_object:
        statement = delete(projects_services_data).where(projects_services_data.c.service_id == service_id)
        await conn.execute(statement)
    else:
        statement = delete(projects_urban_objects_data).where(
            projects_urban_objects_data.c.public_service_id == service_id
        )
        await conn.execute(statement)

        project_geometry = None
        territories_cte = None
        if not scenario.is_regional:
            project_geometry = (
                select(projects_territory_data.c.geometry).where(
                    projects_territory_data.c.project_id == scenario.project_id
                )
            ).scalar_subquery()
        else:
            territories_cte = include_child_territories_cte(scenario.territory_id)

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .cte(name="public_urban_object_ids")
        )

        statement = (
            select(urban_objects_data)
            .select_from(
                urban_objects_data.join(
                    object_geometries_data,
                    object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                urban_objects_data.c.service_id == service_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, project_geometry) if not scenario.is_regional else True,
                (
                    object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                    if scenario.is_regional
                    else True
                ),
            )
        )
        urban_objects = (await conn.execute(statement)).mappings().all()
        if urban_objects:
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "public_urban_object_id": row.urban_object_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
    await conn.commit()

    return {"status": "ok"}
