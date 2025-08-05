"""Projects physical objects internal logic is defined here."""

from collections import defaultdict

from geoalchemy2.functions import (
    ST_AsEWKB,
    ST_Centroid,
    ST_GeomFromWKB,
    ST_Intersection,
    ST_Intersects,
    ST_IsEmpty,
    ST_Within,
)
from sqlalchemy import delete, insert, literal, or_, select, text, union_all, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.functions import coalesce

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    projects_buildings_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_territory_data,
    projects_urban_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import (
    ScenarioPhysicalObjectDTO,
    ScenarioPhysicalObjectWithGeometryDTO,
    ScenarioUrbanObjectDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyEdited,
    EntityAlreadyExists,
    EntityNotFoundById,
)
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario
from idu_api.urban_api.logic.impl.helpers.projects_urban_objects import get_scenario_urban_object_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import (
    SRID,
    check_existence,
    extract_values_from_model,
    get_context_territories_geometry,
    include_child_territories_cte,
)
from idu_api.urban_api.schemas import (
    PhysicalObjectPatch,
    PhysicalObjectPut,
    PhysicalObjectWithGeometryPost,
    ScenarioBuildingPatch,
    ScenarioBuildingPost,
    ScenarioBuildingPut,
)
from idu_api.urban_api.utils.query_filters import EqFilter, RecursiveFilter, apply_filters


async def get_physical_objects_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[ScenarioPhysicalObjectDTO]:
    """Get physical objects by scenario identifier."""

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
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    public_urban_objects_query = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_object"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, project_geometry) if not scenario.is_regional else True,
            (
                object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                if scenario.is_regional
                else True
            ),
        )
        .distinct()
    )

    # Step 3: Collect all physical objects from `user_projects.urban_objects_data`
    scenario_urban_objects_query = (
        select(
            coalesce(
                projects_physical_objects_data.c.physical_object_id,
                physical_objects_data.c.physical_object_id,
            ).label("physical_object_id"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            coalesce(
                projects_physical_objects_data.c.name,
                physical_objects_data.c.name,
            ).label("name"),
            coalesce(
                projects_physical_objects_data.c.properties,
                physical_objects_data.c.properties,
            ).label("properties"),
            coalesce(
                projects_physical_objects_data.c.created_at,
                physical_objects_data.c.created_at,
            ).label("created_at"),
            coalesce(
                projects_physical_objects_data.c.updated_at,
                physical_objects_data.c.updated_at,
            ).label("updated_at"),
            coalesce(
                projects_buildings_data.c.building_id,
                buildings_data.c.building_id,
            ).label("building_id"),
            coalesce(
                projects_buildings_data.c.floors,
                buildings_data.c.floors,
            ).label("floors"),
            coalesce(
                projects_buildings_data.c.building_area_official,
                buildings_data.c.building_area_official,
            ).label("building_area_official"),
            coalesce(
                projects_buildings_data.c.building_area_modeled,
                buildings_data.c.building_area_modeled,
            ).label("building_area_modeled"),
            coalesce(
                projects_buildings_data.c.project_type,
                buildings_data.c.project_type,
            ).label("project_type"),
            coalesce(
                projects_buildings_data.c.floor_type,
                buildings_data.c.floor_type,
            ).label("floor_type"),
            coalesce(
                projects_buildings_data.c.wall_material,
                buildings_data.c.wall_material,
            ).label("wall_material"),
            coalesce(
                projects_buildings_data.c.built_year,
                buildings_data.c.built_year,
            ).label("built_year"),
            coalesce(
                projects_buildings_data.c.exploitation_start_year,
                buildings_data.c.exploitation_start_year,
            ).label("exploitation_start_year"),
            coalesce(
                projects_buildings_data.c.properties,
                buildings_data.c.properties,
            ).label("building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.physical_object_id.isnot(None)).label("is_scenario_object"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
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
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_buildings_data,
                projects_buildings_data.c.physical_object_id == projects_physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
        )
        .distinct()
    )

    # Apply optional filters
    def apply_common_filters(query):
        return apply_filters(
            query,
            EqFilter(physical_object_types_dict, "physical_object_type_id", physical_object_type_id),
            RecursiveFilter(
                physical_object_types_dict,
                "physical_object_function_id",
                physical_object_function_id,
                physical_object_functions_dict,
            ),
        )

    public_urban_objects_query = apply_common_filters(public_urban_objects_query)
    scenario_urban_objects_query = apply_common_filters(scenario_urban_objects_query)

    union_query = union_all(public_urban_objects_query, scenario_urban_objects_query)
    result = (await conn.execute(union_query)).mappings().all()

    grouped_objects = defaultdict(lambda: {"territories": []})
    for obj in result:
        physical_object_id = obj["physical_object_id"]
        is_scenario_physical_object = obj["is_scenario_object"]
        key = physical_object_id if not is_scenario_physical_object else f"scenario_{physical_object_id}"

        if key not in grouped_objects:
            grouped_objects[key].update({k: v for k, v in obj.items() if k in ScenarioPhysicalObjectDTO.fields()})

        territory = {"territory_id": obj["territory_id"], "name": obj["territory_name"]}
        grouped_objects[key]["territories"].append(territory)

    return [ScenarioPhysicalObjectDTO(**row) for row in grouped_objects.values()]


async def get_physical_objects_with_geometry_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[ScenarioPhysicalObjectWithGeometryDTO]:
    """Get list of physical objects with geometry by scenario identifier."""

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
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    public_urban_objects_query = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_physical_object"),
            literal(False).label("is_scenario_geometry"),
        )
        .select_from(
            urban_objects_data.join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, project_geometry) if not scenario.is_regional else True,
            (
                object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                if scenario.is_regional
                else True
            ),
        )
    )

    # Step 3: Collect all physical objects from `user_projects.urban_objects_data`
    scenario_urban_objects_query = (
        select(
            coalesce(
                projects_physical_objects_data.c.physical_object_id,
                physical_objects_data.c.physical_object_id,
            ).label("physical_object_id"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            coalesce(
                projects_physical_objects_data.c.name,
                physical_objects_data.c.name,
            ).label("name"),
            coalesce(
                projects_physical_objects_data.c.properties,
                physical_objects_data.c.properties,
            ).label("properties"),
            coalesce(
                projects_physical_objects_data.c.created_at,
                physical_objects_data.c.created_at,
            ).label("created_at"),
            coalesce(
                projects_physical_objects_data.c.updated_at,
                physical_objects_data.c.updated_at,
            ).label("updated_at"),
            coalesce(
                projects_buildings_data.c.building_id,
                buildings_data.c.building_id,
            ).label("building_id"),
            coalesce(
                projects_buildings_data.c.floors,
                buildings_data.c.floors,
            ).label("floors"),
            coalesce(
                projects_buildings_data.c.building_area_official,
                buildings_data.c.building_area_official,
            ).label("building_area_official"),
            coalesce(
                projects_buildings_data.c.building_area_modeled,
                buildings_data.c.building_area_modeled,
            ).label("building_area_modeled"),
            coalesce(
                projects_buildings_data.c.project_type,
                buildings_data.c.project_type,
            ).label("project_type"),
            coalesce(
                projects_buildings_data.c.floor_type,
                buildings_data.c.floor_type,
            ).label("floor_type"),
            coalesce(
                projects_buildings_data.c.wall_material,
                buildings_data.c.wall_material,
            ).label("wall_material"),
            coalesce(
                projects_buildings_data.c.built_year,
                buildings_data.c.built_year,
            ).label("built_year"),
            coalesce(
                projects_buildings_data.c.exploitation_start_year,
                buildings_data.c.exploitation_start_year,
            ).label("exploitation_start_year"),
            coalesce(
                projects_buildings_data.c.properties,
                buildings_data.c.properties,
            ).label("building_properties"),
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
            coalesce(
                projects_object_geometries_data.c.geometry,
                object_geometries_data.c.geometry,
            ).label("geometry"),
            coalesce(
                projects_object_geometries_data.c.centre_point,
                object_geometries_data.c.centre_point,
            ).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.physical_object_id.isnot(None)).label("is_scenario_physical_object"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
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
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_buildings_data,
                projects_buildings_data.c.physical_object_id == projects_physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
        )
    )

    # Apply optional filters
    def apply_common_filters(query):
        return apply_filters(
            query,
            EqFilter(physical_object_types_dict, "physical_object_type_id", physical_object_type_id),
            RecursiveFilter(
                physical_object_types_dict,
                "physical_object_function_id",
                physical_object_function_id,
                physical_object_functions_dict,
            ),
        )

    public_urban_objects_query = apply_common_filters(public_urban_objects_query)
    scenario_urban_objects_query = apply_common_filters(scenario_urban_objects_query)

    union_query = union_all(public_urban_objects_query, scenario_urban_objects_query)
    result = (await conn.execute(union_query)).mappings().all()

    grouped_objects = defaultdict()
    for obj in result:
        physical_object_id = obj["physical_object_id"]
        is_scenario_physical_object = obj["is_scenario_physical_object"]
        object_key = physical_object_id if not is_scenario_physical_object else f"scenario_{physical_object_id}"
        geometry_id = obj["object_geometry_id"]
        is_scenario_geometry = obj["is_scenario_geometry"]
        geometry_key = geometry_id if not is_scenario_geometry else f"scenario_{geometry_id}"
        key = (object_key, geometry_key)

        if key not in grouped_objects:
            grouped_objects.update({key: obj})

    return [ScenarioPhysicalObjectWithGeometryDTO(**group) for group in grouped_objects.values()]


async def get_context_physical_objects_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[ScenarioPhysicalObjectDTO]:
    """Get list of physical objects for 'context' of the project territory."""

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

    # Step 3: Collect all physical objects from `public` intersecting context geometry
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    public_urban_objects_query = select(
        physical_objects_data.c.physical_object_id,
        physical_object_types_dict.c.physical_object_type_id,
        physical_object_types_dict.c.name.label("physical_object_type_name"),
        physical_object_functions_dict.c.physical_object_function_id,
        physical_object_functions_dict.c.name.label("physical_object_function_name"),
        physical_objects_data.c.name,
        physical_objects_data.c.properties,
        physical_objects_data.c.created_at,
        physical_objects_data.c.updated_at,
        *building_columns,
        buildings_data.c.properties.label("building_properties"),
        territories_data.c.territory_id,
        territories_data.c.name.label("territory_name"),
        literal(False).label("is_scenario_object"),
    ).select_from(
        urban_objects_data.join(
            physical_objects_data,
            physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
        )
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
            physical_object_types_dict,
            physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
        )
        .join(
            physical_object_functions_dict,
            physical_object_functions_dict.c.physical_object_function_id
            == physical_object_types_dict.c.physical_object_function_id,
        )
        .outerjoin(
            buildings_data,
            buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
        )
    )

    # Step 4: Collect all physical objects from parent regional scenario intersecting context geometry
    scenario_urban_objects_query = (
        select(
            coalesce(
                projects_physical_objects_data.c.physical_object_id,
                physical_objects_data.c.physical_object_id,
            ).label("physical_object_id"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            coalesce(
                projects_physical_objects_data.c.name,
                physical_objects_data.c.name,
            ).label("name"),
            coalesce(
                projects_physical_objects_data.c.properties,
                physical_objects_data.c.properties,
            ).label("properties"),
            coalesce(
                projects_physical_objects_data.c.created_at,
                physical_objects_data.c.created_at,
            ).label("created_at"),
            coalesce(
                projects_physical_objects_data.c.updated_at,
                physical_objects_data.c.updated_at,
            ).label("updated_at"),
            coalesce(
                projects_buildings_data.c.building_id,
                buildings_data.c.building_id,
            ).label("building_id"),
            coalesce(
                projects_buildings_data.c.floors,
                buildings_data.c.floors,
            ).label("floors"),
            coalesce(
                projects_buildings_data.c.building_area_official,
                buildings_data.c.building_area_official,
            ).label("building_area_official"),
            coalesce(
                projects_buildings_data.c.building_area_modeled,
                buildings_data.c.building_area_modeled,
            ).label("building_area_modeled"),
            coalesce(
                projects_buildings_data.c.project_type,
                buildings_data.c.project_type,
            ).label("project_type"),
            coalesce(
                projects_buildings_data.c.floor_type,
                buildings_data.c.floor_type,
            ).label("floor_type"),
            coalesce(
                projects_buildings_data.c.wall_material,
                buildings_data.c.wall_material,
            ).label("wall_material"),
            coalesce(
                projects_buildings_data.c.built_year,
                buildings_data.c.built_year,
            ).label("built_year"),
            coalesce(
                projects_buildings_data.c.exploitation_start_year,
                buildings_data.c.exploitation_start_year,
            ).label("exploitation_start_year"),
            coalesce(
                projects_buildings_data.c.properties,
                buildings_data.c.properties,
            ).label("building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.physical_object_id.isnot(None)).label("is_scenario_object"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
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
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_buildings_data,
                projects_buildings_data.c.physical_object_id == projects_physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == parent_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
        )
        .distinct()
    )

    # Apply optional filters
    def apply_common_filters(query):
        return apply_filters(
            query,
            EqFilter(physical_object_types_dict, "physical_object_type_id", physical_object_type_id),
            RecursiveFilter(
                physical_object_types_dict,
                "physical_object_function_id",
                physical_object_function_id,
                physical_object_functions_dict,
            ),
        )

    public_urban_objects_query = apply_common_filters(public_urban_objects_query)
    scenario_urban_objects_query = apply_common_filters(scenario_urban_objects_query)

    union_query = union_all(public_urban_objects_query, scenario_urban_objects_query)

    result = (await conn.execute(union_query)).mappings().all()

    grouped_data = defaultdict(lambda: {"territories": []})
    for row in result:
        physical_object_id = row["physical_object_id"]
        is_scenario_physical_object = row["is_scenario_object"]
        key = physical_object_id if not is_scenario_physical_object else f"scenario_{physical_object_id}"
        if key not in grouped_data:
            grouped_data[key].update({k: v for k, v in row.items() if k in ScenarioPhysicalObjectDTO.fields()})

        territory = {"territory_id": row.territory_id, "name": row.territory_name}
        grouped_data[key]["territories"].append(territory)

    return [ScenarioPhysicalObjectDTO(**row) for row in grouped_data.values()]


async def get_context_physical_objects_with_geometry_from_db(
    conn: AsyncConnection,
    project_id: int,
    user: UserDTO | None,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[ScenarioPhysicalObjectWithGeometryDTO]:
    """Get list of physical objects with geometry for 'context' of the project territory."""

    parent_id, context_geom, context_ids = await get_context_territories_geometry(conn, project_id, user)

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

    # Step 3: Collect all physical objects from `public` intersecting context geometry
    intersected_geom = ST_Intersection(object_geometries_data.c.geometry, context_geom)
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    public_urban_objects_query = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(intersected_geom).label("geometry"),
            ST_AsEWKB(ST_Centroid(intersected_geom)).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_physical_object"),
            literal(False).label("is_scenario_geometry"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
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
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(~ST_IsEmpty(intersected_geom))
        .distinct()
    )

    # Step 4: Collect all physical objects from parent regional scenario intersecting context geometry
    geom_expr = ST_Intersection(
        coalesce(
            projects_object_geometries_data.c.geometry,
            object_geometries_data.c.geometry,
        ),
        context_geom,
    )
    scenario_urban_objects_query = (
        select(
            coalesce(
                projects_physical_objects_data.c.physical_object_id,
                physical_objects_data.c.physical_object_id,
            ).label("physical_object_id"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            coalesce(
                projects_physical_objects_data.c.name,
                physical_objects_data.c.name,
            ).label("name"),
            coalesce(
                projects_physical_objects_data.c.properties,
                physical_objects_data.c.properties,
            ).label("properties"),
            coalesce(
                projects_physical_objects_data.c.created_at,
                physical_objects_data.c.created_at,
            ).label("created_at"),
            coalesce(
                projects_physical_objects_data.c.updated_at,
                physical_objects_data.c.updated_at,
            ).label("updated_at"),
            coalesce(
                projects_buildings_data.c.building_id,
                buildings_data.c.building_id,
            ).label("building_id"),
            coalesce(
                projects_buildings_data.c.floors,
                buildings_data.c.floors,
            ).label("floors"),
            coalesce(
                projects_buildings_data.c.building_area_official,
                buildings_data.c.building_area_official,
            ).label("building_area_official"),
            coalesce(
                projects_buildings_data.c.building_area_modeled,
                buildings_data.c.building_area_modeled,
            ).label("building_area_modeled"),
            coalesce(
                projects_buildings_data.c.project_type,
                buildings_data.c.project_type,
            ).label("project_type"),
            coalesce(
                projects_buildings_data.c.floor_type,
                buildings_data.c.floor_type,
            ).label("floor_type"),
            coalesce(
                projects_buildings_data.c.wall_material,
                buildings_data.c.wall_material,
            ).label("wall_material"),
            coalesce(
                projects_buildings_data.c.built_year,
                buildings_data.c.built_year,
            ).label("built_year"),
            coalesce(
                projects_buildings_data.c.exploitation_start_year,
                buildings_data.c.exploitation_start_year,
            ).label("exploitation_start_year"),
            coalesce(
                projects_buildings_data.c.properties,
                buildings_data.c.properties,
            ).label("building_properties"),
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
            (projects_urban_objects_data.c.physical_object_id.isnot(None)).label("is_scenario_physical_object"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
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
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_buildings_data,
                projects_buildings_data.c.physical_object_id == projects_physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == parent_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            ~ST_IsEmpty(geom_expr),
        )
        .distinct()
    )

    # Apply optional filters
    def apply_common_filters(query):
        return apply_filters(
            query,
            EqFilter(physical_object_types_dict, "physical_object_type_id", physical_object_type_id),
            RecursiveFilter(
                physical_object_types_dict,
                "physical_object_function_id",
                physical_object_function_id,
                physical_object_functions_dict,
            ),
        )

    public_urban_objects_query = apply_common_filters(public_urban_objects_query)
    scenario_urban_objects_query = apply_common_filters(scenario_urban_objects_query)

    union_query = union_all(public_urban_objects_query, scenario_urban_objects_query)
    result = (await conn.execute(union_query)).mappings().all()

    return [ScenarioPhysicalObjectWithGeometryDTO(**phys_obj) for phys_obj in result]


async def get_scenario_physical_object_by_id_from_db(
    conn: AsyncConnection, physical_object_id: int
) -> ScenarioPhysicalObjectDTO:
    """Get scenario physical object by identifier."""

    project_building_columns = [
        col for col in projects_buildings_data.c if col.name not in ("physical_object_id", "properties")
    ]

    statement = (
        select(
            projects_physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            projects_physical_objects_data.c.name,
            projects_physical_objects_data.c.properties,
            projects_physical_objects_data.c.created_at,
            projects_physical_objects_data.c.updated_at,
            literal(True).label("is_scenario_object"),
            *project_building_columns,
            projects_buildings_data.c.properties.label("building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_urban_objects_data.join(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
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
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id
                == projects_physical_objects_data.c.physical_object_type_id,
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                projects_buildings_data,
                projects_buildings_data.c.physical_object_id == projects_physical_objects_data.c.physical_object_id,
            )
        )
        .where(projects_physical_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()
    if not result:
        raise EntityNotFoundById(physical_object_id, "scenario physical object")

    territories = [{"territory_id": row.territory_id, "name": row.territory_name} for row in result]
    physical_object = {k: v for k, v in result[0].items() if k in ScenarioPhysicalObjectDTO.fields()}

    return ScenarioPhysicalObjectDTO(**physical_object, territories=territories)


async def add_physical_object_with_geometry_to_db(
    conn: AsyncConnection,
    physical_object: PhysicalObjectWithGeometryPost,
    scenario_id: int,
    user: UserDTO,
) -> ScenarioUrbanObjectDTO:
    """Create scenario physical object with geometry."""

    await check_scenario(conn, scenario_id, user, to_edit=True)

    if not await check_existence(conn, territories_data, conditions={"territory_id": physical_object.territory_id}):
        raise EntityNotFoundById(physical_object.territory_id, "territory")

    if not await check_existence(
        conn,
        physical_object_types_dict,
        conditions={"physical_object_type_id": physical_object.physical_object_type_id},
    ):
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    statement = (
        insert(projects_physical_objects_data)
        .values(
            public_physical_object_id=None,
            physical_object_type_id=physical_object.physical_object_type_id,
            name=physical_object.name,
            properties=physical_object.properties,
        )
        .returning(projects_physical_objects_data.c.physical_object_id)
    )
    physical_object_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(projects_object_geometries_data)
        .values(
            public_object_geometry_id=None,
            territory_id=physical_object.territory_id,
            geometry=ST_GeomFromWKB(physical_object.geometry.as_shapely_geometry().wkb, text(str(SRID))),
            centre_point=ST_GeomFromWKB(physical_object.centre_point.as_shapely_geometry().wkb, text(str(SRID))),
            address=physical_object.address,
            osm_id=physical_object.osm_id,
        )
        .returning(projects_object_geometries_data.c.object_geometry_id)
    )
    object_geometry_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(projects_urban_objects_data)
        .values(scenario_id=scenario_id, physical_object_id=physical_object_id, object_geometry_id=object_geometry_id)
        .returning(urban_objects_data.c.urban_object_id)
    )
    urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    await conn.commit()

    return (await get_scenario_urban_object_by_ids_from_db(conn, [urban_object_id]))[0]


async def put_physical_object_to_db(
    conn: AsyncConnection,
    physical_object: PhysicalObjectPut,
    scenario_id: int,
    physical_object_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> ScenarioPhysicalObjectDTO:
    """Update scenario physical object by all its attributes."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn,
        projects_physical_objects_data if is_scenario_object else physical_objects_data,
        conditions={"physical_object_id": physical_object_id},
    ):
        raise EntityNotFoundById(physical_object_id, "physical object")

    if not await check_existence(
        conn,
        physical_object_types_dict,
        conditions={"physical_object_type_id": physical_object.physical_object_type_id},
    ):
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    if not is_scenario_object:
        statement = (
            select(projects_physical_objects_data.c.physical_object_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_physical_objects_data,
                    projects_physical_objects_data.c.physical_object_id
                    == projects_urban_objects_data.c.physical_object_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_physical_objects_data.c.public_physical_object_id == physical_object_id,
            )
            .limit(1)
        )
        public_physical_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_physical_object is not None:
            raise EntityAlreadyEdited("physical object", scenario_id)

    if is_scenario_object:
        statement = (
            update(projects_physical_objects_data)
            .where(projects_physical_objects_data.c.physical_object_id == physical_object_id)
            .values(**extract_values_from_model(physical_object, to_update=True))
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        updated_physical_object_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_physical_objects_data)
            .values(
                public_physical_object_id=physical_object_id,
                **extract_values_from_model(physical_object),
            )
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        updated_physical_object_id = (await conn.execute(statement)).scalar_one()

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
                urban_objects_data.c.physical_object_id == physical_object_id,
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
                            "physical_object_id": updated_physical_object_id,
                            "public_service_id": row.service_id,
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
                .where(projects_urban_objects_data.c.public_physical_object_id == physical_object_id)
                .values(physical_object_id=updated_physical_object_id, public_physical_object_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_physical_object_by_id_from_db(conn, updated_physical_object_id)


async def patch_physical_object_to_db(
    conn: AsyncConnection,
    physical_object: PhysicalObjectPatch,
    scenario_id: int,
    physical_object_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> ScenarioPhysicalObjectDTO:
    """Update scenario physical object by only given attributes."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if is_scenario_object:
        statement = select(projects_physical_objects_data).where(
            projects_physical_objects_data.c.physical_object_id == physical_object_id
        )
    else:
        statement = select(physical_objects_data).where(
            physical_objects_data.c.physical_object_id == physical_object_id
        )
    requested_physical_object = (await conn.execute(statement)).mappings().one_or_none()
    if requested_physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    if physical_object.physical_object_type_id is not None:
        if not await check_existence(
            conn,
            physical_object_types_dict,
            conditions={"physical_object_type_id": physical_object.physical_object_type_id},
        ):
            raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    if not is_scenario_object:
        statement = (
            select(projects_physical_objects_data.c.physical_object_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_physical_objects_data,
                    projects_physical_objects_data.c.physical_object_id
                    == projects_urban_objects_data.c.physical_object_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_physical_objects_data.c.public_physical_object_id == physical_object_id,
            )
            .limit(1)
        )
        public_physical_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_physical_object is not None:
            raise EntityAlreadyEdited("physical object", scenario_id)

    values = extract_values_from_model(physical_object, exclude_unset=True, to_update=True)

    if is_scenario_object:
        statement = (
            update(projects_physical_objects_data)
            .where(projects_physical_objects_data.c.physical_object_id == physical_object_id)
            .values(**values)
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        updated_physical_object_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_physical_objects_data)
            .values(
                public_physical_object_id=physical_object_id,
                physical_object_type_id=values.get(
                    "physical_object_type_id", requested_physical_object.physical_object_type_id
                ),
                name=values.get("name", requested_physical_object.name),
                properties=values.get("properties", requested_physical_object.properties),
            )
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        updated_physical_object_id = (await conn.execute(statement)).scalar_one()

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
                urban_objects_data.c.physical_object_id == physical_object_id,
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
                            "physical_object_id": updated_physical_object_id,
                            "public_service_id": row.service_id,
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
                .where(projects_urban_objects_data.c.public_physical_object_id == physical_object_id)
                .values(physical_object_id=updated_physical_object_id, public_physical_object_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_physical_object_by_id_from_db(conn, updated_physical_object_id)


async def delete_physical_object_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    physical_object_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> dict:
    """Delete scenario physical object."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn,
        projects_physical_objects_data if is_scenario_object else physical_objects_data,
        conditions={"physical_object_id": physical_object_id},
    ):
        raise EntityNotFoundById(physical_object_id, "physical object")

    if not is_scenario_object:
        statement = (
            select(urban_objects_data.c.physical_object_id)
            .select_from(
                projects_urban_objects_data.join(
                    urban_objects_data,
                    urban_objects_data.c.urban_object_id == projects_urban_objects_data.c.public_urban_object_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                urban_objects_data.c.physical_object_id == physical_object_id,
            )
            .limit(1)
        )
        public_urban_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_urban_object is not None:
            statement = (
                select(projects_urban_objects_data.c.public_physical_object_id)
                .where(
                    projects_urban_objects_data.c.scenario_id == scenario_id,
                    projects_urban_objects_data.c.public_physical_object_id == physical_object_id,
                )
                .limit(1)
            )
            public_physical_object = (await conn.execute(statement)).scalar_one_or_none()
            if public_physical_object is None:
                raise EntityAlreadyEdited("physical object", scenario_id)

    if is_scenario_object:
        statement = delete(projects_physical_objects_data).where(
            projects_physical_objects_data.c.physical_object_id == physical_object_id
        )
        await conn.execute(statement)
    else:
        statement = delete(projects_urban_objects_data).where(
            projects_urban_objects_data.c.public_physical_object_id == physical_object_id
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
                urban_objects_data.c.physical_object_id == physical_object_id,
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


async def update_physical_objects_by_function_id_to_db(
    conn: AsyncConnection,
    physical_objects: list[PhysicalObjectWithGeometryPost],
    scenario_id: int,
    user: UserDTO,
    physical_object_function_id: int,
) -> list[ScenarioUrbanObjectDTO]:
    """Delete all physical objects by physical object function identifier
    and upload new objects with the same function for given scenario."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    territories = {phys_obj.territory_id for phys_obj in physical_objects}
    statement = select(territories_data.c.territory_id).where(territories_data.c.territory_id.in_(territories))
    result = (await conn.execute(statement)).scalars().all()
    if len(territories) > len(list(result)):
        raise EntitiesNotFoundByIds("territory")

    physical_object_types = {phys_obj.physical_object_type_id for phys_obj in physical_objects}
    statement = select(physical_object_types_dict.c.physical_object_function_id).where(
        physical_object_types_dict.c.physical_object_type_id.in_(physical_object_types)
    )
    result = (await conn.execute(statement)).scalars().all()
    if len(physical_object_types) > len(list(result)):
        raise EntitiesNotFoundByIds("physical object type")
    if any(physical_object_function_id != function_id for function_id in result):
        raise ValueError("You can only upload physical objects with given physical object function")

    project_geometry = (
        select(projects_territory_data.c.geometry)
        .where(projects_territory_data.c.project_id == scenario.project_id)
        .cte(name="project_geometry")
    )

    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .where(ST_Intersects(object_geometries_data.c.geometry, project_geometry.c.geometry))
        .cte(name="objects_intersecting")
    )

    # Шаг 1: Получить все public_urban_object_id для данного scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id).where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.isnot(None),
        )
    ).cte(name="public_urban_object_ids")

    # Шаг 2: Собрать все записи из public.urban_objects_data по собранным public_urban_object_id
    public_urban_objects_query = (
        select(urban_objects_data.c.urban_object_id)
        .select_from(
            urban_objects_data.join(
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
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            object_geometries_data.c.object_geometry_id.in_(select(objects_intersecting)),
            physical_object_types_dict.c.physical_object_function_id == physical_object_function_id,
        )
        .cte(name="public_urban_objects_query")
    )

    await conn.execute(
        insert(projects_urban_objects_data).from_select(
            (
                projects_urban_objects_data.c.scenario_id,
                projects_urban_objects_data.c.public_urban_object_id,
            ),
            select(
                literal(scenario_id).label("scenario_id"),
                public_urban_objects_query.c.urban_object_id,
            ),
        )
    )

    scenario_urban_objects_query = (
        select(
            projects_urban_objects_data.c.urban_object_id,
            projects_urban_objects_data.c.physical_object_id,
            projects_urban_objects_data.c.object_geometry_id,
            projects_urban_objects_data.c.public_physical_object_id,
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
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
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            physical_object_types_dict.c.physical_object_function_id == physical_object_function_id,
        )
    )
    result = (await conn.execute(scenario_urban_objects_query)).mappings().all()

    scenario_physical_objects = set(obj.physical_object_id for obj in result if obj.physical_object_id is not None)
    scenario_object_geometries = set(obj.object_geometry_id for obj in result if obj.object_geometry_id is not None)
    scenario_urban_objects = set(obj.urban_object_id for obj in result if obj.public_physical_object_id is not None)

    await conn.execute(
        delete(projects_physical_objects_data).where(
            projects_physical_objects_data.c.physical_object_id.in_(scenario_physical_objects)
        )
    )
    await conn.execute(
        delete(projects_object_geometries_data).where(
            projects_object_geometries_data.c.object_geometry_id.in_(scenario_object_geometries)
        )
    )

    await conn.execute(
        delete(projects_urban_objects_data).where(
            projects_urban_objects_data.c.urban_object_id.in_(scenario_urban_objects)
        )
    )

    statement = (
        insert(projects_physical_objects_data)
        .values(
            [
                {
                    "public_physical_object_id": None,
                    "physical_object_type_id": physical_object.physical_object_type_id,
                    "name": physical_object.name,
                    "properties": physical_object.properties,
                }
                for physical_object in physical_objects
            ]
        )
        .returning(projects_physical_objects_data.c.physical_object_id)
    )
    physical_object_ids = list((await conn.execute(statement)).scalars().all())

    statement = (
        insert(projects_object_geometries_data)
        .values(
            [
                {
                    "public_object_geometry_id": None,
                    "territory_id": physical_object.territory_id,
                    "geometry": ST_GeomFromWKB(physical_object.geometry.as_shapely_geometry().wkb, text(str(SRID))),
                    "centre_point": ST_GeomFromWKB(
                        physical_object.centre_point.as_shapely_geometry().wkb, text(str(SRID))
                    ),
                    "address": physical_object.address,
                    "osm_id": physical_object.osm_id,
                }
                for physical_object in physical_objects
            ]
        )
        .returning(projects_object_geometries_data.c.object_geometry_id)
    )
    object_geometry_ids = list((await conn.execute(statement)).scalars().all())

    statement = (
        insert(projects_urban_objects_data)
        .values(
            [
                {
                    "scenario_id": scenario_id,
                    "physical_object_id": physical_object_ids[i],
                    "object_geometry_id": object_geometry_ids[i],
                }
                for i in range(len(physical_objects))
            ]
        )
        .returning(urban_objects_data.c.urban_object_id)
    )
    urban_object_ids = (await conn.execute(statement)).scalars().all()
    await conn.commit()

    return await get_scenario_urban_object_by_ids_from_db(conn, list(urban_object_ids))


async def add_building_to_db(
    conn: AsyncConnection,
    building: ScenarioBuildingPost,
    scenario_id: int,
    user: UserDTO,
) -> ScenarioPhysicalObjectDTO:
    """Add building to physical object for given scenario."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn,
        projects_physical_objects_data if building.is_scenario_object else physical_objects_data,
        conditions={"physical_object_id": building.physical_object_id},
    ):
        raise EntityNotFoundById(building.physical_object_id, "physical object")

    if not building.is_scenario_object:
        statement = (
            select(projects_physical_objects_data.c.physical_object_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_physical_objects_data,
                    projects_physical_objects_data.c.physical_object_id
                    == projects_urban_objects_data.c.physical_object_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_physical_objects_data.c.public_physical_object_id == building.physical_object_id,
            )
            .limit(1)
        )
        public_physical_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_physical_object is not None:
            raise EntityAlreadyExists("scenario physical object", building.physical_object_id)

    if await check_existence(
        conn,
        projects_buildings_data if building.is_scenario_object else buildings_data,
        conditions={"physical_object_id": building.physical_object_id},
    ):
        raise EntityAlreadyExists("building", building.physical_object_id)

    if building.is_scenario_object:
        statement = insert(projects_buildings_data).values(**building.model_dump(exclude={"is_scenario_object"}))
        await conn.execute(statement)
        await conn.commit()
        return await get_scenario_physical_object_by_id_from_db(conn, building.physical_object_id)

    statement = (
        insert(projects_physical_objects_data)
        .from_select(
            [
                "physical_object_type_id",
                "name",
                "properties",
                "public_physical_object_id",
            ],
            select(
                physical_objects_data.c.physical_object_type_id,
                physical_objects_data.c.name,
                physical_objects_data.c.properties,
                literal(building.physical_object_id).label("public_physical_object_id"),
            ).where(physical_objects_data.c.physical_object_id == building.physical_object_id),
        )
        .returning(projects_physical_objects_data.c.physical_object_id)
    )
    scenario_physical_object_id = (await conn.execute(statement)).scalar_one()

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == scenario.project_id)
    ).alias("project_geometry")

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_not(None),
        )
        .alias("public_urban_object_ids")
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
            urban_objects_data.c.physical_object_id == building.physical_object_id,
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
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
                        "physical_object_id": scenario_physical_object_id,
                        "public_service_id": row.service_id,
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
            .where(projects_urban_objects_data.c.public_physical_object_id == building.physical_object_id)
            .values(physical_object_id=scenario_physical_object_id, public_physical_object_id=None)
        )
    )

    statement = insert(projects_buildings_data).values(
        **building.model_dump(exclude={"is_scenario_object", "physical_object_id"}),
        physical_object_id=scenario_physical_object_id,
    )

    await conn.execute(statement)
    await conn.commit()
    return await get_scenario_physical_object_by_id_from_db(conn, scenario_physical_object_id)


async def put_building_to_db(
    conn: AsyncConnection,
    building: ScenarioBuildingPut,
    scenario_id: int,
    user: UserDTO,
) -> ScenarioPhysicalObjectDTO:
    """Update or create building for given scenario."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn,
        projects_physical_objects_data if building.is_scenario_object else physical_objects_data,
        conditions={"physical_object_id": building.physical_object_id},
    ):
        raise EntityNotFoundById(building.physical_object_id, "physical object")

    if not building.is_scenario_object:
        statement = (
            select(projects_physical_objects_data.c.physical_object_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_physical_objects_data,
                    projects_physical_objects_data.c.physical_object_id
                    == projects_urban_objects_data.c.physical_object_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_physical_objects_data.c.public_physical_object_id == building.physical_object_id,
            )
            .limit(1)
        )
        public_physical_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_physical_object is not None:
            raise EntityAlreadyExists("scenario physical object", building.physical_object_id)

    if building.is_scenario_object:
        if not await check_existence(
            conn,
            projects_buildings_data if building.is_scenario_object else buildings_data,
            conditions={"physical_object_id": building.physical_object_id},
        ):
            statement = insert(projects_buildings_data).values(**building.model_dump(exclude={"is_scenario_object"}))
        else:
            statement = (
                update(projects_buildings_data)
                .where(projects_buildings_data.c.physical_object_id == building.physical_object_id)
                .values(**building.model_dump(exclude={"is_scenario_object"}))
            )
        await conn.execute(statement)
        await conn.commit()
        return await get_scenario_physical_object_by_id_from_db(conn, building.physical_object_id)

    statement = (
        insert(projects_physical_objects_data)
        .from_select(
            [
                "physical_object_type_id",
                "name",
                "properties",
                "public_physical_object_id",
            ],
            select(
                physical_objects_data.c.physical_object_type_id,
                physical_objects_data.c.name,
                physical_objects_data.c.properties,
                literal(building.physical_object_id).label("public_physical_object_id"),
            ).where(physical_objects_data.c.physical_object_id == building.physical_object_id),
        )
        .returning(projects_physical_objects_data.c.physical_object_id)
    )
    scenario_physical_object_id = (await conn.execute(statement)).scalar_one()

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == scenario.project_id)
    ).alias("project_geometry")

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_not(None),
        )
        .alias("public_urban_object_ids")
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
            urban_objects_data.c.physical_object_id == building.physical_object_id,
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
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
                        "physical_object_id": scenario_physical_object_id,
                        "public_service_id": row.service_id,
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
            .where(projects_urban_objects_data.c.public_physical_object_id == building.physical_object_id)
            .values(physical_object_id=scenario_physical_object_id, public_physical_object_id=None)
        )
    )

    statement = insert(projects_buildings_data).values(
        **building.model_dump(exclude={"is_scenario_object", "physical_object_id"}),
        physical_object_id=scenario_physical_object_id,
    )

    await conn.execute(statement)
    await conn.commit()
    return await get_scenario_physical_object_by_id_from_db(conn, scenario_physical_object_id)


async def patch_building_to_db(
    conn: AsyncConnection,
    building: ScenarioBuildingPatch,
    scenario_id: int,
    building_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> ScenarioPhysicalObjectDTO:
    """Update building for given scenario."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if is_scenario_object:
        statement = select(projects_buildings_data).where(projects_buildings_data.c.building_id == building_id)
    else:
        statement = select(buildings_data).where(buildings_data.c.building_id == building_id)
    requested_building = (await conn.execute(statement)).mappings().one_or_none()
    if requested_building is None:
        raise EntityNotFoundById(building_id, "building")

    if not await check_existence(
        conn,
        projects_buildings_data if is_scenario_object else buildings_data,
        conditions={"building_id": building_id},
    ):
        raise EntityNotFoundById(building_id, "building")

    values = extract_values_from_model(building, exclude_unset=True)

    if is_scenario_object:
        statement = (
            update(projects_buildings_data)
            .where(projects_buildings_data.c.building_id == building_id)
            .values(**values)
            .returning(projects_buildings_data.c.physical_object_id)
        )
        scenario_physical_object_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_physical_objects_data)
            .from_select(
                [
                    "physical_object_type_id",
                    "name",
                    "properties",
                    "public_physical_object_id",
                ],
                select(
                    physical_objects_data.c.physical_object_type_id,
                    physical_objects_data.c.name,
                    physical_objects_data.c.properties,
                    literal(requested_building.physical_object_id).label("public_physical_object_id"),
                ).where(physical_objects_data.c.physical_object_id == requested_building.physical_object_id),
            )
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        scenario_physical_object_id = (await conn.execute(statement)).scalar_one()

        project_geometry = (
            select(projects_territory_data.c.geometry).where(
                projects_territory_data.c.project_id == scenario.project_id
            )
        ).alias("project_geometry")

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .alias("public_urban_object_ids")
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
                urban_objects_data.c.physical_object_id == requested_building.physical_object_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
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
                            "physical_object_id": scenario_physical_object_id,
                            "public_service_id": row.service_id,
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
                .where(projects_urban_objects_data.c.public_physical_object_id == requested_building.physical_object_id)
                .values(physical_object_id=scenario_physical_object_id, public_physical_object_id=None)
            )
        )

        statement = insert(projects_buildings_data).values(
            physical_object_id=scenario_physical_object_id,
            properties=values.get("properties", requested_building.properties),
            floors=values.get("floors", requested_building.floors),
            building_area_official=values.get("building_area_official", requested_building.building_area_official),
            building_area_modeled=values.get("building_area_modeled", requested_building.building_area_modeled),
            project_type=values.get("project_type", requested_building.project_type),
            floor_type=values.get("floor_type", requested_building.floor_type),
            wall_material=values.get("wall_material", requested_building.wall_material),
            built_year=values.get("built_year", requested_building.built_year),
            exploitation_start_year=values.get("exploitation_start_year", requested_building.exploitation_start_year),
        )

        await conn.execute(statement)

    await conn.commit()
    return await get_scenario_physical_object_by_id_from_db(conn, scenario_physical_object_id)


async def delete_building_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    building_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> dict[str, str]:
    """Delete building for given scenario."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if is_scenario_object:
        statement = select(projects_buildings_data).where(projects_buildings_data.c.building_id == building_id)
    else:
        statement = select(buildings_data).where(buildings_data.c.building_id == building_id)
    requested_building = (await conn.execute(statement)).mappings().one_or_none()
    if requested_building is None:
        raise EntityNotFoundById(building_id, "building")

    if not await check_existence(
        conn,
        projects_buildings_data if is_scenario_object else buildings_data,
        conditions={"building_id": building_id},
    ):
        raise EntityNotFoundById(building_id, "building")

    if is_scenario_object:
        await conn.execute(delete(projects_buildings_data).where(projects_buildings_data.c.building_id == building_id))
    else:
        statement = (
            insert(projects_physical_objects_data)
            .from_select(
                [
                    "physical_object_type_id",
                    "name",
                    "properties",
                    "public_physical_object_id",
                ],
                select(
                    physical_objects_data.c.physical_object_type_id,
                    physical_objects_data.c.name,
                    physical_objects_data.c.properties,
                    literal(requested_building.physical_object_id).label("public_physical_object_id"),
                ).where(physical_objects_data.c.physical_object_id == requested_building.physical_object_id),
            )
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        scenario_physical_object_id = (await conn.execute(statement)).scalar_one()

        project_geometry = (
            select(projects_territory_data.c.geometry).where(
                projects_territory_data.c.project_id == scenario.project_id
            )
        ).alias("project_geometry")

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .alias("public_urban_object_ids")
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
                urban_objects_data.c.physical_object_id == requested_building.physical_object_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
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
                            "physical_object_id": scenario_physical_object_id,
                            "public_service_id": row.service_id,
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
                .where(projects_urban_objects_data.c.public_physical_object_id == requested_building.physical_object_id)
                .values(physical_object_id=scenario_physical_object_id, public_physical_object_id=None)
            )
        )

    await conn.commit()
    return {"status": "ok"}
