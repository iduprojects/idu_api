"""Projects geometries internal logic is defined here."""

from collections import defaultdict

from geoalchemy2.functions import (
    ST_AsEWKB,
    ST_Centroid,
    ST_Intersection,
    ST_Intersects,
    ST_IsEmpty,
    ST_Within,
)
from sqlalchemy import delete, insert, literal, or_, select, union_all, update
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
    ScenarioGeometryDTO,
    ScenarioGeometryWithAllObjectsDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyEdited, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario
from idu_api.urban_api.logic.impl.helpers.utils import (
    check_existence,
    extract_values_from_model,
    get_context_territories_geometry,
    include_child_territories_cte,
)
from idu_api.urban_api.schemas import ObjectGeometryPatch, ObjectGeometryPut
from idu_api.urban_api.utils.query_filters import EqFilter, RecursiveFilter, apply_filters


async def get_geometries_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    physical_object_id: int | None,
    service_id: int | None,
) -> list[ScenarioGeometryDTO]:
    """Get geometries by scenario identifier."""

    project = await check_scenario(conn, scenario_id, user, return_value=True)

    project_geometry = None
    territories_cte = None
    if not project.is_regional:
        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
        ).scalar_subquery()
    else:
        territories_cte = include_child_territories_cte(project.territory_id)

    # Step 1: Get all the public_urban_object_id for a given scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    # Step 2: Collect all geometries from `public.urban_objects_data`
    public_urban_objects_query = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
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
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, project_geometry) if not project.is_regional else True,
            (
                object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                if project.is_regional
                else True
            ),
        )
    )

    # Apply optional filters
    if physical_object_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_objects_data.c.physical_object_id == physical_object_id
        )
    if service_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(services_data.c.service_id == service_id)

    rows = (await conn.execute(public_urban_objects_query)).mappings().all()

    public_objects = []
    for row in rows:
        public_objects.append(
            {
                "object_geometry_id": row.object_geometry_id,
                "territory_id": row.territory_id,
                "territory_name": row.territory_name,
                "geometry": row.geometry,
                "centre_point": row.centre_point,
                "address": row.address,
                "osm_id": row.osm_id,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "is_scenario_object": False,
            }
        )

    # Step 3: Collect all geometries from `user_projects.urban_objects_data`
    scenario_urban_objects_query = (
        select(
            projects_urban_objects_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            ST_AsEWKB(projects_object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_object_geometries_data.c.centre_point).label("centre_point"),
            projects_object_geometries_data.c.created_at,
            projects_object_geometries_data.c.updated_at,
            object_geometries_data.c.object_geometry_id.label("public_object_geometry_id"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("public_geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("public_centre_point"),
            object_geometries_data.c.created_at.label("public_created_at"),
            object_geometries_data.c.updated_at.label("public_updated_at"),
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                ),
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
        )
    )

    # Apply optional filters
    if physical_object_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            physical_objects_data.c.physical_object_id == physical_object_id
        )
    if service_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(services_data.c.service_id == service_id)

    rows = (await conn.execute(scenario_urban_objects_query)).mappings().all()

    scenario_objects = []
    for row in rows:
        is_scenario_geometry = row.object_geometry_id is not None and row.public_object_geometry_id is None
        scenario_objects.append(
            {
                "object_geometry_id": row.object_geometry_id or row.public_object_geometry_id,
                "territory_id": row.territory_id,
                "territory_name": row.territory_name,
                "geometry": row.geometry if is_scenario_geometry else row.public_geometry,
                "centre_point": row.centre_point if is_scenario_geometry else row.public_centre_point,
                "address": row.address if is_scenario_geometry else row.public_address,
                "osm_id": row.osm_id if is_scenario_geometry else row.public_osm_id,
                "created_at": row.created_at if is_scenario_geometry else row.public_created_at,
                "updated_at": row.updated_at if is_scenario_geometry else row.public_updated_at,
                "is_scenario_object": is_scenario_geometry,
            }
        )

    grouped_objects = defaultdict()
    for obj in public_objects + scenario_objects:
        geometry_id = obj["object_geometry_id"]
        is_scenario_geometry = obj["is_scenario_object"]

        existing_entry = grouped_objects.get(geometry_id)
        if existing_entry is None:
            grouped_objects[geometry_id] = {
                "object_geometry_id": geometry_id,
                "territory_id": obj.get("territory_id"),
                "territory_name": obj.get("territory_name"),
                "geometry": obj.get("geometry"),
                "centre_point": obj.get("centre_point"),
                "address": obj.get("address"),
                "osm_id": obj.get("osm_id"),
                "created_at": obj.get("created_at"),
                "updated_at": obj.get("updated_at"),
                "is_scenario_object": is_scenario_geometry,
            }
        elif existing_entry.get("is_scenario_object") != is_scenario_geometry:
            grouped_objects[-geometry_id] = {
                "object_geometry_id": geometry_id,
                "territory_id": obj.get("territory_id"),
                "territory_name": obj.get("territory_name"),
                "geometry": obj.get("geometry"),
                "centre_point": obj.get("centre_point"),
                "address": obj.get("address"),
                "osm_id": obj.get("osm_id"),
                "created_at": obj.get("created_at"),
                "updated_at": obj.get("updated_at"),
                "is_scenario_object": is_scenario_geometry,
            }

    return [ScenarioGeometryDTO(**row) for row in list(grouped_objects.values())]


async def get_geometries_with_all_objects_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    physical_object_type_id: int | None,
    service_type_id: int | None,
    physical_object_function_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioGeometryWithAllObjectsDTO]:
    """Get geometries with list of physical objects and services by scenario identifier."""

    project = await check_scenario(conn, scenario_id, user, return_value=True)

    project_geometry = None
    territories_cte = None
    if not project.is_regional:
        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
        ).scalar_subquery()
    else:
        territories_cte = include_child_territories_cte(project.territory_id)

    # Step 1: Get all the public_urban_object_id for a given scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    # Step 2: Collect all geometries from `public.urban_objects_data`
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    public_urban_objects_query = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties.label("service_properties"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            literal(False).label("is_scenario_geometry"),
            literal(False).label("is_scenario_physical_object"),
            literal(False).label("is_scenario_service"),
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
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
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
            ST_Within(object_geometries_data.c.geometry, project_geometry) if not project.is_regional else True,
            (
                object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
                if project.is_regional
                else True
            ),
        )
    )

    coalesce_building_columns = [
        coalesce(up_col, pub_col).label(pub_col.name)
        for pub_col, up_col in zip(buildings_data.c, projects_buildings_data.c)
        if pub_col.name not in ("physical_object_id", "properties")
    ]

    # Step 3: Collect all geometries from `user_projects.urban_objects_data`
    scenario_urban_objects_query = (
        select(
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            coalesce(projects_physical_objects_data.c.properties, physical_objects_data.c.properties).label(
                "physical_object_properties"
            ),
            *coalesce_building_columns,
            coalesce(projects_buildings_data.c.properties, buildings_data.c.properties).label("building_properties"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_object_geometries_data.c.address, object_geometries_data.c.address).label("address"),
            coalesce(projects_object_geometries_data.c.osm_id, object_geometries_data.c.osm_id).label("osm_id"),
            coalesce(
                ST_AsEWKB(projects_object_geometries_data.c.geometry), ST_AsEWKB(object_geometries_data.c.geometry)
            ).label("geometry"),
            coalesce(
                ST_AsEWKB(projects_object_geometries_data.c.centre_point),
                ST_AsEWKB(object_geometries_data.c.centre_point),
            ).label("centre_point"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(projects_services_data.c.is_capacity_real, services_data.c.is_capacity_real).label(
                "is_capacity_real"
            ),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("service_properties"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
            (projects_urban_objects_data.c.physical_object_id.isnot(None)).label("is_scenario_physical_object"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_service"),
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
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

    union_query = union_all(
        public_urban_objects_query,
        scenario_urban_objects_query,
    ).cte(name="union_query")
    statement = select(union_query)

    # Apply optional filters
    statement = apply_filters(
        statement,
        EqFilter(union_query, "physical_object_type_id", physical_object_type_id),
        RecursiveFilter(
            union_query,
            "physical_object_function_id",
            physical_object_function_id,
            physical_object_functions_dict,
        ),
        EqFilter(union_query, "service_type_id", service_type_id),
        RecursiveFilter(union_query, "urban_function_id", urban_function_id, urban_functions_dict),
    )

    result = (await conn.execute(statement)).mappings().all()

    def initialize_group(group, obj, is_scenario_geometry):
        group.update(
            {
                "object_geometry_id": obj["object_geometry_id"],
                "territory_id": obj["territory_id"],
                "territory_name": obj["territory_name"],
                "geometry": obj["geometry"],
                "centre_point": obj["centre_point"],
                "address": obj["address"],
                "osm_id": obj["osm_id"],
                "is_scenario_object": is_scenario_geometry,
            }
        )

    def add_physical_object(group, obj):
        phys_obj_id = obj["physical_object_id"]
        is_scenario_physical_object = obj["is_scenario_physical_object"]
        existing_phys_obj = group["physical_objects"].get(phys_obj_id)
        key = phys_obj_id if not is_scenario_physical_object else f"scenario_{phys_obj_id}"
        if existing_phys_obj is None:
            group["physical_objects"][key] = {
                "physical_object_id": obj["physical_object_id"],
                "physical_object_type": {
                    "id": obj["physical_object_type_id"],
                    "name": obj["physical_object_type_name"],
                },
                "name": obj["physical_object_name"],
                "building": (
                    {
                        "id": obj["building_id"],
                        "properties": obj["building_properties"],
                        "floors": obj["floors"],
                        "building_area_official": obj["building_area_official"],
                        "building_area_modeled": obj["building_area_modeled"],
                        "project_type": obj["project_type"],
                        "floor_type": obj["floor_type"],
                        "wall_material": obj["wall_material"],
                        "built_year": obj["built_year"],
                        "exploitation_start_year": obj["exploitation_start_year"],
                    }
                    if obj["building_id"]
                    else None
                ),
                "is_scenario_object": is_scenario_physical_object,
            }

    def add_service(group, obj):
        service_id = obj["service_id"]
        is_scenario_service = obj["is_scenario_service"]
        existing_service = group["services"].get(service_id)
        key = service_id if not is_scenario_service else f"scenario_{service_id}"
        if existing_service is None:
            group["services"][key] = {
                "service_id": obj["service_id"],
                "service_type": {
                    "id": obj["service_type_id"],
                    "name": obj["service_type_name"],
                },
                "territory_type": (
                    {
                        "id": obj["territory_type_id"],
                        "name": obj["territory_type_name"],
                    }
                    if obj["territory_type_id"]
                    else None
                ),
                "name": obj["service_name"],
                "capacity": obj["capacity"],
                "is_capacity_real": obj["is_capacity_real"],
                "properties": obj["service_properties"],
                "is_scenario_object": is_scenario_service,
            }

    grouped_objects = defaultdict(lambda: {"physical_objects": defaultdict(dict), "services": defaultdict(dict)})

    for row in result:
        geometry_id = row["object_geometry_id"]
        is_scenario_geometry = row["is_scenario_geometry"]
        key = geometry_id if not is_scenario_geometry else f"scenario_{geometry_id}"

        if key not in grouped_objects:
            initialize_group(grouped_objects[key], row, is_scenario_geometry)

        add_physical_object(grouped_objects[key], row)
        if row["service_id"] is not None:
            add_service(grouped_objects[key], row)

    for key, group in grouped_objects.items():
        group["physical_objects"] = list(group["physical_objects"].values())
        group["services"] = list(group["services"].values())

    return [ScenarioGeometryWithAllObjectsDTO(**group) for group in grouped_objects.values()]


async def get_context_geometries_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    physical_object_id: int | None,
    service_id: int | None,
) -> list[ScenarioGeometryDTO]:
    """Get list of geometries for 'context' of the project territory."""

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

    # Step 3: Collect all geometries from `public` intersecting context geometry
    intersected_geom = ST_Intersection(object_geometries_data.c.geometry, context_geom)
    public_geoms_query = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
            ST_AsEWKB(intersected_geom).label("geometry"),
            ST_AsEWKB(ST_Centroid(intersected_geom)).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_object"),
        )
        .select_from(
            urban_objects_data.join(
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
        )
        .where(~ST_IsEmpty(intersected_geom))
        .distinct()
    )

    # Step 4: Collect all geometries from parent regional scenario intersecting context geometry
    geom_expr = ST_Intersection(
        coalesce(
            projects_object_geometries_data.c.geometry,
            object_geometries_data.c.geometry,
        ),
        context_geom,
    )
    regional_scenario_geoms_query = (
        select(
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
                projects_object_geometries_data.c.created_at,
                object_geometries_data.c.created_at,
            ).label("created_at"),
            coalesce(
                projects_object_geometries_data.c.updated_at,
                object_geometries_data.c.updated_at,
            ).label("updated_at"),
            ST_AsEWKB(geom_expr).label("geometry"),
            ST_AsEWKB(ST_Centroid(geom_expr)).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (object_geometries_data.c.object_geometry_id.isnot(None)).label("is_scenario_object"),
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                ),
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == parent_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            ~ST_IsEmpty(geom_expr),
        )
    )

    if physical_object_id is not None:
        public_geoms_query = public_geoms_query.where(urban_objects_data.c.physical_object_id == physical_object_id)
    if service_id is not None:
        public_geoms_query = public_geoms_query.where(urban_objects_data.c.service_id == service_id)

    union_query = union_all(
        public_geoms_query,
        regional_scenario_geoms_query,
    )
    result = (await conn.execute(union_query)).mappings().all()

    return [ScenarioGeometryDTO(**row) for row in result]


async def get_context_geometries_with_all_objects_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    physical_object_type_id: int | None,
    service_type_id: int | None,
    physical_object_function_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioGeometryWithAllObjectsDTO]:
    """Get geometries with lists of physical objects and services for 'context' of the project territory."""

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

    # Step 3: Collect all geometries from `public` intersecting context geometry
    intersected_geom = ST_Intersection(object_geometries_data.c.geometry, context_geom)
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    public_geoms_query = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(intersected_geom).label("geometry"),
            ST_AsEWKB(ST_Centroid(intersected_geom)).label("centre_point"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties.label("service_properties"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            literal(False).label("is_scenario_geometry"),
            literal(False).label("is_scenario_physical_object"),
            literal(False).label("is_scenario_service"),
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
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(~ST_IsEmpty(intersected_geom))
    )

    # Step 4: Collect all geometries from parent regional scenario
    geom_expr = ST_Intersection(
        coalesce(
            projects_object_geometries_data.c.geometry,
            object_geometries_data.c.geometry,
        ),
        context_geom,
    )
    coalesce_building_columns = [
        coalesce(up_col, pub_col).label(pub_col.name)
        for pub_col, up_col in zip(buildings_data.c, projects_buildings_data.c)
        if pub_col.name not in ("physical_object_id", "properties")
    ]
    regional_scenario_geoms_query = (
        select(
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            coalesce(projects_physical_objects_data.c.properties, physical_objects_data.c.properties).label(
                "physical_object_properties"
            ),
            *coalesce_building_columns,
            coalesce(projects_buildings_data.c.properties, buildings_data.c.properties).label("building_properties"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_object_geometries_data.c.address, object_geometries_data.c.address).label("address"),
            coalesce(projects_object_geometries_data.c.osm_id, object_geometries_data.c.osm_id).label("osm_id"),
            ST_AsEWKB(geom_expr).label("geometry"),
            ST_AsEWKB(ST_Centroid(geom_expr)).label("centre_point"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(projects_services_data.c.is_capacity_real, services_data.c.is_capacity_real).label(
                "is_capacity_real"
            ),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("service_properties"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
            (projects_urban_objects_data.c.physical_object_id.isnot(None)).label("is_scenario_physical_object"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_service"),
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
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
            projects_urban_objects_data.c.scenario_id == parent_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            ~ST_IsEmpty(geom_expr),
        )
    )

    union_query = union_all(
        public_geoms_query,
        regional_scenario_geoms_query,
    ).cte(name="union_query")
    statement = select(union_query)

    statement = apply_filters(
        statement,
        EqFilter(union_query, "physical_object_type_id", physical_object_type_id),
        RecursiveFilter(
            union_query,
            "physical_object_function_id",
            physical_object_function_id,
            physical_object_functions_dict,
        ),
        EqFilter(union_query, "service_type_id", service_type_id),
        RecursiveFilter(union_query, "urban_function_id", urban_function_id, urban_functions_dict),
    )

    result = (await conn.execute(statement)).mappings().all()

    def initialize_group(group, obj, is_scenario_geometry):
        group.update(
            {
                "object_geometry_id": obj["object_geometry_id"],
                "territory_id": obj["territory_id"],
                "territory_name": obj["territory_name"],
                "geometry": obj["geometry"],
                "centre_point": obj["centre_point"],
                "address": obj["address"],
                "osm_id": obj["osm_id"],
                "is_scenario_object": is_scenario_geometry,
            }
        )

    def add_physical_object(group, obj):
        phys_obj_id = obj["physical_object_id"]
        is_scenario_physical_object = obj["is_scenario_physical_object"]
        existing_phys_obj = group["physical_objects"].get(phys_obj_id)
        key = phys_obj_id if not is_scenario_physical_object else f"scenario_{phys_obj_id}"
        if existing_phys_obj is None:
            group["physical_objects"][key] = {
                "physical_object_id": obj["physical_object_id"],
                "physical_object_type": {
                    "id": obj["physical_object_type_id"],
                    "name": obj["physical_object_type_name"],
                },
                "name": obj["physical_object_name"],
                "building": (
                    {
                        "id": obj["building_id"],
                        "properties": obj["building_properties"],
                        "floors": obj["floors"],
                        "building_area_official": obj["building_area_official"],
                        "building_area_modeled": obj["building_area_modeled"],
                        "project_type": obj["project_type"],
                        "floor_type": obj["floor_type"],
                        "wall_material": obj["wall_material"],
                        "built_year": obj["built_year"],
                        "exploitation_start_year": obj["exploitation_start_year"],
                    }
                    if obj["building_id"]
                    else None
                ),
                "is_scenario_object": is_scenario_physical_object,
            }

    def add_service(group, obj):
        service_id = obj["service_id"]
        is_scenario_service = obj["is_scenario_service"]
        existing_service = group["services"].get(service_id)
        key = service_id if not is_scenario_service else f"scenario_{service_id}"
        if existing_service is None:
            group["services"][key] = {
                "service_id": obj["service_id"],
                "service_type": {
                    "id": obj["service_type_id"],
                    "name": obj["service_type_name"],
                },
                "territory_type": (
                    {
                        "id": obj["territory_type_id"],
                        "name": obj["territory_type_name"],
                    }
                    if obj["territory_type_id"]
                    else None
                ),
                "name": obj["service_name"],
                "capacity": obj["capacity"],
                "is_capacity_real": obj["is_capacity_real"],
                "properties": obj["service_properties"],
                "is_scenario_object": is_scenario_service,
            }

    grouped_objects = defaultdict(lambda: {"physical_objects": defaultdict(dict), "services": defaultdict(dict)})

    for row in result:
        geometry_id = row["object_geometry_id"]
        is_scenario_geometry = row["is_scenario_geometry"]
        key = geometry_id if not is_scenario_geometry else f"scenario_{geometry_id}"

        if key not in grouped_objects:
            initialize_group(grouped_objects[key], row, is_scenario_geometry)

        add_physical_object(grouped_objects[key], row)
        if row["service_id"] is not None:
            add_service(grouped_objects[key], row)

    for key, group in grouped_objects.items():
        group["physical_objects"] = list(group["physical_objects"].values())
        group["services"] = list(group["services"].values())

    return [ScenarioGeometryWithAllObjectsDTO(**group) for group in grouped_objects.values()]


async def get_scenario_object_geometry_by_id_from_db(
    conn: AsyncConnection, object_geometry_id: int
) -> ScenarioGeometryDTO:
    """Get scenario object geometry by identifier."""

    statement = (
        select(
            projects_object_geometries_data.c.object_geometry_id,
            projects_object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            ST_AsEWKB(projects_object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_object_geometries_data.c.centre_point).label("centre_point"),
            projects_object_geometries_data.c.created_at,
            projects_object_geometries_data.c.updated_at,
            literal(True).label("is_scenario_object"),
        )
        .select_from(
            projects_object_geometries_data.join(
                territories_data,
                territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
            )
        )
        .where(projects_object_geometries_data.c.object_geometry_id == object_geometry_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(object_geometry_id, "scenario object geometry")

    return ScenarioGeometryDTO(**result)


async def put_object_geometry_to_db(
    conn: AsyncConnection,
    object_geometry: ObjectGeometryPut,
    scenario_id: int,
    object_geometry_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> ScenarioGeometryDTO:
    """Update scenario object geometry by all its attributes."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn,
        projects_object_geometries_data if is_scenario_object else object_geometries_data,
        conditions={"object_geometry_id": object_geometry_id},
    ):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    if not await check_existence(conn, territories_data, conditions={"territory_id": object_geometry.territory_id}):
        raise EntityNotFoundById(object_geometry.territory_id, "territory")

    if not is_scenario_object:
        statement = (
            select(projects_object_geometries_data.c.object_geometry_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_object_geometries_data,
                    projects_object_geometries_data.c.object_geometry_id
                    == projects_urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_object_geometries_data.c.public_object_geometry_id == object_geometry_id,
            )
            .limit(1)
        )
        public_object_geometry = (await conn.execute(statement)).one_or_none()
        if public_object_geometry is not None:
            raise EntityAlreadyEdited("object geometry", scenario_id)

    if is_scenario_object:
        statement = (
            update(projects_object_geometries_data)
            .where(projects_object_geometries_data.c.object_geometry_id == object_geometry_id)
            .values(**extract_values_from_model(object_geometry, to_update=True))
            .returning(projects_object_geometries_data.c.object_geometry_id)
        )
        updated_object_geometry_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_object_geometries_data)
            .values(
                public_object_geometry_id=object_geometry_id,
                **extract_values_from_model(object_geometry),
            )
            .returning(projects_object_geometries_data.c.object_geometry_id)
        )
        updated_object_geometry_id = (await conn.execute(statement)).scalar_one()

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
                urban_objects_data.c.object_geometry_id == object_geometry_id,
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
                            "public_physical_object_id": row.physical_object_id,
                            "public_service_id": row.service_id,
                            "object_geometry_id": updated_object_geometry_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
        await conn.execute(
            (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.public_object_geometry_id == object_geometry_id)
                .values(object_geometry_id=updated_object_geometry_id, public_object_geometry_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_object_geometry_by_id_from_db(conn, updated_object_geometry_id)


async def patch_object_geometry_to_db(
    conn: AsyncConnection,
    object_geometry: ObjectGeometryPatch,
    scenario_id: int,
    object_geometry_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> ScenarioGeometryDTO:
    """Update scenario object geometry by only given attributes."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if is_scenario_object:
        statement = select(projects_object_geometries_data).where(
            projects_object_geometries_data.c.object_geometry_id == object_geometry_id
        )
    else:
        statement = select(object_geometries_data).where(
            object_geometries_data.c.object_geometry_id == object_geometry_id
        )
    requested_geometry = (await conn.execute(statement)).mappings().one_or_none()
    if requested_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    if object_geometry.territory_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": object_geometry.territory_id}):
            raise EntityNotFoundById(object_geometry.territory_id, "territory")

    if not is_scenario_object:
        statement = (
            select(projects_object_geometries_data.c.object_geometry_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_object_geometries_data,
                    projects_object_geometries_data.c.object_geometry_id
                    == projects_urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_object_geometries_data.c.public_object_geometry_id == object_geometry_id,
            )
            .limit(1)
        )
        public_object_geometry = (await conn.execute(statement)).scalar_one_or_none()
        if public_object_geometry is not None:
            raise EntityAlreadyEdited("object geometry", scenario_id)

    values = extract_values_from_model(object_geometry, exclude_unset=True, to_update=True)
    if is_scenario_object:
        statement = (
            update(projects_object_geometries_data)
            .where(projects_object_geometries_data.c.object_geometry_id == object_geometry_id)
            .values(**values)
            .returning(projects_object_geometries_data.c.object_geometry_id)
        )
        updated_object_geometry_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_object_geometries_data)
            .values(
                public_object_geometry_id=object_geometry_id,
                territory_id=values.get("territory_id", requested_geometry.territory_id),
                geometry=values.get("geometry", requested_geometry.geometry),
                centre_point=values.get("centre_point", requested_geometry.centre_point),
                address=values.get("address", requested_geometry.address),
                osm_id=values.get("osm_id", requested_geometry.osm_id),
            )
            .returning(projects_object_geometries_data.c.object_geometry_id)
        )
        updated_object_geometry_id = (await conn.execute(statement)).scalar_one()

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
                urban_objects_data.c.object_geometry_id == object_geometry_id,
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
                            "public_physical_object_id": row.physical_object_id,
                            "public_service_id": row.service_id,
                            "object_geometry_id": updated_object_geometry_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
        await conn.execute(
            (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.public_object_geometry_id == object_geometry_id)
                .values(object_geometry_id=updated_object_geometry_id, public_object_geometry_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_object_geometry_by_id_from_db(conn, updated_object_geometry_id)


async def delete_object_geometry_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    object_geometry_id: int,
    is_scenario_object: bool,
    user: UserDTO,
) -> dict:
    """Delete scenario physical object."""

    scenario = await check_scenario(conn, scenario_id, user, to_edit=True, return_value=True)

    if not await check_existence(
        conn, projects_object_geometries_data if is_scenario_object else object_geometries_data
    ):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    if not is_scenario_object:
        statement = (
            select(urban_objects_data.c.object_geometry_id)
            .select_from(
                projects_urban_objects_data.join(
                    urban_objects_data,
                    urban_objects_data.c.urban_object_id == projects_urban_objects_data.c.public_urban_object_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                urban_objects_data.c.object_geometry_id == object_geometry_id,
            )
            .limit(1)
        )
        public_urban_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_urban_object is not None:
            statement = (
                select(projects_urban_objects_data.c.public_object_geometry_id)
                .where(
                    projects_urban_objects_data.c.scenario_id == scenario_id,
                    projects_urban_objects_data.c.public_object_geometry_id == object_geometry_id,
                )
                .limit(1)
            )
            public_geometry = (await conn.execute(statement)).scalar_one_or_none()
            if public_geometry is None:
                raise EntityAlreadyEdited("object geometry", scenario_id)

    if is_scenario_object:
        statement = delete(projects_object_geometries_data).where(
            projects_object_geometries_data.c.object_geometry_id == object_geometry_id
        )
        await conn.execute(statement)
    else:
        statement = delete(projects_urban_objects_data).where(
            projects_urban_objects_data.c.public_object_geometry_id == object_geometry_id
        )
        await conn.execute(statement)

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
                urban_objects_data.c.object_geometry_id == object_geometry_id,
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

    await conn.commit()

    return {"status": "ok"}
