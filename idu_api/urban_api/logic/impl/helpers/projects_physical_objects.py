"""Projects physical objects internal logic is defined here."""

from collections import defaultdict
from datetime import datetime, timezone

from geoalchemy2.functions import ST_GeomFromWKB, ST_Intersects, ST_Within
from sqlalchemy import delete, insert, literal, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

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
from idu_api.urban_api.dto import PhysicalObjectDTO, ScenarioPhysicalObjectDTO, ScenarioUrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    EntityNotFoundByParams,
)
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario, get_project_by_scenario_id
from idu_api.urban_api.logic.impl.helpers.projects_urban_objects import get_scenario_urban_object_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import (
    SRID,
    check_existence,
    extract_values_from_model,
    get_context_territories_geometry,
)
from idu_api.urban_api.schemas import PhysicalObjectPatch, PhysicalObjectPut, PhysicalObjectWithGeometryPost


async def get_physical_objects_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[ScenarioPhysicalObjectDTO]:
    """Get physical objects by scenario identifier."""

    project = await get_project_by_scenario_id(conn, scenario_id, user_id)

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
    ).alias("project_geometry")

    # Шаг 1: Получить все public_urban_object_id для данного scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).alias("public_urban_object_ids")

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    project_building_columns = [
        col for col in projects_buildings_data.c if col.name not in ("physical_object_id", "properties")
    ]

    # Шаг 2: Собрать все записи из public.urban_objects_data по собранным public_urban_object_id
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
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
        )
    )

    # Шаг 3: Собрать все записи из user_projects.urban_objects_data для данного сценария
    scenario_urban_objects_query = (
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
            physical_objects_data.c.physical_object_id.label("public_physical_object_id"),
            physical_objects_data.c.name.label("public_name"),
            physical_objects_data.c.properties.label("public_properties"),
            physical_objects_data.c.created_at.label("public_created_at"),
            physical_objects_data.c.updated_at.label("public_updated_at"),
            *project_building_columns,
            projects_buildings_data.c.properties.label("building_properties"),
            buildings_data.c.building_id.label("public_building_id"),
            buildings_data.c.properties.label("public_building_properties"),
            buildings_data.c.floors.label("public_floors"),
            buildings_data.c.building_area_official.label("public_building_area_official"),
            buildings_data.c.building_area_modeled.label("public_building_area_modeled"),
            buildings_data.c.project_type.label("public_project_type"),
            buildings_data.c.floor_type.label("public_floor_type"),
            buildings_data.c.wall_material.label("public_wall_material"),
            buildings_data.c.built_year.label("public_built_year"),
            buildings_data.c.exploitation_start_year.label("public_exploitation_start_year"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
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
    )

    if physical_object_type_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id
        )
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id
        )
    elif physical_object_function_id is not None:
        physical_object_functions_cte = (
            select(
                physical_object_functions_dict.c.physical_object_function_id,
                physical_object_functions_dict.c.parent_id,
            )
            .where(physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id)
            .cte(recursive=True)
        )
        physical_object_functions_cte = physical_object_functions_cte.union_all(
            select(
                physical_object_functions_dict.c.physical_object_function_id,
                physical_object_functions_dict.c.parent_id,
            ).join(
                physical_object_functions_cte,
                physical_object_functions_dict.c.parent_id
                == physical_object_functions_cte.c.physical_object_function_id,
            )
        )
        public_urban_objects_query = public_urban_objects_query.where(
            physical_object_types_dict.c.physical_object_function_id.in_(
                select(physical_object_functions_cte.c.physical_object_function_id)
            )
        )
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            physical_object_functions_dict.c.physical_object_function_id.in_(
                select(physical_object_functions_cte.c.physical_object_function_id)
            )
        )

    rows = (await conn.execute(public_urban_objects_query)).mappings().all()
    public_objects = []
    for row in rows:
        public_objects.append({**row, "is_scenario_object": False})

    rows = (await conn.execute(scenario_urban_objects_query)).mappings().all()
    scenario_objects = []
    for row in rows:
        is_scenario_physical_object = row.physical_object_id is not None and row.public_physical_object_id is None
        scenario_objects.append(
            {
                "physical_object_id": row.physical_object_id or row.public_physical_object_id,
                "physical_object_type_id": row.physical_object_type_id,
                "physical_object_type_name": row.physical_object_type_name,
                "physical_object_function_id": row.physical_object_function_id,
                "physical_object_function_name": row.physical_object_function_name,
                "name": row.name if is_scenario_physical_object else row.public_name,
                "building_id": (row.building_id if is_scenario_physical_object else row.public_building_id),
                "building_properties": (
                    row.building_properties if is_scenario_physical_object else row.public_building_properties
                ),
                "floors": row.floors if is_scenario_physical_object else row.public_floors,
                "building_area_official": (
                    row.building_area_official if is_scenario_physical_object else row.public_building_area_official
                ),
                "building_area_modeled": (
                    row.building_area_modeled if is_scenario_physical_object else row.public_building_area_modeled
                ),
                "project_type": row.project_type if is_scenario_physical_object else row.public_project_type,
                "floor_type": row.floor_type if is_scenario_physical_object else row.public_floor_type,
                "wall_material": row.wall_material if is_scenario_physical_object else row.public_wall_material,
                "built_year": row.built_year if is_scenario_physical_object else row.public_built_year,
                "exploitation_start_year": (
                    row.exploitation_start_year if is_scenario_physical_object else row.public_exploitation_start_year
                ),
                "territory_id": row.territory_id,
                "territory_name": row.territory_name,
                "properties": row.properties if is_scenario_physical_object else row.public_properties,
                "created_at": row.created_at if is_scenario_physical_object else row.public_created_at,
                "updated_at": row.updated_at if is_scenario_physical_object else row.public_updated_at,
                "is_scenario_object": is_scenario_physical_object,
            }
        )

    grouped_objects = defaultdict(lambda: {"territories": []})
    for obj in public_objects + scenario_objects:
        physical_object_id = obj["physical_object_id"]
        is_scenario_physical_object = obj["is_scenario_object"]
        key = physical_object_id if not is_scenario_physical_object else f"scenario_{physical_object_id}"

        if key not in grouped_objects:
            grouped_objects[key].update({k: v for k, v in obj.items() if k in ScenarioPhysicalObjectDTO.fields()})

        territory = {"territory_id": obj["territory_id"], "name": obj["territory_name"]}
        grouped_objects[key]["territories"].append(territory)

    return [ScenarioPhysicalObjectDTO(**row) for row in grouped_objects.values()]


async def get_context_physical_objects_from_db(
    conn: AsyncConnection,
    project_id: int,
    user_id: str,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[PhysicalObjectDTO]:
    """Get list of physical objects for 'context' of the project territory."""

    context_geom, context_ids = await get_context_territories_geometry(conn, project_id, user_id)

    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            ).join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
        )
        .where(
            object_geometries_data.c.territory_id.in_(context_ids)
            | ST_Intersects(object_geometries_data.c.geometry, context_geom)
        )
        .cte(name="objects_intersecting")
    )

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]

    statement = select(
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

    if physical_object_type_id is not None and physical_object_function_id is not None:
        raise EntityNotFoundByParams(
            "physical object type and function", physical_object_type_id, physical_object_function_id
        )
    if physical_object_type_id is not None:
        statement = statement.where(physical_object_types_dict.c.physical_object_type_id == physical_object_type_id)

    elif physical_object_function_id is not None:
        physical_object_functions_cte = (
            select(
                physical_object_functions_dict.c.physical_object_function_id,
                physical_object_functions_dict.c.parent_id,
            )
            .where(physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id)
            .cte(recursive=True)
        )
        physical_object_functions_cte = physical_object_functions_cte.union_all(
            select(
                physical_object_functions_dict.c.physical_object_function_id,
                physical_object_functions_dict.c.parent_id,
            ).join(
                physical_object_functions_cte,
                physical_object_functions_dict.c.parent_id
                == physical_object_functions_cte.c.physical_object_function_id,
            )
        )
        statement = statement.where(
            physical_object_types_dict.c.physical_object_function_id.in_(
                select(physical_object_functions_cte.c.physical_object_function_id)
            )
        )

    result = (await conn.execute(statement)).mappings().all()

    grouped_data = defaultdict(lambda: {"territories": []})
    for row in result:
        key = row.physical_object_id
        if key not in grouped_data:
            grouped_data[key].update({k: v for k, v in row.items() if k in PhysicalObjectDTO.fields()})

        territory = {"territory_id": row.territory_id, "name": row.territory_name}
        grouped_data[key]["territories"].append(territory)

    return [PhysicalObjectDTO(**row) for row in grouped_data.values()]


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
    if result is None:
        raise EntityNotFoundById(physical_object_id, "scenario physical object")

    territories = [{"territory_id": row.territory_id, "name": row.territory_name} for row in result]
    physical_object = {k: v for k, v in result[0].items() if k in ScenarioPhysicalObjectDTO.fields()}

    return ScenarioPhysicalObjectDTO(**physical_object, territories=territories)


async def add_physical_object_with_geometry_to_db(
    conn: AsyncConnection,
    physical_object: PhysicalObjectWithGeometryPost,
    scenario_id: int,
    user_id: str,
) -> ScenarioUrbanObjectDTO:
    """Create scenario physical object with geometry."""

    await check_scenario(conn, scenario_id, user_id, to_edit=True)

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
    user_id: str,
) -> ScenarioPhysicalObjectDTO:
    """Update scenario physical object by all its attributes."""

    project = await get_project_by_scenario_id(conn, scenario_id, user_id, to_edit=True)

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
        )
        public_physical_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_physical_object is not None:
            raise EntityAlreadyExists("scenario physical object", physical_object_id)

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
            .values(**physical_object.model_dump(), updated_at=datetime.now(timezone.utc))
            .returning(projects_physical_objects_data.c.physical_object_id)
        )
        updated_physical_object_id = (await conn.execute(statement)).scalar_one()

        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
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
                urban_objects_data.c.physical_object_id == physical_object_id,
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
    user_id: str,
) -> ScenarioPhysicalObjectDTO:
    """Update scenario physical object by only given attributes."""

    project = await get_project_by_scenario_id(conn, scenario_id, user_id, to_edit=True)

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
        )
        public_physical_object = (await conn.execute(statement)).scalar_one_or_none()
        if public_physical_object is not None:
            raise EntityAlreadyExists("scenario physical object", physical_object_id)

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

        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
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
                urban_objects_data.c.physical_object_id == physical_object_id,
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
    user_id: str,
) -> dict:
    """Delete scenario physical object."""

    project = await get_project_by_scenario_id(conn, scenario_id, user_id, to_edit=True)

    if not await check_existence(
        conn,
        projects_physical_objects_data if is_scenario_object else physical_objects_data,
        conditions={"physical_object_id": physical_object_id},
    ):
        raise EntityNotFoundById(physical_object_id, "physical object")

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

        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
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
                urban_objects_data.c.physical_object_id == physical_object_id,
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


async def update_physical_objects_by_function_id_to_db(
    conn: AsyncConnection,
    physical_objects: list[PhysicalObjectWithGeometryPost],
    scenario_id: int,
    user_id: str,
    physical_object_function_id: int,
) -> list[ScenarioUrbanObjectDTO]:
    """Delete all physical objects by physical object function identifier
    and upload new objects with the same function for given scenario."""

    project = await get_project_by_scenario_id(conn, scenario_id, user_id, to_edit=True)

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
        .where(projects_territory_data.c.project_id == project.project_id)
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
