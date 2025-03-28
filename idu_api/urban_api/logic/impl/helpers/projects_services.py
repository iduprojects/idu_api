"""Projects services internal logic is defined here."""

from collections import defaultdict

from geoalchemy2.functions import ST_Intersects, ST_Within
from sqlalchemy import delete, insert, literal, or_, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

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
    ScenarioUrbanObjectDTO,
    ServiceDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario, get_project_by_scenario_id
from idu_api.urban_api.logic.impl.helpers.projects_urban_objects import get_scenario_urban_object_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import (
    check_existence,
    extract_values_from_model,
    get_context_territories_geometry,
)
from idu_api.urban_api.schemas import ScenarioServicePost, ServicePatch, ServicePut


async def get_services_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioServiceDTO]:
    """Get services by scenario identifier."""

    project = await get_project_by_scenario_id(conn, scenario_id, user)

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
    ).scalar_subquery()

    # Шаг 1: Получить все public_urban_object_id для данного scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    # Шаг 2: Собрать все записи из public.urban_objects_data по собранным public_urban_object_id
    public_urban_objects_query = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
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
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
        )
    )

    # Шаг 3: Собрать все записи из user_projects.urban_objects_data для данного сценария
    scenario_urban_objects_query = (
        select(
            projects_services_data.c.service_id,
            projects_services_data.c.name,
            projects_services_data.c.capacity,
            projects_services_data.c.is_capacity_real,
            projects_services_data.c.properties,
            projects_services_data.c.created_at,
            projects_services_data.c.updated_at,
            services_data.c.service_id.label("public_service_id"),
            services_data.c.name.label("public_name"),
            services_data.c.capacity.label("public_capacity"),
            services_data.c.is_capacity_real.label("public_is_capacity_real"),
            services_data.c.properties.label("public_properties"),
            services_data.c.created_at.label("public_created_at"),
            services_data.c.updated_at.label("public_updated_at"),
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
        )
    )

    if service_type_id is not None and urban_function_id is not None:
        raise EntityNotFoundByParams("service type and urban function", service_type_id, urban_function_id)
    if service_type_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            service_types_dict.c.service_type_id == service_type_id
        )
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            service_types_dict.c.service_type_id == service_type_id
        )
    elif urban_function_id is not None:
        urban_functions_cte = (
            select(
                urban_functions_dict.c.urban_function_id,
                urban_functions_dict.c.parent_urban_function_id,
            )
            .where(urban_functions_dict.c.urban_function_id == urban_function_id)
            .cte(recursive=True)
        )
        urban_functions_cte = urban_functions_cte.union_all(
            select(
                urban_functions_dict.c.urban_function_id,
                urban_functions_dict.c.parent_urban_function_id,
            ).join(
                urban_functions_cte,
                urban_functions_dict.c.parent_urban_function_id == urban_functions_cte.c.urban_function_id,
            )
        )
        public_urban_objects_query = public_urban_objects_query.where(
            urban_functions_dict.c.urban_function_id.in_(select(urban_functions_cte))
        )
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            urban_functions_dict.c.urban_function_id.in_(select(urban_functions_cte))
        )

    rows = (await conn.execute(public_urban_objects_query)).mappings().all()
    public_objects = []
    for row in rows:
        public_objects.append({**row, "is_scenario_object": False})

    rows = (await conn.execute(scenario_urban_objects_query)).mappings().all()
    scenario_objects = []
    for row in rows:
        is_scenario_service = row.service_id is not None and row.public_service_id is None
        if row.service_id is not None or row.public_service_id is not None:
            scenario_objects.append(
                {
                    "service_id": row.service_id or row.public_service_id,
                    "service_type_id": row.service_type_id,
                    "service_type_name": row.service_type_name,
                    "urban_function_id": row.urban_function_id,
                    "urban_function_name": row.urban_function_name,
                    "service_type_capacity_modeled": row.service_type_capacity_modeled,
                    "service_type_code": row.service_type_code,
                    "infrastructure_type": row.infrastructure_type,
                    "service_type_properties": row.service_type_properties,
                    "territory_type_id": row.territory_type_id,
                    "territory_type_name": row.territory_type_name,
                    "territory_id": row.territory_id,
                    "territory_name": row.territory_name,
                    "name": row.name if is_scenario_service else row.public_name,
                    "capacity": row.capacity if is_scenario_service else row.public_capacity,
                    "is_capacity_real": row.is_capacity_real if is_scenario_service else row.public_is_capacity_real,
                    "properties": row.properties if is_scenario_service else row.public_properties,
                    "created_at": row.created_at if is_scenario_service else row.public_created_at,
                    "updated_at": row.updated_at if is_scenario_service else row.public_updated_at,
                    "is_scenario_object": is_scenario_service,
                }
            )

    grouped_objects = defaultdict(lambda: {"territories": []})
    for obj in public_objects + scenario_objects:
        service_id = obj["service_id"]
        is_scenario_service = obj["is_scenario_object"]
        key = service_id if not is_scenario_service else f"scenario_{service_id}"

        if key not in grouped_objects:
            grouped_objects[key].update({k: v for k, v in obj.items() if k in ScenarioServiceDTO.fields()})

        territory = {"territory_id": obj["territory_id"], "name": obj["territory_name"]}
        grouped_objects[key]["territories"].append(territory)

    return [ScenarioServiceDTO(**row) for row in grouped_objects.values()]


async def get_context_services_from_db(
    conn: AsyncConnection,
    project_id: int,
    user: UserDTO | None,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ServiceDTO]:
    """Get list of services for 'context' of the project territory."""

    context_geom, context_ids = await get_context_territories_geometry(conn, project_id, user)

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

    statement = select(
        services_data,
        service_types_dict.c.urban_function_id,
        urban_functions_dict.c.name.label("urban_function_name"),
        service_types_dict.c.name.label("service_type_name"),
        service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
        service_types_dict.c.code.label("service_type_code"),
        service_types_dict.c.infrastructure_type,
        service_types_dict.c.properties.label("service_type_properties"),
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.territory_id,
        territories_data.c.name.label("territory_name"),
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

    if service_type_id is not None and urban_function_id is not None:
        raise EntityNotFoundByParams("service type and urban function", service_type_id, urban_function_id)
    if service_type_id is not None:
        statement = statement.where(service_types_dict.c.service_type_id == service_type_id)
    elif urban_function_id is not None:
        urban_functions_cte = (
            select(
                urban_functions_dict.c.urban_function_id,
                urban_functions_dict.c.parent_urban_function_id,
            )
            .where(urban_functions_dict.c.urban_function_id == urban_function_id)
            .cte(recursive=True)
        )
        urban_functions_cte = urban_functions_cte.union_all(
            select(
                urban_functions_dict.c.urban_function_id,
                urban_functions_dict.c.parent_urban_function_id,
            ).join(
                urban_functions_cte,
                urban_functions_dict.c.parent_urban_function_id == urban_functions_cte.c.urban_function_id,
            )
        )
        statement = statement.where(urban_functions_dict.c.urban_function_id.in_(select(urban_functions_cte)))

    result = (await conn.execute(statement)).mappings().all()

    grouped_data = defaultdict(lambda: {"territories": []})
    for row in result:
        key = row.service_id
        if key not in grouped_data:
            grouped_data[key].update({k: v for k, v in row.items() if k in ServiceDTO.fields()})

        territory = {"territory_id": row.territory_id, "name": row.territory_name}
        grouped_data[key]["territories"].append(territory)

    return [ServiceDTO(**row) for row in grouped_data.values()]


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

    project = await get_project_by_scenario_id(conn, scenario_id, user, to_edit=True)

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
            raise EntityAlreadyExists("scenario service", service_id)

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
                urban_objects_data.c.service_id == service_id,
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

    project = await get_project_by_scenario_id(conn, scenario_id, user, to_edit=True)

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
            raise EntityAlreadyExists("scenario service", service_id)

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
                urban_objects_data.c.service_id == service_id,
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

    project = await get_project_by_scenario_id(conn, scenario_id, user, to_edit=True)

    if not await check_existence(
        conn,
        projects_services_data if is_scenario_object else services_data,
        conditions={"service_id": service_id},
    ):
        raise EntityNotFoundById(service_id, "service")

    if is_scenario_object:
        statement = delete(projects_services_data).where(projects_services_data.c.service_id == service_id)
        await conn.execute(statement)
    else:
        statement = delete(projects_urban_objects_data).where(
            projects_urban_objects_data.c.public_service_id == service_id
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
                urban_objects_data.c.service_id == service_id,
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
