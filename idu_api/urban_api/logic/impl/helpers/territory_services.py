"""Territories services internal logic is defined here."""

from collections import defaultdict
from collections.abc import Callable
from typing import Any, Literal

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import PageDTO, ServiceDTO, ServicesCountCapacityDTO, ServiceTypesDTO, ServiceWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.impl.helpers.territory_objects import check_territory_existence
from idu_api.urban_api.utils.pagination import paginate_dto

func: Callable


async def get_service_types_by_territory_id_from_db(conn: AsyncConnection, territory_id: int) -> list[ServiceTypesDTO]:
    """Get all service types that are located in given territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    territories_cte = (
        select(territories_data.c.territory_id)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )

    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id).where(territories_data.c.parent_id == territories_cte.c.territory_id)
    )

    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            territories_data.join(
                object_geometries_data,
                object_geometries_data.c.territory_id == territories_data.c.territory_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                services_data,
                services_data.c.service_id == urban_objects_data.c.service_id,
            )
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(territories_data.c.territory_id.in_(select(territories_cte)))
        .order_by(service_types_dict.c.service_type_id)
        .distinct()
    )
    service_types = (await conn.execute(statement)).mappings().all()

    return [ServiceTypesDTO(**s) for s in service_types]


async def get_services_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    service_type_id: int | None,
    urban_function_id: int | None,
    name: str | None,
    include_child_territories: bool,
    cities_only: bool,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None = "asc",
    paginate: bool = False,
) -> list[ServiceDTO] | PageDTO[ServiceDTO]:
    """Get list of services by territory id."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
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
        )
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                service_types_dict.c.urban_function_id == urban_functions_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .distinct()
    )

    if include_child_territories:
        territories_cte = (
            select(territories_data.c.territory_id, territories_data.c.is_city)
            .where(territories_data.c.territory_id == territory_id)
            .cte(recursive=True)
        )
        territories_cte = territories_cte.union_all(
            select(territories_data.c.territory_id, territories_data.c.is_city).where(
                territories_data.c.parent_id == territories_cte.c.territory_id
            )
        )

        if cities_only:
            territories_cte = select(territories_cte.c.territory_id).where(territories_cte.c.is_city.is_(cities_only))

        statement = statement.where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(object_geometries_data.c.territory_id == territory_id)

    if service_type_id is not None and urban_function_id is not None:
        raise EntityNotFoundByParams("service type and urban function", service_type_id, urban_function_id)
    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
    elif urban_function_id is not None:
        functions_cte = (
            select(urban_functions_dict.c.urban_function_id)
            .where(urban_functions_dict.c.urban_function_id == urban_function_id)
            .cte(recursive=True)
        )
        functions_cte = functions_cte.union_all(
            select(urban_functions_dict.c.urban_function_id).where(
                urban_functions_dict.c.parent_urban_function_id == functions_cte.c.urban_function_id
            )
        )
        statement = statement.where(service_types_dict.c.urban_function_id.in_(select(functions_cte)))
    if name is not None:
        statement = statement.where(services_data.c.name.ilike(f"%{name}%"))
    if order_by is not None:
        order = services_data.c.created_at if order_by == "created_at" else services_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(services_data.c.service_id.desc())
        else:
            statement = statement.order_by(services_data.c.service_id)

    if paginate:
        return await paginate_dto(conn, statement, transformer=lambda x: [ServiceDTO(**item) for item in x])

    result = (await conn.execute(statement)).mappings().all()
    return [ServiceDTO(**service) for service in result]


async def get_services_with_geometry_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    service_type_id: int | None,
    urban_function_id: int | None,
    name: str | None,
    include_child_territories: bool,
    cities_only: bool,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None = "asc",
    paginate: bool = False,
) -> list[ServiceWithGeometryDTO] | PageDTO[ServiceWithGeometryDTO]:
    """Get list of services with objects geometries by territory id."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
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
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                service_types_dict.c.urban_function_id == urban_functions_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .distinct()
    )

    if include_child_territories:
        territories_cte = (
            select(territories_data.c.territory_id, territories_data.c.is_city)
            .where(territories_data.c.territory_id == territory_id)
            .cte(recursive=True)
        )
        territories_cte = territories_cte.union_all(
            select(territories_data.c.territory_id, territories_data.c.is_city).where(
                territories_data.c.parent_id == territories_cte.c.territory_id
            )
        )

        if cities_only:
            territories_cte = select(territories_cte.c.territory_id).where(territories_cte.c.is_city.is_(cities_only))

        statement = statement.where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(object_geometries_data.c.territory_id == territory_id)

    if service_type_id is not None and urban_function_id is not None:
        raise EntityNotFoundByParams("service type and urban function", service_type_id, urban_function_id)
    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
    elif urban_function_id is not None:
        functions_cte = (
            select(urban_functions_dict.c.urban_function_id)
            .where(urban_functions_dict.c.urban_function_id == urban_function_id)
            .cte(recursive=True)
        )
        functions_cte = functions_cte.union_all(
            select(urban_functions_dict.c.urban_function_id).where(
                urban_functions_dict.c.parent_urban_function_id == functions_cte.c.urban_function_id
            )
        )
        statement = statement.where(service_types_dict.c.urban_function_id.in_(select(functions_cte)))
    if name is not None:
        statement = statement.where(services_data.c.name.ilike(f"%{name}%"))
    if order_by is not None:
        order = services_data.c.created_at if order_by == "created_at" else services_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(services_data.c.service_id.desc())
        else:
            statement = statement.order_by(services_data.c.service_id)

    if paginate:
        return await paginate_dto(conn, statement, transformer=lambda x: [ServiceWithGeometryDTO(**item) for item in x])

    result = (await conn.execute(statement)).mappings().all()
    return [ServiceWithGeometryDTO(**service) for service in result]


async def get_services_capacity_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    level: int,
    service_type_id: int | None,
) -> list[ServicesCountCapacityDTO]:
    """Get summary capacity and count of services for sub-territories of given territory at the given level."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    territories_cte = (
        select(territories_data.c.territory_id, territories_data.c.parent_id, territories_data.c.level)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )
    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id, territories_data.c.parent_id, territories_data.c.level).where(
            territories_data.c.parent_id == territories_cte.c.territory_id
        )
    )

    level_territories = select(territories_cte).where(territories_cte.c.level >= level).alias("level_territories")
    territories_list = (await conn.execute(select(level_territories))).mappings().all()

    territory_ids = [territory.territory_id for territory in territories_list]

    statement = (
        select(
            level_territories.c.territory_id,
            func.count(services_data.c.service_id).label("count"),
            func.coalesce(func.sum(services_data.c.capacity_real), 0).label("capacity"),
        )
        .select_from(
            level_territories.outerjoin(
                object_geometries_data, level_territories.c.territory_id == object_geometries_data.c.territory_id
            )
            .outerjoin(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
        )
        .group_by(level_territories.c.territory_id)
    )

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)

    services_data_results = (await conn.execute(statement)).mappings().all()

    services_data_dict = {result["territory_id"]: result for result in services_data_results}

    for t_id in territory_ids:
        if t_id not in services_data_dict:
            services_data_dict[t_id] = {"territory_id": t_id, "count": 0, "capacity": 0}

    hierarchy = defaultdict(list)
    for territory in territories_list:
        hierarchy[territory.parent_id].append(territory.territory_id)

    def build_hierarchy(tid: int) -> dict[str, Any]:
        total_count = services_data_dict[tid]["count"]
        total_capacity = services_data_dict[tid]["capacity"]

        for child_id in hierarchy.get(tid, []):
            child_data = build_hierarchy(child_id)
            total_count += child_data["count"]
            total_capacity += child_data["capacity"]

        return {"territory_id": tid, "count": total_count, "capacity": total_capacity}

    result = []
    for territory in territories_list:
        if territory.level == level:
            result.append(build_hierarchy(territory.territory_id))

    return [ServicesCountCapacityDTO(**territory) for territory in result]
