"""Territories services internal logic is defined here."""

from typing import Callable, Literal, Optional

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
from idu_api.urban_api.dto import PageDTO, ServiceDTO, ServiceWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.utils.pagination import paginate_dto

func: Callable


async def get_services_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    service_type_id: int | None,
    name: str | None,
    order_by: Optional[Literal["created_at", "updated_at"]],
    ordering: Optional[Literal["asc", "desc"]] = "asc",
    paginate: bool = False,
) -> list[ServiceDTO] | PageDTO[ServiceDTO]:
    """Get list of services by territory id."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    territories_cte = (
        select(territories_data.c.territory_id)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )

    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id)
        .where(territories_data.c.parent_id == territories_cte.c.territory_id)
    )

    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
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
        .where(object_geometries_data.c.territory_id.in_(territories_cte))
    ).distinct()

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
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
    name: str | None,
    order_by: Optional[Literal["created_at", "updated_at"]],
    ordering: Optional[Literal["asc", "desc"]] = "asc",
    paginate: bool = False,
) -> list[ServiceWithGeometryDTO] | PageDTO[ServiceWithGeometryDTO]:
    """Get list of services with objects geometries by territory id."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    territories_cte = (
        select(territories_data.c.territory_id)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )

    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id)
        .where(territories_data.c.parent_id == territories_cte.c.territory_id)
    )

    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            territory_types_dict.c.name.label("territory_type_name"),
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
        .where(object_geometries_data.c.territory_id.in_(territories_cte))
    ).distinct()

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
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
    service_type_id: int | None,
) -> int:
    """Get summary capacity of services for given territory.

    Could be specified by service type.
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(func.sum(services_data.c.capacity_real))
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            object_geometries_data.c.territory_id == territory_id,
            urban_objects_data.c.service_id.is_not(None),
        )
    )

    if service_type_id is not None:
        statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)
        service_type = (await conn.execute(statement)).one_or_none()
        if service_type is None:
            raise EntityNotFoundById(service_type_id, "service type")

    result = (await conn.execute(statement)).scalar_one_or_none()

    return result if result is not None else 0
