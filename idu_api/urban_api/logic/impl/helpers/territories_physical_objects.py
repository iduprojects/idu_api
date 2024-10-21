"""Territories physical objects internal logic is defined here."""

from typing import Literal, Optional

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import PageDTO, PhysicalObjectDataDTO, PhysicalObjectWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.utils.pagination import paginate_dto


async def get_physical_objects_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    physical_object_type: int | None,
    name: str | None,
    order_by: Optional[Literal["created_at", "updated_at"]],
    ordering: Optional[Literal["asc", "desc"]] = "asc",
    paginate: bool = False,
) -> list[PhysicalObjectDataDTO] | PageDTO[PhysicalObjectDataDTO]:
    """Get physical objects by territory id, optional physical object type."""

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
        select(territories_data.c.territory_id).where(territories_data.c.parent_id == territories_cte.c.territory_id)
    )

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(object_geometries_data.c.territory_id.in_(select(territories_cte)))
        .distinct()
    )

    if physical_object_type is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type)
    if name is not None:
        statement = statement.where(physical_objects_data.c.name.ilike(f"%{name}%"))
    if order_by is not None:
        order = physical_objects_data.c.created_at if order_by == "created_at" else physical_objects_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(physical_objects_data.c.physical_object_id.desc())
        else:
            statement = statement.order_by(physical_objects_data.c.physical_object_id)

    if paginate:
        return await paginate_dto(conn, statement, transformer=lambda x: [PhysicalObjectDataDTO(**item) for item in x])

    result = (await conn.execute(statement)).mappings().all()
    return [PhysicalObjectDataDTO(**building) for building in result]


async def get_physical_objects_with_geometry_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    physical_object_type: int | None,
    name: str | None,
    order_by: Optional[Literal["created_at", "updated_at"]],
    ordering: Optional[Literal["asc", "desc"]] = "asc",
    paginate: bool = False,
) -> list[PhysicalObjectWithGeometryDTO] | PageDTO[PhysicalObjectWithGeometryDTO]:
    """Get physical objects with geometry by territory id, optional physical object type."""

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
        select(territories_data.c.territory_id).where(territories_data.c.parent_id == territories_cte.c.territory_id)
    )

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(object_geometries_data.c.territory_id.in_(select(territories_cte)))
        .distinct()
    )

    if physical_object_type is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type)
    if name is not None:
        statement = statement.where(physical_objects_data.c.name.ilike(f"%{name}%"))
    if order_by is not None:
        order = physical_objects_data.c.created_at if order_by == "created_at" else physical_objects_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(physical_objects_data.c.physical_object_id.desc())
        else:
            statement = statement.order_by(physical_objects_data.c.physical_object_id)

    if paginate:
        return await paginate_dto(
            conn, statement, transformer=lambda x: [PhysicalObjectWithGeometryDTO(**item) for item in x]
        )

    result = (await conn.execute(statement)).mappings().all()
    return [PhysicalObjectWithGeometryDTO(**building) for building in result]
