"""Territories physical objects internal logic is defined here."""

from typing import Literal

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import PageDTO, PhysicalObjectDataDTO, PhysicalObjectTypeDTO, PhysicalObjectWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.impl.helpers.territory_objects import check_territory_existence
from idu_api.urban_api.utils.pagination import paginate_dto


async def get_physical_object_types_by_territory_id_from_db(
    conn: AsyncConnection, territory_id: int
) -> list[PhysicalObjectTypeDTO]:
    """Get all physical object types that are located in given territory."""

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
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
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
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(territories_data.c.territory_id.in_(select(territories_cte)))
        .order_by(physical_object_types_dict.c.physical_object_type_id)
        .distinct()
    )
    physical_object_types = (await conn.execute(statement)).mappings().all()

    return [PhysicalObjectTypeDTO(**s) for s in physical_object_types]


async def get_physical_objects_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
    name: str | None,
    include_child_territories: bool,
    cities_only: bool,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None = "asc",
    paginate: bool = False,
) -> list[PhysicalObjectDataDTO] | PageDTO[PhysicalObjectDataDTO]:
    """Get physical objects by territory id, optional physical object type, is_city and physical object function."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
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
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
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

    if physical_object_type_id is not None and physical_object_function_id is not None:
        raise EntityNotFoundByParams(
            "physical object type and function", physical_object_type_id, physical_object_function_id
        )
    if physical_object_type_id is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type_id)
    elif physical_object_function_id is not None:
        functions_cte = (
            select(physical_object_functions_dict.c.physical_object_function_id)
            .where(physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id)
            .cte(recursive=True)
        )
        functions_cte = functions_cte.union_all(
            select(physical_object_functions_dict.c.physical_object_function_id).where(
                physical_object_functions_dict.c.parent_id == functions_cte.c.physical_object_function_id
            )
        )
        statement = statement.where(physical_object_types_dict.c.physical_object_function_id.in_(select(functions_cte)))
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
    return [PhysicalObjectDataDTO(**phys_obj) for phys_obj in result]


async def get_physical_objects_with_geometry_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
    name: str | None,
    include_child_territories: bool,
    cities_only: bool,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None = "asc",
    paginate: bool = False,
) -> list[PhysicalObjectWithGeometryDTO] | PageDTO[PhysicalObjectWithGeometryDTO]:
    """Get physical objects with geometry by territory id,
    optional physical object type and physical_object_function_id."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
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
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
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

    if physical_object_type_id is not None and physical_object_function_id is not None:
        raise EntityNotFoundByParams(
            "physical object type and function", physical_object_type_id, physical_object_function_id
        )
    if physical_object_type_id is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type_id)
    elif physical_object_function_id is not None:
        functions_cte = (
            select(physical_object_functions_dict.c.physical_object_function_id)
            .where(physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id)
            .cte(recursive=True)
        )
        functions_cte = functions_cte.union_all(
            select(physical_object_functions_dict.c.physical_object_function_id).where(
                physical_object_functions_dict.c.parent_id == functions_cte.c.physical_object_function_id
            )
        )
        statement = statement.where(physical_object_types_dict.c.physical_object_function_id.in_(select(functions_cte)))
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
    return [PhysicalObjectWithGeometryDTO(**phys_obj) for phys_obj in result]
