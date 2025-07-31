"""Territories physical objects internal logic is defined here."""

from collections import defaultdict
from typing import Literal, Sequence

from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import RowMapping, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import PageDTO, PhysicalObjectDTO, PhysicalObjectTypeDTO, PhysicalObjectWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, include_child_territories_cte
from idu_api.urban_api.utils.pagination import paginate_dto
from idu_api.urban_api.utils.query_filters import CustomFilter, EqFilter, ILikeFilter, RecursiveFilter, apply_filters


async def get_physical_object_types_by_territory_id_from_db(
    conn: AsyncConnection, territory_id: int, include_child_territories: bool, cities_only: bool
) -> list[PhysicalObjectTypeDTO]:
    """Get all physical object types that are located in given territory."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
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
        .order_by(physical_object_types_dict.c.physical_object_type_id)
        .distinct()
    )

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        statement = statement.where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(object_geometries_data.c.territory_id == territory_id)

    physical_object_types = (await conn.execute(statement)).mappings().all()

    return [PhysicalObjectTypeDTO(**pot) for pot in physical_object_types]


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
) -> list[PhysicalObjectDTO] | PageDTO[PhysicalObjectDTO]:
    """Get physical objects by territory id, optional physical object type, is_city and physical object function."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
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
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
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
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .distinct()
    )

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        territory_filter = CustomFilter(
            lambda q: q.where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
        )
    else:
        territory_filter = EqFilter(object_geometries_data, "territory_id", territory_id)

    statement = apply_filters(
        statement,
        territory_filter,
        EqFilter(physical_objects_data, "physical_object_type_id", physical_object_type_id),
        RecursiveFilter(
            physical_object_types_dict,
            "physical_object_function_id",
            physical_object_function_id,
            physical_object_functions_dict,
        ),
        ILikeFilter(physical_objects_data, "name", name),
    )

    order_column = {
        "created_at": physical_objects_data.c.created_at,
        "updated_at": physical_objects_data.c.updated_at,
    }.get(order_by, physical_objects_data.c.physical_object_id)

    if ordering == "desc":
        order_column = order_column.desc()

    statement = statement.order_by(order_column)

    def group_objects(rows: Sequence[RowMapping]) -> list[PhysicalObjectDTO]:
        """Group territories by physical object identifier."""

        grouped_data = defaultdict(lambda: {"territories": []})
        for row in rows:
            key = row.physical_object_id
            if key not in grouped_data:
                grouped_data[key].update({**row})
                grouped_data[key].pop("territory_id")
                grouped_data[key].pop("territory_name")

            territory = {"territory_id": row["territory_id"], "name": row["territory_name"]}
            grouped_data[key]["territories"].append(territory)

        return [PhysicalObjectDTO(**phys_obj) for phys_obj in grouped_data.values()]

    if paginate:
        return await paginate_dto(conn, statement, transformer=group_objects)

    result = (await conn.execute(statement)).mappings().all()

    return group_objects(result)


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

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
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
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
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
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .distinct()
    )

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        territory_filter = CustomFilter(
            lambda q: q.where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
        )
    else:
        territory_filter = EqFilter(object_geometries_data, "territory_id", territory_id)

    statement = apply_filters(
        statement,
        territory_filter,
        EqFilter(physical_objects_data, "physical_object_type_id", physical_object_type_id),
        RecursiveFilter(
            physical_object_types_dict,
            "physical_object_function_id",
            physical_object_function_id,
            physical_object_functions_dict,
        ),
        ILikeFilter(physical_objects_data, "name", name),
    )

    order_column = {
        "created_at": physical_objects_data.c.created_at,
        "updated_at": physical_objects_data.c.updated_at,
    }.get(order_by, physical_objects_data.c.physical_object_id)

    if ordering == "desc":
        order_column = order_column.desc()

    statement = statement.order_by(order_column)

    if paginate:
        return await paginate_dto(
            conn, statement, transformer=lambda x: [PhysicalObjectWithGeometryDTO(**item) for item in x]
        )

    result = (await conn.execute(statement)).mappings().all()

    return [PhysicalObjectWithGeometryDTO(**phys_obj) for phys_obj in result]
