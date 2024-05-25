"""
Territories endpoints logic of getting entities from the database is defined here.
"""

from datetime import date, datetime
from typing import Callable, List, Optional, Tuple

from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, exists, func, insert, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    functional_zones_data,
    indicators_dict,
    living_buildings_data,
    object_geometries_data,
    physical_objects_data,
    services_data,
    territories_data,
    territory_indicators_data,
    territory_types_dict,
    urban_objects_data,
)
from urban_api.dto import (
    FunctionalZoneDataDTO,
    IndicatorsDTO,
    IndicatorValueDTO,
    LivingBuildingsWithGeometryDTO,
    PhysicalObjectsDataDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithoutGeometryDTO,
)
from urban_api.schemas import TerritoriesDataPost, TerritoryTypesPost

func: Callable

async def get_territory_types_from_db(session: AsyncConnection) -> list[TerritoryTypeDTO]:
    """
    Get all territory type objects
    """

    statement = select(territory_types_dict).order_by(territory_types_dict.c.territory_type_id)

    return [TerritoryTypeDTO(*data) for data in await session.execute(statement)]


async def add_territory_type_to_db(
    territory_type: TerritoryTypesPost,
    session: AsyncConnection,
) -> TerritoryTypeDTO:
    """
    Create territory type object
    """

    statement = select(territory_types_dict).where(territory_types_dict.c.name == territory_type.name)
    result = (await session.execute(statement)).one_or_none()
    if result is not None:
        raise HTTPException(status_code=400, detail="Invalid input (territory type already exists)")

    statement = (
        insert(territory_types_dict)
        .values(
            name=territory_type.name,
        )
        .returning(territory_types_dict)
    )
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return TerritoryTypeDTO(**result)


async def get_territory_by_id_from_db(
    territory_id: int, session: AsyncConnection
) -> Tuple[TerritoryDTO, TerritoryTypeDTO]:
    """
    Get territory object by id
    """

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territories_data.c.parent_id,
        territories_data.c.name,
        cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
        territories_data.c.level,
        territories_data.c.properties,
        cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
        territories_data.c.admin_center,
        territories_data.c.okato_code,
    ).where(territories_data.c.territory_id == territory_id)

    result = (await session.execute(statement)).one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail="Given id is not found")

    territory = TerritoryDTO(*result)

    statement = select(territory_types_dict).where(
        territory_types_dict.c.territory_type_id == territory.territory_type_id
    )

    result = (await session.execute(statement)).mappings().one()

    territory_type = TerritoryTypeDTO(**result)

    return territory, territory_type


async def add_territory_to_db(
    territory: TerritoriesDataPost,
    session: AsyncConnection,
) -> Tuple[TerritoryDTO, TerritoryTypeDTO]:
    """
    Create territory object
    """

    if territory.parent_id != 0:
        statement = select(territories_data).filter(territories_data.c.territory_id == territory.parent_id)
        check_parent_id = (await session.execute(statement)).one_or_none()
        if check_parent_id is None:
            raise HTTPException(status_code=404, detail="Given parent_id is not found")

    statement = (
        insert(territories_data)
        .values(
            territory_type_id=territory.territory_type_id,
            parent_id=territory.parent_id,
            name=territory.name,
            geometry=ST_GeomFromText(str(territory.geometry.as_shapely_geometry())),
            level=territory.level,
            properties=territory.properties,
            centre_point=ST_GeomFromText(str(territory.centre_point.as_shapely_geometry())),
            admin_center=territory.admin_center,
            okato_code=territory.okato_code,
        )
        .returning(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territories_data.c.parent_id,
            territories_data.c.name,
            cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
            territories_data.c.level,
            territories_data.c.properties,
            cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
            territories_data.c.admin_center,
            territories_data.c.okato_code,
        )
    )
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    territory = TerritoryDTO(**result)

    statement = select(territory_types_dict).where(
        territory_types_dict.c.territory_type_id == territory.territory_type_id
    )

    result = (await session.execute(statement)).mappings().one()

    territory_type = TerritoryTypeDTO(**result)

    return territory, territory_type


async def get_services_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
    service_type_id: Optional[int],
) -> list[ServiceDTO]:
    """
    Get service objects by territory id
    """

    statement = (
        select(services_data)
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)

    result = (await session.execute(statement)).mappings().all()

    return [ServiceDTO(**service) for service in result]


async def get_services_with_geometry_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
    service_type_id: Optional[int],
) -> list[ServiceWithGeometryDTO]:
    """
    Get service objects with geometry by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    is_found_territory_id = (await session.execute(statement)).one_or_none()
    if is_found_territory_id is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            services_data,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)

    result = (await session.execute(statement)).mappings().all()

    return [ServiceWithGeometryDTO(**service) for service in result]


async def get_services_capacity_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
    service_type_id: Optional[int],
) -> int:
    """
    Get aggregated capacity of services by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    is_found_territory_id = (await session.execute(statement)).one_or_none()
    if is_found_territory_id is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(func.sum(services_data.c.capacity_real))
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            object_geometries_data.c.territory_id == territory_id, services_data.c.service_type_id == service_type_id
        )
    )

    result = (await session.execute(statement)).scalar()

    return result


async def get_indicators_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
) -> List[IndicatorsDTO]:
    """
    Get indicators by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    is_found_territory_id = (await session.execute(statement)).one_or_none()
    if is_found_territory_id is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(indicators_dict)
        .select_from(
            territory_indicators_data.join(
                indicators_dict, territory_indicators_data.c.indicator_id == indicators_dict.c.indicator_id
            )
        )
        .where(territory_indicators_data.c.territory_id == territory_id)
    )

    result = (await session.execute(statement)).mappings().all()

    return [IndicatorsDTO(**indicator) for indicator in result]


async def get_indicator_values_by_territory_id_from_db(
    territory_id: int, session: AsyncConnection, date_type: Optional[str], date_value: Optional[datetime]
) -> List[IndicatorValueDTO]:
    """
    Get indicator values by territory id, optional time period
    """

    statement = select(exists().where(territories_data.c.territory_id == territory_id))
    if not (await session.execute(statement)).scalar_one():
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = select(territory_indicators_data).where(territory_indicators_data.c.territory_id == territory_id)

    if date_type is not None:
        statement = statement.where(territory_indicators_data.c.date_type == date_type)
    if date_value is not None:
        statement = statement.where(territory_indicators_data.c.date_value == date_value)

    result = (await session.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**indicator_value) for indicator_value in result]


async def get_physical_objects_by_territory_id_from_db(
    territory_id: int, session: AsyncConnection, physical_object_type: Optional[int]
) -> List[PhysicalObjectsDataDTO]:
    """
    Get physical objects by territory id, optional physical object type
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    is_found_territory_id = (await session.execute(statement)).one_or_none()
    if is_found_territory_id is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.physical_object_type_id,
            physical_objects_data.c.name,
            object_geometries_data.c.address,
            physical_objects_data.c.properties,
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            ).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    if physical_object_type is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type)

    result = (await session.execute(statement)).mappings().all()

    return [PhysicalObjectsDataDTO(**physical_object) for physical_object in result]


async def get_physical_objects_with_geometry_by_territory_id_from_db(
    territory_id: int, session: AsyncConnection, physical_object_type: Optional[int]
) -> List[PhysicalObjectWithGeometryDTO]:
    """
    Get physical objects with geometry by territory id, optional physical object type
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    is_found_territory_id = (await session.execute(statement)).one_or_none()
    if is_found_territory_id is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.physical_object_type_id,
            physical_objects_data.c.name,
            object_geometries_data.c.address,
            physical_objects_data.c.properties,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            ).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    if physical_object_type is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type)

    result = (await session.execute(statement)).mappings().all()
    if result is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    return [PhysicalObjectWithGeometryDTO(**physical_object) for physical_object in result]


async def get_living_buildings_with_geometry_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
) -> List[LivingBuildingsWithGeometryDTO]:
    """
    Get living buildings with geometry by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    is_found_territory_id = (await session.execute(statement)).one_or_none()
    if is_found_territory_id is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.physical_object_id,
            living_buildings_data.c.residents_number,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
        )
        .select_from(
            living_buildings_data.join(
                urban_objects_data,
                living_buildings_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            ).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    result = (await session.execute(statement)).mappings().all()

    return [LivingBuildingsWithGeometryDTO(**living_building) for living_building in result]


async def get_functional_zones_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
    functional_zone_type_id: Optional[int],
) -> List[FunctionalZoneDataDTO]:
    """
    Get functional zones with geometry by territory id
    """

    statement = select(
        functional_zones_data.c.functional_zone_id,
        functional_zones_data.c.territory_id,
        functional_zones_data.c.functional_zone_type_id,
        cast(ST_AsGeoJSON(functional_zones_data.c.geometry), JSONB).label("geometry"),
    ).where(functional_zones_data.c.territory_id == territory_id)

    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    result = (await session.execute(statement)).mappings().all()

    return [FunctionalZoneDataDTO(**zone) for zone in result]


async def get_territories_by_parent_id_from_db(
    parent_id: Optional[int], session: AsyncConnection, get_all_levels: Optional[bool], territory_type_id: Optional[int]
) -> List[Tuple[TerritoryDTO, TerritoryTypeDTO]]:
    """
    Get a territory or list of territories by parent, territory type could be specified in parameters
    """

    if parent_id is not None:
        statement = select(territories_data.c.territory_id).where(territories_data.c.territory_id == parent_id)
        is_found_parent_id = (await session.execute(statement)).one_or_none()
        if is_found_parent_id is None:
            raise HTTPException(status_code=404, detail="Given parent id is not found")

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territories_data.c.parent_id,
        territories_data.c.name,
        cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
        territories_data.c.level,
        territories_data.c.properties,
        cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
        territories_data.c.admin_center,
        territories_data.c.okato_code,
    )

    if get_all_levels:
        cte_statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )
        if territory_type_id is not None:
            cte_statement = cte_statement.where(territories_data.c.territory_type_id == territory_type_id)
        cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

        recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

        statement = select(cte_statement.union_all(recursive_part))

    else:
        statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )

        if territory_type_id is not None:
            statement = statement.where(territories_data.c.territory_type_id == territory_type_id)

    result = (await session.execute(statement)).mappings().all()

    territories = [TerritoryDTO(**territory) for territory in result]

    statements = [
        (select(territory_types_dict).where(territory_types_dict.c.territory_type_id == territory.territory_type_id))
        for territory in territories
    ]

    territory_types = []
    for statement in statements:
        result = (await session.execute(statement)).mappings().one()
        territory_types.append(TerritoryTypeDTO(**result))

    return list(zip(territories, territory_types))


async def get_territories_without_geometry_by_parent_id_from_db(
    parent_id: int,
    session: AsyncConnection,
    get_all_levels: bool,
    ordering: str,
    created_at: date,
    name: str,
    page: int,
    page_size: int,
) -> Tuple[int, List[Tuple[TerritoryWithoutGeometryDTO, TerritoryTypeDTO]]]:
    """
    Get a territory or list of territories by parent, territory type could be specified in parameters
    """

    if parent_id != 0:
        statement = select(territories_data).where(territories_data.c.territory_id == parent_id)
        is_found_parent_id = (await session.execute(statement)).one_or_none()
        if is_found_parent_id is None:
            raise HTTPException(status_code=404, detail="Given parent id is not found")

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territories_data.c.parent_id,
        territories_data.c.name,
        territories_data.c.level,
        territories_data.c.properties,
        territories_data.c.admin_center,
        territories_data.c.okato_code,
    )

    if get_all_levels:
        cte_statement = statement.where(
            (
                territories_data.c.parent_id == parent_id
                if parent_id is not None
                else territories_data.c.parent_id.is_(None)
            )
        )
        if created_at is not None:
            cte_statement = cte_statement.where(func.date(territories_data.c.created_at) == created_at)
        if name is not None:
            cte_statement = cte_statement.where(territories_data.c.name.ilike(f"%{name}%"))
        if ordering in ["created_at", "updated_at"]:
            cte_statement = cte_statement.order_by(getattr(territories_data.c, ordering).desc())
        cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

        recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

        statement = select(cte_statement.union_all(recursive_part))
    else:
        statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )
        if created_at is not None:
            statement = statement.where(func.date(territories_data.c.created_at) == created_at)
        if name is not None:
            statement = statement.where(territories_data.c.name.ilike(f"%{name}%"))
        if ordering in ["created_at", "updated_at"]:
            statement = statement.order_by(getattr(territories_data.c, ordering).desc())

    statement = statement.offset((page - 1) * page_size).limit(page_size)

    total_count = (await session.execute(select(func.count()).select_from(statement.subquery()))).scalar()
    result = (await session.execute(statement)).mappings().all()

    territories = [TerritoryWithoutGeometryDTO(**territory) for territory in result]

    statements = [
        (select(territory_types_dict).where(territory_types_dict.c.territory_type_id == territory.territory_type_id))
        for territory in territories
    ]
    territory_types = []
    for statement_i in statements:
        result = (await session.execute(statement_i)).mappings().one()
        territory_types.append(TerritoryTypeDTO(**result))

    return total_count, list(zip(territories, territory_types))
