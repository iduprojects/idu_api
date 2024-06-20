"""
Territories endpoints logic of getting entities from the database is defined here.
"""

from datetime import date, datetime
from typing import Callable, List, Literal, Optional

import shapely.geometry as geom
from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    functional_zones_data,
    indicators_dict,
    living_buildings_data,
    measurement_units_dict,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_indicators_data,
    territory_types_dict,
    urban_objects_data,
)
from urban_api.dto import (
    FunctionalZoneDataDTO,
    IndicatorDTO,
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
from urban_api.schemas import TerritoriesDataPatch, TerritoriesDataPost, TerritoriesDataPut, TerritoryTypesPost

func: Callable


async def get_territory_types_from_db(session: AsyncConnection) -> list[TerritoryTypeDTO]:
    """
    Get all territory type objects
    """

    statement = select(territory_types_dict).order_by(territory_types_dict.c.territory_type_id)

    return [TerritoryTypeDTO(**data) for data in (await session.execute(statement)).mappings().all()]


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


async def get_territories_by_id_from_db(territory_ids: list[int], session: AsyncConnection) -> List[TerritoryDTO]:
    """
    Get territory objects by ids list
    """

    statement = (
        select(
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
            territories_data.c.created_at,
            territories_data.c.updated_at,
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            )
        )
        .where(territories_data.c.territory_id.in_(territory_ids))
    )

    results = (await session.execute(statement)).mappings().all()

    return [TerritoryDTO(**territory) for territory in results]


async def get_territory_by_id_from_db(territory_id: int, session: AsyncConnection) -> TerritoryDTO:
    """
    Get territory object by id
    """

    results = await get_territories_by_id_from_db([territory_id], session)
    if len(results) == 0:
        raise HTTPException(status_code=404, detail="Given id is not found")

    return results[0]


async def add_territory_to_db(
    territory: TerritoriesDataPost,
    session: AsyncConnection,
) -> TerritoryDTO:
    """
    Create territory object
    """

    if territory.parent_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == territory.parent_id)
        parent_territory = (await session.execute(statement)).one_or_none()
        if parent_territory is None:
            raise HTTPException(status_code=404, detail="Given parent id is not found")

    statement = (
        insert(territories_data)
        .values(
            territory_type_id=territory.territory_type_id,
            parent_id=territory.parent_id,
            name=territory.name,
            geometry=ST_GeomFromText(str(territory.geometry.as_shapely_geometry()), text("4326")),
            level=territory.level,
            properties=territory.properties,
            centre_point=ST_GeomFromText(str(territory.centre_point.as_shapely_geometry()), text("4326")),
            admin_center=territory.admin_center,
            okato_code=territory.okato_code,
        )
        .returning(territories_data)
    )
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return await get_territory_by_id_from_db(result.territory_id, session)


async def get_services_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
    service_type_id: Optional[int],
    name: Optional[str],
) -> list[ServiceDTO]:
    """
    Get service objects by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity_real,
            services_data.c.properties,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id)
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
    if name is not None:
        statement = statement.where(services_data.c.name.ilike(f"%{name}%"))

    result = (await session.execute(statement)).mappings().all()

    return [ServiceDTO(**service) for service in result]


async def get_services_with_geometry_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
    service_type_id: Optional[int],
    name: Optional[str],
) -> list[ServiceWithGeometryDTO]:
    """
    Get service objects with geometry by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity_real,
            services_data.c.properties,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            territory_types_dict.c.territory_type_id,
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
            .join(territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id)
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
    if name is not None:
        statement = statement.where(services_data.c.name.ilike(f"%{name}%"))

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
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
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
) -> List[IndicatorDTO]:
    """
    Get indicators by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(indicators_dict, measurement_units_dict.c.name.label("measurement_unit_name"))
        .select_from(
            territory_indicators_data.join(
                indicators_dict, territory_indicators_data.c.indicator_id == indicators_dict.c.indicator_id
            ).outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
        )
        .where(territory_indicators_data.c.territory_id == territory_id)
    )

    result = (await session.execute(statement)).mappings().all()

    return [IndicatorDTO(**indicator) for indicator in result]


async def get_indicator_values_by_territory_id_from_db(
    territory_id: int, session: AsyncConnection, date_type: Optional[str], date_value: Optional[datetime]
) -> List[IndicatorValueDTO]:
    """
    Get indicator values by territory id, optional time period
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = select(territory_indicators_data).where(territory_indicators_data.c.territory_id == territory_id)

    if date_type is not None:
        statement = statement.where(territory_indicators_data.c.date_type == date_type)
    if date_value is not None:
        statement = statement.where(territory_indicators_data.c.date_value == date_value)

    result = (await session.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**indicator_value) for indicator_value in result]


async def get_physical_objects_by_territory_id_from_db(
    territory_id: int, session: AsyncConnection, physical_object_type: Optional[int], name: Optional[str]
) -> List[PhysicalObjectsDataDTO]:
    """
    Get physical objects by territory id, optional physical object type
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name,
            object_geometries_data.c.address,
            physical_objects_data.c.properties,
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
        )
        .where(object_geometries_data.c.territory_id == territory_id)
    )

    if physical_object_type is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type)
    if name is not None:
        statement = statement.where(physical_objects_data.c.name.ilike(f"%{name}%"))

    result = (await session.execute(statement)).mappings().all()

    return [PhysicalObjectsDataDTO(**physical_object) for physical_object in result]


async def get_physical_objects_with_geometry_by_territory_id_from_db(
    territory_id: int, session: AsyncConnection, physical_object_type: Optional[int], name: Optional[str]
) -> List[PhysicalObjectWithGeometryDTO]:
    """
    Get physical objects with geometry by territory id, optional physical object type
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
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
    if name is not None:
        statement = statement.where(physical_objects_data.c.name.ilike(f"%{name}%"))

    result = (await session.execute(statement)).mappings().all()

    return [PhysicalObjectWithGeometryDTO(**physical_object) for physical_object in result]


async def get_living_buildings_with_geometry_by_territory_id_from_db(
    territory_id: int,
    session: AsyncConnection,
) -> List[LivingBuildingsWithGeometryDTO]:
    """
    Get living buildings with geometry by territory id
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        select(
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.residents_number,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.address.label("physical_object_address"),
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            living_buildings_data.join(
                urban_objects_data,
                living_buildings_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == living_buildings_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
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

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

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
) -> List[TerritoryDTO]:
    """
    Get a territory or list of territories by parent, territory type could be specified in parameters
    """

    if parent_id is not None:
        statement = select(territories_data.c.territory_id).where(territories_data.c.territory_id == parent_id)
        parent_territory = (await session.execute(statement)).one_or_none()
        if parent_territory is None:
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
        territories_data.c.created_at,
        territories_data.c.updated_at,
        territory_types_dict.c.name.label("territory_type_name"),
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        )
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

    return [TerritoryDTO(**territory) for territory in result]


async def get_territories_without_geometry_by_parent_id_from_db(
    parent_id: Optional[int],
    session: AsyncConnection,
    get_all_levels: bool,
    order_by: Optional[Literal["created_at", "updated_at"]],
    created_at: Optional[date],
    name: Optional[str],
    ordering: Optional[Literal["asc", "desc"]] = "asc",
) -> List[TerritoryWithoutGeometryDTO]:
    """
    Get a territory or list of territories without geometry by parent,
    ordering and filters can be specified in parameters
    """

    if parent_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == parent_id)
        parent_territory = (await session.execute(statement)).one_or_none()
        if parent_territory is None:
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
        territories_data.c.created_at,
        territories_data.c.updated_at,
        territory_types_dict.c.name.label("territory_type_name"),
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        )
    )

    if get_all_levels:
        cte_statement = statement.where(
            (
                territories_data.c.parent_id == parent_id
                if parent_id is not None
                else territories_data.c.parent_id.is_(None)
            )
        )
        cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

        recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

        statement = select(cte_statement.union_all(recursive_part))
    else:
        statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )

    requested_territories = statement.cte("requested_territories")
    statement = select(requested_territories)

    if name is not None:
        statement = statement.where(requested_territories.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(requested_territories.c.created_at) == created_at)
    if order_by is not None:
        order = requested_territories.c.created_at if order_by == "created_at" else requested_territories.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        statement = statement.order_by(requested_territories.c.territory_id)

    result = (await session.execute(statement)).mappings().all()

    return [TerritoryWithoutGeometryDTO(**territory) for territory in result]


async def get_common_territory_for_geometry(
    session: AsyncConnection, geometry: geom.Polygon | geom.MultiPolygon | geom.Point
) -> TerritoryDTO | None:
    statement = (
        select(territories_data.c.territory_id)
        .where(func.ST_Covers(territories_data.c.geometry, ST_GeomFromText(str(geometry), text("4326"))))
        .order_by(territories_data.c.level.desc())
        .limit(1)
    )

    territory_id = (await session.execute(statement)).scalar()

    if territory_id is None:
        return None

    return await get_territory_by_id_from_db(territory_id, session)


async def get_intersecting_territories_for_geometry(
    session: AsyncConnection, parent_territory: int, geometry: geom.Polygon | geom.MultiPolygon | geom.Point
) -> list[TerritoryDTO]:
    level_subqery = (
        select(territories_data.c.level + 1)
        .where(territories_data.c.territory_id == parent_territory)
        .scalar_subquery()
    )

    given_geometry = select(ST_GeomFromText(str(geometry), text("4326"))).cte("given_geometry")

    statement = select(territories_data.c.territory_id).where(
        territories_data.c.level == level_subqery,
        (
            func.ST_Intersects(territories_data.c.geometry, select(given_geometry).scalar_subquery())
            | func.ST_Covers(select(given_geometry).scalar_subquery(), territories_data.c.geometry)
            | func.ST_Covers(territories_data.c.geometry, select(given_geometry).scalar_subquery())
        ),
    )

    territory_ids = (await session.execute(statement)).scalars().all()

    return await get_territories_by_id_from_db(territory_ids, session)


async def put_territory_to_db(
    territory_id: int,
    territory: TerritoriesDataPut,
    session: AsyncConnection,
) -> TerritoryDTO:
    """
    Put territory object
    """

    if territory.parent_id is not None:
        statement = select(territories_data).filter(territories_data.c.territory_id == territory.parent_id)
        check_parent_id = (await session.execute(statement)).one_or_none()
        if check_parent_id is None:
            raise HTTPException(status_code=404, detail="Given parent_id is not found")

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    requested_territory = (await session.execute(statement)).one_or_none()
    if requested_territory is None:
        raise HTTPException(status_code=404, detail="Given territory_id is not found")

    statement = (
        update(territories_data)
        .where(territories_data.c.territory_id == territory_id)
        .values(
            territory_type_id=territory.territory_type_id,
            parent_id=territory.parent_id,
            name=territory.name,
            geometry=ST_GeomFromText(str(territory.geometry.as_shapely_geometry()), text("4326")),
            level=territory.level,
            properties=territory.properties,
            centre_point=ST_GeomFromText(str(territory.centre_point.as_shapely_geometry()), text("4326")),
            admin_center=territory.admin_center,
            okato_code=territory.okato_code,
            updated_at=datetime.utcnow(),
        )
        .returning(territories_data)
    )
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return await get_territory_by_id_from_db(result.territory_id, session)


async def patch_territory_to_db(
    territory_id: int,
    territory: TerritoriesDataPatch,
    session: AsyncConnection,
) -> TerritoryDTO:
    """
    Patch territory object
    """

    if territory.parent_id is not None:
        statement = select(territories_data).filter(territories_data.c.territory_id == territory.parent_id)
        check_parent_id = (await session.execute(statement)).one_or_none()
        if check_parent_id is None:
            raise HTTPException(status_code=404, detail="Given parent_id is not found")

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    requested_territory = (await session.execute(statement)).one_or_none()
    if requested_territory is None:
        raise HTTPException(status_code=404, detail="Given territory_id is not found")

    statement = (
        update(territories_data)
        .where(territories_data.c.territory_id == territory_id)
        .returning(territories_data)
        .values(updated_at=datetime.utcnow())
    )

    values_to_update = {}
    for k, v in territory.model_dump(exclude={"geometry", "centre_point"}).items():
        if v is not None:
            if k == "territory_type_id":
                new_statement = select(territory_types_dict).where(
                    territory_types_dict.c.territory_type_id == territory.territory_type_id
                )
                territory_type = (await session.execute(new_statement)).one_or_none()
                if territory_type is None:
                    raise HTTPException(status_code=404, detail="Given territory type id is not found")
                values_to_update.update({k: v})
            else:
                values_to_update.update({k: v})

    values_to_update.update({"geometry": ST_GeomFromText(str(territory.geometry.as_shapely_geometry()), text("4326"))})
    values_to_update.update(
        {"centre_point": ST_GeomFromText(str(territory.centre_point.as_shapely_geometry()), text("4326"))}
    )

    statement = statement.values(**values_to_update)
    result = (await session.execute(statement)).mappings().one()
    await session.commit()

    return await get_territory_by_id_from_db(result.territory_id, session)
