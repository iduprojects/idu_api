"""
Physical objects endpoints logic of getting entities from the database is defined here.
"""

from typing import Callable, Dict, List, Optional

from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, insert, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_objects_data,
)
from urban_api.dto import (
    LivingBuildingsDTO,
    ObjectGeometryDTO,
    PhysicalObjectsTypesDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
)
from urban_api.schemas import LivingBuildingsDataPost, PhysicalObjectsDataPost, PhysicalObjectsTypesPost

func: Callable


async def get_physical_object_types_from_db(session: AsyncConnection) -> List[PhysicalObjectsTypesDTO]:
    """
    Get all physical object type objects
    """

    statement = select(physical_object_types_dict).order_by(physical_object_types_dict.c.physical_object_type_id)

    return [PhysicalObjectsTypesDTO(*data) for data in await session.execute(statement)]


async def add_physical_object_type_to_db(
    physical_object_type: PhysicalObjectsTypesPost,
    session: AsyncConnection,
) -> PhysicalObjectsTypesDTO:
    """
    Create physical object type object
    """

    statement = select(physical_object_types_dict).where(physical_object_types_dict.c.name == physical_object_type.name)
    result = (await session.execute(statement)).one_or_none()
    if result is not None:
        raise HTTPException(status_code=400, detail="Invalid input (physical object type already exists)")

    statement = (
        insert(physical_object_types_dict)
        .values(
            name=physical_object_type.name,
        )
        .returning(physical_object_types_dict)
    )
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return PhysicalObjectsTypesDTO(**result)


async def add_physical_object_with_geometry_to_db(
    physical_object: PhysicalObjectsDataPost,
    session: AsyncConnection,
) -> Dict[str, int]:
    """
    Create physical object with geometry
    """

    statement = select(territories_data).where(territories_data.c.territory_id == physical_object.territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object.physical_object_type_id
    )
    physical_object_type = (await session.execute(statement)).one_or_none()
    if physical_object_type is None:
        raise HTTPException(status_code=404, detail="Given physical object type id is not found")

    statement = (
        insert(physical_objects_data)
        .values(
            physical_object_type_id=physical_object.physical_object_type_id,
            name=physical_object.name,
            properties=physical_object.properties,
        )
        .returning(physical_objects_data.c.physical_object_id)
    )

    physical_object_id = (await session.execute(statement)).scalar_one()

    statement = (
        insert(object_geometries_data)
        .values(
            territory_id=physical_object.territory_id,
            geometry=ST_GeomFromText(str(physical_object.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(str(physical_object.centre_point.as_shapely_geometry()), text("4326")),
            address=physical_object.address,
        )
        .returning(object_geometries_data.c.object_geometry_id)
    )

    object_geometry_id = (await session.execute(statement)).scalar_one()

    statement = insert(urban_objects_data).values(
        physical_object_id=physical_object_id, object_geometry_id=object_geometry_id
    )

    await session.execute(statement)

    await session.commit()

    return dict(
        physical_object_id=physical_object_id,
        object_geometry_id=object_geometry_id,
        territory_id=physical_object.territory_id,
    )


async def get_living_building_by_id_from_db(
    living_building_id: int,
    session: AsyncConnection,
) -> LivingBuildingsDTO:
    """
    Create living building object
    """

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
            object_geometries_data.c.address.label("physical_object_type_address"),
        )
        .select_from(
            living_buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == living_buildings_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(living_buildings_data.c.living_building_id == living_building_id)
    )

    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return LivingBuildingsDTO(**result)


async def add_living_building_to_db(
    living_building: LivingBuildingsDataPost,
    session: AsyncConnection,
) -> LivingBuildingsDTO:
    """
    Create living building object
    """

    statement = select(physical_objects_data).where(
        physical_objects_data.c.physical_object_id == living_building.physical_object_id
    )
    physical_object = (await session.execute(statement)).one_or_none()
    if physical_object is None:
        raise HTTPException(status_code=404, detail="Given physical object id is not found")

    statement = (
        insert(living_buildings_data)
        .values(
            physical_object_id=living_building.physical_object_id,
            residents_number=living_building.residents_number,
            living_area=living_building.living_area,
            properties=living_building.properties,
        )
        .returning(living_buildings_data.c.living_building_id)
    )

    living_building_id = (await session.execute(statement)).scalar_one()

    await session.commit()

    return await get_living_building_by_id_from_db(living_building_id, session)


async def get_services_by_physical_object_id_from_db(
    physical_object_id: int,
    service_type_id: Optional[int],
    territory_type_id: Optional[int],
    session: AsyncConnection,
) -> List[ServiceDTO]:
    """
    Get service or list of services by physical object id,
    could be specified by service type id and territory type id
    """

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await session.execute(statement)).one_or_none()
    if physical_object is None:
        raise HTTPException(status_code=404, detail="Given physical object id is not found")

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
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id)
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
    )

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)

    if territory_type_id is not None:
        statement = statement.where(territory_types_dict.c.territory_type_id == territory_type_id)

    result = (await session.execute(statement)).mappings().all()

    return [ServiceDTO(**service) for service in result]


async def get_services_with_geometry_by_physical_object_id_from_db(
    physical_object_id: int,
    service_type_id: Optional[int],
    territory_type_id: Optional[int],
    session: AsyncConnection,
) -> List[ServiceWithGeometryDTO]:
    """
    Get service or list of services with geometry by physical object id,
    could be specified by service type id and territory type id
    """

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await session.execute(statement)).one_or_none()
    if physical_object is None:
        raise HTTPException(status_code=404, detail="Given physical object id is not found")

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
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id)
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
    )

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)

    if territory_type_id is not None:
        statement = statement.where(territory_types_dict.c.territory_type_id == territory_type_id)

    result = (await session.execute(statement)).mappings().all()

    return [ServiceWithGeometryDTO(**service) for service in result]


async def get_physical_object_geometries_from_db(
    physical_object_id: int,
    session: AsyncConnection,
) -> List[ObjectGeometryDTO]:
    """
    Get geometry or list of geometries by physical object id
    """

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await session.execute(statement)).one_or_none()
    if physical_object is None:
        raise HTTPException(status_code=404, detail="Given physical object id is not found")

    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            object_geometries_data.c.address,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            urban_objects_data.join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
    )

    result = (await session.execute(statement)).mappings().all()

    return [ObjectGeometryDTO(**geometry) for geometry in result]
