"""Service handlers logic of getting entities from the database is defined here."""

from datetime import datetime, timezone
from typing import Callable

from fastapi import HTTPException
from sqlalchemy import and_, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import ServiceDTO, ServiceWithTerritoriesDTO
from idu_api.urban_api.schemas import ServicesDataPatch, ServicesDataPost, ServicesDataPut

func: Callable


async def get_service_by_id_from_db(
    conn: AsyncConnection,
    service_id: int,
) -> ServiceDTO:
    """
    Get service object by id
    """

    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            services_data.join(
                service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id
            ).join(territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id)
        )
        .where(services_data.c.service_id == service_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()

    return ServiceDTO(**result)


async def add_service_to_db(
    conn: AsyncConnection,
    service: ServicesDataPost,
) -> ServiceDTO:
    """
    Create service object
    """

    statement = select(urban_objects_data).where(
        and_(
            urban_objects_data.c.physical_object_id == service.physical_object_id,
            urban_objects_data.c.object_geometry_id == service.object_geometry_id,
        )
    )
    urban_object = (await conn.execute(statement)).one_or_none()
    if urban_object is None:
        raise HTTPException(status_code=404, detail="Given physical object id and object geometry id are not found")

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service.service_type_id)
    service_type = (await conn.execute(statement)).one_or_none()
    if service_type is None:
        raise HTTPException(status_code=404, detail="Given service type id is not found")

    if service.territory_type_id is not None:
        statement = select(territory_types_dict).where(
            territory_types_dict.c.territory_type_id == service.territory_type_id
        )
        territory_type = (await conn.execute(statement)).one_or_none()
        if territory_type is None:
            raise HTTPException(status_code=404, detail="Given territory type id is not found")

    statement = (
        insert(services_data)
        .values(
            service_type_id=service.service_type_id,
            territory_type_id=service.territory_type_id,
            name=service.name,
            capacity_real=service.capacity_real,
            properties=service.properties,
        )
        .returning(services_data.c.service_id)
    )

    service_id = (await conn.execute(statement)).scalar_one()

    statement = (
        update(urban_objects_data)
        .where(
            and_(
                urban_objects_data.c.physical_object_id == service.physical_object_id,
                urban_objects_data.c.object_geometry_id == service.object_geometry_id,
            )
        )
        .values(service_id=service_id)
    )

    await conn.execute(statement)

    await conn.commit()

    return await get_service_by_id_from_db(conn, service_id)


async def put_service_to_db(
    conn: AsyncConnection,
    service: ServicesDataPut,
    service_id: int,
) -> ServiceDTO:
    """
    Put service object
    """

    statement = select(services_data).where(services_data.c.service_id == service_id)
    requested_service = (await conn.execute(statement)).one_or_none()
    if requested_service is None:
        raise HTTPException(status_code=404, detail="Given service id is not found")

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service.service_type_id)
    service_type = (await conn.execute(statement)).one_or_none()
    if service_type is None:
        raise HTTPException(status_code=404, detail="Given service type id is not found")

    if service.territory_type_id is not None:
        statement = select(territory_types_dict).where(
            territory_types_dict.c.territory_type_id == service.territory_type_id
        )
        territory_type = (await conn.execute(statement)).one_or_none()
        if territory_type is None:
            raise HTTPException(status_code=404, detail="Given territory type id is not found")

    statement = (
        update(services_data)
        .where(services_data.c.service_id == service_id)
        .values(
            service_type_id=service.service_type_id,
            territory_type_id=service.territory_type_id,
            name=service.name,
            capacity_real=service.capacity_real,
            properties=service.properties,
            updated_at=datetime.utcnow(),
        )
        .returning(services_data)
    )

    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_service_by_id_from_db(conn, result.service_id)


async def patch_service_to_db(
    conn: AsyncConnection,
    service: ServicesDataPatch,
    service_id: int,
) -> ServiceDTO:
    """
    Patch service object
    """

    statement = select(services_data).where(services_data.c.service_id == service_id)
    requested_service = (await conn.execute(statement)).one_or_none()
    if requested_service is None:
        raise HTTPException(status_code=404, detail="Given service id is not found")

    statement = (
        update(services_data)
        .where(services_data.c.service_id == service_id)
        .returning(services_data)
        .values(updated_at=datetime.now(timezone.utc))
    )

    values_to_update = {}
    for k, v in service.model_dump().items():
        if v is not None:
            if k == "service_type_id":
                new_statement = select(service_types_dict).where(
                    service_types_dict.c.service_type_id == service.service_type_id
                )
                service_type = (await conn.execute(new_statement)).one_or_none()
                if service_type is None:
                    raise HTTPException(status_code=404, detail="Given service type id is not found")
            elif k == "territory_type_id":
                new_statement = select(territory_types_dict).where(
                    territory_types_dict.c.territory_type_id == service.territory_type_id
                )
                territory_type = (await conn.execute(new_statement)).one_or_none()
                if territory_type is None:
                    raise HTTPException(status_code=404, detail="Given territory type id is not found")
            values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_service_by_id_from_db(conn, result.service_id)


async def get_service_with_territories_by_id_from_db(
    conn: AsyncConnection,
    service_id: int,
) -> ServiceWithTerritoriesDTO:
    """
    Get service object by id
    """

    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            services_data.join(
                service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id
            ).join(territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id)
        )
        .where(services_data.c.service_id == service_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail="Given service id is not found")

    statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
        )
        .select_from(
            services_data.join(urban_objects_data, urban_objects_data.c.service_id == services_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
        )
        .where(services_data.c.service_id == service_id)
    ).distinct()

    territories = (await conn.execute(statement)).mappings().all()

    return ServiceWithTerritoriesDTO(**result, territories=territories)


async def add_service_to_object_in_db(
    conn: AsyncConnection,
    service_id: int,
    physical_object_id: int,
    object_geometry_id: int,
) -> ServiceDTO:
    """
    Add existing service to physical object
    """

    statement = select(urban_objects_data).where(
        and_(
            urban_objects_data.c.physical_object_id == physical_object_id,
            urban_objects_data.c.object_geometry_id == object_geometry_id,
        )
    )
    urban_object = (await conn.execute(statement)).one_or_none()
    if urban_object is None:
        raise HTTPException(status_code=404, detail="Given physical object id and object geometry id are not found")

    statement = select(services_data).where(services_data.c.service_id == service_id)
    service = (await conn.execute(statement)).one_or_none()
    if service is None:
        raise HTTPException(status_code=404, detail="Given service id is not found")

    statement = insert(urban_objects_data).values(
        service_id=service_id, physical_object_id=physical_object_id, object_geometry_id=object_geometry_id
    )

    await conn.execute(statement)

    await conn.commit()

    return await get_service_by_id_from_db(conn, service_id)
