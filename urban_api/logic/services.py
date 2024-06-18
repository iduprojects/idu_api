"""
Service endpoints logic of getting entities from the database is defined here.
"""

from typing import Callable

from fastapi import HTTPException
from sqlalchemy import and_, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    service_types_dict,
    services_data,
    territory_types_dict,
    urban_objects_data,
)
from urban_api.dto import ServiceDTO
from urban_api.schemas import ServicesDataPatch, ServicesDataPost, ServicesDataPut

func: Callable


async def get_service_by_id_from_db(
    service_id: int,
    session: AsyncConnection,
) -> ServiceDTO:
    """
    Get service object by id
    """

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
            services_data.join(
                service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id
            ).join(territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id)
        )
        .where(services_data.c.service_id == service_id)
    )

    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return ServiceDTO(**result)


async def add_service_to_db(
    service: ServicesDataPost,
    session: AsyncConnection,
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
    urban_object = (await session.execute(statement)).one_or_none()
    if urban_object is None:
        raise HTTPException(status_code=404, detail="Given physical object id and object geometry id are not found")

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service.service_type_id)
    service_type = (await session.execute(statement)).one_or_none()
    if service_type is None:
        raise HTTPException(status_code=404, detail="Given service type id is not found")

    if service.territory_type_id is not None:
        statement = select(territory_types_dict).where(
            territory_types_dict.c.territory_type_id == service.territory_type_id
        )
        territory_type = (await session.execute(statement)).one_or_none()
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

    service_id = (await session.execute(statement)).scalar_one()

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

    await session.execute(statement)

    await session.commit()

    return await get_service_by_id_from_db(service_id, session)


async def put_service_to_db(
    service: ServicesDataPut,
    service_id: int,
    session: AsyncConnection,
) -> ServiceDTO:
    """
    Put service object
    """

    statement = select(services_data).where(services_data.c.service_id == service_id)
    requested_service = (await session.execute(statement)).one_or_none()
    if requested_service is None:
        raise HTTPException(status_code=404, detail="Given service id is not found")

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service.service_type_id)
    service_type = (await session.execute(statement)).one_or_none()
    if service_type is None:
        raise HTTPException(status_code=404, detail="Given service type id is not found")

    if service.territory_type_id is not None:
        statement = select(territory_types_dict).where(
            territory_types_dict.c.territory_type_id == service.territory_type_id
        )
        territory_type = (await session.execute(statement)).one_or_none()
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
        )
        .returning(services_data)
    )

    result = (await session.execute(statement)).mappings().one()
    await session.commit()

    return await get_service_by_id_from_db(result.service_id, session)


async def patch_service_to_db(
    service: ServicesDataPatch,
    service_id: int,
    session: AsyncConnection,
) -> ServiceDTO:
    """
    Patch service object
    """

    statement = select(services_data).where(services_data.c.service_id == service_id)
    requested_service = (await session.execute(statement)).one_or_none()
    if requested_service is None:
        raise HTTPException(status_code=404, detail="Given service id is not found")

    statement = update(services_data).where(services_data.c.service_id == service_id).returning(services_data)

    values_to_update = {}
    for k, v in service.model_dump().items():
        if v is not None:
            if k == "service_type_id":
                new_statement = select(service_types_dict).where(
                    service_types_dict.c.service_type_id == service.service_type_id
                )
                service_type = (await session.execute(new_statement)).one_or_none()
                if service_type is None:
                    raise HTTPException(status_code=404, detail="Given service type id is not found")
            elif k == "territory_type_id":
                new_statement = select(territory_types_dict).where(
                    territory_types_dict.c.territory_type_id == service.territory_type_id
                )
                territory_type = (await session.execute(new_statement)).one_or_none()
                if territory_type is None:
                    raise HTTPException(status_code=404, detail="Given territory type id is not found")
            values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    result = (await session.execute(statement)).mappings().one()
    await session.commit()

    return await get_service_by_id_from_db(result.service_id, session)
