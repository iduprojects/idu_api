"""Service handlers logic of getting entities from the database is defined here."""

from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import and_, delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import ServiceDTO, ServiceWithTerritoriesDTO, UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.urban_objects import get_urban_object_by_id_from_db
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
            ).outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .where(services_data.c.service_id == service_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(service_id, "service")

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
    urban_objects = (await conn.execute(statement)).mappings().all()
    if not list(urban_objects):
        raise EntityNotFoundByParams("urban object", service.physical_object_id, service.object_geometry_id)

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service.service_type_id)
    service_type = (await conn.execute(statement)).one_or_none()
    if service_type is None:
        raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        statement = select(territory_types_dict).where(
            territory_types_dict.c.territory_type_id == service.territory_type_id
        )
        territory_type = (await conn.execute(statement)).one_or_none()
        if territory_type is None:
            raise EntityNotFoundById(service.territory_type_id, "territory type")

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

    flag = False
    for urban_object in urban_objects:
        if urban_object.service_id is None:
            statement = (
                update(urban_objects_data)
                .where(urban_objects_data.c.urban_object_id == urban_object.urban_object_id)
                .values(service_id=service_id)
            )
            flag = True
            break

    if not flag:
        statement = insert(urban_objects_data).values(
            service_id=service_id,
            physical_object_id=service.physical_object_id,
            object_geometry_id=service.object_geometry_id,
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
        raise EntityNotFoundById(service_id, "service")

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service.service_type_id)
    service_type = (await conn.execute(statement)).one_or_none()
    if service_type is None:
        raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        statement = select(territory_types_dict).where(
            territory_types_dict.c.territory_type_id == service.territory_type_id
        )
        territory_type = (await conn.execute(statement)).one_or_none()
        if territory_type is None:
            raise EntityNotFoundById(service.territory_type_id, "territory type")

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
        raise EntityNotFoundById(service_id, "service")

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
                    raise EntityNotFoundById(service.service_type_id, "service type")
            elif k == "territory_type_id":
                new_statement = select(territory_types_dict).where(
                    territory_types_dict.c.territory_type_id == service.territory_type_id
                )
                territory_type = (await conn.execute(new_statement)).one_or_none()
                if territory_type is None:
                    raise EntityNotFoundById(service.territory_type_id, "teritory type")
            values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_service_by_id_from_db(conn, result.service_id)


async def delete_service_in_db(
    conn: AsyncConnection,
    service_id: int,
) -> dict:
    """Delete service object."""

    statement = select(services_data).where(services_data.c.service_id == service_id)
    requested_service = (await conn.execute(statement)).one_or_none()
    if requested_service is None:
        raise EntityNotFoundById(service_id, "service")

    statement = delete(services_data).where(services_data.c.service_id == service_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


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
        raise EntityNotFoundById(service_id, "service")

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
) -> UrbanObjectDTO:
    """
    Add existing service to physical object
    """

    statement = select(urban_objects_data).where(
        and_(
            urban_objects_data.c.physical_object_id == physical_object_id,
            urban_objects_data.c.object_geometry_id == object_geometry_id,
        )
    )
    urban_objects = (await conn.execute(statement)).mappings().all()
    if not list(urban_objects):
        raise EntityNotFoundByParams("urban object", physical_object_id, object_geometry_id)

    statement = select(services_data).where(services_data.c.service_id == service_id)
    service = (await conn.execute(statement)).one_or_none()
    if service is None:
        raise EntityNotFoundById(service_id, "service")

    flag = False
    for urban_object in urban_objects:
        if urban_object.service_id is None:
            statement = (
                update(urban_objects_data)
                .where(urban_objects_data.c.urban_object_id == urban_object.urban_object_id)
                .values(service_id=service_id)
                .returning(urban_objects_data.c.urban_object_id)
            )
            flag = True
        if urban_object.service_id == service_id:
            raise EntityAlreadyExists("urban object", physical_object_id, object_geometry_id, service_id)

    if not flag:
        statement = (
            insert(urban_objects_data)
            .values(
                service_id=service_id,
                physical_object_id=physical_object_id,
                object_geometry_id=object_geometry_id,
            )
            .returning(urban_objects_data.c.urban_object_id)
        )

    urban_object_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_urban_object_by_id_from_db(conn, urban_object_id)
