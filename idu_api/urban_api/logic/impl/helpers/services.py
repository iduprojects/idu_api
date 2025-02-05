"""Services internal logic is defined here."""

from typing import Callable

from sqlalchemy import delete, insert, select, update
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
from idu_api.urban_api.dto import ServiceDTO, UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.impl.helpers.urban_objects import get_urban_objects_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, extract_values_from_model
from idu_api.urban_api.schemas import ServicePatch, ServicePost, ServicePut

func: Callable


async def get_service_by_id_from_db(conn: AsyncConnection, service_id: int) -> ServiceDTO:
    """Get service object by id."""

    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            services_data.join(
                service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id
            )
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
            .join(urban_objects_data, urban_objects_data.c.service_id == services_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
        )
        .where(services_data.c.service_id == service_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()
    if not result:
        raise EntityNotFoundById(service_id, "service")

    territories = [{"territory_id": row["territory_id"], "name": row.territory_name} for row in result]
    service = {k: v for k, v in result[0].items() if k not in ("territory_id", "territory_name")}

    return ServiceDTO(**service, territories=territories)


async def add_service_to_db(conn: AsyncConnection, service: ServicePost) -> ServiceDTO:
    """Create service object."""

    statement = select(urban_objects_data).where(
        urban_objects_data.c.physical_object_id == service.physical_object_id,
        urban_objects_data.c.object_geometry_id == service.object_geometry_id,
    )
    urban_objects = (await conn.execute(statement)).mappings().all()
    if not urban_objects:
        raise EntityNotFoundByParams("urban object", service.physical_object_id, service.object_geometry_id)

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service.service_type_id}):
        raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        if not await check_existence(
            conn, territory_types_dict, conditions={"territory_type_id": service.territory_type_id}
        ):
            raise EntityNotFoundById(service.territory_type_id, "territory type")

    statement = (
        insert(services_data)
        .values(**service.model_dump(exclude={"physical_object_id", "object_geometry_id"}))
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


async def put_service_to_db(conn: AsyncConnection, service: ServicePut, service_id: int) -> ServiceDTO:
    """Update service object by all its attributes."""

    if not await check_existence(conn, services_data, conditions={"service_id": service_id}):
        raise EntityNotFoundById(service_id, "service")

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service.service_type_id}):
        raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        if not await check_existence(
            conn, territory_types_dict, conditions={"territory_type_id": service.territory_type_id}
        ):
            raise EntityNotFoundById(service.territory_type_id, "territory type")

    values = extract_values_from_model(service, to_update=True)
    statement = update(services_data).where(services_data.c.service_id == service_id).values(**values)

    await conn.execute(statement)
    await conn.commit()

    return await get_service_by_id_from_db(conn, service_id)


async def patch_service_to_db(
    conn: AsyncConnection,
    service: ServicePatch,
    service_id: int,
) -> ServiceDTO:
    """Update service object by only given attributes."""

    if not await check_existence(conn, services_data, conditions={"service_id": service_id}):
        raise EntityNotFoundById(service_id, "service")

    if service.service_type_id is not None:
        if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service.service_type_id}):
            raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        if not await check_existence(
            conn, territory_types_dict, conditions={"territory_type_id": service.territory_type_id}
        ):
            raise EntityNotFoundById(service.territory_type_id, "territory type")

    values = extract_values_from_model(service, exclude_unset=True, to_update=True)
    statement = update(services_data).where(services_data.c.service_id == service_id).values(**values)

    await conn.execute(statement)
    await conn.commit()

    return await get_service_by_id_from_db(conn, service_id)


async def delete_service_from_db(conn: AsyncConnection, service_id: int) -> dict:
    """Delete service object."""

    if not await check_existence(conn, services_data, conditions={"service_id": service_id}):
        raise EntityNotFoundById(service_id, "service")

    statement = delete(services_data).where(services_data.c.service_id == service_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def add_service_to_object_in_db(
    conn: AsyncConnection,
    service_id: int,
    physical_object_id: int,
    object_geometry_id: int,
) -> UrbanObjectDTO:
    """Add existing service to physical object."""

    if not await check_existence(conn, services_data, conditions={"service_id": service_id}):
        raise EntityNotFoundById(service_id, "service")

    statement = select(urban_objects_data).where(
        urban_objects_data.c.physical_object_id == physical_object_id,
        urban_objects_data.c.object_geometry_id == object_geometry_id,
    )
    urban_objects = (await conn.execute(statement)).mappings().all()
    if not urban_objects:
        raise EntityNotFoundByParams("urban object", physical_object_id, object_geometry_id)

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

    return (await get_urban_objects_by_ids_from_db(conn, [urban_object_id]))[0]
