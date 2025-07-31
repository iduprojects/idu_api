"""Buffers internal logic is defined here."""

from collections.abc import Callable

from geoalchemy2.functions import ST_AsEWKB
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    buffer_types_dict,
    buffers_data,
    default_buffer_values_dict,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import (
    BufferDTO,
    BufferTypeDTO,
    DefaultBufferValueDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntityAlreadyExists,
    EntityNotFoundById,
    EntityNotFoundByParams,
)
from idu_api.urban_api.logic.impl.helpers.utils import (
    check_existence,
    extract_values_from_model,
)
from idu_api.urban_api.schemas import (
    BufferPut,
    BufferTypePost,
    DefaultBufferValuePost,
    DefaultBufferValuePut,
)

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString


async def get_buffer_types_from_db(conn: AsyncConnection) -> list[BufferTypeDTO]:
    """Get all buffer type objects."""

    statement = select(buffer_types_dict).order_by(buffer_types_dict.c.buffer_type_id)

    return [BufferTypeDTO(**buffer_type) for buffer_type in (await conn.execute(statement)).mappings().all()]


async def add_buffer_type_to_db(conn: AsyncConnection, buffer_type: BufferTypePost) -> BufferTypeDTO:
    """Create a new buffer type object."""

    if await check_existence(conn, buffer_types_dict, conditions={"name": buffer_type.name}):
        raise EntityAlreadyExists("buffer type", buffer_type.name)

    statement = insert(buffer_types_dict).values(**buffer_type.model_dump()).returning(buffer_types_dict)
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return BufferTypeDTO(**result)


async def get_all_default_buffer_values_from_db(conn: AsyncConnection) -> list[DefaultBufferValueDTO]:
    """Get a list of all buffer types with default value for each physical object/service type."""

    statement = select(
        default_buffer_values_dict.c.buffer_value,
        buffer_types_dict.c.buffer_type_id,
        buffer_types_dict.c.name.label("buffer_type_name"),
        physical_object_types_dict.c.physical_object_type_id,
        physical_object_types_dict.c.name.label("physical_object_type_name"),
        service_types_dict.c.service_type_id,
        service_types_dict.c.name.label("service_type_name"),
    ).select_from(
        default_buffer_values_dict.join(
            buffer_types_dict,
            buffer_types_dict.c.buffer_type_id == default_buffer_values_dict.c.buffer_type_id,
        )
        .outerjoin(
            physical_object_types_dict,
            physical_object_types_dict.c.physical_object_type_id
            == default_buffer_values_dict.c.physical_object_type_id,
        )
        .outerjoin(
            service_types_dict,
            service_types_dict.c.service_type_id == default_buffer_values_dict.c.service_type_id,
        )
    )

    result = (await conn.execute(statement)).mappings().all()

    return [DefaultBufferValueDTO(**value) for value in result]


async def get_default_buffer_value_from_db(
    conn: AsyncConnection,
    default_buffer_value_id: int,
) -> DefaultBufferValueDTO:
    """Get default buffer value by identifier."""

    statement = (
        select(
            default_buffer_values_dict.c.buffer_value,
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
        )
        .select_from(
            default_buffer_values_dict.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == default_buffer_values_dict.c.buffer_type_id,
            )
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id
                == default_buffer_values_dict.c.physical_object_type_id,
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == default_buffer_values_dict.c.service_type_id,
            )
        )
        .where(default_buffer_values_dict.c.default_buffer_value_id == default_buffer_value_id)
    )

    result = (await conn.execute(statement)).mappings().one()

    return DefaultBufferValueDTO(**result)


async def add_default_buffer_value_to_db(
    conn: AsyncConnection, buffer_value: DefaultBufferValuePost
) -> DefaultBufferValueDTO:
    """Add a new default buffer value."""

    if not await check_existence(conn, buffer_types_dict, conditions={"buffer_type_id": buffer_value.buffer_type_id}):
        raise EntityNotFoundById(buffer_value.buffer_type_id, "buffer type")

    if buffer_value.physical_object_type_id is not None:
        if not await check_existence(
            conn,
            physical_object_types_dict,
            conditions={"physical_object_type_id": buffer_value.physical_object_type_id},
        ):
            raise EntityNotFoundById(buffer_value.physical_object_type_id, "physical object type")

    if buffer_value.service_type_id is not None:
        if not await check_existence(
            conn,
            service_types_dict,
            conditions={"service_type_id": buffer_value.service_type_id},
        ):
            raise EntityNotFoundById(buffer_value.service_type_id, "service type")

    if await check_existence(
        conn,
        default_buffer_values_dict,
        conditions={
            "buffer_type_id": buffer_value.buffer_type_id,
            "physical_object_type_id": buffer_value.physical_object_type_id,
            "service_type_id": buffer_value.service_type_id,
        },
    ):
        raise EntityAlreadyExists(
            "default buffer value",
            buffer_value.buffer_type_id,
            buffer_value.physical_object_type_id,
            buffer_value.service_type_id,
        )

    statement = (
        insert(default_buffer_values_dict)
        .values(**buffer_value.model_dump())
        .returning(default_buffer_values_dict.c.default_buffer_value_id)
    )
    default_buffer_value_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_default_buffer_value_from_db(conn, default_buffer_value_id)


async def put_default_buffer_value_to_db(
    conn: AsyncConnection, buffer_value: DefaultBufferValuePut
) -> DefaultBufferValueDTO:
    """Add or update a default buffer value."""

    if not await check_existence(conn, buffer_types_dict, conditions={"buffer_type_id": buffer_value.buffer_type_id}):
        raise EntityNotFoundById(buffer_value.buffer_type_id, "buffer type")

    if buffer_value.physical_object_type_id is not None:
        if not await check_existence(
            conn,
            physical_object_types_dict,
            conditions={"physical_object_type_id": buffer_value.physical_object_type_id},
        ):
            raise EntityNotFoundById(buffer_value.physical_object_type_id, "physical object type")

    if buffer_value.service_type_id is not None:
        if not await check_existence(
            conn,
            service_types_dict,
            conditions={"service_type_id": buffer_value.service_type_id},
        ):
            raise EntityNotFoundById(buffer_value.service_type_id, "service type")

    if await check_existence(
        conn,
        default_buffer_values_dict,
        conditions={
            "buffer_type_id": buffer_value.buffer_type_id,
            "physical_object_type_id": buffer_value.physical_object_type_id,
            "service_type_id": buffer_value.service_type_id,
        },
    ):
        statement = (
            update(default_buffer_values_dict)
            .where(
                default_buffer_values_dict.c.buffer_type_id == buffer_value.buffer_type_id,
                (
                    default_buffer_values_dict.c.physical_object_type_id == buffer_value.physical_object_type_id
                    if buffer_value.physical_object_type_id is not None
                    else default_buffer_values_dict.c.physical_object_type_id.is_(None)
                ),
                (
                    default_buffer_values_dict.c.service_type_id == buffer_value.service_type_id
                    if buffer_value.service_type_id is not None
                    else default_buffer_values_dict.c.service_type_id.is_(None)
                ),
            )
            .values(**buffer_value.model_dump())
            .returning(default_buffer_values_dict.c.default_buffer_value_id)
        )
    else:
        statement = (
            insert(default_buffer_values_dict)
            .values(**buffer_value.model_dump())
            .returning(default_buffer_values_dict.c.default_buffer_value_id)
        )

    default_buffer_value_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_default_buffer_value_from_db(conn, default_buffer_value_id)


async def get_buffer_from_db(conn: AsyncConnection, buffer_type_id: int, urban_object_id: int) -> BufferDTO:
    """Get buffer object by identifier."""

    statement = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            urban_objects_data.c.urban_object_id,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(buffers_data.c.geometry).label("geometry"),
            buffers_data.c.is_custom,
        )
        .select_from(
            buffers_data.join(buffer_types_dict, buffer_types_dict.c.buffer_type_id == buffers_data.c.buffer_type_id)
            .join(urban_objects_data, urban_objects_data.c.urban_object_id == buffers_data.c.urban_object_id)
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
        )
        .where(buffers_data.c.buffer_type_id == buffer_type_id, buffers_data.c.urban_object_id == urban_object_id)
    )

    result = (await conn.execute(statement)).mappings().one()

    return BufferDTO(**result)


async def put_buffer_to_db(conn: AsyncConnection, buffer: BufferPut) -> BufferDTO:
    """Create or update a new buffer object."""

    if not await check_existence(conn, buffer_types_dict, conditions={"buffer_type_id": buffer.buffer_type_id}):
        raise EntityNotFoundById(buffer.buffer_type_id, "buffer type")

    if not await check_existence(conn, urban_objects_data, conditions={"urban_object_id": buffer.urban_object_id}):
        raise EntityNotFoundById(buffer.urban_object_id, "urban object")

    values = extract_values_from_model(buffer, exclude_unset=True, allow_null_geometry=True)

    if await check_existence(
        conn,
        buffers_data,
        conditions={"buffer_type_id": buffer.buffer_type_id, "urban_object_id": buffer.urban_object_id},
    ):
        statement = (
            update(buffers_data)
            .where(
                buffers_data.c.buffer_type_id == buffer.buffer_type_id,
                buffers_data.c.urban_object_id == buffer.urban_object_id,
            )
            .values(**values, is_custom=buffer.geometry is not None)
        )
    else:
        statement = insert(buffers_data).values(**values, is_custom=buffer.geometry is not None)

    await conn.execute(statement)
    await conn.commit()

    return await get_buffer_from_db(conn, buffer.buffer_type_id, buffer.urban_object_id)


async def delete_buffer_from_db(conn: AsyncConnection, buffer_type_id: int, urban_object_id: int) -> dict:
    """Delete buffer object."""

    if not await check_existence(
        conn,
        buffers_data,
        conditions={"buffer_type_id": buffer_type_id, "urban_object_id": urban_object_id},
    ):
        raise EntityNotFoundByParams("buffer", buffer_type_id, urban_object_id)

    statement = delete(buffers_data).where(
        buffers_data.c.buffer_type_id == buffer_type_id,
        buffers_data.c.urban_object_id == urban_object_id,
    )

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
