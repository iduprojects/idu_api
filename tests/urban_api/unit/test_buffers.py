"""Unit tests for buffer objects are defined here."""

from unittest.mock import AsyncMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon
from sqlalchemy import delete, insert, select, update

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
from idu_api.urban_api.logic.impl.helpers.buffers import (
    add_buffer_type_to_db,
    add_default_buffer_value_to_db,
    delete_buffer_from_db,
    get_all_default_buffer_values_from_db,
    get_buffer_from_db,
    get_buffer_types_from_db,
    get_default_buffer_value_from_db,
    put_buffer_to_db,
    put_default_buffer_value_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import extract_values_from_model
from idu_api.urban_api.schemas import (
    Buffer,
    BufferPut,
    BufferType,
    BufferTypePost,
    DefaultBufferValue,
    DefaultBufferValuePost,
    DefaultBufferValuePut,
)
from tests.urban_api.helpers.connection import MockConnection

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString | MultiPoint

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_buffer_types_from_db(mock_conn: MockConnection):
    """Test the get_buffer_types_from_db function."""

    # Arrange
    statement = select(buffer_types_dict).order_by(buffer_types_dict.c.buffer_type_id)

    # Act
    result = await get_buffer_types_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, BufferTypeDTO) for item in result), "Each item should be a BufferTypeDTO."
    assert isinstance(BufferType.from_dto(result[0]), BufferType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_buffer_type_to_db(mock_conn: MockConnection, buffer_type_post_req: BufferTypePost):
    """Test the add_buffer_type_to_db function."""

    # Arrange
    insert_statement = (
        insert(buffer_types_dict).values(**buffer_type_post_req.model_dump()).returning(buffer_types_dict)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.buffers.check_existence") as mock_check_existence:
        with pytest.raises(EntityAlreadyExists):
            await add_buffer_type_to_db(mock_conn, buffer_type_post_req)
        mock_check_existence.return_value = False
        result = await add_buffer_type_to_db(mock_conn, buffer_type_post_req)

    # Assert
    assert isinstance(result, BufferTypeDTO), "Result should be a BufferTypeDTO."
    assert isinstance(BufferType.from_dto(result), BufferType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(insert_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_all_default_buffer_values_from_db(mock_conn: MockConnection):
    """Test the get_all_default_buffer_values_from_db function."""

    # Arrange
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

    # Act
    result = await get_all_default_buffer_values_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, DefaultBufferValueDTO) for item in result
    ), "Each item should be a DefaultBufferValueDTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_default_buffer_value_from_db(mock_conn: MockConnection):
    """Test the get_default_buffer_value_from_db function."""

    # Arrange
    default_buffer_value_id = 1
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

    # Act
    result = await get_default_buffer_value_from_db(mock_conn, default_buffer_value_id)

    # Assert
    assert isinstance(result, DefaultBufferValueDTO), "Result should be a DefaultBufferValueDTO."
    assert isinstance(
        DefaultBufferValue.from_dto(result), DefaultBufferValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_default_buffer_value_to_db(
    mock_conn: MockConnection, default_buffer_value_post_req: DefaultBufferValuePost
):
    """Test the add_default_buffer_value_to_db function."""

    # Arrange
    async def check_default_buffer_value(conn, table, conditions=None):
        if table == default_buffer_values_dict:
            return False
        return True

    async def check_physical_object_type(conn, table, conditions=None):
        if table == physical_object_types_dict:
            return False
        return True

    async def check_service_type(conn, table, conditions=None):
        if table == service_types_dict:
            return False
        return True

    statement = (
        insert(default_buffer_values_dict)
        .values(**default_buffer_value_post_req.model_dump())
        .returning(default_buffer_values_dict.c.default_buffer_value_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_default_buffer_value_to_db(mock_conn, default_buffer_value_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_physical_object_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_default_buffer_value_to_db(mock_conn, default_buffer_value_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            req = default_buffer_value_post_req
            req.service_type_id = 1
            req.physical_object_type_id = None
            await add_default_buffer_value_to_db(mock_conn, req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_default_buffer_value),
    ):
        result = await add_default_buffer_value_to_db(mock_conn, default_buffer_value_post_req)

    # Assert
    assert isinstance(result, DefaultBufferValueDTO), "Result should be a DefaultBufferValueDTO."
    assert isinstance(
        DefaultBufferValue.from_dto(result), DefaultBufferValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_default_buffer_value_to_db(
    mock_conn: MockConnection, default_buffer_value_put_req: DefaultBufferValuePut
):
    """Test the put_default_buffer_value_to_db function."""

    # Arrange
    async def check_default_buffer_value(conn, table, conditions=None):
        if table == default_buffer_values_dict:
            return False
        return True

    async def check_physical_object_type(conn, table, conditions=None):
        if table == physical_object_types_dict:
            return False
        return True

    async def check_service_type(conn, table, conditions=None):
        if table == service_types_dict:
            return False
        return True

    statement_insert = (
        insert(default_buffer_values_dict)
        .values(**default_buffer_value_put_req.model_dump())
        .returning(default_buffer_values_dict.c.default_buffer_value_id)
    )
    statement_update = (
        update(default_buffer_values_dict)
        .where(
            default_buffer_values_dict.c.buffer_type_id == default_buffer_value_put_req.buffer_type_id,
            (
                default_buffer_values_dict.c.physical_object_type_id
                == default_buffer_value_put_req.physical_object_type_id
                if default_buffer_value_put_req.physical_object_type_id is not None
                else default_buffer_values_dict.c.physical_object_type_id.is_(None)
            ),
            (
                default_buffer_values_dict.c.service_type_id == default_buffer_value_put_req.service_type_id
                if default_buffer_value_put_req.service_type_id is not None
                else default_buffer_values_dict.c.service_type_id.is_(None)
            ),
        )
        .values(**default_buffer_value_put_req.model_dump())
        .returning(default_buffer_values_dict.c.default_buffer_value_id)
    )

    # Act
    await put_default_buffer_value_to_db(mock_conn, default_buffer_value_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_physical_object_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_default_buffer_value_to_db(mock_conn, default_buffer_value_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            req = default_buffer_value_put_req
            req.service_type_id = 1
            req.physical_object_type_id = None
            await add_default_buffer_value_to_db(mock_conn, req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_default_buffer_value),
    ):
        result = await add_default_buffer_value_to_db(mock_conn, default_buffer_value_put_req)

    # Assert
    assert isinstance(result, DefaultBufferValueDTO), "Result should be a DefaultBufferValueDTO."
    assert isinstance(
        DefaultBufferValue.from_dto(result), DefaultBufferValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def get_buffer_from_db(mock_conn: MockConnection):
    """Test the get_buffer_from_db function."""

    # Arrange
    buffer_type_id, urban_object_id = 1, 1
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

    # Act
    result = await get_buffer_from_db(mock_conn, buffer_type_id, urban_object_id)

    # Assert
    assert isinstance(result, BufferDTO), "Result should be a BufferDTO."
    assert isinstance(Buffer.from_dto(result), Buffer), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_put_buffer_to_db(mock_conn: MockConnection, buffer_put_req: BufferPut):
    """Test the put_buffer_to_db function."""

    # Arrange
    async def check_urban_object(conn, table, conditions=None):
        if table == urban_objects_data:
            return False
        return True

    async def check_buffer_type(conn, table, conditions=None):
        if table == buffer_types_dict:
            return False
        return True

    async def check_buffer(conn, table, conditions=None):
        if table == buffers_data:
            return False
        return True

    values = extract_values_from_model(buffer_put_req, exclude_unset=True, allow_null_geometry=True)
    statement_update = (
        update(buffers_data)
        .where(
            buffers_data.c.buffer_type_id == buffer_put_req.buffer_type_id,
            buffers_data.c.urban_object_id == buffer_put_req.urban_object_id,
        )
        .values(**values, is_custom=True)
    )
    statement_insert = insert(buffers_data).values(**values, is_custom=buffer_put_req.geometry is not None)

    # Act
    await put_buffer_to_db(mock_conn, buffer_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_urban_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_buffer_to_db(mock_conn, buffer_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_buffer_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_buffer_to_db(mock_conn, buffer_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.buffers.check_existence",
        new=AsyncMock(side_effect=check_buffer),
    ):
        result = await put_buffer_to_db(mock_conn, buffer_put_req)

    # Assert
    assert isinstance(result, BufferDTO), "Result should be a BufferDTO."
    assert isinstance(Buffer.from_dto(result), Buffer), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_delete_buffer_from_db(mock_conn: MockConnection):
    """Test the delete_buffer_from_db function."""

    # Arrange
    buffer_type_id, urban_object_id = 1, 1
    statement = delete(buffers_data).where(
        buffers_data.c.buffer_type_id == buffer_type_id,
        buffers_data.c.urban_object_id == urban_object_id,
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.buffers.check_existence") as mock_check_existence:
        result = await delete_buffer_from_db(mock_conn, buffer_type_id, urban_object_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundByParams):
            await delete_buffer_from_db(mock_conn, buffer_type_id, urban_object_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()
