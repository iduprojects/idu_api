"""Unit tests for services objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.sql import delete, insert, select, update

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
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.services import (
    add_service_to_db,
    add_service_to_object_in_db,
    delete_service_from_db,
    get_service_by_id_from_db,
    patch_service_to_db,
    put_service_to_db,
)
from idu_api.urban_api.schemas import Service, ServicePatch, ServicePost, ServicePut, UrbanObject
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_service_by_id_from_db(mock_conn: MockConnection):
    """Test the get_service_by_id_from_db function."""

    # Arrange
    service_id = 1
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

    # Act
    result = await get_service_by_id_from_db(mock_conn, service_id)

    # Assert
    assert isinstance(result, ServiceDTO), "Result should be a ServiceDTO."
    assert isinstance(Service.from_dto(result), Service), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_service_to_db(mock_conn: MockConnection, service_post_req: ServicePost):
    """Test the add_service_to_db function."""

    # Arrange
    urban_objects_statement = select(urban_objects_data).where(
        urban_objects_data.c.physical_object_id == service_post_req.physical_object_id,
        urban_objects_data.c.object_geometry_id == service_post_req.object_geometry_id,
    )
    insert_service_statement = (
        insert(services_data)
        .values(**service_post_req.model_dump(exclude={"physical_object_id", "object_geometry_id"}))
        .returning(services_data.c.service_id)
    )
    insert_urban_object_statement = insert(urban_objects_data).values(
        service_id=1,
        physical_object_id=service_post_req.physical_object_id,
        object_geometry_id=service_post_req.object_geometry_id,
    )

    # Act
    result = await add_service_to_db(mock_conn, service_post_req)

    # Assert
    assert isinstance(result, ServiceDTO), "Result should be a ServiceDTO."
    assert isinstance(Service.from_dto(result), Service), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(urban_objects_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_service_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_urban_object_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_service_to_db(mock_conn: MockConnection, service_put_req: ServicePut):
    """Test the put_service_to_db function."""

    # Arrange
    async def check_service(conn, table, conditions, not_conditions=None):
        if table == services_data:
            return False
        return True

    async def check_service_type(conn, table, conditions, not_conditions=None):
        if table == service_types_dict:
            return False
        return True

    async def check_territory_type(conn, table, conditions, not_conditions=None):
        if table == territory_types_dict:
            return False
        return True

    service_id = 1
    update_service_statement = (
        update(services_data)
        .where(services_data.c.service_id == service_id)
        .values(**service_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.services.check_existence",
        new=AsyncMock(side_effect=check_service),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_service_to_db(mock_conn, service_put_req, service_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.services.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_service_to_db(mock_conn, service_put_req, service_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.services.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_service_to_db(mock_conn, service_put_req, service_id)
    result = await put_service_to_db(mock_conn, service_put_req, service_id)

    # Assert
    assert isinstance(result, ServiceDTO), "Result should be a ServiceDTO."
    assert isinstance(Service.from_dto(result), Service), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_service_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_patch_service_to_db(mock_conn: MockConnection, service_patch_req: ServicePatch):
    """Test the patch_service_to_db function."""

    # Arrange
    async def check_service(conn, table, conditions, not_conditions=None):
        if table == services_data:
            return False
        return True

    async def check_service_type(conn, table, conditions, not_conditions=None):
        if table == service_types_dict:
            return False
        return True

    async def check_territory_type(conn, table, conditions, not_conditions=None):
        if table == territory_types_dict:
            return False
        return True

    service_id = 1
    update_service_statement = (
        update(services_data)
        .where(services_data.c.service_id == service_id)
        .values(**service_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.services.check_existence",
        new=AsyncMock(side_effect=check_service),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_service_to_db(mock_conn, service_patch_req, service_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.services.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_service_to_db(mock_conn, service_patch_req, service_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.services.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_service_to_db(mock_conn, service_patch_req, service_id)
    result = await patch_service_to_db(mock_conn, service_patch_req, service_id)

    # Assert
    assert isinstance(result, ServiceDTO), "Result should be a ServiceDTO."
    assert isinstance(Service.from_dto(result), Service), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_service_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_service_in_db(mock_conn: MockConnection):
    """Test the delete_service_in_db function."""

    # Arrange
    service_id = 1
    delete_service_statement = delete(services_data).where(services_data.c.service_id == service_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.services.check_existence") as mock_check_existence:
        result = await delete_service_from_db(mock_conn, service_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_service_from_db(mock_conn, service_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_service_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_add_service_to_object_in_db(mock_conn: MockConnection):
    """Test the add_service_to_object_in_db function."""

    # Arrange
    service_id = 2
    physical_object_id = 1
    object_geometry_id = 1
    urban_objects_statement = select(urban_objects_data).where(
        urban_objects_data.c.physical_object_id == physical_object_id,
        urban_objects_data.c.object_geometry_id == object_geometry_id,
    )
    insert_urban_object_statement = (
        insert(urban_objects_data)
        .values(
            service_id=service_id,
            physical_object_id=physical_object_id,
            object_geometry_id=object_geometry_id,
        )
        .returning(urban_objects_data.c.urban_object_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_service_to_object_in_db(mock_conn, 1, physical_object_id, object_geometry_id)
    with patch("idu_api.urban_api.logic.impl.helpers.services.check_existence") as mock_check_existence:
        result = await add_service_to_object_in_db(mock_conn, service_id, physical_object_id, object_geometry_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await add_service_to_object_in_db(mock_conn, service_id, physical_object_id, object_geometry_id)

    # Assert
    assert isinstance(result, UrbanObjectDTO), "Result should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(urban_objects_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_urban_object_statement))
    mock_conn.commit_mock.assert_called_once()
