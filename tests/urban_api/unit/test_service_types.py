"""Unit tests for service types are defined here."""

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.sql import delete, insert, select, update

from idu_api.common.db.entities import (
    object_service_types_dict,
    physical_object_functions_dict,
    physical_object_types_dict,
    service_types_dict,
    soc_group_values_data,
    soc_groups_dict,
    urban_functions_dict,
)
from idu_api.urban_api.dto import (
    PhysicalObjectTypeDTO,
    ServiceTypeDTO,
    ServiceTypesHierarchyDTO,
    SocGroupWithServiceTypesDTO,
    UrbanFunctionDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.service_types import (
    add_service_type_to_db,
    add_urban_function_to_db,
    delete_service_type_from_db,
    delete_urban_function_from_db,
    get_physical_object_types_by_service_type_id_from_db,
    get_service_type_by_id_from_db,
    get_service_types_from_db,
    get_service_types_hierarchy_from_db,
    get_social_groups_by_service_type_id_from_db,
    get_urban_function_by_id_from_db,
    get_urban_functions_by_parent_id_from_db,
    patch_service_type_to_db,
    patch_urban_function_to_db,
    put_service_type_to_db,
    put_urban_function_to_db,
)
from idu_api.urban_api.schemas import (
    PhysicalObjectType,
    ServiceType,
    ServiceTypePatch,
    ServiceTypePost,
    SocGroupWithServiceTypes,
    UrbanFunction,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_service_types_from_db(mock_conn: MockConnection):
    """Test the get_service_types_from_db function."""

    # Arrange
    urban_function_id = 1
    name = "mock_string"
    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            service_types_dict.join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            service_types_dict.c.urban_function_id == urban_function_id, service_types_dict.c.name.ilike(f"%{name}%")
        )
        .order_by(service_types_dict.c.service_type_id)
    )

    # Act
    result = await get_service_types_from_db(mock_conn, urban_function_id, name)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ServiceTypeDTO) for item in result), "Each item should be a ServiceTypeDTO."
    assert isinstance(ServiceType.from_dto(result[0]), ServiceType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_service_type_by_id_from_db(mock_conn: MockConnection):
    """Test the get_service_type_by_id_from_db function."""

    # Arrange
    service_type_id = 1
    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            service_types_dict.join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(service_types_dict.c.service_type_id == service_type_id)
    )

    # Act
    result = await get_service_type_by_id_from_db(mock_conn, service_type_id)

    # Assert
    assert isinstance(result, ServiceTypeDTO), "Result should be a ServiceTypeDTO."
    assert isinstance(ServiceType.from_dto(result), ServiceType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_service_type_to_db(mock_conn: MockConnection, service_type_post_req: ServiceTypePost):
    """Test the add_service_type_to_db function."""

    # Arrange
    async def check_service_type(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    async def check_urban_function(conn, table, conditions):
        if table == urban_functions_dict:
            return False
        return True

    statement_insert = (
        insert(service_types_dict)
        .values(**service_type_post_req.model_dump())
        .returning(service_types_dict.c.service_type_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_service_type_to_db(mock_conn, service_type_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_urban_function),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_service_type_to_db(mock_conn, service_type_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        result = await add_service_type_to_db(mock_conn, service_type_post_req)

    # Assert
    assert isinstance(result, ServiceTypeDTO), "Result should be a ServiceTypeDTO."
    assert isinstance(ServiceType.from_dto(result), ServiceType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_service_type_to_db(mock_conn: MockConnection, service_type_put_req: ServiceTypePatch):
    """Test the put_service_type_to_db function."""

    # Arrange
    async def check_service_type_name(conn, table, conditions, not_conditions=None):
        if table == service_types_dict and conditions == {"name": service_type_put_req.name}:
            return False
        return True

    async def check_urban_function(conn, table, conditions, not_conditions=None):
        if table == urban_functions_dict:
            return False
        return True

    statement_update = (
        update(service_types_dict)
        .where(service_types_dict.c.name == service_type_put_req.name)
        .values(**service_type_put_req.model_dump())
        .returning(service_types_dict.c.service_type_id)
    )
    statement_insert = (
        insert(service_types_dict)
        .values(**service_type_put_req.model_dump())
        .returning(service_types_dict.c.service_type_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_urban_function),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_service_type_to_db(mock_conn, service_type_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_service_type_name),
    ):
        await put_service_type_to_db(mock_conn, service_type_put_req)
    result = await put_service_type_to_db(mock_conn, service_type_put_req)

    # Assert
    assert isinstance(result, ServiceTypeDTO), "Result should be a ServiceTypeDTO."
    assert isinstance(ServiceType.from_dto(result), ServiceType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_patch_service_type_to_db(mock_conn: MockConnection, service_type_patch_req: ServiceTypePatch):
    """Test the patch_service_type_to_db function."""

    # Arrange
    service_type_id = 1

    async def check_service_type_id(conn, table, conditions, not_conditions=None):
        if table == service_types_dict and conditions == {"service_type_id": service_type_id}:
            return False
        return True

    async def check_service_type_name(conn, table, conditions, not_conditions=None):
        if table == service_types_dict and conditions == {"name": service_type_patch_req.name}:
            return False
        return True

    async def check_urban_function(conn, table, conditions, not_conditions=None):
        if table == urban_functions_dict:
            return False
        return True

    statement_update = (
        update(service_types_dict)
        .where(service_types_dict.c.service_type_id == service_type_id)
        .values(**service_type_patch_req.model_dump(exclude_unset=True))
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await patch_service_type_to_db(mock_conn, service_type_id, service_type_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_service_type_id),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_service_type_to_db(mock_conn, service_type_id, service_type_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_urban_function),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_service_type_to_db(mock_conn, service_type_id, service_type_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_service_type_name),
    ):
        result = await patch_service_type_to_db(mock_conn, service_type_id, service_type_patch_req)

    # Assert
    assert isinstance(result, ServiceTypeDTO), "Result should be a ServiceTypeDTO."
    assert isinstance(ServiceType.from_dto(result), ServiceType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_service_type_from_db(mock_conn: MockConnection):
    """Test the delete_service_type_from_db function."""

    # Arrange
    service_type_id = 1
    statement = delete(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.service_types.check_existence") as mock_check_existence:
        result = await delete_service_type_from_db(mock_conn, service_type_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_service_type_from_db(mock_conn, service_type_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_urban_functions_by_parent_id_from_db(mock_conn: MockConnection):
    """Test the get_urban_functions_by_parent_id_from_db function."""

    # Arrange
    parent_id = 1
    name = "mock_string"
    urban_functions_parents = urban_functions_dict.alias("urban_functions_parents")
    statement = select(
        urban_functions_dict, urban_functions_parents.c.name.label("parent_urban_function_name")
    ).select_from(
        urban_functions_dict.outerjoin(
            urban_functions_parents,
            urban_functions_parents.c.urban_function_id == urban_functions_dict.c.parent_id,
        ),
    )
    cte_statement = statement.where(urban_functions_dict.c.parent_id == parent_id)
    cte_statement = cte_statement.cte(name="urban_function_recursive", recursive=True)
    recursive_part = statement.join(
        cte_statement, urban_functions_dict.c.parent_id == cte_statement.c.urban_function_id
    )
    recursive_statement = select(cte_statement.union_all(recursive_part))
    statement = statement.where(urban_functions_dict.c.parent_id == parent_id)
    requested_urban_functions = statement.cte("requested_urban_functions")
    statement = select(requested_urban_functions)
    recursive_statement = select(recursive_statement.cte("requested_urban_functions"))
    statement_with_filters = statement.where(requested_urban_functions.c.name.ilike(f"%{name}%"))

    # Act
    await get_urban_functions_by_parent_id_from_db(mock_conn, parent_id, None, True)
    await get_urban_functions_by_parent_id_from_db(mock_conn, parent_id, name, False)
    with patch("idu_api.urban_api.logic.impl.helpers.service_types.check_existence") as mock_check_existence:
        result = await get_urban_functions_by_parent_id_from_db(mock_conn, parent_id, None, False)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_urban_functions_by_parent_id_from_db(mock_conn, parent_id, None, False)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, UrbanFunctionDTO) for item in result), "Each item should be a UrbanFunctionDTO."
    assert isinstance(UrbanFunction.from_dto(result[0]), UrbanFunction), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_urban_function_by_id_from_db(mock_conn: MockConnection):
    """Test the get_urban_function_by_id_from_db function."""

    # Arrange
    urban_function_id = 1
    urban_functions_parents = urban_functions_dict.alias("urban_functions_parents")
    statement = (
        select(urban_functions_dict, urban_functions_parents.c.name.label("parent_urban_function_name"))
        .select_from(
            urban_functions_dict.outerjoin(
                urban_functions_parents,
                urban_functions_parents.c.urban_function_id == urban_functions_dict.c.parent_id,
            ),
        )
        .where(urban_functions_dict.c.urban_function_id == urban_function_id)
    )

    # Act
    result = await get_urban_function_by_id_from_db(mock_conn, urban_function_id)

    # Assert
    assert isinstance(result, UrbanFunctionDTO), "Result should be a UrbanFunctionDTO."
    assert isinstance(UrbanFunction.from_dto(result), UrbanFunction), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_urban_function_to_db(mock_conn: MockConnection, urban_function_post_req: UrbanFunctionPost):
    """Test the add_urban_function_to_db function."""

    # Arrange
    async def check_function(conn, table, conditions):
        if conditions == {"name": urban_function_post_req.name}:
            return False
        return True

    async def check_parent_function(conn, table, conditions):
        if conditions == {"urban_function_id": urban_function_post_req.parent_id}:
            return False
        return True

    statement = (
        insert(urban_functions_dict)
        .values(**urban_function_post_req.model_dump())
        .returning(urban_functions_dict.c.urban_function_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_urban_function_to_db(mock_conn, urban_function_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_parent_function),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_urban_function_to_db(mock_conn, urban_function_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_function),
    ):
        result = await add_urban_function_to_db(mock_conn, urban_function_post_req)

    # Assert
    assert isinstance(result, UrbanFunctionDTO), "Result should be a UrbanFunctionDTO."
    assert isinstance(UrbanFunction.from_dto(result), UrbanFunction), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_urban_function_to_db(mock_conn: MockConnection, urban_function_put_req: UrbanFunctionPut):
    """Test the put_urban_function_to_db function."""

    # Arrange
    async def check_function(conn, table, conditions):
        if conditions == {"name": urban_function_put_req.name}:
            return False
        return True

    async def check_parent_function(conn, table, conditions):
        if conditions == {"urban_function_id": urban_function_put_req.parent_id}:
            return False
        return True

    statement_insert = (
        insert(urban_functions_dict)
        .values(
            **urban_function_put_req.model_dump(),
        )
        .returning(urban_functions_dict.c.urban_function_id)
    )
    statement_update = (
        update(urban_functions_dict)
        .where(urban_functions_dict.c.name == urban_function_put_req.name)
        .values(**urban_function_put_req.model_dump())
        .returning(urban_functions_dict.c.urban_function_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_parent_function),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_urban_function_to_db(mock_conn, urban_function_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_function),
    ):
        await put_urban_function_to_db(mock_conn, urban_function_put_req)
    result = await put_urban_function_to_db(mock_conn, urban_function_put_req)

    # Assert
    assert isinstance(result, UrbanFunctionDTO), "Result should be a UrbanFunctionDTO."
    assert isinstance(UrbanFunction.from_dto(result), UrbanFunction), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_patch_urban_function_to_db(mock_conn: MockConnection, urban_function_patch_req: UrbanFunctionPatch):
    """Test the patch_urban_function_to_db function."""

    # Arrange
    urban_function_id = 1

    async def check_function_id(conn, table, conditions, not_conditions=None):
        if conditions == {"urban_function_id": urban_function_id}:
            return False
        return True

    async def check_function_name(conn, table, conditions, not_conditions=None):
        if conditions == {"name": urban_function_patch_req.name}:
            return False
        return True

    async def check_parent_function(conn, table, conditions, not_conditions=None):
        if conditions == {"urban_function_id": urban_function_patch_req.parent_id}:
            return False
        return True

    statement = (
        update(urban_functions_dict)
        .where(urban_functions_dict.c.urban_function_id == urban_function_id)
        .values(**urban_function_patch_req.model_dump(exclude_unset=True))
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await patch_urban_function_to_db(mock_conn, urban_function_id, urban_function_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_function_id),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_urban_function_to_db(mock_conn, urban_function_id, urban_function_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_parent_function),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_urban_function_to_db(mock_conn, urban_function_id, urban_function_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_function_name),
    ):
        result = await patch_urban_function_to_db(mock_conn, urban_function_id, urban_function_patch_req)

    # Assert
    assert isinstance(result, UrbanFunctionDTO), "Result should be a UrbanFunctionDTO."
    assert isinstance(UrbanFunction.from_dto(result), UrbanFunction), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_urban_function_from_db(mock_conn: MockConnection):
    """Test the delete_urban_function_from_db function."""

    # Arrange
    urban_function_id = 1
    statement = delete(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.service_types.check_existence") as mock_check_existence:
        result = await delete_urban_function_from_db(mock_conn, urban_function_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_urban_function_from_db(mock_conn, urban_function_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_service_types_hierarchy_from_db(mock_conn: MockConnection):
    """Test the get_service_types_hierarchy_from_db function."""

    # Arrange
    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            service_types_dict.join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .order_by(service_types_dict.c.service_type_id)
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_service_types_hierarchy_from_db(mock_conn, {1, 2})
    result = await get_service_types_hierarchy_from_db(mock_conn, None)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ServiceTypesHierarchyDTO) for item in result
    ), "Each item should be a ServiceTypesHierarchyDTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_physical_object_types_by_service_type_id_from_db(mock_conn: MockConnection):
    """Test the get_service_types_by_physical_object_type_id_from_db function."""

    # Arrange
    async def check_service_type_id(conn, table, conditions, not_conditions=None):
        if table == service_types_dict:
            return False
        return True

    service_type_id = 1
    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            physical_object_types_dict.join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            ).join(
                object_service_types_dict,
                object_service_types_dict.c.physical_object_type_id
                == physical_object_types_dict.c.physical_object_type_id,
            )
        )
        .where(object_service_types_dict.c.service_type_id == service_type_id)
        .order_by(physical_object_types_dict.c.physical_object_type_id)
        .distinct()
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_service_type_id),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_physical_object_types_by_service_type_id_from_db(mock_conn, service_type_id)
    result = await get_physical_object_types_by_service_type_id_from_db(mock_conn, service_type_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, PhysicalObjectTypeDTO) for item in result
    ), "Each item should be a PhysicalObjectTypeDTO."
    assert isinstance(
        PhysicalObjectType.from_dto(result[0]), PhysicalObjectType
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_with(str(statement))


@pytest.mark.asyncio
async def test_get_social_groups_by_service_type_id_from_db(mock_conn: MockConnection):
    """Test the get_social_groups_by_service_type_id_from_db function."""

    # Arrange
    async def check_service_type_id(conn, table, conditions, not_conditions=None):
        if table == service_types_dict:
            return False
        return True

    service_type_id = 1
    statement = (
        select(
            soc_groups_dict,
            service_types_dict.c.service_type_id.label("id"),
            service_types_dict.c.name.label("service_type_name"),
            soc_group_values_data.c.infrastructure_type,
        )
        .select_from(
            service_types_dict.join(
                soc_group_values_data,
                soc_group_values_data.c.service_type_id == service_types_dict.c.service_type_id,
            ).join(soc_groups_dict, soc_groups_dict.c.soc_group_id == soc_group_values_data.c.soc_group_id)
        )
        .where(service_types_dict.c.service_type_id == service_type_id)
        .order_by(soc_groups_dict.c.soc_group_id)
        .distinct()
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.service_types.check_existence",
        new=AsyncMock(side_effect=check_service_type_id),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_social_groups_by_service_type_id_from_db(mock_conn, service_type_id)
    result = await get_social_groups_by_service_type_id_from_db(mock_conn, service_type_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, SocGroupWithServiceTypesDTO) for item in result
    ), "Each item should be a SocGroupWithServiceTypesDTO."
    assert isinstance(
        SocGroupWithServiceTypes.from_dto(result[0]), SocGroupWithServiceTypes
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_with(str(statement))
