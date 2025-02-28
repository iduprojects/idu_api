"""Unit tests for urban objects are defined here."""

from unittest.mock import AsyncMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import select, update

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.urban_objects import (
    get_urban_objects_by_ids_from_db,
    get_urban_objects_by_object_geometry_id_from_db,
    get_urban_objects_by_physical_object_id_from_db,
    get_urban_objects_by_service_id_from_db,
    get_urban_objects_by_territory_id_from_db,
    patch_urban_object_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    include_child_territories_cte,
)
from idu_api.urban_api.schemas import UrbanObject, UrbanObjectPatch
from tests.urban_api.helpers.connection import MockConnection


@pytest.mark.asyncio
async def test_get_urban_objects_by_ids_from_db(mock_conn: MockConnection):
    """Test the get_urban_objects_by_ids_from_db function."""

    # Arrange
    ids = [1]
    not_found_ids = [1, 2]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    statement = (
        select(
            urban_objects_data,
            physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            object_geometries_data.c.created_at.label("object_geometry_created_at"),
            object_geometries_data.c.updated_at.label("object_geometry_updated_at"),
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties.label("service_properties"),
            services_data.c.created_at.label("service_created_at"),
            services_data.c.updated_at.label("service_updated_at"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(urban_objects_data.c.urban_object_id.in_(ids))
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_urban_objects_by_ids_from_db(mock_conn, not_found_ids)
    with pytest.raises(TooManyObjectsError):
        await get_urban_objects_by_ids_from_db(mock_conn, too_many_ids)
    result = await get_urban_objects_by_ids_from_db(mock_conn, ids)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, UrbanObjectDTO) for item in result), "Each item should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result[0]), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_urban_objects_by_physical_object_id(mock_conn: MockConnection):
    """Test the get_urban_objects_by_physical_object_id function."""

    # Arrange
    physical_object_id = 1
    statement = select(urban_objects_data.c.urban_object_id).where(
        urban_objects_data.c.physical_object_id == physical_object_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_urban_objects_by_physical_object_id_from_db(mock_conn, physical_object_id)
    result = await get_urban_objects_by_physical_object_id_from_db(mock_conn, physical_object_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, UrbanObjectDTO) for item in result), "Each item should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result[0]), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_urban_object_by_object_geometry_id_from_db(mock_conn: MockConnection):
    """Test the get_urban_object_by_object_geometry_id_from_db function."""

    # Arrange
    object_geometry_id = 1
    statement = select(urban_objects_data.c.urban_object_id).where(
        urban_objects_data.c.object_geometry_id == object_geometry_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_urban_objects_by_object_geometry_id_from_db(mock_conn, object_geometry_id)
    result = await get_urban_objects_by_object_geometry_id_from_db(mock_conn, object_geometry_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, UrbanObjectDTO) for item in result), "Each item should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result[0]), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_urban_object_by_service_id_from_db(mock_conn: MockConnection):
    """Test the get_urban_object_by_service_id_from_db function."""

    # Arrange
    service_id = 1
    statement = select(urban_objects_data.c.urban_object_id).where(urban_objects_data.c.service_id == service_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_urban_objects_by_service_id_from_db(mock_conn, service_id)
    result = await get_urban_objects_by_service_id_from_db(mock_conn, service_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, UrbanObjectDTO) for item in result), "Each item should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result[0]), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_urban_objects_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_urban_objects_by_territory_id_from_db function."""

    # Arrange
    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_physical_object_type(conn, table, conditions):
        if table == physical_object_types_dict:
            return False
        return True

    async def check_service_type(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    territory_id = 1
    service_type_id = 1
    physical_object_type_id = 1
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    territories_cte = include_child_territories_cte(territory_id)
    statement = (
        select(
            urban_objects_data,
            physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            object_geometries_data.c.created_at.label("object_geometry_created_at"),
            object_geometries_data.c.updated_at.label("object_geometry_updated_at"),
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties.label("service_properties"),
            services_data.c.created_at.label("service_created_at"),
            services_data.c.updated_at.label("service_updated_at"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            object_geometries_data.c.territory_id.in_(select(territories_cte)),
            physical_objects_data.c.physical_object_type_id == physical_object_type_id,
            services_data.c.service_type_id == service_type_id,
        )
        .order_by(urban_objects_data.c.urban_object_id)
        .distinct()
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_urban_objects_by_territory_id_from_db(mock_conn, territory_id, None, None)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_urban_objects_by_territory_id_from_db(mock_conn, territory_id, None, physical_object_type_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_urban_objects_by_territory_id_from_db(mock_conn, territory_id, service_type_id, None)
    result = await get_urban_objects_by_territory_id_from_db(
        mock_conn, territory_id, service_type_id, physical_object_type_id
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, UrbanObjectDTO) for item in result), "Each item should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result[0]), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_patch_urban_object_to_db(mock_conn: MockConnection, urban_object_patch_req: UrbanObjectPatch):
    """Test the patch_urban_object_to_db function."""

    # Arrange
    urban_object_id = 1

    async def check_urban_object(conn, table, conditions, not_conditions=None):
        if table == urban_objects_data:
            return False
        return True

    async def check_physical_object(conn, table, conditions, not_conditions=None):
        if table == physical_objects_data:
            return False
        return True

    async def check_object_geometry(conn, table, conditions, not_conditions=None):
        if table == object_geometries_data:
            return False
        return True

    async def check_service(conn, table, conditions, not_conditions=None):
        if table == services_data:
            return False
        return True

    statement = (
        update(urban_objects_data)
        .where(urban_objects_data.c.urban_object_id == urban_object_id)
        .values(**urban_object_patch_req.model_dump(exclude_unset=True))
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await patch_urban_object_to_db(mock_conn, urban_object_patch_req, urban_object_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_urban_object_to_db(mock_conn, urban_object_patch_req, urban_object_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence",
        new=AsyncMock(side_effect=check_object_geometry),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_urban_object_to_db(mock_conn, urban_object_patch_req, urban_object_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence",
        new=AsyncMock(side_effect=check_service),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_urban_object_to_db(mock_conn, urban_object_patch_req, urban_object_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.urban_objects.check_existence",
        new=AsyncMock(side_effect=check_urban_object),
    ):
        result = await patch_urban_object_to_db(mock_conn, urban_object_patch_req, urban_object_id)

    # Assert
    assert isinstance(result, UrbanObjectDTO), "Result should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()
