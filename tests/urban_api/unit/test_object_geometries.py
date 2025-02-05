"""Unit tests for objects geometries are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, call, patch

import pytest
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import ObjectGeometryDTO, PhysicalObjectDTO, UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.object_geometries import (
    add_object_geometry_to_physical_object_to_db,
    delete_object_geometry_in_db,
    get_object_geometry_by_ids_from_db,
    get_physical_objects_by_object_geometry_id_from_db,
    patch_object_geometry_to_db,
    put_object_geometry_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, OBJECTS_NUMBER_LIMIT
from idu_api.urban_api.schemas import (
    ObjectGeometry,
    ObjectGeometryPatch,
    ObjectGeometryPost,
    ObjectGeometryPut,
    PhysicalObject,
    UrbanObject,
)
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_physical_objects_by_object_geometry_id_from_db(mock_conn: MockConnection):
    """Test the get_physical_objects_by_object_geometry_id_from_db function."""

    # Arrange
    object_geometry_id = 1
    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
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
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(urban_objects_data.c.object_geometry_id == object_geometry_id)
        .distinct()
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence") as mock_check_existence:
        result = await get_physical_objects_by_object_geometry_id_from_db(mock_conn, object_geometry_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_physical_objects_by_object_geometry_id_from_db(mock_conn, object_geometry_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, PhysicalObjectDTO) for item in result), "Each item should be a PhysicalObjectDTO."
    assert isinstance(PhysicalObject.from_dto(result[0]), PhysicalObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_object_geometry_by_ids_from_db(mock_conn: MockConnection):
    """Test the get_object_geometry_by_ids_from_db function."""

    # Arrange
    ids = [1]
    not_found_ids = [1, 2]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            object_geometries_data.join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(object_geometries_data.c.object_geometry_id.in_(ids))
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_object_geometry_by_ids_from_db(mock_conn, not_found_ids)
    with pytest.raises(TooManyObjectsError):
        await get_object_geometry_by_ids_from_db(mock_conn, too_many_ids)
    result = await get_object_geometry_by_ids_from_db(mock_conn, ids)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ObjectGeometryDTO) for item in result), "Each item should be an ObjectGeometryDTO."
    assert isinstance(ObjectGeometry.from_dto(result[0]), ObjectGeometry), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_put_object_geometry_to_db(mock_conn: MockConnection, object_geometries_put_req: ObjectGeometryPut):
    """Test the put_object_geometry_to_db function."""

    # Arrange
    async def check_object_geometry(conn, table, conditions):
        if table == object_geometries_data:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    object_geometry_id = 1
    statement_update = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(
            territory_id=object_geometries_put_req.territory_id,
            geometry=ST_GeomFromText(object_geometries_put_req.geometry.as_shapely_geometry().wkt, text("4326")),
            centre_point=ST_GeomFromText(
                object_geometries_put_req.centre_point.as_shapely_geometry().wkt, text("4326")
            ),
            address=object_geometries_put_req.address,
            osm_id=object_geometries_put_req.osm_id,
            updated_at=datetime.now(timezone.utc),
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence",
        new=AsyncMock(side_effect=check_object_geometry),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_object_geometry_to_db(mock_conn, object_geometries_put_req, object_geometry_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_object_geometry_to_db(mock_conn, object_geometries_put_req, object_geometry_id)
    result = await put_object_geometry_to_db(mock_conn, object_geometries_put_req, object_geometry_id)

    # Assert
    assert isinstance(result, ObjectGeometryDTO), "Result should be an ObjectGeometryDTO."
    assert isinstance(ObjectGeometry.from_dto(result), ObjectGeometry), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_patch_object_geometry_to_db(mock_conn: MockConnection, object_geometries_patch_req: ObjectGeometryPatch):
    """Test the patch_object_geometry_to_db function."""

    # Arrange
    async def check_object_geometry(conn, table, conditions):
        if table == object_geometries_data:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    object_geometry_id = 1
    statement_update = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(**object_geometries_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence",
        new=AsyncMock(side_effect=check_object_geometry),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_object_geometry_to_db(mock_conn, object_geometries_patch_req, object_geometry_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_object_geometry_to_db(mock_conn, object_geometries_patch_req, object_geometry_id)
    result = await patch_object_geometry_to_db(mock_conn, object_geometries_patch_req, object_geometry_id)

    # Assert
    assert isinstance(result, ObjectGeometryDTO), "Result should be an instance of ObjectGeometryDTO."
    assert isinstance(ObjectGeometry.from_dto(result), ObjectGeometry), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_object_geometry_in_db(mock_conn: MockConnection):
    """Test the delete_object_geometry_in_db function."""

    # Arrange
    object_geometry_id = 1
    statement_delete = delete(object_geometries_data).where(
        object_geometries_data.c.object_geometry_id == object_geometry_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence") as mock_check_existence:
        result = await delete_object_geometry_in_db(mock_conn, object_geometry_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_object_geometry_in_db(mock_conn, object_geometry_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement_delete))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_add_object_geometry_to_physical_object_to_db(
    mock_conn: MockConnection, object_geometries_post_req: ObjectGeometryPost
):
    """Test the add_object_geometry_to_physical_object_to_db function."""

    # Arrange
    async def check_physical_object(conn, table, conditions):
        if table == physical_objects_data:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    object_geometry_id = 1
    physical_object_id = 1
    statement_insert_geometry = (
        insert(object_geometries_data)
        .values(
            territory_id=object_geometries_post_req.territory_id,
            geometry=ST_GeomFromText(object_geometries_post_req.geometry.as_shapely_geometry().wkt, text("4326")),
            centre_point=ST_GeomFromText(
                object_geometries_post_req.centre_point.as_shapely_geometry().wkt, text("4326")
            ),
            address=object_geometries_post_req.address,
            osm_id=object_geometries_post_req.osm_id,
        )
        .returning(object_geometries_data.c.object_geometry_id)
    )
    statement_insert_urban_object = (
        insert(urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id)
        .returning(urban_objects_data.c.urban_object_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence",
        new=AsyncMock(side_effect=check_physical_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_object_geometry_to_physical_object_to_db(
                mock_conn, physical_object_id, object_geometries_post_req
            )
    with patch(
        "idu_api.urban_api.logic.impl.helpers.object_geometries.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_object_geometry_to_physical_object_to_db(
                mock_conn, physical_object_id, object_geometries_post_req
            )
    result = await add_object_geometry_to_physical_object_to_db(
        mock_conn, physical_object_id, object_geometries_post_req
    )

    # Assert
    assert isinstance(result, UrbanObjectDTO), "Result should be an instance of UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_has_calls(
        [
            call(str(statement_insert_geometry)),
            call(str(statement_insert_urban_object)),
        ],
        any_order=False,
    )
    mock_conn.commit_mock.assert_called_once()
