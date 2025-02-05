"""Unit tests for physical objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, call, patch

import pytest
from geoalchemy2.functions import (
    ST_AsGeoJSON,
    ST_Buffer,
    ST_CoveredBy,
    ST_Covers,
    ST_GeomFromText,
    ST_Intersects,
)
from geoalchemy2.types import Geography, Geometry
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon
from sqlalchemy import cast, delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    living_buildings_data,
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
from idu_api.urban_api.dto import (
    LivingBuildingDTO,
    ObjectGeometryDTO,
    PhysicalObjectDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    UrbanObjectDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.physical_objects import (
    add_living_building_to_db,
    add_physical_object_to_object_geometry_to_db,
    add_physical_object_with_geometry_to_db,
    delete_living_building_from_db,
    delete_physical_object_from_db,
    get_living_buildings_by_physical_object_id_from_db,
    get_physical_object_by_id_from_db,
    get_physical_object_geometries_from_db,
    get_physical_objects_around_from_db,
    get_physical_objects_with_geometry_by_ids_from_db,
    get_services_by_physical_object_id_from_db,
    get_services_with_geometry_by_physical_object_id_from_db,
    patch_living_building_to_db,
    patch_physical_object_to_db,
    put_living_building_to_db,
    put_physical_object_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, OBJECTS_NUMBER_LIMIT
from idu_api.urban_api.schemas import (
    LivingBuilding,
    LivingBuildingPatch,
    LivingBuildingPost,
    LivingBuildingPut,
    ObjectGeometry,
    PhysicalObject,
    PhysicalObjectPatch,
    PhysicalObjectPost,
    PhysicalObjectPut,
    PhysicalObjectWithGeometry,
    PhysicalObjectWithGeometryPost,
    Service,
    ServiceWithGeometry,
    UrbanObject,
)
from tests.urban_api.helpers.connection import MockConnection

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString | MultiPoint

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_physical_objects_with_geometry_by_ids_from_db(mock_conn: MockConnection):
    """Test the get_physical_objects_with_geometry_by_ids_from_db function."""

    # Arrange
    ids = [1]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id.in_(ids))
        .distinct()
    )

    # Act
    with pytest.raises(TooManyObjectsError):
        await get_physical_objects_with_geometry_by_ids_from_db(mock_conn, too_many_ids)
    result = await get_physical_objects_with_geometry_by_ids_from_db(mock_conn, ids)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(obj, PhysicalObjectWithGeometryDTO) for obj in result
    ), "Each item should be a PhysicalObjectWithGeometryDTO."
    assert isinstance(
        PhysicalObjectWithGeometry.from_dto(result[0]), PhysicalObjectWithGeometry
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_physical_objects_around_from_db(mock_conn: MockConnection, shapely_geometry: Geom):
    """Test the get_physical_objects_around_from_db function."""

    # Arrange
    physical_object_type_id = None
    buffer_meters = 500
    buffered_geometry_cte = select(
        cast(
            ST_Buffer(
                cast(ST_GeomFromText(str(shapely_geometry.wkt), text("4326")), Geography(srid=4326)), buffer_meters
            ),
            Geometry(srid=4326),
        ).label("geometry"),
    ).cte("buffered_geometry_cte")
    fine_territories_cte = (
        select(territories_data.c.territory_id.label("territory_id"))
        .where(ST_CoveredBy(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()))
        .cte("fine_territories_cte")
    )
    possible_territories_cte = (
        select(territories_data.c.territory_id.label("territory_id"))
        .where(
            ST_Intersects(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
            | ST_Covers(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
        )
        .cte("possible_territories_cte")
    )
    statement = (
        select(physical_objects_data.c.physical_object_id)
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            ).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            object_geometries_data.c.territory_id.in_(select(fine_territories_cte.c.territory_id).scalar_subquery())
            | object_geometries_data.c.territory_id.in_(
                select(possible_territories_cte.c.territory_id).scalar_subquery()
            )
            & (
                ST_Intersects(
                    object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
                )
                | ST_Covers(
                    object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
                )
                | ST_CoveredBy(
                    object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
                )
            ),
        )
        .distinct()
    )

    # Act
    result = await get_physical_objects_around_from_db(
        mock_conn, shapely_geometry, physical_object_type_id, buffer_meters
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(obj, PhysicalObjectWithGeometryDTO) for obj in result
    ), "Each item should be a PhysicalObjectWithGeometryDTO."
    assert isinstance(
        PhysicalObjectWithGeometry.from_dto(result[0]), PhysicalObjectWithGeometry
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_physical_object_by_id_from_db(mock_conn: MockConnection):
    """Test the get_physical_object_by_id_from_db function."""

    # Arrange
    physical_object_id = 1
    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            physical_objects_data.join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    # Act
    result = await get_physical_object_by_id_from_db(mock_conn, physical_object_id)

    # Assert
    assert isinstance(result, PhysicalObjectDTO), "Result should be a PhysicalObjectDTO."
    assert isinstance(PhysicalObject.from_dto(result), PhysicalObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_physical_object_with_geometry_to_db(
    mock_conn: MockConnection, physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost
):
    """Test the add_physical_object_with_geometry_to_db function."""

    # Arrange
    async def check_physical_object_type(conn, table, conditions):
        if table == physical_object_types_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    statement_insert_physical_object = (
        insert(physical_objects_data)
        .values(
            physical_object_type_id=physical_object_with_geometry_post_req.physical_object_type_id,
            name=physical_object_with_geometry_post_req.name,
            properties=physical_object_with_geometry_post_req.properties,
        )
        .returning(physical_objects_data.c.physical_object_id)
    )
    statement_insert_geometry = (
        insert(object_geometries_data)
        .values(
            territory_id=physical_object_with_geometry_post_req.territory_id,
            geometry=ST_GeomFromText(
                physical_object_with_geometry_post_req.geometry.as_shapely_geometry().wkt, text("4326")
            ),
            centre_point=ST_GeomFromText(
                physical_object_with_geometry_post_req.centre_point.as_shapely_geometry().wkt, text("4326")
            ),
            address=physical_object_with_geometry_post_req.address,
            osm_id=physical_object_with_geometry_post_req.osm_id,
        )
        .returning(object_geometries_data.c.object_geometry_id)
    )
    statement_insert_urban_object = (
        insert(urban_objects_data)
        .values(physical_object_id=1, object_geometry_id=1)
        .returning(urban_objects_data.c.urban_object_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_physical_object_with_geometry_to_db(mock_conn, physical_object_with_geometry_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_physical_object_with_geometry_to_db(mock_conn, physical_object_with_geometry_post_req)
    result = await add_physical_object_with_geometry_to_db(mock_conn, physical_object_with_geometry_post_req)

    # Assert
    assert isinstance(result, UrbanObjectDTO), "Result should be an UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert_physical_object))
    mock_conn.execute_mock.assert_any_call(str(statement_insert_geometry))
    mock_conn.execute_mock.assert_any_call(str(statement_insert_urban_object))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_physical_object_to_db(mock_conn: MockConnection, physical_object_put_req: PhysicalObjectPut):
    """Test the put_physical_object_to_db function."""

    # Arrange
    async def check_physical_object_type(conn, table, conditions):
        if table == physical_object_types_dict:
            return False
        return True

    async def check_physical_object(conn, table, conditions):
        if table == physical_objects_data:
            return False
        return True

    physical_object_id = 1
    statement_update = (
        update(physical_objects_data)
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .values(**physical_object_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_physical_object_to_db(mock_conn, physical_object_put_req, physical_object_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_physical_object_to_db(mock_conn, physical_object_put_req, physical_object_id)
    result = await put_physical_object_to_db(mock_conn, physical_object_put_req, physical_object_id)

    # Assert
    assert isinstance(result, PhysicalObjectDTO), "Result should be a PhysicalObjectDTO."
    assert isinstance(PhysicalObject.from_dto(result), PhysicalObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_patch_physical_object_to_db(mock_conn: MockConnection, physical_object_patch_req: PhysicalObjectPatch):
    """Test the patch_physical_object_to_db function."""

    # Arrange
    async def check_physical_object_type(conn, table, conditions):
        if table == physical_object_types_dict:
            return False
        return True

    async def check_physical_object(conn, table, conditions):
        if table == physical_objects_data:
            return False
        return True

    physical_object_id = 1
    statement_update = (
        update(physical_objects_data)
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .values(**physical_object_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_physical_object_to_db(mock_conn, physical_object_patch_req, physical_object_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_physical_object_to_db(mock_conn, physical_object_patch_req, physical_object_id)
    result = await patch_physical_object_to_db(mock_conn, physical_object_patch_req, physical_object_id)

    # Assert
    assert isinstance(result, PhysicalObjectDTO), "Result should be a PhysicalObjectDTO."
    assert isinstance(PhysicalObject.from_dto(result), PhysicalObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_physical_object_from_db(mock_conn: MockConnection):
    """Test the delete_physical_object_in_db function."""

    # Arrange
    physical_object_id = 1
    statement_delete = delete(physical_objects_data).where(
        physical_objects_data.c.physical_object_id == physical_object_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence") as mock_check_existence:
        result = await delete_physical_object_from_db(mock_conn, physical_object_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_physical_object_from_db(mock_conn, physical_object_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement_delete))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_add_living_building_to_db(mock_conn: MockConnection, living_building_post_req: LivingBuildingPost):
    """Test the add_living_building_to_db function."""

    # Arrange
    async def check_physical_object(conn, table, conditions, not_conditions=None):
        if table == physical_objects_data:
            return False
        return True

    async def check_living_building(conn, table, conditions, not_conditions=None):
        if table == living_buildings_data:
            return False
        return True

    statement_insert = (
        insert(living_buildings_data)
        .values(**living_building_post_req.model_dump())
        .returning(living_buildings_data.c.living_building_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_living_building_to_db(mock_conn, living_building_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_living_building_to_db(mock_conn, living_building_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_living_building),
    ):
        result = await add_living_building_to_db(mock_conn, living_building_post_req)

    # Assert
    assert isinstance(result, PhysicalObjectDTO), "Result should be a PhysicalObjectDTO."
    assert isinstance(PhysicalObject.from_dto(result), PhysicalObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_living_building_to_db(mock_conn: MockConnection, living_building_put_req: LivingBuildingPut):
    """Test the put_living_building_to_db function."""

    # Arrange
    async def check_physical_object(conn, table, conditions, not_conditions=None):
        if table == physical_objects_data:
            return False
        return True

    async def check_living_building(conn, table, conditions, not_conditions=None):
        if table == living_buildings_data:
            return False
        return True

    statement_insert = insert(living_buildings_data).values(**living_building_put_req.model_dump())
    statement_update = (
        update(living_buildings_data)
        .where(living_buildings_data.c.physical_object_id == living_building_put_req.physical_object_id)
        .values(**living_building_put_req.model_dump())
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_living_building_to_db(mock_conn, living_building_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_living_building),
    ):
        await put_living_building_to_db(mock_conn, living_building_put_req)
    result = await put_living_building_to_db(mock_conn, living_building_put_req)

    # Assert
    assert isinstance(result, PhysicalObjectDTO), "Result should be a PhysicalObjectDTO."
    assert isinstance(PhysicalObject.from_dto(result), PhysicalObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_patch_living_building_to_db(mock_conn: MockConnection, living_building_patch_req: LivingBuildingPatch):
    """Test the patch_living_building_to_db function."""

    # Arrange
    living_building_id = 1

    async def check_physical_object(conn, table, conditions, not_conditions=None):
        if table == physical_objects_data:
            return False
        return True

    async def check_living_building_id(conn, table, conditions, not_conditions=None):
        if table == living_buildings_data and conditions == {"living_building_id": living_building_id}:
            return False
        return True

    async def check_living_building(conn, table, conditions, not_conditions=None):
        if table == living_buildings_data and conditions == {
            "physical_object_id": living_building_patch_req.physical_object_id
        }:
            return False
        return True

    statement_update = (
        update(living_buildings_data)
        .where(living_buildings_data.c.living_building_id == living_building_id)
        .values(**living_building_patch_req.model_dump(exclude_unset=True))
        .returning(living_buildings_data.c.physical_object_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await patch_living_building_to_db(mock_conn, living_building_patch_req, living_building_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_living_building_id),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_living_building_to_db(mock_conn, living_building_patch_req, living_building_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_living_building_to_db(mock_conn, living_building_patch_req, living_building_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_living_building),
    ):
        result = await patch_living_building_to_db(mock_conn, living_building_patch_req, living_building_id)

    # Assert
    assert isinstance(result, PhysicalObjectDTO), "Result should be a PhysicalObjectDTO."
    assert isinstance(PhysicalObject.from_dto(result), PhysicalObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_living_building_from_db(mock_conn: MockConnection):
    """Test the delete_physical_object_in_db function."""

    # Arrange
    living_building_id = 1
    statement_delete = delete(living_buildings_data).where(
        living_buildings_data.c.living_building_id == living_building_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence") as mock_check_existence:
        result = await delete_living_building_from_db(mock_conn, living_building_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_living_building_from_db(mock_conn, living_building_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement_delete))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_living_buildings_by_physical_object_id_from_db(mock_conn: MockConnection):
    """Test the get_living_buildings_by_physical_object_id_from_db function."""

    # Arrange
    physical_object_id = 1
    statement = (
        select(
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            living_buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == living_buildings_data.c.physical_object_id,
            ).join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
        )
        .where(living_buildings_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence") as mock_check_existence:
        result = await get_living_buildings_by_physical_object_id_from_db(mock_conn, physical_object_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_living_buildings_by_physical_object_id_from_db(mock_conn, physical_object_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(obj, LivingBuildingDTO) for obj in result), "Each item should be a LivingBuildingDTO."
    assert isinstance(LivingBuilding.from_dto(result[0]), LivingBuilding), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_services_by_physical_object_id_from_db(mock_conn: MockConnection):
    """Test the get_services_by_physical_object_id_from_db function."""

    # Arrange
    physical_object_id = 1
    service_type_id = 1
    territory_type_id = 1
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
            services_data.join(urban_objects_data, urban_objects_data.c.service_id == services_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )
    statement_with_filters = statement.where(
        service_types_dict.c.service_type_id == service_type_id,
        territory_types_dict.c.territory_type_id == territory_type_id,
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence") as mock_check_existence:
        await get_services_by_physical_object_id_from_db(
            mock_conn, physical_object_id, service_type_id, territory_type_id
        )
        result = await get_services_by_physical_object_id_from_db(mock_conn, physical_object_id, None, None)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_services_by_physical_object_id_from_db(mock_conn, physical_object_id, None, None)

            # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(obj, ServiceDTO) for obj in result), "Each item should be a ServiceDTO."
    assert isinstance(Service.from_dto(result[0]), Service), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_services_with_geometry_by_physical_object_id_from_db(mock_conn: MockConnection):
    """Test the get_services_with_geometry_by_physical_object_id_from_db function."""

    # Arrange
    physical_object_id = 1
    service_type_id = 1
    territory_type_id = 1
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
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )
    statement_with_filters = statement.where(
        service_types_dict.c.service_type_id == service_type_id,
        territory_types_dict.c.territory_type_id == territory_type_id,
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence") as mock_check_existence:
        await get_services_with_geometry_by_physical_object_id_from_db(
            mock_conn, physical_object_id, service_type_id, territory_type_id
        )
        result = await get_services_with_geometry_by_physical_object_id_from_db(
            mock_conn, physical_object_id, None, None
        )
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_services_with_geometry_by_physical_object_id_from_db(mock_conn, physical_object_id, None, None)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(obj, ServiceWithGeometryDTO) for obj in result
    ), "Each item should be a ServiceWithGeometryDTO."
    assert isinstance(
        ServiceWithGeometry.from_dto(result[0]), ServiceWithGeometry
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_physical_object_geometries_from_db(mock_conn: MockConnection):
    """Test the get_physical_object_geometries_from_db function."""

    # Arrange
    physical_object_id = 1
    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
        )
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            ).join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence") as mock_check_existence:
        result = await get_physical_object_geometries_from_db(mock_conn, physical_object_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_physical_object_geometries_from_db(mock_conn, physical_object_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(obj, ObjectGeometryDTO) for obj in result), "Each item should be a ObjectGeometryDTO."
    assert isinstance(ObjectGeometry.from_dto(result[0]), ObjectGeometry), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_physical_object_to_object_geometry_to_db(
    mock_conn: MockConnection, physical_object_post_req: PhysicalObjectPost
):
    """Test the add_physical_object_to_object_geometry_to_db function."""

    # Arrange
    async def check_geometry(conn, table, conditions):
        if table == object_geometries_data:
            return False
        return True

    async def check_physical_object_type(conn, table, conditions):
        if table == physical_object_types_dict:
            return False
        return True

    object_geometry_id = 1
    physical_object_id = 1
    statement_insert_physical_object = (
        insert(physical_objects_data)
        .values(**physical_object_post_req.model_dump())
        .returning(physical_objects_data.c.physical_object_id)
    )
    statement_insert_urban_object = (
        insert(urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id)
        .returning(urban_objects_data.c.urban_object_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_geometry),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_physical_object_to_object_geometry_to_db(mock_conn, object_geometry_id, physical_object_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.physical_objects.check_existence",
        new=AsyncMock(side_effect=check_physical_object_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_physical_object_to_object_geometry_to_db(mock_conn, object_geometry_id, physical_object_post_req)
    result = await add_physical_object_to_object_geometry_to_db(mock_conn, physical_object_id, physical_object_post_req)

    # Assert
    assert isinstance(result, UrbanObjectDTO), "Result should be an instance of UrbanObjectDTO."
    assert isinstance(UrbanObject.from_dto(result), UrbanObject), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_has_calls(
        [
            call(str(statement_insert_physical_object)),
            call(str(statement_insert_urban_object)),
        ],
        any_order=False,
    )
    mock_conn.commit_mock.assert_called_once()
