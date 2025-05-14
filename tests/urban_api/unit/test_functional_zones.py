"""Unit tests for functional zone objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB, ST_GeomFromWKB
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon
from sqlalchemy import case, delete, func, insert, or_, select, text, update

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    functional_zones_data,
    profiles_reclamation_data,
    territories_data,
)
from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneTypeDTO,
    ProfilesReclamationDataDTO,
    ProfilesReclamationDataMatrixDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    EntityNotFoundByParams,
)
from idu_api.urban_api.logic.impl.helpers.functional_zones import (
    add_functional_zone_to_db,
    add_functional_zone_type_to_db,
    add_profiles_reclamation_data_to_db,
    delete_functional_zone_from_db,
    delete_profiles_reclamation_data_from_db,
    get_all_sources_from_db,
    get_functional_zone_by_id,
    get_functional_zone_types_from_db,
    get_functional_zones_around_from_db,
    get_profiles_reclamation_data_by_id_from_db,
    get_profiles_reclamation_data_matrix_from_db,
    patch_functional_zone_to_db,
    put_functional_zone_to_db,
    put_profiles_reclamation_data_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import SRID
from idu_api.urban_api.schemas import (
    FunctionalZone,
    FunctionalZonePatch,
    FunctionalZonePost,
    FunctionalZonePut,
    FunctionalZoneType,
    FunctionalZoneTypePost,
    ProfilesReclamationData,
    ProfilesReclamationDataMatrix,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)
from tests.urban_api.helpers.connection import MockConnection

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString | MultiPoint

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_functional_zone_types_from_db(mock_conn: MockConnection):
    """Test the get_functional_zone_types_from_db function."""

    # Arrange
    statement = select(functional_zone_types_dict).order_by(functional_zone_types_dict.c.functional_zone_type_id)

    # Act
    result = await get_functional_zone_types_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, FunctionalZoneTypeDTO) for item in result
    ), "Each item should be a FunctionalZoneTypeDTO."
    assert isinstance(
        FunctionalZoneType.from_dto(result[0]), FunctionalZoneType
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_functional_zone_type_to_db(
    mock_conn: MockConnection, functional_zone_type_post_req: FunctionalZoneTypePost
):
    """Test the add_functional_zone_type_to_db function."""

    # Arrange
    insert_statement = (
        insert(functional_zone_types_dict)
        .values(**functional_zone_type_post_req.model_dump())
        .returning(functional_zone_types_dict)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence") as mock_check_existence:
        with pytest.raises(EntityAlreadyExists):
            await add_functional_zone_type_to_db(mock_conn, functional_zone_type_post_req)
        mock_check_existence.return_value = False
        result = await add_functional_zone_type_to_db(mock_conn, functional_zone_type_post_req)

    # Assert
    assert isinstance(result, FunctionalZoneTypeDTO), "Result should be a FunctionalZoneTypeDTO."
    assert isinstance(
        FunctionalZoneType.from_dto(result), FunctionalZoneType
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(insert_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_all_sources_from_db(mock_conn: MockConnection):
    """Test the get_all_sources_from_db function."""

    # Arrange
    statement = (
        select(profiles_reclamation_data.c.source_profile_id)
        .order_by(profiles_reclamation_data.c.source_profile_id)
        .distinct()
    )

    # Act
    await get_all_sources_from_db(mock_conn)
    result = await get_all_sources_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, int) for item in result), "Each item should be a Integer."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_profiles_reclamation_data_matrix_from_db(mock_conn: MockConnection):
    """Test the get_profiles_reclamation_data_matrix_from_db function."""

    # Arrange
    labels = [1]
    territory_id_int, territory_id_none = 1, None
    statement_for_sources = select(profiles_reclamation_data.c.source_profile_id).where(
        profiles_reclamation_data.c.source_profile_id.in_(labels)
    )
    statement_with_territory = (
        select(profiles_reclamation_data)
        .where(
            profiles_reclamation_data.c.source_profile_id.in_(labels),
            profiles_reclamation_data.c.target_profile_id.in_(labels),
            or_(
                profiles_reclamation_data.c.territory_id == territory_id_int,
                profiles_reclamation_data.c.territory_id.is_(None),
            ),
        )
        .order_by(
            profiles_reclamation_data.c.source_profile_id,
            case(
                (profiles_reclamation_data.c.territory_id == territory_id_int, 0),
                else_=1,
            ).desc(),
        )
    )
    statement_with_territory_none = (
        select(profiles_reclamation_data)
        .where(
            profiles_reclamation_data.c.source_profile_id.in_(labels),
            profiles_reclamation_data.c.target_profile_id.in_(labels),
            profiles_reclamation_data.c.territory_id.is_(None),
        )
        .order_by(
            profiles_reclamation_data.c.source_profile_id,
            case(
                (profiles_reclamation_data.c.territory_id.is_(None), 0),
                else_=1,
            ).desc(),
        )
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence") as mock_check_existence:
        await get_profiles_reclamation_data_matrix_from_db(mock_conn, labels, territory_id_none)
        result = await get_profiles_reclamation_data_matrix_from_db(mock_conn, labels, territory_id_int)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_profiles_reclamation_data_matrix_from_db(mock_conn, labels, territory_id_int)
        with pytest.raises(EntitiesNotFoundByIds):
            await get_profiles_reclamation_data_matrix_from_db(mock_conn, [1, 2], territory_id_int)

    # Assert
    assert isinstance(result, ProfilesReclamationDataMatrixDTO), "Result should be a ProfilesReclamationDataMatrixDTO."
    assert isinstance(
        ProfilesReclamationDataMatrix.from_dto(result), ProfilesReclamationDataMatrix
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_for_sources))
    mock_conn.execute_mock.assert_any_call(str(statement_with_territory))
    mock_conn.execute_mock.assert_any_call(str(statement_with_territory_none))


@pytest.mark.asyncio
async def test_get_profiles_reclamation_data_by_id_from_db(mock_conn: MockConnection):
    """Test the get_profiles_reclamation_data_by_id_from_db function."""

    # Arrange
    profile_reclamation_id = 1
    statement = (
        select(profiles_reclamation_data, territories_data.c.name.label("territory_name"))
        .select_from(
            profiles_reclamation_data.outerjoin(
                territories_data,
                territories_data.c.territory_id == profiles_reclamation_data.c.territory_id,
            )
        )
        .where(profiles_reclamation_data.c.profile_reclamation_id == profile_reclamation_id)
        .order_by(profiles_reclamation_data.c.profile_reclamation_id)
    )

    # Act
    result = await get_profiles_reclamation_data_by_id_from_db(mock_conn, profile_reclamation_id)

    # Assert
    assert isinstance(result, ProfilesReclamationDataDTO), "Result should be a ProfilesReclamationDataDTO."
    assert isinstance(
        ProfilesReclamationData.from_dto(result), ProfilesReclamationData
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.execute_mock.assert_called_once()


@pytest.mark.asyncio
async def test_add_profiles_reclamation_data_to_db(
    mock_conn: MockConnection, profiles_reclamation_post_req: ProfilesReclamationDataPost
):
    """Test the add_profiles_reclamation_data_to_db function."""

    # Arrange
    async def check_profiles_reclamation_data(conn, table, conditions=None):
        if table == profiles_reclamation_data:
            return False
        return True

    async def check_territories_data(conn, table, conditions=None):
        if table == profiles_reclamation_data:
            return False
        if table == territories_data:
            return False
        return True

    statement = (
        insert(profiles_reclamation_data)
        .values(**profiles_reclamation_post_req.model_dump())
        .returning(profiles_reclamation_data.c.profile_reclamation_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_profiles_reclamation_data_to_db(mock_conn, profiles_reclamation_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_territories_data),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_profiles_reclamation_data_to_db(mock_conn, profiles_reclamation_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_profiles_reclamation_data),
    ):
        result = await add_profiles_reclamation_data_to_db(mock_conn, profiles_reclamation_post_req)

    # Assert
    assert isinstance(result, ProfilesReclamationDataDTO), "Result should be a ProfilesReclamationDataDTO."
    assert isinstance(
        ProfilesReclamationData.from_dto(result), ProfilesReclamationData
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_profiles_reclamation_data_to_db(
    mock_conn: MockConnection, profiles_reclamation_put_req: ProfilesReclamationDataPut
):
    """Test the put_profiles_reclamation_data_to_db function."""

    # Arrange
    async def check_profiles_reclamation_data(conn, table, conditions=None):
        if table == profiles_reclamation_data:
            return False
        return True

    async def check_territories_data(conn, table, conditions=None):
        if table == profiles_reclamation_data:
            return False
        if table == territories_data:
            return False
        return True

    statement_insert = (
        insert(profiles_reclamation_data)
        .values(**profiles_reclamation_put_req.model_dump())
        .returning(profiles_reclamation_data.c.profile_reclamation_id)
    )
    statement_update = (
        update(profiles_reclamation_data)
        .where(
            profiles_reclamation_data.c.source_profile_id == profiles_reclamation_put_req.source_profile_id,
            profiles_reclamation_data.c.target_profile_id == profiles_reclamation_put_req.target_profile_id,
            profiles_reclamation_data.c.territory_id == profiles_reclamation_put_req.territory_id,
        )
        .values(**profiles_reclamation_put_req.model_dump())
        .returning(profiles_reclamation_data.c.profile_reclamation_id)
    )

    # Act
    await put_profiles_reclamation_data_to_db(mock_conn, profiles_reclamation_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_territories_data),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_profiles_reclamation_data_to_db(mock_conn, profiles_reclamation_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_profiles_reclamation_data),
    ):
        result = await put_profiles_reclamation_data_to_db(mock_conn, profiles_reclamation_put_req)

    # Assert
    assert isinstance(result, ProfilesReclamationDataDTO), "Result should be a ProfilesReclamationDataDTO."
    assert isinstance(
        ProfilesReclamationData.from_dto(result), ProfilesReclamationData
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_delete_profiles_reclamation_data_from_db(mock_conn: MockConnection):
    """Test the delete_profiles_reclamation_data_from_db function."""

    # Arrange
    source_id, target_id = 1, 1
    territory_id_int, territory_id_none = 1, None
    statement_with_territory = delete(profiles_reclamation_data).where(
        profiles_reclamation_data.c.source_profile_id == source_id,
        profiles_reclamation_data.c.target_profile_id == target_id,
        profiles_reclamation_data.c.territory_id == territory_id_int,
    )
    statement_with_territory_none = delete(profiles_reclamation_data).where(
        profiles_reclamation_data.c.source_profile_id == source_id,
        profiles_reclamation_data.c.target_profile_id == target_id,
        profiles_reclamation_data.c.territory_id.is_(None),
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence") as mock_check_existence:
        await delete_profiles_reclamation_data_from_db(mock_conn, source_id, target_id, territory_id_none)
        result = await delete_profiles_reclamation_data_from_db(mock_conn, source_id, target_id, territory_id_int)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundByParams):
            await delete_profiles_reclamation_data_from_db(mock_conn, source_id, target_id, territory_id_int)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(statement_with_territory))
    mock_conn.execute_mock.assert_any_call(str(statement_with_territory_none))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_get_functional_zone_by_id(mock_conn: MockConnection):
    """Test the get_functional_zone_by_id function."""

    # Arrange
    functional_zone_id = 1
    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zone_types_dict.c.description.label("functional_zone_type_description"),
            functional_zones_data.c.name,
            ST_AsEWKB(functional_zones_data.c.geometry).label("geometry"),
            functional_zones_data.c.year,
            functional_zones_data.c.source,
            functional_zones_data.c.properties,
            functional_zones_data.c.created_at,
            functional_zones_data.c.updated_at,
        )
        .select_from(
            functional_zones_data.join(
                territories_data,
                territories_data.c.territory_id == functional_zones_data.c.territory_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == functional_zones_data.c.functional_zone_type_id,
            )
        )
        .where(functional_zones_data.c.functional_zone_id.in_([functional_zone_id]))
    )

    # Act
    result = await get_functional_zone_by_id(mock_conn, functional_zone_id)

    # Assert
    assert isinstance(result, FunctionalZoneDTO), "Result should be a FunctionalZoneDTO."
    assert isinstance(FunctionalZone.from_dto(result), FunctionalZone), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_functional_zone_to_db(mock_conn: MockConnection, functional_zone_post_req: FunctionalZonePost):
    """Test the add_functional_zone_to_db function."""

    # Arrange
    async def check_territory(conn, table, conditions=None):
        if table == territories_data:
            return False
        return True

    async def check_functional_zone_type(conn, table, conditions=None):
        if table == functional_zone_types_dict:
            return False
        return True

    statement = (
        insert(functional_zones_data)
        .values(
            territory_id=functional_zone_post_req.territory_id,
            name=functional_zone_post_req.name,
            functional_zone_type_id=functional_zone_post_req.functional_zone_type_id,
            geometry=ST_GeomFromWKB(functional_zone_post_req.geometry.as_shapely_geometry().wkb, text(str(SRID))),
            year=functional_zone_post_req.year,
            source=functional_zone_post_req.source,
            properties=functional_zone_post_req.properties,
        )
        .returning(functional_zones_data.c.functional_zone_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_functional_zone_to_db(mock_conn, functional_zone_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_functional_zone_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_functional_zone_to_db(mock_conn, functional_zone_post_req)
    result = await add_functional_zone_to_db(mock_conn, functional_zone_post_req)

    # Assert
    assert isinstance(result, FunctionalZoneDTO), "Result should be a FunctionalZoneDTO."
    assert isinstance(FunctionalZone.from_dto(result), FunctionalZone), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_functional_zone_to_db(mock_conn: MockConnection, functional_zone_put_req: FunctionalZonePut):
    """Test the put_functional_zone_to_db function."""

    # Arrange
    async def check_territory(conn, table, conditions=None):
        if table == territories_data:
            return False
        return True

    async def check_functional_zone_type(conn, table, conditions=None):
        if table == functional_zone_types_dict:
            return False
        return True

    functional_zone_id = 1
    statement = (
        update(functional_zones_data)
        .where(functional_zones_data.c.functional_zone_id == functional_zone_id)
        .values(
            territory_id=functional_zone_put_req.territory_id,
            functional_zone_type_id=functional_zone_put_req.functional_zone_type_id,
            name=functional_zone_put_req.name,
            geometry=ST_GeomFromWKB(functional_zone_put_req.geometry.as_shapely_geometry().wkb, text(str(SRID))),
            year=functional_zone_put_req.year,
            source=functional_zone_put_req.source,
            properties=functional_zone_put_req.properties,
            updated_at=datetime.now(timezone.utc),
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_functional_zone_to_db(mock_conn, functional_zone_id, functional_zone_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_functional_zone_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_functional_zone_to_db(mock_conn, functional_zone_id, functional_zone_put_req)
    result = await put_functional_zone_to_db(mock_conn, functional_zone_id, functional_zone_put_req)

    # Assert
    assert isinstance(result, FunctionalZoneDTO), "Result should be a FunctionalZoneDTO."
    assert isinstance(FunctionalZone.from_dto(result), FunctionalZone), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_patch_functional_zone_to_db(mock_conn: MockConnection, functional_zone_patch_req: FunctionalZonePatch):
    """Test the patch_functional_zone_to_db function."""

    # Arrange
    async def check_territory(conn, table, conditions=None):
        if table == territories_data:
            return False
        return True

    async def check_functional_zone_type(conn, table, conditions=None):
        if table == functional_zone_types_dict:
            return False
        return True

    functional_zone_id = 1
    statement = (
        update(functional_zones_data)
        .where(functional_zones_data.c.functional_zone_id == functional_zone_id)
        .values(**functional_zone_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_functional_zone_to_db(mock_conn, functional_zone_id, functional_zone_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence",
        new=AsyncMock(side_effect=check_functional_zone_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_functional_zone_to_db(mock_conn, functional_zone_id, functional_zone_patch_req)
    result = await patch_functional_zone_to_db(mock_conn, functional_zone_id, functional_zone_patch_req)

    # Assert
    assert isinstance(result, FunctionalZoneDTO), "Result should be a FunctionalZoneDTO."
    assert isinstance(FunctionalZone.from_dto(result), FunctionalZone), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_functional_zone_from_db(mock_conn: MockConnection):
    """Test the delete_functional_zone_from_db function."""

    # Arrange
    functional_zone_id = 1
    statement = delete(functional_zones_data).where(functional_zones_data.c.functional_zone_id == functional_zone_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.functional_zones.check_existence") as mock_check_existence:
        result = await delete_functional_zone_from_db(mock_conn, functional_zone_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_functional_zone_from_db(mock_conn, functional_zone_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_functional_zones_around_from_db(mock_conn: MockConnection, shapely_geometry: Geom):
    """Test the get_functional_zones_around_from_db function."""

    # Arrange
    year, source = 1, "mock_string"
    functional_zone_type_id = None
    given_geometry = select(ST_GeomFromWKB(shapely_geometry.wkb, text(str(SRID)))).scalar_subquery()
    statement = (
        select(functional_zones_data.c.functional_zone_id)
        .where(
            functional_zones_data.c.year == year,
            functional_zones_data.c.source == source,
            func.ST_Intersects(functional_zones_data.c.geometry, given_geometry),
        )
        .distinct()
    )

    # Act
    result = await get_functional_zones_around_from_db(
        mock_conn, shapely_geometry, year, source, functional_zone_type_id
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(obj, FunctionalZoneDTO) for obj in result), "Each item should be a FunctionalZoneDTO."
    assert isinstance(FunctionalZone.from_dto(result[0]), FunctionalZone), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
