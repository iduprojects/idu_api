"""
Territories endpoints logic of getting entities from the database is defined here.
"""
import json
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import select, insert, delete
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromGeoJSON

from urban_api.exceptions.logic.common import EntityNotFoundById
from urban_api.db.entities import (
    territories_data,
    territory_types_dict,
    object_geometries_data,
    urban_objects_data,
    services_data
)
from urban_api.dto import TerritoryTypeDTO, TerritoryDTO
from urban_api.schemas import TerritoryTypesPost, TerritoriesDataPost
from urban_api.schemas.geometries import Geometry


async def get_territory_types_from_db(
        session: AsyncConnection
) -> list[TerritoryTypeDTO]:
    """
    Get all territory type objects
    """
    statement = select(territory_types_dict).order_by(territory_types_dict.c.territory_type_id)

    return [TerritoryTypeDTO(*data) for data in await session.execute(statement)]


async def add_territory_type_to_db(
        territory_type: TerritoryTypesPost,
        session: AsyncConnection,
) -> TerritoryTypeDTO:
    """
    Create territory type object
    """
    statement = select(territory_types_dict).where(territory_types_dict.c.name == territory_type.name)
    result = (await session.execute(statement)).first()
    if result:
        raise HTTPException(status_code=400, detail="Invalid input (territory type already exists)")

    statement = insert(territory_types_dict).values(
        name=territory_type.name,
    ).returning(territory_types_dict)
    result = list(await session.execute(statement))[0]

    await session.commit()

    return TerritoryTypeDTO(*result)


async def get_territory_by_id_from_db(
        territory_id: int,
        session: AsyncConnection
) -> TerritoryDTO:
    """
    Get territory object by id
    """
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territories_data.c.parent_id,
        territories_data.c.name,
        json.dumps(ST_AsGeoJSON(territories_data.c.geometry)),
        territories_data.c.level,
        territories_data.c.properties,
        json.dumps(ST_AsGeoJSON(territories_data.c.centre_point)),
        territories_data.c.admin_center,
        territories_data.c.okato_code,
    ).where(territories_data.c.territory_id == territory_id)

    result = (await session.execute(statement)).first()
    if not result:
        raise HTTPException(status_code=404, detail="Given id is not found")

    print(TerritoryDTO(*result))

    return TerritoryDTO(*result)


async def add_territory_to_db(
        territory: TerritoriesDataPost,
        session: AsyncConnection,
) -> TerritoryDTO:
    """
    Create territory object
    """

    statement = insert(territories_data).values(
        territory_type_id=territory.territory_type_id,
        parent_id=territory.parent_id,
        name=territory.name,
        geometry=ST_GeomFromGeoJSON(str(territory.geometry.dict())),
        level=territory.level,
        properties=territory.properties,
        centre_point=ST_GeomFromGeoJSON(str(territory.centre_point.dict())),
        admin_center=territory.admin_center,
        okato_code=territory.okato_code
    ).returning(territories_data)
    result = list(await session.execute(statement))[0]

    await session.commit()

    statement = select(territories_data).filter(territories_data.c.territory_id == territory.parent_id)
    check_parent_id = (await session.execute(statement)).first()
    if not check_parent_id:
        statement = delete(territories_data).where(territories_data.c.territory_id == result.territory_id)
        await session.execute(statement)

        await session.commit()

        raise HTTPException(status_code=404, detail="Given parent_id is not found")

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territories_data.c.parent_id,
        territories_data.c.name,
        ST_AsGeoJSON(territories_data.c.geometry),
        territories_data.c.level,
        territories_data.c.properties,
        ST_AsGeoJSON(territories_data.c.centre_point),
        territories_data.c.admin_center,
        territories_data.c.okato_code,
    ).where(territories_data.c.territory_id == result.territory_id)

    result = (await session.execute(statement)).first()

    return TerritoryDTO(
        territory_id=result.territory_id,
        territory_type_id=result.territory_type_id,
        parent_id=result.parent_id,
        name=result.name,
        geometry=json.loads(result.geometry),
        level=result.level,
        properties=result.properties,
        centre_point=json.loads(result.centre_point),
        admin_center=result.admin_center,
        okato_code=result.okato_code
    )


async def get_services_by_territory_id_from_db(
        territory_id: int,
        session: AsyncConnection,
        service_type: int | None = None,
) -> TerritoryDTO:
    """
    Get services objects by territory id
    """
    statement = select()

    result = (await session.execute(statement)).first()
    if not result:
        raise HTTPException(status_code=404, detail="Given id is not found")

    return TerritoryDTO(*result)
