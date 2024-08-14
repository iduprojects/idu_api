from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import projects_data
from idu_api.urban_api.dto import ProjectDTO


async def get_project_by_id_from_db(conn: AsyncConnection, project_id: int) -> ProjectDTO:
    """Get project object by id."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    try:
        result = (await conn.execute(statement)).mappings().one()
    except:
        raise HTTPException(status_code=404, detail="Given id is not found")

    return ProjectDTO(**result)
