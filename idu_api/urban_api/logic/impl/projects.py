from datetime import datetime

from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import projects_data, projects_territory_data
from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.logic.impl.helpers.projects import get_project_by_id_from_db
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import ProjectPatch, ProjectPost, ProjectPut


class UserProjectServiceImpl(UserProjectService):
    """Service to manipulate projects entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn
        
    async def post_project_to_db(self, project: ProjectPost) -> ProjectDTO:
        conn = self._conn
        statement_for_territory = (
            insert(projects_territory_data)
            .values(
                parent_id=project.project_territory_info.parent_id,
                geometry=ST_GeomFromText(str(project.project_territory_info.geometry.as_shapely_geometry()),
                                         text("4326")),
                centre_point=ST_GeomFromText(
                    str(project.project_territory_info.centre_point.as_shapely_geometry()), text("4326")
                ),
                properties=project.project_territory_info.properties,
            )
            .returning(projects_territory_data)
        )
        result_for_territory = (await conn.execute(statement_for_territory)).mappings().one()

        statement_for_project = (
            insert(projects_data)
            .values(
                user_id=project.user_id,
                name=project.name,
                project_territory_id=result_for_territory.project_territory_id,
                description=project.description,
                public=project.public,
                image_url=project.image_url,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            .returning(projects_data.project_id)
        )
        result_for_project = (await conn.execute(statement_for_project)).scalar()

        await conn.commit()

        return await get_project_by_id_from_db(conn, result_for_project)

    async def get_projects_from_db(self) -> list[ProjectDTO]:
        conn = self._conn
        statement = select(projects_data).order_by(projects_data.c.project_id)
        results = (await conn.execute(statement)).mappings().all()

        return [ProjectDTO(**result) for result in results]

    async def get_project_territory_by_id_from_db(self, project_id: int) -> ProjectTerritoryDTO:
        conn = self._conn
        statement_for_project = select(projects_data.c.project_territory_id).where(
            projects_data.c.project_id == project_id)
        try:
            result_for_project = (await conn.execute(statement_for_project)).mappings().one()
        except:
            raise HTTPException(status_code=404, detail="Given id is not found in projects_data")

        statement = select(
            projects_territory_data.c.project_territory_id,
            projects_territory_data.c.parent_id,
            cast(ST_AsGeoJSON(projects_territory_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_territory_data.c.centre_point), JSONB).label("centre_point"),
            projects_territory_data.c.properties,
        ).where(projects_territory_data.c.project_territory_id == result_for_project.project_territory_id)
        try:
            result = (await conn.execute(statement)).mappings().one_or_none()
        except:
            raise HTTPException(status_code=404, detail="Given id is not found in projects_territory_data")

        return ProjectTerritoryDTO(**result)

    async def delete_project_from_db(self, project_id: int) -> int:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.project_id == project_id)
        result = (await conn.execute(statement)).one_or_none()

        if result is None:
            raise HTTPException(status_code=404, detail="Project not found")

        statement_for_territory = delete(projects_territory_data).where(
            projects_territory_data.c.project_territory_id == result.project_territory_id
        )

        statement_for_project = delete(projects_data).where(projects_data.c.project_id == project_id)

        await conn.execute(statement_for_project)
        await conn.execute(statement_for_territory)

        await conn.commit()

        return project_id

    async def put_project_to_db(self, project: ProjectPut, project_id: int) -> ProjectDTO:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.project_id == project_id)
        requested_project = (await conn.execute(statement)).one_or_none()
        if requested_project is None:
            raise HTTPException(status_code=404, detail="Given project_id is not found")

        statement_for_territory = (
            update(projects_territory_data)
            .where(projects_territory_data.c.project_territory_id == requested_project.project_territory_id)
            .values(
                parent_id=project.project_territory_info.parent_id,
                geometry=ST_GeomFromText(str(project.project_territory_info.geometry.as_shapely_geometry()),
                                         text("4326")),
                centre_point=ST_GeomFromText(
                    str(project.project_territory_info.centre_point.as_shapely_geometry()), text("4326")
                ),
                properties=project.project_territory_info.properties,
            )
        )

        await conn.execute(statement_for_territory)

        statement = (
            update(projects_data)
            .where(projects_data.c.project_id == project_id)
            .values(
                user_id=project.user_id,
                name=project.name,
                description=project.description,
                public=project.public,
                image_url=project.image_url,
                updated_at=datetime.now(),
            )
            .returning(projects_data)
        )
        result = (await conn.execute(statement)).mappings().one()

        await conn.commit()

        return await get_project_by_id_from_db(conn, result.project_id)

    async def patch_project_to_db(self, project: ProjectPatch, project_id: int) -> ProjectDTO:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.project_id == project_id)
        requested_project = (await conn.execute(statement)).one_or_none()
        if requested_project is None:
            raise HTTPException(status_code=404, detail="Given project_id is not found")

        new_values_for_project = {}
        new_values_for_territory = {}

        for k, v in project.model_dump(exclude={"project_territory_info"}).items():
            if v is not None:
                new_values_for_project.update({k: v})

        if project.project_territory_info is not None:
            for k, v in project.project_territory_info.model_dump(exclude_unset=True).items():
                if k == "geometry" and v is not None:
                    new_values_for_territory["geometry"] = ST_GeomFromText(
                        str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")
                    )
                elif k == "centre_point" and v is not None:
                    new_values_for_territory["centre_point"] = ST_GeomFromText(
                        str(project.project_territory_info.centre_point.as_shapely_geometry()), text("4326")
                    )
                else:
                    new_values_for_territory[k] = v

        if new_values_for_project:
            statement_for_project = (
                update(projects_data)
                .where(projects_data.c.project_id == project_id)
                .values(updated_at=datetime.now(), **new_values_for_project)
                .returning(projects_data)
            )
            await conn.execute(statement_for_project)

        if new_values_for_territory:
            statement_for_territory = (
                update(projects_territory_data)
                .where(projects_territory_data.c.project_territory_id == requested_project.project_territory_id)
                .values(**new_values_for_territory)
            )
            await conn.execute(statement_for_territory)

        await conn.commit()

        return await get_project_by_id_from_db(conn, project_id)
