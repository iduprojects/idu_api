from datetime import datetime

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, or_, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import projects_data, projects_territory_data
from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import ProjectPatch, ProjectPost, ProjectPut


class UserProjectServiceImpl(UserProjectService):
    """Service to manipulate projects entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_project_by_id_from_db(self, project_id: int, user_id: str) -> ProjectDTO | int:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.project_id == project_id)
        result = (await conn.execute(statement)).mappings().one_or_none()
        if result is None:
            return 404
        elif result.user_id != user_id and result.public is False:
            return 403

        return ProjectDTO(**result)

    async def post_project_to_db(self, project: ProjectPost, user_id: str) -> ProjectDTO:
        conn = self._conn
        statement_for_territory = (
            insert(projects_territory_data)
            .values(
                parent_territory_id=project.project_territory_info.parent_territory_id,
                geometry=ST_GeomFromText(
                    str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")
                ),
                centre_point=ST_GeomFromText(
                    str(project.project_territory_info.centre_point.as_shapely_geometry()), text("4326")
                ),
                properties=project.project_territory_info.properties,
            )
            .returning(projects_territory_data.c.project_territory_id)
        )
        result_for_territory = (await conn.execute(statement_for_territory)).scalar()

        statement_for_project = (
            insert(projects_data)
            .values(
                user_id=user_id,
                name=project.name,
                project_territory_id=result_for_territory,
                description=project.description,
                public=project.public,
                image_url=project.image_url,
            )
            .returning(projects_data.c.project_id)
        )
        result_for_project = (await conn.execute(statement_for_project)).scalar()

        await conn.commit()

        return await self.get_project_by_id_from_db(result_for_project, user_id)

    async def get_all_available_projects_from_db(self, user_id) -> list[ProjectDTO]:
        conn = self._conn
        statement = (
            select(projects_data)
            .where(or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)))
            .order_by(projects_data.c.project_id)
        )
        results = (await conn.execute(statement)).mappings().all()

        return [ProjectDTO(**result) for result in results]

    async def get_user_projects_from_db(self, user_id: str) -> list[ProjectDTO]:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.user_id == user_id).order_by(projects_data.c.project_id)
        results = (await conn.execute(statement)).mappings().all()

        return [ProjectDTO(**result) for result in results]

    async def get_project_territory_by_id_from_db(self, project_id: int, user_id: str) -> ProjectTerritoryDTO | int:
        conn = self._conn
        statement_for_project = select(projects_data).where(projects_data.c.project_id == project_id)
        result_for_project = (await conn.execute(statement_for_project)).mappings().one_or_none()
        if result_for_project is None:
            return 404
        elif result_for_project.user_id != user_id and result_for_project.public is False:
            return 403

        statement = select(
            projects_territory_data.c.project_territory_id,
            projects_territory_data.c.parent_territory_id,
            cast(ST_AsGeoJSON(projects_territory_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_territory_data.c.centre_point), JSONB).label("centre_point"),
            projects_territory_data.c.properties,
        ).where(projects_territory_data.c.project_territory_id == result_for_project.project_territory_id)
        result = (await conn.execute(statement)).mappings().one_or_none()
        if result is None:
            return 404

        return ProjectTerritoryDTO(**result)

    async def delete_project_from_db(self, project_id: int, user_id: str) -> dict | int:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.project_id == project_id)
        result = (await conn.execute(statement)).one_or_none()

        if result is None:
            return 404
        elif result.user_id != user_id:
            return 403

        statement_for_project = delete(projects_data).where(projects_data.c.project_id == project_id)

        statement_for_territory = delete(projects_territory_data).where(
            projects_territory_data.c.project_territory_id == result.project_territory_id
        )

        await conn.execute(statement_for_project)
        await conn.execute(statement_for_territory)

        await conn.commit()

        return {"status": "ok"}

    async def put_project_to_db(self, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO | int:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.project_id == project_id)
        requested_project = (await conn.execute(statement)).one_or_none()
        if requested_project is None:
            return 404
        elif requested_project.user_id != user_id:
            return 403

        statement_for_territory = (
            update(projects_territory_data)
            .where(projects_territory_data.c.project_territory_id == requested_project.project_territory_id)
            .values(
                parent_territory_id=project.project_territory_info.parent_territory_id,
                geometry=ST_GeomFromText(
                    str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")
                ),
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
                user_id=user_id,
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

        return await self.get_project_by_id_from_db(result.project_id, user_id)

    async def patch_project_to_db(self, project: ProjectPatch, project_id: int, user_id: str) -> ProjectDTO | int:
        conn = self._conn
        statement = select(projects_data).where(projects_data.c.project_id == project_id)
        requested_project = (await conn.execute(statement)).one_or_none()
        if requested_project is None:
            return 404
        elif requested_project.user_id != user_id:
            return 403

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

        return await self.get_project_by_id_from_db(project_id, user_id)
