"""Service types internal logic is defined here."""

from typing import Callable

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    service_types_dict,
    urban_functions_dict, physical_object_types_dict, physical_object_functions_dict, object_service_types_dict,
)
from idu_api.urban_api.dto import ServiceTypeDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO, PhysicalObjectTypeDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import build_recursive_query, check_existence, extract_values_from_model
from idu_api.urban_api.schemas import (
    ServiceTypePatch,
    ServiceTypePost,
    ServiceTypePut,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)

func: Callable


async def get_service_types_from_db(
    conn: AsyncConnection,
    urban_function_id: int | None,
    name: str | None,
) -> list[ServiceTypeDTO]:
    """Get all service type objects."""

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

    if urban_function_id is not None:
        statement = statement.where(service_types_dict.c.urban_function_id == urban_function_id)
    if name is not None:
        statement = statement.where(service_types_dict.c.name.ilike(f"%{name}%"))

    return [ServiceTypeDTO(**data) for data in (await conn.execute(statement)).mappings().all()]


async def get_service_type_by_id_from_db(conn: AsyncConnection, service_type_id: int) -> ServiceTypeDTO:
    """Get service type object by identifier."""

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
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(service_type_id, "service type")

    return ServiceTypeDTO(**result)


async def add_service_type_to_db(
    conn: AsyncConnection,
    service_type: ServiceTypePost,
) -> ServiceTypeDTO:
    """Create service type object."""

    if not await check_existence(
        conn, urban_functions_dict, conditions={"urban_function_id": service_type.urban_function_id}
    ):
        raise EntityNotFoundById(service_type.urban_function_id, "urban function")

    if await check_existence(conn, service_types_dict, conditions={"name": service_type.name}):
        raise EntityAlreadyExists("service type", service_type.name)

    statement = (
        insert(service_types_dict).values(**service_type.model_dump()).returning(service_types_dict.c.service_type_id)
    )
    service_type_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_service_type_by_id_from_db(conn, service_type_id)


async def put_service_type_to_db(conn: AsyncConnection, service_type: ServiceTypePut) -> ServiceTypeDTO:
    """Update service type object by all its attributes."""

    if not await check_existence(
        conn, urban_functions_dict, conditions={"urban_function_id": service_type.urban_function_id}
    ):
        raise EntityNotFoundById(service_type.urban_function_id, "urban function")

    if await check_existence(conn, service_types_dict, conditions={"name": service_type.name}):
        statement = (
            update(service_types_dict)
            .where(service_types_dict.c.name == service_type.name)
            .values(**service_type.model_dump())
            .returning(service_types_dict.c.service_type_id)
        )
    else:
        statement = (
            insert(service_types_dict)
            .values(**service_type.model_dump())
            .returning(service_types_dict.c.service_type_id)
        )

    service_type_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_service_type_by_id_from_db(conn, service_type_id)


async def patch_service_type_to_db(
    conn: AsyncConnection,
    service_type_id: int,
    service_type: ServiceTypePatch,
) -> ServiceTypeDTO:
    """Update service type object by only given attributes."""

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service_type_id}):
        raise EntityNotFoundById(service_type_id, "service type")

    if service_type.urban_function_id is not None:
        if not await check_existence(
            conn, urban_functions_dict, conditions={"urban_function_id": service_type.urban_function_id}
        ):
            raise EntityNotFoundById(service_type.urban_function_id, "urban function")

    if service_type.name is not None:
        if await check_existence(
            conn,
            service_types_dict,
            conditions={"name": service_type.name},
            not_conditions={"service_type_id": service_type_id},
        ):
            raise EntityAlreadyExists("service type", service_type.name)

    statement = (
        update(service_types_dict)
        .where(service_types_dict.c.service_type_id == service_type_id)
        .values(**service_type.model_dump(exclude_unset=True))
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_service_type_by_id_from_db(conn, service_type_id)


async def delete_service_type_from_db(conn: AsyncConnection, service_type_id: int) -> dict:
    """Delete service type object by id."""

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service_type_id}):
        raise EntityNotFoundById(service_type_id, "service type")

    statement = delete(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_urban_functions_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    name: str | None,
    get_all_subtree: bool,
) -> list[UrbanFunctionDTO]:
    """Get an urban function or list of urban functions by parent."""

    if parent_id is not None:
        if not await check_existence(conn, urban_functions_dict, conditions={"urban_function_id": parent_id}):
            raise EntityNotFoundById(parent_id, "urban function")

    urban_functions_parents = urban_functions_dict.alias("urban_functions_parents")
    statement = select(
        urban_functions_dict, urban_functions_parents.c.name.label("parent_urban_function_name")
    ).select_from(
        urban_functions_dict.outerjoin(
            urban_functions_parents,
            urban_functions_parents.c.urban_function_id == urban_functions_dict.c.parent_id,
        ),
    )

    if get_all_subtree:
        statement = build_recursive_query(
            statement,
            urban_functions_dict,
            parent_id,
            "urban_function_recursive",
            "urban_function_id",
        )
    else:
        statement = statement.where(
            urban_functions_dict.c.parent_id == parent_id
            if parent_id is not None
            else urban_functions_dict.c.parent_id.is_(None)
        )

    requested_urban_functions = statement.cte("requested_urban_functions")
    statement = select(requested_urban_functions)

    if name is not None:
        statement = statement.where(requested_urban_functions.c.name.ilike(f"%{name}%"))

    result = (await conn.execute(statement)).mappings().all()

    return [UrbanFunctionDTO(**urban_function) for urban_function in result]


async def get_urban_function_by_id_from_db(conn: AsyncConnection, urban_function_id: int) -> UrbanFunctionDTO:
    """Get urban function object by identifier."""

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

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(urban_function_id, "urban function")

    return UrbanFunctionDTO(**result)


async def add_urban_function_to_db(
    conn: AsyncConnection,
    urban_function: UrbanFunctionPost,
) -> UrbanFunctionDTO:
    """Create urban function object."""

    if urban_function.parent_id is not None:
        if not await check_existence(
            conn, urban_functions_dict, conditions={"urban_function_id": urban_function.parent_id}
        ):
            raise EntityNotFoundById(urban_function.parent_id, "urban function")

    if await check_existence(conn, urban_functions_dict, conditions={"name": urban_function.name}):
        raise EntityAlreadyExists("urban function", urban_function.name)

    statement = (
        insert(urban_functions_dict)
        .values(**urban_function.model_dump(exclude={"parent_id"}), parent_id=urban_function.parent_id)
        .returning(urban_functions_dict.c.urban_function_id)
    )
    urban_function_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_urban_function_by_id_from_db(conn, urban_function_id)


async def put_urban_function_to_db(
    conn: AsyncConnection,
    urban_function: UrbanFunctionPut,
) -> UrbanFunctionDTO:
    """Update urban function object by all its attributes."""

    if urban_function.parent_id is not None:
        if not await check_existence(
            conn, urban_functions_dict, conditions={"urban_function_id": urban_function.parent_id}
        ):
            raise EntityNotFoundById(urban_function.parent_id, "urban function")

    if await check_existence(
        conn,
        urban_functions_dict,
        conditions={"name": urban_function.name},
    ):
        statement = (
            update(urban_functions_dict)
            .where(urban_functions_dict.c.name == urban_function.name)
            .values(**urban_function.model_dump())
            .returning(urban_functions_dict.c.urban_function_id)
        )
    else:
        statement = (
            insert(urban_functions_dict)
            .values(**urban_function.model_dump())
            .returning(urban_functions_dict.c.urban_function_id)
        )

    urban_function_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_urban_function_by_id_from_db(conn, urban_function_id)


async def patch_urban_function_to_db(
    conn: AsyncConnection,
    urban_function_id: int,
    urban_function: UrbanFunctionPatch,
) -> UrbanFunctionDTO:
    """Update urban function object by getting only given attributes."""

    if not await check_existence(conn, urban_functions_dict, conditions={"urban_function_id": urban_function_id}):
        raise EntityNotFoundById(urban_function_id, "urban function")

    if urban_function.parent_id is not None:
        if not await check_existence(
            conn, urban_functions_dict, conditions={"urban_function_id": urban_function.parent_id}
        ):
            raise EntityNotFoundById(urban_function.parent_id, "urban function")

    if urban_function.name is not None:
        if await check_existence(
            conn,
            urban_functions_dict,
            conditions={"name": urban_function.name},
            not_conditions={"urban_function_id": urban_function_id},
        ):
            raise EntityAlreadyExists("urban function", urban_function.name)

    values = extract_values_from_model(urban_function, exclude_unset=True)
    statement = (
        update(urban_functions_dict)
        .where(urban_functions_dict.c.urban_function_id == urban_function_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_urban_function_by_id_from_db(conn, urban_function_id)


async def delete_urban_function_from_db(conn: AsyncConnection, urban_function_id: int) -> dict:
    """Delete urban function object by id."""

    if not await check_existence(conn, urban_functions_dict, conditions={"urban_function_id": urban_function_id}):
        raise EntityNotFoundById(urban_function_id, "urban function")

    statement = delete(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_service_types_hierarchy_from_db(
    conn: AsyncConnection, ids: set[int] | None
) -> list[ServiceTypesHierarchyDTO]:
    """Get service types hierarchy (from top-level urban function to service type)
    based on a list of required service type ids.

    If the list of identifiers was not passed, it returns the full hierarchy.
    """

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

    if ids is not None:
        query = select(service_types_dict.c.service_type_id).where(service_types_dict.c.service_type_id.in_(ids))
        service_types = (await conn.execute(query)).scalars().all()
        if len(list(service_types)) < len(ids):
            raise EntitiesNotFoundByIds("service_type")
        statement = statement.where(service_types_dict.c.service_type_id.in_(ids))

    service_types = (await conn.execute(statement)).mappings().all()

    statement = select(urban_functions_dict).order_by(urban_functions_dict.c.level)
    urban_functions = (await conn.execute(statement)).mappings().all()

    def build_filtered_hierarchy(parent_id: int = None) -> list[ServiceTypesHierarchyDTO]:
        children = []
        for uf in [uf for uf in urban_functions if uf.parent_id == parent_id]:
            filtered_children = build_filtered_hierarchy(uf.urban_function_id)

            relevant_service_types = [
                ServiceTypeDTO(**s) for s in service_types if s.urban_function_id == uf.urban_function_id
            ]

            if filtered_children or relevant_service_types:
                children.append(
                    ServiceTypesHierarchyDTO(
                        **uf, children=filtered_children if filtered_children else relevant_service_types
                    )
                )

        return children

    return build_filtered_hierarchy()


async def get_physical_object_types_by_service_type_id_from_db(
    conn: AsyncConnection,
    service_type_id: int | None,
) -> list[PhysicalObjectTypeDTO]:
    """Get all available physical object types by service type identifier."""

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service_type_id}):
        raise EntityNotFoundById(service_type_id, "service type")

    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            physical_object_types_dict.join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .join(
                object_service_types_dict,
                object_service_types_dict.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
        )
        .where(object_service_types_dict.c.service_type_id == service_type_id)
        .order_by(physical_object_types_dict.c.physical_object_type_id)
    )

    return [PhysicalObjectTypeDTO(**data) for data in (await conn.execute(statement)).mappings().all()]
