"""Service types handlers logic of getting entities from the database is defined here."""

from typing import Callable

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    service_types_dict,
    urban_functions_dict,
)
from idu_api.urban_api.dto import ServiceTypesDTO, UrbanFunctionDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.schemas import ServiceTypesPost, UrbanFunctionPost

func: Callable


async def get_service_types_from_db(
    conn: AsyncConnection,
    urban_function_id: int | None,
) -> list[ServiceTypesDTO]:
    """Get all service type objects."""

    statement = select(service_types_dict).order_by(service_types_dict.c.service_type_id)

    if urban_function_id is not None:
        statement = statement.where(service_types_dict.c.urban_function_id == urban_function_id)

    return [ServiceTypesDTO(**data) for data in (await conn.execute(statement)).mappings().all()]


async def add_service_type_to_db(
    conn: AsyncConnection,
    service_type: ServiceTypesPost,
) -> ServiceTypesDTO:
    """Create service type object."""

    statement = select(urban_functions_dict).where(
        urban_functions_dict.c.urban_function_id == service_type.urban_function_id
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(service_type.urban_function_id, "urban function")

    statement = select(service_types_dict).where(service_types_dict.c.name == service_type.name)
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise EntityAlreadyExists("service type", service_type.name)

    statement = (
        insert(service_types_dict)
        .values(
            name=service_type.name,
            urban_function_id=service_type.urban_function_id,
            capacity_modeled=service_type.capacity_modeled,
            code=service_type.code,
        )
        .returning(service_types_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return ServiceTypesDTO(**result)


async def get_urban_functions_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    name: str | None,
    get_all_subtree: bool,
) -> list[UrbanFunctionDTO]:
    """Get an urban function or list of urban functions by parent."""

    if parent_id is not None:
        statement = select(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == parent_id)
        parent_urban_function = (await conn.execute(statement)).one_or_none()
        if parent_urban_function is None:
            raise EntityNotFoundById(parent_id, "urban function")

    statement = select(urban_functions_dict)

    if get_all_subtree:
        cte_statement = statement.where(
            urban_functions_dict.c.parent_urban_function_id == parent_id
            if parent_id is not None
            else urban_functions_dict.c.parent_urban_function_id.is_(None)
        )
        cte_statement = cte_statement.cte(name="urban_function_recursive", recursive=True)

        recursive_part = statement.join(
            cte_statement, urban_functions_dict.c.parent_urban_function_id == cte_statement.c.urban_function_id
        )

        statement = select(cte_statement.union_all(recursive_part))
    else:
        statement = statement.where(
            urban_functions_dict.c.parent_urban_function_id == parent_id
            if parent_id is not None
            else urban_functions_dict.c.parent_urban_function_id.is_(None)
        )

    requested_urban_functions = statement.cte("requested_urban_functions")

    statement = select(requested_urban_functions)

    if name is not None:
        statement = statement.where(requested_urban_functions.c.name.ilike(f"%{name}%"))

    result = (await conn.execute(statement)).mappings().all()

    return [UrbanFunctionDTO(**indicator) for indicator in result]


async def add_urban_function_to_db(
    conn: AsyncConnection,
    urban_function: UrbanFunctionPost,
) -> UrbanFunctionDTO:
    """Create urban function object."""

    if urban_function.parent_id is not None:
        statement = select(urban_functions_dict).where(
            urban_functions_dict.c.urban_function_id == urban_function.parent_id
        )
        parent_urban_function = (await conn.execute(statement)).one_or_none()
        if parent_urban_function is None:
            raise EntityNotFoundById(urban_function.parent_id, "urban function")

    statement = select(urban_functions_dict).where(urban_functions_dict.c.name == urban_function.name)
    urban_function_name = (await conn.execute(statement)).one_or_none()
    if urban_function_name is not None:
        raise EntityAlreadyExists("urban function", urban_function.name)

    statement = select(urban_functions_dict).where(urban_functions_dict.c.list_label == urban_function.list_label)
    urban_function_list_label = (await conn.execute(statement)).one_or_none()
    if urban_function_list_label is not None:
        raise EntityAlreadyExists("urban function", urban_function.list_label)

    statement = (
        insert(urban_functions_dict)
        .values(
            parent_urban_function_id=urban_function.parent_id,
            name=urban_function.name,
            level=urban_function.level,
            list_label=urban_function.list_label,
            code=urban_function.code,
        )
        .returning(urban_functions_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return UrbanFunctionDTO(**result)
