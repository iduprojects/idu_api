"""Service types handlers logic of getting entities from the database is defined here."""

from typing import Callable

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    service_types_dict,
    urban_functions_dict,
)
from idu_api.urban_api.dto import ServiceTypesDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.schemas import (
    ServiceTypesPatch,
    ServiceTypesPost,
    ServiceTypesPut,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)

func: Callable


async def get_service_types_from_db(
    conn: AsyncConnection,
    urban_function_id: int | None,
) -> list[ServiceTypesDTO]:
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
            infrastructure_type=service_type.infrastructure_type,
            properties=service_type.properties,
        )
        .returning(service_types_dict.c.service_type_id)
    )
    service_type_id = (await conn.execute(statement)).scalar_one()

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
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return ServiceTypesDTO(**result)


async def put_service_type_to_db(
    conn: AsyncConnection,
    service_type_id: int,
    service_type: ServiceTypesPut,
) -> ServiceTypesDTO:
    """Update service type object by all its attributes."""

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(service_type_id, "service type")

    statement = select(urban_functions_dict).where(
        urban_functions_dict.c.urban_function_id == service_type.urban_function_id
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(service_type.urban_function_id, "urban function")

    statement = select(service_types_dict).where(
        service_types_dict.c.name == service_type.name, service_types_dict.c.service_type_id != service_type_id
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise EntityAlreadyExists("service type", service_type.name)

    statement = (
        update(service_types_dict)
        .where(service_types_dict.c.service_type_id == service_type_id)
        .values(
            name=service_type.name,
            urban_function_id=service_type.urban_function_id,
            capacity_modeled=service_type.capacity_modeled,
            code=service_type.code,
            infrastructure_type=service_type.infrastructure_type,
            properties=service_type.properties,
        )
    )
    await conn.execute(statement)
    await conn.commit()

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
    result = (await conn.execute(statement)).mappings().one()

    return ServiceTypesDTO(**result)


async def patch_service_type_to_db(
    conn: AsyncConnection,
    service_type_id: int,
    service_type: ServiceTypesPatch,
) -> ServiceTypesDTO:
    """Update service type object by only given attributes."""

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(service_type_id, "service type")

    if service_type.urban_function_id is not None:
        statement = select(urban_functions_dict).where(
            urban_functions_dict.c.urban_function_id == service_type.urban_function_id
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is None:
            raise EntityNotFoundById(service_type.urban_function_id, "urban function")

    if service_type.name is not None:
        statement = select(service_types_dict).where(
            service_types_dict.c.name == service_type.name, service_types_dict.c.service_type_id != service_type_id
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is not None:
            raise EntityAlreadyExists("service type", service_type.name)

    statement = update(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)

    values_to_update = {}
    for k, v in service_type.model_dump(exclude_unset=True).items():
        values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    await conn.execute(statement)

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
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return ServiceTypesDTO(**result)


async def delete_service_type_from_db(conn: AsyncConnection, service_type_id: int) -> dict:
    """Delete service type object by id."""

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)
    service_type = (await conn.execute(statement)).scalar_one_or_none()
    if service_type is None:
        raise EntityNotFoundById(service_type_id, "service type")

    statement = delete(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


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

    urban_functions_parents = urban_functions_dict.alias("urban_functions_parents")
    statement = select(
        urban_functions_dict, urban_functions_parents.c.name.label("parent_urban_function_name")
    ).select_from(
        urban_functions_dict.outerjoin(
            urban_functions_parents,
            urban_functions_parents.c.urban_function_id == urban_functions_dict.c.parent_urban_function_id,
        ),
    )

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

    return [UrbanFunctionDTO(**urban_function) for urban_function in result]


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

    statement = (
        insert(urban_functions_dict)
        .values(
            parent_urban_function_id=urban_function.parent_id,
            name=urban_function.name,
            code=urban_function.code,
        )
        .returning(urban_functions_dict.c.urban_function_id)
    )
    urban_function_id = (await conn.execute(statement)).scalar_one()

    urban_functions_parents = urban_functions_dict.alias("urban_functions_parents")
    statement = (
        select(urban_functions_dict, urban_functions_parents.c.name.label("parent_urban_function_name"))
        .select_from(
            urban_functions_dict.outerjoin(
                urban_functions_parents,
                urban_functions_parents.c.urban_function_id == urban_functions_dict.c.parent_urban_function_id,
            ),
        )
        .where(urban_functions_dict.c.urban_function_id == urban_function_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return UrbanFunctionDTO(**result)


async def put_urban_function_to_db(
    conn: AsyncConnection,
    urban_function_id: int,
    urban_function: UrbanFunctionPut,
) -> UrbanFunctionDTO:
    """Update urban function object by getting all its attributes."""

    statement = select(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(urban_function_id, "urban function")

    if urban_function.parent_id is not None:
        statement = select(urban_functions_dict).where(
            urban_functions_dict.c.urban_function_id == urban_function.parent_id
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is None:
            raise EntityNotFoundById(urban_function.parent_id, "urban function")

    statement = select(urban_functions_dict).where(
        urban_functions_dict.c.name == urban_function.name,
        urban_functions_dict.c.urban_function_id != urban_function_id,
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise EntityAlreadyExists("urban function", urban_function.name)

    statement = (
        update(urban_functions_dict)
        .where(urban_functions_dict.c.urban_function_id == urban_function_id)
        .values(
            name=urban_function.name,
            parent_urban_function_id=urban_function.parent_id,
            code=urban_function.code,
        )
    )

    await conn.execute(statement)

    urban_functions_parents = urban_functions_dict.alias("urban_functions_parents")
    statement = (
        select(urban_functions_dict, urban_functions_parents.c.name.label("parent_urban_function_name"))
        .select_from(
            urban_functions_dict.outerjoin(
                urban_functions_parents,
                urban_functions_parents.c.urban_function_id == urban_functions_dict.c.parent_urban_function_id,
            ),
        )
        .where(urban_functions_dict.c.urban_function_id == urban_function_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return UrbanFunctionDTO(**result)


async def patch_urban_function_to_db(
    conn: AsyncConnection,
    urban_function_id: int,
    urban_function: UrbanFunctionPatch,
) -> UrbanFunctionDTO:
    """Update urban function object by getting only given attributes."""

    statement = select(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(urban_function_id, "urban function")

    if urban_function.parent_id is not None:
        statement = select(urban_functions_dict).where(
            urban_functions_dict.c.urban_function_id == urban_function.parent_id
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is None:
            raise EntityNotFoundById(urban_function.parent_id, "urban function")

    if urban_function.name is not None:
        statement = select(urban_functions_dict).where(
            urban_functions_dict.c.name == urban_function.name,
            urban_functions_dict.c.urban_function_id != urban_function_id,
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is not None:
            raise EntityAlreadyExists("urban function", urban_function.name)

    statement = update(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)

    values_to_update = {}
    for k, v in urban_function.model_dump(exclude_unset=True).items():
        if k == "parent_id":
            values_to_update.update({"parent_urban_function_id": v})
            continue
        values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    await conn.execute(statement)

    urban_functions_parents = urban_functions_dict.alias("urban_functions_parents")
    statement = (
        select(urban_functions_dict, urban_functions_parents.c.name.label("parent_urban_function_name"))
        .select_from(
            urban_functions_dict.outerjoin(
                urban_functions_parents,
                urban_functions_parents.c.urban_function_id == urban_functions_dict.c.parent_urban_function_id,
            ),
        )
        .where(urban_functions_dict.c.urban_function_id == urban_function_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return UrbanFunctionDTO(**result)


async def delete_urban_function_from_db(conn: AsyncConnection, urban_function_id: int) -> dict:
    """Delete urban function object by id."""

    statement = select(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)
    urban_function = (await conn.execute(statement)).scalar_one_or_none()
    if urban_function is None:
        raise EntityNotFoundById(urban_function_id, "urban function")

    statement = delete(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


async def get_service_types_hierarchy_from_db(
    conn: AsyncConnection, service_type_ids: str | None
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

    if service_type_ids is not None:
        ids = [int(service_type_id.strip()) for service_type_id in service_type_ids.split(",")]
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
        for uf in [uf for uf in urban_functions if uf.parent_urban_function_id == parent_id]:
            filtered_children = build_filtered_hierarchy(uf.urban_function_id)

            relevant_service_types = [
                ServiceTypesDTO(**s) for s in service_types if s.urban_function_id == uf.urban_function_id
            ]

            if filtered_children or relevant_service_types:
                children.append(
                    ServiceTypesHierarchyDTO(
                        **uf, children=filtered_children if filtered_children else relevant_service_types
                    )
                )

        return children

    return build_filtered_hierarchy()
