"""Physical objects types internal logic is defined here."""

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    physical_object_functions_dict,
    physical_object_types_dict, service_types_dict, urban_functions_dict, object_service_types_dict,
)
from idu_api.urban_api.dto import (
    PhysicalObjectFunctionDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectTypesHierarchyDTO, ServiceTypeDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import build_recursive_query, check_existence
from idu_api.urban_api.schemas import (
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectTypePatch,
    PhysicalObjectTypePost,
)


async def get_physical_object_types_from_db(
    conn: AsyncConnection,
    physical_object_function_id: int | None,
    name: str | None = None,
) -> list[PhysicalObjectTypeDTO]:
    """Get all physical object type objects."""

    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            physical_object_types_dict.join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .order_by(physical_object_types_dict.c.physical_object_type_id)
    )

    if physical_object_function_id is not None:
        statement = statement.where(physical_object_types_dict.c.physical_object_function_id == physical_object_function_id)
    if name is not None:
        statement = statement.where(physical_object_types_dict.c.name.ilike(f"%{name}%"))

    return [PhysicalObjectTypeDTO(**data) for data in (await conn.execute(statement)).mappings().all()]


async def get_physical_object_type_by_id_from_db(
    conn: AsyncConnection, physical_object_type_id: int
) -> list[PhysicalObjectTypeDTO]:
    """Get physical object type by its identifier."""

    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            physical_object_types_dict.join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(physical_object_types_dict.c.physical_object_type_id == physical_object_type_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(physical_object_type_id, "physical object type")

    return PhysicalObjectTypeDTO(**result)


async def add_physical_object_type_to_db(
    conn: AsyncConnection,
    physical_object_type: PhysicalObjectTypePost,
) -> PhysicalObjectTypeDTO:
    """Create physical object type object."""

    if not await check_existence(
        conn,
        physical_object_functions_dict,
        conditions={"physical_object_function_id": physical_object_type.physical_object_function_id},
    ):
        raise EntityNotFoundById(physical_object_type.physical_object_function_id, "physical object function")

    if await check_existence(conn, physical_object_types_dict, conditions={"name": physical_object_type.name}):
        raise EntityAlreadyExists("physical object type", physical_object_type.name)

    statement = (
        insert(physical_object_types_dict)
        .values(**physical_object_type.model_dump())
        .returning(physical_object_types_dict.c.physical_object_type_id)
    )
    physical_object_type_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_physical_object_type_by_id_from_db(conn, physical_object_type_id)


async def patch_physical_object_type_to_db(
    conn: AsyncConnection,
    physical_object_type_id: int,
    physical_object_type: PhysicalObjectTypePatch,
) -> PhysicalObjectTypeDTO:
    """Update physical object type object by only given attributes."""

    if not await check_existence(
        conn, physical_object_types_dict, conditions={"physical_object_type_id": physical_object_type_id}
    ):
        raise EntityNotFoundById(physical_object_type_id, "physical object type")

    if physical_object_type.physical_object_function_id is not None:
        if not await check_existence(
            conn,
            physical_object_functions_dict,
            conditions={"physical_object_function_id": physical_object_type.physical_object_function_id},
        ):
            raise EntityNotFoundById(physical_object_type.physical_object_function_id, "physical object function")

    if physical_object_type.name is not None:
        if await check_existence(
            conn,
            physical_object_types_dict,
            conditions={"name": physical_object_type.name},
            not_conditions={"physical_object_type_id": physical_object_type_id},
        ):
            raise EntityAlreadyExists("physical object type", physical_object_type.name)

    statement = (
        update(physical_object_types_dict)
        .where(physical_object_types_dict.c.physical_object_type_id == physical_object_type_id)
        .values(**physical_object_type.model_dump(exclude_unset=True))
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_physical_object_type_by_id_from_db(conn, physical_object_type_id)


async def delete_physical_object_type_from_db(conn: AsyncConnection, physical_object_type_id: int) -> dict:
    """Delete physical object type object by id."""

    if not await check_existence(
        conn, physical_object_types_dict, conditions={"physical_object_type_id": physical_object_type_id}
    ):
        raise EntityNotFoundById(physical_object_type_id, "physical object type")

    statement = delete(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object_type_id
    )
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_physical_object_functions_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    name: str | None,
    get_all_subtree: bool,
) -> list[PhysicalObjectFunctionDTO]:
    """Get a physical object function or list of physical object functions by parent."""

    if parent_id is not None:
        if not await check_existence(
            conn, physical_object_functions_dict, conditions={"physical_object_function_id": parent_id}
        ):
            raise EntityNotFoundById(parent_id, "physical object function")

    physical_object_functions_parents = physical_object_functions_dict.alias("physical_object_functions_parents")
    statement = select(
        physical_object_functions_dict, physical_object_functions_parents.c.name.label("parent_name")
    ).select_from(
        physical_object_functions_dict.outerjoin(
            physical_object_functions_parents,
            physical_object_functions_parents.c.physical_object_function_id
            == physical_object_functions_dict.c.parent_id,
        ),
    )

    if get_all_subtree:
        statement = build_recursive_query(
            statement,
            physical_object_functions_dict,
            parent_id,
            "physical_object_function_recursive",
            "physical_object_function_id",
        )
    else:
        statement = statement.where(
            physical_object_functions_dict.c.parent_id == parent_id
            if parent_id is not None
            else physical_object_functions_dict.c.parent_id.is_(None)
        )

    requested_physical_object_functions = statement.cte("requested_physical_object_functions")

    statement = select(requested_physical_object_functions)

    if name is not None:
        statement = statement.where(requested_physical_object_functions.c.name.ilike(f"%{name}%"))

    result = (await conn.execute(statement)).mappings().all()

    return [PhysicalObjectFunctionDTO(**physical_object_function) for physical_object_function in result]


async def get_physical_object_function_by_id_from_db(
    conn: AsyncConnection, physical_object_function_id: int
) -> list[PhysicalObjectFunctionDTO]:
    """Get physical object function by its identifier."""

    physical_object_functions_parents = physical_object_functions_dict.alias("physical_object_functions_parents")
    statement = (
        select(physical_object_functions_dict, physical_object_functions_parents.c.name.label("parent_name"))
        .select_from(
            physical_object_functions_dict.outerjoin(
                physical_object_functions_parents,
                physical_object_functions_parents.c.physical_object_function_id
                == physical_object_functions_dict.c.parent_id,
            ),
        )
        .where(physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(physical_object_function_id, "physical object function")

    return PhysicalObjectFunctionDTO(**result)


async def add_physical_object_function_to_db(
    conn: AsyncConnection,
    physical_object_function: PhysicalObjectFunctionPost,
) -> PhysicalObjectFunctionDTO:
    """Create physical object function object."""

    if physical_object_function.parent_id is not None:
        if not await check_existence(
            conn,
            physical_object_functions_dict,
            conditions={"physical_object_function_id": physical_object_function.parent_id},
        ):
            raise EntityNotFoundById(physical_object_function.parent_id, "physical object function")

    if await check_existence(conn, physical_object_functions_dict, conditions={"name": physical_object_function.name}):
        raise EntityAlreadyExists("physical object function", physical_object_function.name)

    statement = (
        insert(physical_object_functions_dict)
        .values(**physical_object_function.model_dump())
        .returning(physical_object_functions_dict.c.physical_object_function_id)
    )
    physical_object_function_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_physical_object_function_by_id_from_db(conn, physical_object_function_id)


async def put_physical_object_function_to_db(
    conn: AsyncConnection,
    physical_object_function: PhysicalObjectFunctionPut,
) -> PhysicalObjectFunctionDTO:
    """Update physical object function object by getting all its attributes."""

    if physical_object_function.parent_id is not None:
        if not await check_existence(
            conn,
            physical_object_functions_dict,
            conditions={"physical_object_function_id": physical_object_function.parent_id},
        ):
            raise EntityNotFoundById(physical_object_function.parent_id, "physical object function")

    if await check_existence(
        conn,
        physical_object_functions_dict,
        conditions={"name": physical_object_function.name},
    ):
        statement = (
            update(physical_object_functions_dict)
            .where(physical_object_functions_dict.c.name == physical_object_function.name)
            .values(**physical_object_function.model_dump())
            .returning(physical_object_functions_dict.c.physical_object_function_id)
        )
    else:
        statement = (
            insert(physical_object_functions_dict)
            .values(**physical_object_function.model_dump())
            .returning(physical_object_functions_dict.c.physical_object_function_id)
        )

    physical_object_function_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_physical_object_function_by_id_from_db(conn, physical_object_function_id)


async def patch_physical_object_function_to_db(
    conn: AsyncConnection,
    physical_object_function_id: int,
    physical_object_function: PhysicalObjectFunctionPatch,
) -> PhysicalObjectFunctionDTO:
    """Update physical object function object by getting only given attributes."""

    if not await check_existence(
        conn, physical_object_functions_dict, conditions={"physical_object_function_id": physical_object_function_id}
    ):
        raise EntityNotFoundById(physical_object_function_id, "physical object function")

    values_to_update = physical_object_function.model_dump(exclude_unset=True)

    if "parent_id" in values_to_update and values_to_update["parent_id"] is not None:
        if not await check_existence(
            conn,
            physical_object_functions_dict,
            conditions={"physical_object_function_id": physical_object_function.parent_id},
        ):
            raise EntityNotFoundById(physical_object_function.parent_id, "physical object function")

    if "name" in values_to_update:
        if await check_existence(
            conn,
            physical_object_functions_dict,
            conditions={"name": physical_object_function.name},
            not_conditions={"physical_object_function_id": physical_object_function_id},
        ):
            raise EntityAlreadyExists("physical object function", physical_object_function.name)

    statement = (
        update(physical_object_functions_dict)
        .where(physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id)
        .values(**values_to_update)
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_physical_object_function_by_id_from_db(conn, physical_object_function_id)


async def delete_physical_object_function_from_db(conn: AsyncConnection, physical_object_function_id: int) -> dict:
    """Delete physical object function object by id."""

    if not await check_existence(
        conn, physical_object_functions_dict, conditions={"physical_object_function_id": physical_object_function_id}
    ):
        raise EntityNotFoundById(physical_object_function_id, "physical object function")

    statement = delete(physical_object_functions_dict).where(
        physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id
    )
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_physical_object_types_hierarchy_from_db(
    conn: AsyncConnection, ids: set[int] | None
) -> list[PhysicalObjectTypesHierarchyDTO]:
    """Get physical object types hierarchy (from top-level physical object function to physical object type)
    based on a list of required physical object type ids.

    If the list of identifiers was not passed, it returns the full hierarchy.
    """

    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            physical_object_types_dict.outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .order_by(physical_object_types_dict.c.physical_object_type_id)
    )

    if ids is not None:
        query = select(physical_object_types_dict.c.physical_object_type_id).where(
            physical_object_types_dict.c.physical_object_type_id.in_(ids)
        )
        physical_object_types = (await conn.execute(query)).scalars().all()
        if len(physical_object_types) < len(ids):
            raise EntitiesNotFoundByIds("physical object type")
        statement = statement.where(physical_object_types_dict.c.physical_object_type_id.in_(ids))

    physical_object_types = (await conn.execute(statement)).mappings().all()

    statement = select(physical_object_functions_dict).order_by(physical_object_functions_dict.c.level)
    physical_object_functions = (await conn.execute(statement)).mappings().all()

    def build_filtered_hierarchy(parent_id: int | None = None) -> list[PhysicalObjectTypesHierarchyDTO]:
        children = []
        for pof in [pof for pof in physical_object_functions if pof.parent_id == parent_id]:
            filtered_children = build_filtered_hierarchy(pof.physical_object_function_id)

            relevant_physical_object_types = [
                PhysicalObjectTypeDTO(**p)
                for p in physical_object_types
                if p.physical_object_function_id == pof.physical_object_function_id
            ]

            if filtered_children or relevant_physical_object_types:
                children.append(
                    PhysicalObjectTypesHierarchyDTO(
                        **pof, children=filtered_children if filtered_children else relevant_physical_object_types
                    )
                )

        return children

    return build_filtered_hierarchy()


async def get_service_types_by_physical_object_type_id_from_db(
    conn: AsyncConnection,
    physical_object_type_id: int | None,
) -> list[PhysicalObjectTypeDTO]:
    """Get all available service types by physical object type identifier."""

    if not await check_existence(
        conn, physical_object_types_dict, conditions={"physical_object_type_id": physical_object_type_id}
    ):
        raise EntityNotFoundById(physical_object_type_id, "physical object type")

    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            service_types_dict.join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .join(
                object_service_types_dict,
                object_service_types_dict.c.service_type_id == service_types_dict.c.service_type_id,
            )
        )
        .where(object_service_types_dict.c.physical_object_type_id == physical_object_type_id)
        .order_by(service_types_dict.c.service_type_id)
    )

    return [ServiceTypeDTO(**data) for data in (await conn.execute(statement)).mappings().all()]
