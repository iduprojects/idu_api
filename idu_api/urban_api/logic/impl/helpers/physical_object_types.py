"""Physical objects types handlers logic of getting entities from the database is defined here."""

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    physical_object_functions_dict,
    physical_object_types_dict,
)
from idu_api.urban_api.dto import (
    PhysicalObjectFunctionDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectTypesHierarchyDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.schemas import (
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectsTypesPatch,
    PhysicalObjectsTypesPost,
)


async def get_physical_object_types_from_db(conn: AsyncConnection) -> list[PhysicalObjectTypeDTO]:
    """Get all physical object type objects."""

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

    return [PhysicalObjectTypeDTO(**data) for data in (await conn.execute(statement)).mappings().all()]


async def add_physical_object_type_to_db(
    conn: AsyncConnection,
    physical_object_type: PhysicalObjectsTypesPost,
) -> PhysicalObjectTypeDTO:
    """Create physical object type object."""

    statement = select(physical_object_types_dict).where(physical_object_types_dict.c.name == physical_object_type.name)
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise EntityAlreadyExists("physical object type", physical_object_type.name)

    statement = select(physical_object_functions_dict).where(
        physical_object_functions_dict.c.physical_object_function_id == physical_object_type.physical_object_function_id
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityAlreadyExists("physical object function", physical_object_type.physical_object_function_id)

    statement = (
        insert(physical_object_types_dict)
        .values(
            name=physical_object_type.name,
            physical_object_function_id=physical_object_type.physical_object_function_id,
        )
        .returning(physical_object_types_dict.c.physical_object_type_id)
    )
    result_id = (await conn.execute(statement)).scalar_one()

    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            physical_object_types_dict.outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(physical_object_types_dict.c.physical_object_type_id == result_id)
    )

    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return PhysicalObjectTypeDTO(**result)


async def patch_physical_object_type_to_db(
    conn: AsyncConnection,
    physical_object_type_id: int,
    physical_object_type: PhysicalObjectsTypesPatch,
) -> PhysicalObjectTypeDTO:
    """Update physical object type object by only given attributes."""

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object_type_id
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(physical_object_type_id, "physical object type")

    if physical_object_type.physical_object_function_id is not None:
        statement = select(physical_object_functions_dict).where(
            physical_object_functions_dict.c.physical_object_function_id
            == physical_object_type.physical_object_function_id
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is None:
            raise EntityNotFoundById(physical_object_type.physical_object_function_id, "physical object function")

    if physical_object_type.name is not None:
        statement = select(physical_object_types_dict).where(
            physical_object_types_dict.c.name == physical_object_type.name,
            physical_object_types_dict.c.physical_object_type_id != physical_object_type_id,
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is not None:
            raise EntityAlreadyExists("physical object type", physical_object_type.name)

    statement = update(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object_type_id
    )

    values_to_update = {}
    for k, v in physical_object_type.model_dump(exclude_unset=True).items():
        values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    await conn.execute(statement)

    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            physical_object_types_dict.outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(physical_object_types_dict.c.physical_object_type_id == physical_object_type_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return PhysicalObjectTypeDTO(**result)


async def delete_physical_object_type_from_db(conn: AsyncConnection, physical_object_type_id: int) -> dict:
    """Delete physical object type object by id."""

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object_type_id
    )
    physical_object_type = (await conn.execute(statement)).scalar_one_or_none()
    if physical_object_type is None:
        raise EntityNotFoundById(physical_object_type_id, "physical object type")

    statement = delete(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object_type_id
    )
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


async def get_physical_object_functions_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    name: str | None,
    get_all_subtree: bool,
) -> list[PhysicalObjectFunctionDTO]:
    """Get a physical object function or list of physical object functions by parent."""

    if parent_id is not None:
        statement = select(physical_object_functions_dict).where(
            physical_object_functions_dict.c.physical_object_function_id == parent_id
        )
        parent_physical_object_function = (await conn.execute(statement)).one_or_none()
        if parent_physical_object_function is None:
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
        cte_statement = statement.where(
            physical_object_functions_dict.c.parent_id == parent_id
            if parent_id is not None
            else physical_object_functions_dict.c.parent_id.is_(None)
        )
        cte_statement = cte_statement.cte(name="physical_object_function_recursive", recursive=True)

        recursive_part = statement.join(
            cte_statement,
            physical_object_functions_dict.c.parent_id == cte_statement.c.physical_object_function_id,
        )

        statement = select(cte_statement.union_all(recursive_part))
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


async def add_physical_object_function_to_db(
    conn: AsyncConnection,
    physical_object_function: PhysicalObjectFunctionPost,
) -> PhysicalObjectFunctionDTO:
    """Create physical object function object."""

    if physical_object_function.parent_id is not None:
        statement = select(physical_object_functions_dict).where(
            physical_object_functions_dict.c.physical_object_function_id == physical_object_function.parent_id
        )
        parent_physical_object_function = (await conn.execute(statement)).one_or_none()
        if parent_physical_object_function is None:
            raise EntityNotFoundById(physical_object_function.parent_id, "physical object function")

    statement = select(physical_object_functions_dict).where(
        physical_object_functions_dict.c.name == physical_object_function.name
    )
    physical_object_function_name = (await conn.execute(statement)).one_or_none()
    if physical_object_function_name is not None:
        raise EntityAlreadyExists("physical object function", physical_object_function.name)

    statement = (
        insert(physical_object_functions_dict)
        .values(
            parent_id=physical_object_function.parent_id,
            name=physical_object_function.name,
            code=physical_object_function.code,
        )
        .returning(physical_object_functions_dict.c.physical_object_function_id)
    )
    physical_object_function_id = (await conn.execute(statement)).scalar_one()

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
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return PhysicalObjectFunctionDTO(**result)


async def put_physical_object_function_to_db(
    conn: AsyncConnection,
    physical_object_function_id: int,
    physical_object_function: PhysicalObjectFunctionPut,
) -> PhysicalObjectFunctionDTO:
    """Update physical object function object by getting all its attributes."""

    statement = select(physical_object_functions_dict).where(
        physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(physical_object_function_id, "physical object function")

    if physical_object_function.parent_id is not None:
        statement = select(physical_object_functions_dict).where(
            physical_object_functions_dict.c.physical_object_function_id == physical_object_function.parent_id
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is None:
            raise EntityNotFoundById(physical_object_function.parent_id, "physical object function")

    statement = select(physical_object_functions_dict).where(
        physical_object_functions_dict.c.name == physical_object_function.name,
        physical_object_functions_dict.c.physical_object_function_id != physical_object_function_id,
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise EntityAlreadyExists("physical object function", physical_object_function.name)

    statement = (
        update(physical_object_functions_dict)
        .where(physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id)
        .values(
            name=physical_object_function.name,
            parent_id=physical_object_function.parent_id,
            code=physical_object_function.code,
        )
    )

    await conn.execute(statement)

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
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return PhysicalObjectFunctionDTO(**result)


async def patch_physical_object_function_to_db(
    conn: AsyncConnection,
    physical_object_function_id: int,
    physical_object_function: PhysicalObjectFunctionPatch,
) -> PhysicalObjectFunctionDTO:
    """Update physical object function object by getting only given attributes."""

    statement = select(physical_object_functions_dict).where(
        physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(physical_object_function_id, "physical object function")

    if physical_object_function.parent_id is not None:
        statement = select(physical_object_functions_dict).where(
            physical_object_functions_dict.c.physical_object_function_id == physical_object_function.parent_id
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is None:
            raise EntityNotFoundById(physical_object_function.parent_id, "physical object function")

    if physical_object_function.name is not None:
        statement = select(physical_object_functions_dict).where(
            physical_object_functions_dict.c.name == physical_object_function.name,
            physical_object_functions_dict.c.physical_object_function_id != physical_object_function_id,
        )
        result = (await conn.execute(statement)).one_or_none()
        if result is not None:
            raise EntityAlreadyExists("physical object function", physical_object_function.name)

    statement = update(physical_object_functions_dict).where(
        physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id
    )

    values_to_update = {}
    for k, v in physical_object_function.model_dump(exclude_unset=True).items():
        values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    await conn.execute(statement)

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
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return PhysicalObjectFunctionDTO(**result)


async def delete_physical_object_function_from_db(conn: AsyncConnection, physical_object_function_id: int) -> dict:
    """Delete physical object function object by id."""

    statement = select(physical_object_functions_dict).where(
        physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id
    )
    physical_object_function = (await conn.execute(statement)).scalar_one_or_none()
    if physical_object_function is None:
        raise EntityNotFoundById(physical_object_function_id, "physical object function")

    statement = delete(physical_object_functions_dict).where(
        physical_object_functions_dict.c.physical_object_function_id == physical_object_function_id
    )
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


async def get_physical_object_types_hierarchy_from_db(
    conn: AsyncConnection, physical_object_type_ids: str | None
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

    if physical_object_type_ids is not None:
        ids = [int(physical_object_type_id.strip()) for physical_object_type_id in physical_object_type_ids.split(",")]
        query = select(physical_object_types_dict.c.physical_object_type_id).where(
            physical_object_types_dict.c.physical_object_type_id.in_(ids)
        )
        physical_object_types = (await conn.execute(query)).scalars().all()
        if len(list(physical_object_types)) < len(ids):
            raise EntitiesNotFoundByIds("physical object type")
        statement = statement.where(physical_object_types_dict.c.physical_object_type_id.in_(ids))

    physical_object_types = (await conn.execute(statement)).mappings().all()

    statement = select(physical_object_functions_dict).order_by(physical_object_functions_dict.c.level)
    physical_object_functions = (await conn.execute(statement)).mappings().all()

    def build_filtered_hierarchy(parent_id: int = None) -> list[PhysicalObjectTypesHierarchyDTO]:
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
