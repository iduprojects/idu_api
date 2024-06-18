"""
Service types endpoints logic of getting entities from the database is defined here.
"""

from typing import Callable, List, Optional

from fastapi import HTTPException
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    service_types_dict,
    service_types_normatives_data,
    territories_data,
    urban_functions_dict,
)
from urban_api.dto import ServiceTypesDTO, ServiceTypesNormativesDTO, UrbanFunctionDTO
from urban_api.schemas import ServiceTypesNormativesDataPost, ServiceTypesPost, UrbanFunctionPost

func: Callable


async def get_service_types_from_db(
    urban_function_id: Optional[int],
    session: AsyncConnection,
) -> List[ServiceTypesDTO]:
    """
    Get all service type objects
    """

    statement = select(service_types_dict).order_by(service_types_dict.c.service_type_id)

    if urban_function_id is not None:
        statement = statement.where(service_types_dict.c.urban_function_id == urban_function_id)

    return [ServiceTypesDTO(**data) for data in (await session.execute(statement)).mappings().all()]


async def add_service_type_to_db(
    service_type: ServiceTypesPost,
    session: AsyncConnection,
) -> ServiceTypesDTO:
    """
    Create service type object
    """

    statement = select(service_types_dict).where(service_types_dict.c.name == service_type.name)
    result = (await session.execute(statement)).one_or_none()
    if result is not None:
        raise HTTPException(status_code=400, detail="Invalid input (service type already exists)")

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
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return ServiceTypesDTO(**result)


async def get_urban_functions_by_parent_id_from_db(
    parent_id: Optional[int],
    name: Optional[str],
    session: AsyncConnection,
    get_all_subtree: bool,
) -> List[UrbanFunctionDTO]:
    """
    Get an urban function or list of urban functions by parent
    """

    if parent_id is not None:
        statement = select(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == parent_id)
        parent_urban_function = (await session.execute(statement)).one_or_none()
        if parent_urban_function is None:
            raise HTTPException(status_code=404, detail="Given parent id is not found")

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

    result = (await session.execute(statement)).mappings().all()

    return [UrbanFunctionDTO(**indicator) for indicator in result]


async def add_urban_function_to_db(
    urban_function: UrbanFunctionPost,
    session: AsyncConnection,
) -> UrbanFunctionDTO:
    """
    Create urban function object
    """

    if urban_function.parent_id is not None:
        statement = select(urban_functions_dict).where(
            urban_functions_dict.c.urban_function_id == urban_function.parent_id
        )
        parent_urban_function = (await session.execute(statement)).one_or_none()
        if parent_urban_function is None:
            raise HTTPException(status_code=404, detail="Given parent_id is not found")

    statement = select(urban_functions_dict).where(urban_functions_dict.c.name == urban_function.name)
    check_urban_function_name = (await session.execute(statement)).one_or_none()
    if check_urban_function_name is not None:
        raise HTTPException(status_code=400, detail="Invalid input (urban function already exists)")

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
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return UrbanFunctionDTO(**result)


async def get_service_types_normatives_from_db(
    service_type_id: Optional[int],
    urban_function_id: Optional[int],
    territory_id: Optional[int],
    session: AsyncConnection,
) -> List[ServiceTypesNormativesDTO]:
    """
    Get all service type normative objects
    """

    statement = select(service_types_normatives_data).order_by(service_types_normatives_data.c.normative_id)

    if service_type_id is not None:
        statement = statement.filter(service_types_normatives_data.c.service_type_id == service_type_id)
    if urban_function_id is not None:
        statement = statement.filter(service_types_normatives_data.c.urban_function_id == urban_function_id)
    if territory_id is not None:
        statement = statement.filter(service_types_normatives_data.c.territory_id == territory_id)

    return [ServiceTypesNormativesDTO(**data) for data in (await session.execute(statement)).mappings().all()]


async def add_service_type_normative_to_db(
    service_type_normative: ServiceTypesNormativesDataPost,
    session: AsyncConnection,
) -> ServiceTypesNormativesDTO:
    """
    Create service type normative object
    """

    if service_type_normative.service_type_id is not None:
        statement = select(service_types_dict).where(
            service_types_dict.c.service_type_id == service_type_normative.service_type_id
        )
        service_type = (await session.execute(statement)).one_or_none()
        if service_type is None:
            raise HTTPException(status_code=404, detail="Given service_type_id is not found")

    if service_type_normative.urban_function_id is not None:
        statement = select(urban_functions_dict).where(
            urban_functions_dict.c.urban_function_id == service_type_normative.urban_function_id
        )
        urban_function = (await session.execute(statement)).one_or_none()
        if urban_function is None:
            raise HTTPException(status_code=404, detail="Given urban_function_id is not found")

    statement = select(territories_data).where(territories_data.c.territory_id == service_type_normative.territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory_id is not found")

    statement = (
        insert(service_types_normatives_data)
        .values(
            service_type_id=service_type_normative.service_type_id,
            urban_function_id=service_type_normative.urban_function_id,
            territory_id=service_type_normative.territory_id,
            is_regulated=service_type_normative.is_regulated,
            radius_availability_meters=service_type_normative.radius_availability_meters,
            time_availability_minutes=service_type_normative.time_availability_minutes,
            services_per_1000_normative=service_type_normative.services_per_1000_normative,
            services_capacity_per_1000_normative=service_type_normative.services_capacity_per_1000_normative,
        )
        .returning(service_types_normatives_data)
    )
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return ServiceTypesNormativesDTO(**result)
