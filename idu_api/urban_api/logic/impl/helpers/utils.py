from datetime import datetime, timezone
from typing import Any, TypeVar

from geoalchemy2.functions import ST_GeomFromText, ST_Union
from pydantic import BaseModel
from sqlalchemy import Table, select, text
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.selectable import CTE, Select

from idu_api.common.db.entities import projects_data, territories_data
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError

# The maximum number of records that can be returned in methods that accept a list of IDs as input.
OBJECTS_NUMBER_LIMIT = 25_000

# The maximum number of objects that can be inserted to the database at a time.
OBJECTS_NUMBER_TO_INSERT_LIMIT = 7_000

# The number of decimal places for geometry obtained from the database.
DECIMAL_PLACES = 15

UrbanAPIModel = TypeVar("UrbanAPIModel", bound=BaseModel)


async def check_existence(
    conn: AsyncConnection,
    table: Table,
    conditions: dict[str, Any] | None = None,
    not_conditions: dict[str, Any] | None = None,
) -> bool:
    """
    Universal existence checker function for any table and conditions.

    Args:
        conn (AsyncConnection): An active SQLAlchemy async connection.
        table (Table): SQLAlchemy Table object representing the target table.
        conditions (dict[str, Any]): Conditions to filter the query, provided as key-value pairs for equality checks.
        not_conditions (dict[str, Any]): Conditions to filter the query, provided as key-value pairs for inequality checks.

    Returns:
        bool: True if at least one row matches the conditions, False otherwise.
    """

    conditions = conditions or {}
    not_conditions = not_conditions or {}

    # Build the query with the provided conditions
    where_clauses = [
        getattr(table.c, key) == value if value is not None else getattr(table.c, key).is_(None)
        for key, value in conditions.items()
    ] + [
        getattr(table.c, key) != value if value is not None else getattr(table.c, key).isnot(None)
        for key, value in not_conditions.items()
    ]

    statement = select(table).where(*where_clauses).limit(1)

    # Execute the query and check for results
    result = (await conn.execute(statement)).one_or_none()

    return result is not None


def build_recursive_query(
    statement: Select,
    table: Table,
    parent_id: int | None,
    cte_name: str,
    primary_column: str,
    parent_column: str = "parent_id",
) -> Select:
    """
    Creates a recursive query based on the provided SQLAlchemy statement.

    Args:
        statement (Select): The base SQLAlchemy query.
        table (Table): SQLAlchemy Table object representing the target table.
        parent_id (int | None): The parent identifier value for the initial recursion level. Can be None.
        cte_name (str): The name for the recursive CTE.
        primary_column (str): The name of the primary column.
        parent_column (str): The name of the parent column.

    Returns:
        A recursive SQLAlchemy query.
    """

    # Base part of the CTE
    base_cte = statement.where(
        getattr(table.c, parent_column) == parent_id
        if parent_id is not None
        else getattr(table.c, parent_column).is_(None)
    )

    # Declare the CTE with recursion enabled
    cte_statement = base_cte.cte(name=cte_name, recursive=True)

    # Recursive part of the query
    recursive_part = statement.join(
        cte_statement, getattr(table.c, parent_column) == getattr(cte_statement.c, primary_column)
    )

    # Combine the base and recursive parts
    final_query = select(cte_statement.union_all(recursive_part))

    return final_query


def include_child_territories_cte(territory_id: int, cities_only: bool = False) -> CTE:
    """
    Recursively constructs a Common Table Expression (CTE) to include all child territories
    of a given territory. Optionally, it can filter the results to include only cities.

    Args:
        territory_id (int): The ID of the parent territory for which child territories are to be included.
        cities_only (bool): If True, only territories marked as cities will be included in the result.
                            Defaults to False.

    Returns:
        CTE: A SQLAlchemy CTE object representing the recursive query.
    """

    # Define the base CTE (anchor part)
    base_cte = (
        select(territories_data.c.territory_id, territories_data.c.is_city)
        .where(territories_data.c.territory_id == territory_id)
        .cte(name="recursive_cte", recursive=True)
    )

    # Define the recursive part
    recursive_query = select(territories_data.c.territory_id, territories_data.c.is_city).where(
        territories_data.c.parent_id == base_cte.c.territory_id
    )

    # Combine using UNION ALL
    recursive_cte = base_cte.union_all(recursive_query)

    # Apply cities_only filter if needed (keep it as CTE)
    if cities_only:
        filtered_cte = (
            select(recursive_cte.c.territory_id).where(recursive_cte.c.is_city.is_(True)).cte(name="filtered_cte")
        )
        return filtered_cte

    return recursive_cte


def extract_values_from_model(
    model: UrbanAPIModel,
    exclude_unset: bool = False,
    to_update: bool = False,
    srid: str = "4326",
) -> dict[str, Any]:
    """
    Extracts and processes values from a Pydantic model for database operations.

    Converts geometry fields to WKT format with SRID and optionally adds timestamp.
    Handles potential None values and type safety.

    Args:
        model (BaseModel): Pydantic model instance to extract data from.
        exclude_unset (bool): Whether to exclude fields not explicitly set in the model.
                      If True, only fields with explicitly set values are included.
        to_update (bool): Flag to add 'updated_at' timestamp for update operations.
                  If True, adds a UTC timestamp to the returned dictionary.
        srid (str): Spatial Reference System Identifier (SRID) for geometry fields.
              Defaults to "4326" (WGS 84). Must be a valid SRID string.

    Returns:
        dict: Processed values ready for database insertion/update.
              Geometry fields are converted to WKT format with the specified SRID.
              If `to_update` is True, includes an 'updated_at' timestamp.

    Raises:
        AttributeError: If geometry fields exist but contain None values.
                       Indicates that a required geometry field is missing a value.
        TypeError: If geometry fields have invalid types.
                  Indicates that a geometry field does not implement the required
                  `as_shapely_geometry` method or has an incompatible type.
    """

    # Extract initial values from the model
    values = model.model_dump(exclude_unset=exclude_unset)

    # Process geometry fields if they exist and contain valid values
    for field in ("geometry", "centre_point"):
        if field in values:
            geo_value = getattr(model, field)

            # Validate presence before conversion
            if geo_value is None:
                raise AttributeError(
                    f"Field `{field}` is present but contains None value. "
                    "Either provide a valid value or exclude the field."
                )

            try:
                # Convert to WKT format with SRID
                values[field] = ST_GeomFromText(geo_value.as_shapely_geometry().wkt, text(srid))  # Safe SRID parameter
            except AttributeError as e:
                raise TypeError(
                    f"Invalid type for {field}. Expected geometry object "
                    f"with 'as_shapely_geometry' method. Error: {str(e)}"
                ) from e

    # Add update timestamp if requested
    if to_update:
        values["updated_at"] = datetime.now(timezone.utc)

    return values


async def get_all_context_territories(conn, project_id: int, user_id: int) -> dict[str, Any]:
    """
    Retrieve project territory relations including context territories, unified geometry,
    and all related territories (descendants and ancestors).

    Args:
        conn (AsyncConnection): Database connection object.
        project_id (int): ID of the project to analyze.
        user_id (int): ID of the user requesting the data.

    Returns:
        Dictionary containing:
        - unified_geometry: Subquery for unified geometry of context territories.
        - all_related_territories: Subquery containing all related territory IDs.

    Raises:
        EntityNotFoundById: If project ID does not exist.
        AccessDeniedError: If user does not have permission to access the project.
    """
    # Retrieve the project with proper user access control
    statement = select(projects_data.c.user_id, projects_data.c.public, projects_data.c.properties).where(
        projects_data.c.project_id == project_id
    )
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id and not project.public:
        raise AccessDeniedError(project_id, "project")

    # Get context territories from project properties
    # Select basic information for territories listed in project's context
    context_territories = (
        select(
            territories_data.c.territory_id,
            territories_data.c.geometry,
        )
        .where(territories_data.c.territory_id.in_(project.properties["context"]))
        .subquery()
    )

    # Create unified geometry from context territories
    # Generate a single geometry combining all context territories using PostGIS ST_Union
    unified_geometry = select(ST_Union(context_territories.c.geometry)).scalar_subquery()

    # Find all descendant territories (recursive hierarchy)
    # Base case: Start with context territories
    all_descendants = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_descendants", recursive=True)
    )

    # Recursive case: Find child territories through parent_id relationships
    all_descendants = all_descendants.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_descendants,
                territories_data.c.parent_id == all_descendants.c.territory_id,
            )
        )
    )

    # Find all ancestor territories (recursive hierarchy)
    # Base case: Start with context territories
    all_ancestors = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_ancestors", recursive=True)
    )

    # Recursive case: Find parent territories through territory_id relationships
    all_ancestors = all_ancestors.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_ancestors,
                territories_data.c.territory_id == all_ancestors.c.parent_id,
            )
        )
    )

    # Combine all related territories (both descendants and ancestors)
    # Create a unified list of all territory IDs from both hierarchies
    all_related_territories = (
        select(all_descendants.c.territory_id).union(select(all_ancestors.c.territory_id)).subquery()
    )

    return {
        "geometry": unified_geometry,
        "territories": all_related_territories,
    }
