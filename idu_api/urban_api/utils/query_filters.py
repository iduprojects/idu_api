"""
This module provides classes for applying filters to SQLAlchemy `select()` queries.
It uses the Strategy pattern to define various filter behaviors, such as equality filtering and recursive filtering
(using a Common Table Expression).

Typical usage example:

    query = apply_filters(
        query,
        base_table,
        EqFilter("type_id", some_id),
        RecursiveFilter("function_id", parent_id, recursive_table, id_field="id", parent_field="parent_id"),
    )

This approach avoids code duplication and allows filters to be easily composed and reused across multiple queries.
"""

import abc
from collections.abc import Callable
from typing import Any

from sqlalchemy import CTE, select
from sqlalchemy.sql import Select
from sqlalchemy.sql.schema import Table


class BaseFilter(abc.ABC):
    """
    Abstract base class for all filter strategies.

    Each subclass must implement the `apply()` method to modify the query
    according to its logic.
    """

    def __init__(self, table: Table | CTE, field_name: str, value: Any):
        """
        Initialize the filter with the target field and its value.

        Args:
            table: The SQLAlchemy Table object that contains the target column.
            field_name: The column name to apply the filter to.
            value: The value to filter by. If None, the filter will be skipped.
        """
        self.table = table
        self.field_name = field_name
        self.value = value

    @abc.abstractmethod
    def apply(self, query: Select) -> Select:
        """
        Apply the filter to the given SQLAlchemy query.

        Args:
            query: The SQLAlchemy Select query to modify.

        Returns:
            The modified query with the filter applied.
        """


class EqFilter(BaseFilter):
    """
    A basic equality filter: `table.field == value`.

    If `value` is None, this filter has no effect.
    """

    def apply(self, query: Select) -> Select:
        if self.value is not None:
            return query.where(getattr(self.table.c, self.field_name) == self.value)
        return query


class InFilter(BaseFilter):
    """
    A basic equality filter: `table.field.in_(value)`.

    If `value` is None, this filter has no effect.
    """

    def apply(self, query: Select) -> Select:
        if self.value:
            return query.where(getattr(self.table.c, self.field_name).in_(self.value))
        return query


class ILikeFilter(BaseFilter):
    """
    A case-insensitive LIKE filter using SQL `ILIKE '%value%'`.

    This is useful for text search in string fields. If value is None or an empty string, this filter does nothing.
    """

    def apply(self, query: Select) -> Select:
        if self.value:
            # Apply case-insensitive LIKE: column ILIKE '%value%'
            return query.where(getattr(self.table.c, self.field_name).ilike(f"%{self.value}%"))
        return query


class RecursiveFilter(BaseFilter):
    """
    A recursive filter using a common table expression (CTE).

    It traverses a parent-child hierarchy in a table and filters for all
    descendant values of a given ID. Useful for trees or nested taxonomies.
    """

    def __init__(
        self,
        table: Table | CTE,
        field_name: str,
        value: int | None,
        recursive_table,
        id_field: str | None = None,
        parent_field: str = "parent_id",
        allow_null_value: bool = False,
    ):
        """
        Initialize the recursive filter.

        Args:
            table: The SQLAlchemy Table object that contains the target column.
            field_name: The column on the target table to filter by.
            value: The root ID to start the recursive search from.
            recursive_table: The table with a self-referential relationship.
            id_field: The primary key or unique ID field in the recursive table. The default value is field_name.
            parent_field: The foreign key referring to the parent in the recursive table.
            allow_null_value: If it is true, then the filter will build a hierarchy from the top level.
        """
        super().__init__(table, field_name, value)
        self.recursive_table = recursive_table
        self.id_field = id_field or field_name
        self.parent_field = parent_field
        self.allow_null_value = allow_null_value

    def apply(self, query: Select) -> Select:
        if self.value is None and not self.allow_null_value:
            return query

        # Initial non-recursive term (seed)
        cte = (
            select(
                self.recursive_table.c[self.id_field],
                self.recursive_table.c[self.parent_field],
            )
            .where(
                self.recursive_table.c[self.id_field] == self.value
                if self.value is not None
                else self.recursive_table.c[self.id_field].is_(None)
            )
            .cte(name=f"{self.recursive_table}_cte", recursive=True)
        )

        # Recursive term: join CTE on parent field
        cte = cte.union_all(
            select(
                self.recursive_table.c[self.id_field],
                self.recursive_table.c[self.parent_field],
            ).join(
                cte,
                self.recursive_table.c[self.parent_field] == cte.c[self.id_field],
            )
        )

        # Apply filter to main query
        return query.where(getattr(self.table.c, self.field_name).in_(select(cte.c[self.id_field])))


class CustomFilter(BaseFilter):
    """
    Arbitrary user-defined filter logic.

    Useful when filtering depends on multiple parameters, custom expressions, or branching logic.
    """

    def __init__(self, func: Callable[[Select], Select]):
        """
        Args:
            func: A function that takes a query and returns a modified query.
        """
        self.func = func
        super().__init__(table=..., field_name="", value=None)  # Not used

    def apply(self, query: Select) -> Select:
        return self.func(query)


def apply_filters(query: Select, *filters: BaseFilter) -> Select:
    """
    Apply a series of filters to a SQLAlchemy query.

    Args:
        query: The base Select query to modify.
        filters: One or more BaseFilter instances to apply.

    Returns:
        The resulting query with all filters applied.
    """
    for f in filters:
        query = f.apply(query)
    return query
