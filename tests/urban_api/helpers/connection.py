"""Mock AsyncConnection implementation is defined here."""

from datetime import date, datetime
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import structlog
import pytest
from geoalchemy2.types import Geometry
from sqlalchemy import Column, Enum, Table
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Insert, Select, Update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.connection import PostgresConnectionManager
from idu_api.urban_api.config import DBConfig
from idu_api.urban_api.schemas.enums import NormativeType

__all__ = [
    "MockConnection",
    "MockConnection",
    "MockRow",
    "mock_conn",
    "connection",
]


class MockRow:
    """
    Represents a single row of mock data.
    """

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        self._data = kwargs
        self._mapping = self

    @property
    def data(self):
        return self._data

    def __getitem__(self, item):
        return self._data.get(item)

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def __repr__(self):
        return str(self._data)


class MockResult:
    """
    Represents a collection of mock rows.
    """

    def __init__(self, rows, paging_data=None):
        self.rows = rows
        self.paging = self.Paging(paging_data) if paging_data else None

    def scalar(self):
        """
        Simulate the `scalar_one()` method to return the first value of the first row.
        """
        return [list(row.data.values())[0] for row in self.rows][0]

    def mappings(self):
        """
        Simulate the `mappings()` method to return dictionaries for each row.
        This is typically used when you expect a list of dictionaries.
        """
        return self

    def scalars(self):
        """
        Simulate the `scalars()` method to return the first value of each row.
        This is typically used when you expect a list of values for a single column.
        """
        self.rows = [list(row.data.values())[0] for row in self.rows]
        return self

    def scalar_one(self):
        """
        Simulate the `scalar_one()` method to return the first value of the first row.
        Typically used when you expect a single value from a single row.
        """
        if len(self.rows) == 1:
            return list(self.rows[0].data.values())[0]
        raise ValueError("Expected one row, but got multiple or zero rows.")

    def scalar_one_or_none(self):
        """
        Simulate the `scalar_one_or_none()` method to return the first value of the first row,
        or None if there are no rows.
        """
        if self.rows:
            return list(self.rows[0].data.values())[0]
        return None

    def one_or_none(self):
        """
        Simulate the `one_or_none()` method.
        Returns the first row if there is exactly one, or None if there are no rows.
        Raises an exception if there are more than one row.
        """
        if len(self.rows) == 1:
            return self.rows[0]
        if len(self.rows) == 0:
            return None
        raise ValueError("Expected one or zero rows, but got multiple rows.")

    def one(self):
        """
        Simulate the `one()` method. Returns the only one row if there is exactly one.
        """
        if len(self.rows) == 1:
            return self.rows[0]
        raise ValueError("Expected one row, but got multiple or zero rows.")

    def all(self):
        """
        Simulate the `all()` method. Returns all rows.
        """
        return self.rows

    def __iter__(self):
        return iter(self.rows)

    class Paging:
        """Simulates a pagination object with bookmarks."""

        def __init__(self, paging_data):
            self.bookmark_previous = paging_data.get("previous")
            self.bookmark_next = paging_data.get("next_")
            self.has_previous = paging_data.get("has_previous", False)
            self.has_next = paging_data.get("has_next", False)


class MockConnection:
    """
    A custom connection type for the purpose of these tests.
    """

    def __init__(self, *args, **kwargs):
        self.execute_mock = AsyncMock()
        self.commit_mock = AsyncMock()
        self.args = args
        self.kwargs = kwargs
        self.sync_connection = MagicMock(spec=Connection)
        self.sync_connection.execute.side_effect = self._sync_execute
        self.sync_connection.commit.side_effect = self._sync_commit

    async def execute(self, query, paging_data=None):
        """
        Return a mock result based on the query.
        """
        await self.execute_mock(str(query))
        columns = self._get_query_columns(query)
        data = {col: self._mock_value(dtype) for col, dtype in columns.items()}
        return MockResult([MockRow(**data)], paging_data=paging_data)

    async def commit(self):
        """
        Simulate the `commit` method.
        """
        await self.commit_mock()
        return self

    def _sync_execute(self, query):
        columns = self._get_query_columns(query)
        data = {col: self._mock_value(dtype) for col, dtype in columns.items()}
        return MockResult([MockRow(**data)])

    def _sync_commit(self):
        pass

    @staticmethod
    def _get_query_columns(query):
        """
        Extract column names and their types from a SQLAlchemy query or table.
        """

        def get_column_type(col: Column):
            if isinstance(col.type, Geometry) or col.name.removeprefix("public_") in {"geometry", "centre_point"}:
                return "GeometryType"
            if isinstance(col.type, Enum):
                return col.type
            if col.name == "normative_type":
                return "normative"
            if col.name == "row_num":
                return int
            try:
                return col.type.python_type
            except Exception as e:
                raise NotImplementedError(f"Unsupported column type: {col.type}") from e

        def process_columns(columns):
            """Process columns, including those extracted from a Table."""
            result = {}
            for col in columns:
                if isinstance(col, Table):
                    result.update({c.name: get_column_type(c) for c in col.columns if isinstance(c, Column)})
                else:
                    result[col.name] = get_column_type(col)
            return result

        if isinstance(query, Select):
            return process_columns(query.selected_columns)

        if isinstance(query, (Insert, Update)):
            if query._returning is not None:
                return process_columns(query._returning)
            return {}

        return {}

    @staticmethod
    def _mock_value(dtype):
        """
        Generate a mock value based on the expected Python type.
        """

        if dtype == int:
            return 1
        if dtype == str:
            return "mock_string"
        if dtype == float:
            return 1.0
        if dtype == bool:
            return True
        if dtype == dict:
            return {"context": [1]}
        if dtype == "GeometryType":
            return {"type": "Point", "coordinates": [1, 2]}
        if dtype == datetime:
            return datetime(2024, 1, 1)
        if dtype == date:
            return date(2024, 1, 1)
        if isinstance(dtype, Enum):
            return dtype.enums[0]
        if dtype == "normative":
            return NormativeType.GLOBAL
        return None  # Default for unknown types


@pytest.fixture
def mock_conn():
    return MockConnection()


@pytest.fixture(scope="session")
async def connection(database: DBConfig, logger: structlog.stdlib.BoundLogger) -> AsyncIterator[AsyncConnection]:
    connection_manager = PostgresConnectionManager(
        host=database.addr,
        port=database.port,
        database=database.name,
        user=database.user,
        password=database.password,
        logger=logger,
        pool_size=1,
        application_name="urban-api_integration_tests",
    )
    async with connection_manager.get_connection() as conn:
        yield conn
