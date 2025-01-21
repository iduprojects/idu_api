from datetime import datetime

import pytest
from geoalchemy2.types import Geometry
from shapely.geometry import Point
from sqlalchemy import Column, Table
from sqlalchemy.sql import Insert, Select, Update


def get_query_columns(query):
    """
    Extract column names and their types from a SQLAlchemy query or table.
    """

    def get_column_type(col: Column):
        if isinstance(col.type, Geometry):
            return "GeometryType"
        try:
            return col.type.python_type
        except NotImplementedError:
            return str

    def process_columns(columns):
        """Process columns, including those extracted from a Table."""
        result = {}
        for col in columns:
            if isinstance(col, Table):  # If it's a Table, process its columns
                result.update({c.name: get_column_type(c) for c in col.columns if isinstance(c, Column)})
            else:
                result.update({col.name: get_column_type(col)})
        return result

    if isinstance(query, Select):
        return process_columns(query.selected_columns)

    if isinstance(query, (Insert, Update)):
        if query._returning is not None:
            return process_columns(query._returning)
        return {}

    return {}


class MockRow:
    """
    Represents a single row of mock data.
    """

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        self._data = kwargs

    @property
    def data(self):
        return self._data

    def __getitem__(self, item):
        return self.data.get(item)

    def keys(self):
        return self._data.keys()

    def items(self):
        return self.data.items()

    def __repr__(self):
        return str(self.data)


class MockResult:
    """
    Represents a collection of mock rows.
    """

    def __init__(self, rows):
        self.rows = rows

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
        if self.rows:
            return list(self.rows[0].data.values())[0]
        return None

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

    def all(self):
        """
        Simulate the `all()` method. Returns all rows.
        """
        return self.rows


class MockConnection:
    """
    A custom connection type for the purpose of these tests.
    """

    async def execute(self, query):
        """
        Return a mock result based on the query
        """
        columns = get_query_columns(query)
        data = {col: self._mock_value(dtype) for col, dtype in columns.items()}
        return MockResult([MockRow(**data)])

    async def commit(self):
        return self

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
            return 1.23
        if dtype == bool:
            return True
        if dtype == dict:
            return {}
        if dtype == "GeometryType":
            return Point(1, 1)
        if dtype == datetime:
            return datetime(2024, 1, 1)
        return None  # Default for unknown types


@pytest.fixture
def mock_conn():
    return MockConnection()
