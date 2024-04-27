"""
SQL naming convention for Alembic is defined here.
"""
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base


convention = {
    "all_column_names": lambda constraint, _: "_".join([str(column.name) for column in constraint.columns.values()]),
    "ix": "ix_%(table_name)s_%(all_column_names)s",
    "uq": "%(table_name)s_%(all_column_names)s_key",
    "ck": "%(table_name)s_check_%(constraint_name)s",
    "fk": "%(table_name)s_fk_%(all_column_names)s__%(referred_table_name)s",
    "pk": "%(table_name)s_pk",
}

metadata = MetaData(naming_convention=convention)
DeclarativeBase = declarative_base(metadata=metadata)

__all__ = [
    "DeclarativeBase",
    "metadata",
]
