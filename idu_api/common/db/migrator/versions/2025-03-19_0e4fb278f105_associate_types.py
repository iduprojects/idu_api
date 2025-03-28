# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""associate types

Revision ID: 0e4fb278f105
Revises: b2e19887ea0c
Create Date: 2025-03-19 14:14:48.762295

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0e4fb278f105"
down_revision: Union[str, None] = "b2e19887ea0c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create table to associate `physical_object_types_dict` and `service_types_dict` tables
    op.create_table(
        "object_service_types_dict",
        sa.Column("physical_object_type_id", sa.Integer, nullable=False),
        sa.Column("service_type_id", sa.Integer, nullable=False),
        sa.ForeignKeyConstraint(
            ["physical_object_type_id"],
            ["physical_object_types_dict.physical_object_type_id"],
            name=op.f("objects_service_types_data_fk_physical_object_type_id"),
        ),
        sa.ForeignKeyConstraint(
            ["service_type_id"],
            ["service_types_dict.service_type_id"],
            name=op.f("objects_service_types_data_fk_service_type_id"),
        ),
        sa.PrimaryKeyConstraint(
            "physical_object_type_id", "service_type_id", name=op.f("objects_service_types_dict_pk")
        ),
    )

    # add `service_type_id` and `physical_object_type_id` to `indicators_dict` table
    op.add_column("indicators_dict", sa.Column("physical_object_type_id", sa.Integer, nullable=True))
    op.add_column("indicators_dict", sa.Column("service_type_id", sa.Integer, nullable=True))
    op.create_foreign_key(
        "indicators_dict_fk_physical_object_type_id__pot",
        "indicators_dict",
        "physical_object_types_dict",
        ["physical_object_type_id"],
        ["physical_object_type_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "indicators_dict_fk_service_type_id__service_types_dict",
        "indicators_dict",
        "service_types_dict",
        ["service_type_id"],
        ["service_type_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # drop association table
    op.drop_table("object_service_types_dict")

    # drop new `indicators_dict` columns
    op.drop_column("indicators_dict", "physical_object_type_id")
    op.drop_column("indicators_dict", "service_type_id")
