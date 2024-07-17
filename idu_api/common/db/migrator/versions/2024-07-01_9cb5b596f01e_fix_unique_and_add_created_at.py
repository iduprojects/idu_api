# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix unique and add created_at

Revision ID: 9cb5b596f01e
Revises: a2862d4a7f8b
Create Date: 2024-07-01 15:26:09.926164

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9cb5b596f01e"
down_revision: Union[str, None] = "a2862d4a7f8b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns
    op.add_column(
        "physical_objects_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.add_column(
        "physical_objects_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.add_column(
        "services_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.add_column(
        "services_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )

    # constraints
    op.drop_constraint(
        "urban_objects_data_physical_object_id_object_geometry_id_key", "urban_objects_data", type_="unique"
    )
    op.create_unique_constraint(
        op.f("urban_objects_data_physical_object_id_object_geometry_id_service_id_key"),
        "urban_objects_data",
        ["physical_object_id", "object_geometry_id", "service_id"],
    )


def downgrade() -> None:
    # constraints
    op.drop_constraint(
        op.f("urban_objects_data_physical_object_id_object_geometry_id_service_id_key"),
        "urban_objects_data",
        type_="unique",
    )
    op.create_unique_constraint(
        "urban_objects_data_physical_object_id_object_geometry_id_key",
        "urban_objects_data",
        ["physical_object_id", "object_geometry_id"],
    )

    # columns
    op.drop_column("services_data", "updated_at")
    op.drop_column("services_data", "created_at")
    op.drop_column("physical_objects_data", "updated_at")
    op.drop_column("physical_objects_data", "created_at")
