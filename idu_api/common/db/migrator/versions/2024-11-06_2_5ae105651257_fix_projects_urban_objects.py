# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix projects urban objects

Revision ID: 5ae105651257
Revises: 8a529b789a12
Create Date: 2024-11-06 14:44:03.663602

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5ae105651257"
down_revision: Union[str, None] = "8a529b789a12"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add public columns to `user_projects.urban_objects_data`
    op.add_column(
        "urban_objects_data",
        sa.Column("public_physical_object_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.add_column(
        "urban_objects_data",
        sa.Column("public_object_geometry_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.add_column(
        "urban_objects_data",
        sa.Column("public_service_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "urban_objects_data_fk_public_physical_object_id__pod",
        "urban_objects_data",
        "physical_objects_data",
        ["public_physical_object_id"],
        ["physical_object_id"],
        source_schema="user_projects",
        referent_schema="public",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "urban_objects_data_fk_public_object_geometry_id__ogd",
        "urban_objects_data",
        "object_geometries_data",
        ["public_object_geometry_id"],
        ["object_geometry_id"],
        source_schema="user_projects",
        referent_schema="public",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "urban_objects_data_fk_public_service_id__sd",
        "urban_objects_data",
        "services_data",
        ["public_service_id"],
        ["service_id"],
        source_schema="user_projects",
        referent_schema="public",
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint("urban_objects_data_fk_public_service_id__sd", "urban_objects_data", schema="user_projects")
    op.drop_constraint(
        "urban_objects_data_fk_public_object_geometry_id__ogd", "urban_objects_data", schema="user_projects"
    )
    op.drop_constraint(
        "urban_objects_data_fk_public_physical_object_id__pod", "urban_objects_data", schema="user_projects"
    )
    op.drop_column("urban_objects_data", "public_service_id", schema="user_projects")
    op.drop_column("urban_objects_data", "public_object_geometry_id", schema="user_projects")
    op.drop_column("urban_objects_data", "public_physical_object_id", schema="user_projects")
