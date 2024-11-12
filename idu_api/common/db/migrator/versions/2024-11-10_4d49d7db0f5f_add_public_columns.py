# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add public columns

Revision ID: 4d49d7db0f5f
Revises: 0833fd4ae9a0
Create Date: 2024-11-10 13:11:20.055159

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4d49d7db0f5f"
down_revision: Union[str, None] = "0833fd4ae9a0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add `public_object_geometry_id` to `user_projects.object_geometries_data`
    op.add_column(
        "object_geometries_data",
        sa.Column("public_object_geometry_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "object_geometries_data_fk_public_object_geometry_id__ogd",
        "object_geometries_data",
        "object_geometries_data",
        ["public_object_geometry_id"],
        ["object_geometry_id"],
        source_schema="user_projects",
        referent_schema="public",
        ondelete="SET NULL",
    )

    # add `public_physical_object_id` to `user_projects.physical_objects_data`
    op.add_column(
        "physical_objects_data",
        sa.Column("public_physical_object_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "physical_objects_data_fk_public_physical_object_id__pod",
        "physical_objects_data",
        "physical_objects_data",
        ["public_physical_object_id"],
        ["physical_object_id"],
        source_schema="user_projects",
        referent_schema="public",
        ondelete="SET NULL",
    )

    # add `public_service_id` to `user_projects.services_data`
    op.add_column(
        "services_data",
        sa.Column("public_service_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "services_data_fk_public_service_id__sd",
        "services_data",
        "services_data",
        ["public_service_id"],
        ["service_id"],
        source_schema="user_projects",
        referent_schema="public",
        ondelete="SET NULL",
    )


def downgrade() -> None:
    # drop constraints and public columns
    op.drop_constraint("services_data_fk_public_service_id__sd", "services_data", schema="user_projects")
    op.drop_column("services_data", "public_service_id", schema="user_projects")
    op.drop_constraint(
        "physical_objects_data_fk_public_physical_object_id__pod", "physical_objects_data", schema="user_projects"
    )
    op.drop_column("physical_objects_data", "public_physical_object_id", schema="user_projects")
    op.drop_constraint(
        "object_geometries_data_fk_public_object_geometry_id__ogd", "object_geometries_data", schema="user_projects"
    )
    op.drop_column("object_geometries_data", "public_object_geometry_id", schema="user_projects")
