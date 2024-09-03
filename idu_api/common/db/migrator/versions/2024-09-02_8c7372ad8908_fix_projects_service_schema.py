# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix projects service schema

Revision ID: 8c7372ad8908
Revises: 1c3bc1815782
Create Date: 2024-09-02 13:25:29.242562

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8c7372ad8908"
down_revision: Union[str, None] = "1c3bc1815782"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns
    op.drop_column("services_data", "list_label", schema="user_projects")
    op.alter_column("services_data", "capacity_real", nullable=True, schema="user_projects")
    op.add_column("services_data", sa.Column("territory_type_id", sa.Integer(), nullable=True), schema="user_projects")

    # constraints
    op.create_foreign_key(
        "proj_services_data_fk_territory_type_id__territory_types_dict",
        "services_data",
        "territory_types_dict",
        ["territory_type_id"],
        ["territory_type_id"],
        source_schema="user_projects",
        referent_schema="public",
    )


def downgrade() -> None:
    # constraints
    op.drop_constraint(
        "proj_services_data_fk_territory_type_id__territory_types_dict",
        "services_data",
        "foreignkey",
        schema="user_projects",
    )

    # columns
    op.drop_column("services_data", "territory_type_id", schema="user_projects")
    op.alter_column("services_data", "capacity_real", nullable=False, schema="user_projects")
    op.add_column("services_data", sa.Column("list_label", sa.String(length=20), nullable=True), schema="user_projects")
