# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix territories schema

Revision ID: 5b2ffe0484de
Revises: 79adbe93b346
Create Date: 2024-05-25 20:27:09.410413

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5b2ffe0484de"
down_revision: Union[str, None] = "79adbe93b346"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    # columns

    op.alter_column("indicators_dict", "name_short", existing_type=sa.VARCHAR(length=200), nullable=False)
    op.alter_column("indicators_dict", "measurement_unit_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("object_geometries_data", "territory_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("services_data", "territory_type_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("services_data", "name", existing_type=sa.VARCHAR(length=200), nullable=True)
    op.add_column(
        "territories_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.add_column(
        "territories_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )


def downgrade() -> None:

    # columns

    op.drop_column("territories_data", "updated_at")
    op.drop_column("territories_data", "created_at")
    op.alter_column("services_data", "name", existing_type=sa.VARCHAR(length=200), nullable=False)
    op.alter_column("services_data", "territory_type_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("object_geometries_data", "territory_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("indicators_dict", "measurement_unit_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("indicators_dict", "name_short", existing_type=sa.VARCHAR(length=200), nullable=True)
