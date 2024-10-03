# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add timestamps to project indicators and scenarios

Revision ID: 87a89b9d035f
Revises: 43080527857b
Create Date: 2024-10-03 11:51:30.011001

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "87a89b9d035f"
down_revision: Union[str, None] = "43080527857b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "indicators_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "indicators_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )

    op.add_column(
        "scenarios_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "scenarios_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )


def downgrade() -> None:
    op.drop_column("scenarios_data", "updated_at")
    op.drop_column("scenarios_data", "created_at")

    op.drop_column("indicators_data", "updated_at")
    op.drop_column("indicators_data", "created_at")
