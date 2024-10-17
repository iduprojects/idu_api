# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""update projects_data

Revision ID: 1138c60a0c7f
Revises: c64e20abd7c2
Create Date: 2024-10-17 14:44:32.319418

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "1138c60a0c7f"
down_revision: Union[str, None] = "c64e20abd7c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "projects_data",
        "image_url",
        type_=sa.String(),
        nullable=True,
        schema="user_projects",
    )

    op.add_column(
        "projects_data",
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        schema="user_projects",
    )


def downgrade() -> None:
    op.drop_column("projects_data", "properties", schema="user_projects")

    op.alter_column(
        "projects_data",
        "image_url",
        type_=sa.String(length=200),
        nullable=True,
        schema="user_projects",
    )
