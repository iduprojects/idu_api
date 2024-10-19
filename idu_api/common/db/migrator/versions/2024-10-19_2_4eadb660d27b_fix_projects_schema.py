# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix projects schema

Revision ID: 4eadb660d27b
Revises: 6cf43ed886a2
Create Date: 2024-10-19 19:59:32.996967

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "4eadb660d27b"
down_revision: Union[str, None] = "6cf43ed886a2"
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
