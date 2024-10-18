# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""projects object geometries timestamp

Revision ID: 737f3c396010
Revises: 071c1402fcea
Create Date: 2024-10-18 19:10:04.862572

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "737f3c396010"
down_revision: Union[str, None] = "071c1402fcea"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add timestamp columns
    op.add_column(
        "object_geometries_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "object_geometries_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )


def downgrade() -> None:
    # drop columns
    op.drop_column("object_geometries_data", "created_at", schema="user_projects")
    op.drop_column("object_geometries_data", "updated_at", schema="user_projects")
