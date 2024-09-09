# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix project phys objects

Revision ID: 38ff7a2d4779
Revises: 8c7372ad8908
Create Date: 2024-09-09 17:12:37.149287

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "38ff7a2d4779"
down_revision: Union[str, None] = "8c7372ad8908"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns
    op.alter_column("physical_objects_data", "name", nullable=True, schema="user_projects")


def downgrade() -> None:
    # data
    op.execute(
        sa.text(
            dedent(
                """
                DELETE FROM user_projects.physical_objects_data
                WHERE name IS NULL;
                """
            )
        )
    )

    # columns
    op.alter_column("physical_objects_data", "name", nullable=False, schema="user_projects")
