# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""project images service infrastructure

Revision ID: 5876eee7dce0
Revises: 60365c39941e
Create Date: 2024-10-23 16:47:52.319535

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5876eee7dce0"
down_revision: Union[str, None] = "60365c39941e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # change projects image column
    op.drop_column("projects_data", "image_url", schema="user_projects")

    # set null infrastructure_type
    op.alter_column("service_types_dict", "infrastructure_type", nullable=True)
    op.execute(
        sa.text("""UPDATE service_types_dict SET infrastructure_type = NULL WHERE infrastructure_type IS NOT NULL""")
    )


def downgrade() -> None:
    # change projects image column
    op.add_column(
        "projects_data",
        sa.Column("image_url", sa.String(), nullable=True),
        schema="user_projects",
    )

    # set `basic` infrastructure_type
    op.execute(
        sa.text("""UPDATE service_types_dict SET infrastructure_type = 'basic' WHERE infrastructure_type IS NULL""")
    )
    op.alter_column("service_types_dict", "infrastructure_type", nullable=False)
