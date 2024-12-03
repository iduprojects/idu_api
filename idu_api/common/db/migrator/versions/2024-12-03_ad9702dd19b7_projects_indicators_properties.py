# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""projects indicators properties

Revision ID: ad9702dd19b7
Revises: 26917509c474
Create Date: 2024-12-03 13:04:51.575912

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "ad9702dd19b7"
down_revision: Union[str, None] = "26917509c474"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "indicators_data",
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        schema="user_projects",
    )


def downgrade() -> None:
    op.drop_column("indicators_data", "properties", schema="user_projects")
