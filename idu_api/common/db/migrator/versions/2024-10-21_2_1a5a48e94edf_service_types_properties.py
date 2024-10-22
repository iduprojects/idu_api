# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""service types properties

Revision ID: 1a5a48e94edf
Revises: fba380fb8c8a
Create Date: 2024-10-21 14:02:54.683056

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "1a5a48e94edf"
down_revision: Union[str, None] = "fba380fb8c8a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add column `properties` to `service_types_dict`
    op.add_column(
        "service_types_dict",
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
    )


def downgrade() -> None:
    # drop column `properties` from `service_types_dict`
    op.drop_column("service_types_dict", "properties")
