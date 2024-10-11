# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""osm id infrastructure types dict

Revision ID: 8de8ea793205
Revises: c15cfb20553f
Create Date: 2024-10-10 21:01:54.136299

"""
from tarfile import TruncatedHeaderError
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8de8ea793205"
down_revision: Union[str, None] = "c15cfb20553f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add column `osm_id` to object geometries data
    op.add_column("object_geometries_data", sa.Column("osm_id", sa.String(length=20), nullable=True))
    op.add_column(
        "object_geometries_data", sa.Column("osm_id", sa.String(length=20), nullable=True), schema="user_projects"
    )

    # add column `infrastructure_type` to service types dict
    op.execute(sa.text("CREATE TYPE infrastructure_type AS ENUM ('basic', 'additional', 'comfort')"))
    op.add_column(
        "service_types_dict",
        sa.Column(
            "infrastructure_type",
            sa.Enum("basic", "additional", "comfort", name="infrastructure_type"),
            nullable=True,
        ),
    )
    op.execute(
        sa.text("""UPDATE service_types_dict SET infrastructure_type = 'basic' WHERE infrastructure_type IS NULL""")
    )
    op.alter_column("service_types_dict", "infrastructure_type", nullable=False)


def downgrade() -> None:
    # drop columns
    op.drop_column("object_geometries_data", "osm_id")
    op.drop_column("object_geometries_data", "osm_id", schema="user_projects")
    op.drop_column("service_types_dict", "infrastructure_type")

    # drop enum type
    op.execute(sa.text("DROP TYPE infrastructure_type"))
