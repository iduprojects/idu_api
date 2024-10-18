# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add profiles_reclamation_data

Revision ID: c4f2f39531aa
Revises: 87a89b9d035f
Create Date: 2024-10-03 17:22:41.452522

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "c4f2f39531aa"
down_revision: Union[str, None] = "87a89b9d035f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "profiles_reclamation_data",
        sa.Column("source_profile_id", sa.Integer(), nullable=False),
        sa.Column("target_profile_id", sa.Integer(), nullable=False),
        sa.Column("technical_price", sa.Float(), nullable=False),
        sa.Column("technical_time", sa.Float(), nullable=False),
        sa.Column("biological_price", sa.Float(), nullable=False),
        sa.Column("biological_time", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_profile_id"],
            ["functional_zone_types_dict.functional_zone_type_id"],
            name=op.f("profiles_reclamation_data_fk_source_profile_id__functional_zone_types_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["target_profile_id"],
            ["functional_zone_types_dict.functional_zone_type_id"],
            name=op.f("profiles_reclamation_data_fk_target_profile_id__functional_zone_types_dict"),
        ),
        sa.PrimaryKeyConstraint("source_profile_id", "target_profile_id", name=op.f("profiles_reclamation_data_pk")),
    )


def downgrade() -> None:
    op.drop_table("profiles_reclamation_data")
