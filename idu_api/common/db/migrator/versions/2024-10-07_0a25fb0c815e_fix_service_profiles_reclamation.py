# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix service profiles reclamation

Revision ID: 0a25fb0c815e
Revises: c4f2f39531aa
Create Date: 2024-10-07 13:55:53.346354

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0a25fb0c815e"
down_revision: Union[str, None] = "c4f2f39531aa"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # fix service name column
    op.alter_column("services_data", "name", nullable=True, schema="user_projects")

    # fix profiles reclamation table
    op.drop_table("profiles_reclamation_data")
    op.execute(sa.schema.CreateSequence(sa.Sequence("profile_reclamation_id_seq")))
    op.create_table(
        "profiles_reclamation_data",
        sa.Column(
            "profile_reclamation_id",
            sa.Integer,
            primary_key=True,
            server_default=sa.text("nextval('profile_reclamation_id_seq')"),
        ),
        sa.Column("source_profile_id", sa.Integer(), nullable=False),
        sa.Column("target_profile_id", sa.Integer(), nullable=False),
        sa.Column("territory_id", sa.Integer(), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("profiles_reclamation_data_fk_territory_id__territories_data"),
        ),
        sa.UniqueConstraint(
            "source_profile_id", "target_profile_id", "territory_id", name=op.f("profiles_reclamation_data_unique_key")
        ),
        sa.PrimaryKeyConstraint("profile_reclamation_id", name=op.f("profiles_reclamation_data_pk")),
    )


def downgrade() -> None:
    # return not-null service name column
    op.execute(
        sa.text(
            dedent(
                """
                DELETE FROM user_projects.services_data
                WHERE name IS NULL;
                """
            )
        )
    )
    op.alter_column("services_data", "name", nullable=False, schema="user_projects")

    # fix profiles reclamation table
    op.drop_table("profiles_reclamation_data")
    op.execute(sa.schema.DropSequence(sa.Sequence("profile_reclamation_id_seq")))
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
