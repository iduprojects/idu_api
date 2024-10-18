# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""indicator comments target profiles

Revision ID: 131faa7cad9c
Revises: c64e20abd7c2
Create Date: 2024-10-01 13:10:39.039437

"""
from textwrap import dedent
from typing import Callable, Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy import func

func: Callable

# revision identifiers, used by Alembic.
revision: str = "131faa7cad9c"
down_revision: Union[str, None] = "c64e20abd7c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns
    op.add_column("indicators_data", sa.Column("comment", sa.String(length=300), nullable=True), schema="user_projects")
    op.add_column(
        "territory_indicators_data",
        sa.Column("created_at", sa.TIMESTAMP(True), nullable=False, server_default=func.now()),
    )
    op.add_column(
        "territory_indicators_data",
        sa.Column("updated_at", sa.TIMESTAMP(True), nullable=False, server_default=func.now()),
    )

    # data
    op.execute(
        sa.text(
            dedent(
                """
                DELETE FROM user_projects.scenarios_data
                WHERE target_profile_id IS NOT NULL;
                """
            )
        )
    )
    op.execute(sa.text(dedent("""DELETE FROM user_projects.profiles_data""")))

    # constraints
    op.drop_constraint(
        "profiles_data_fk_target_profile_id__target_profiles_dict",
        "profiles_data",
        "foreignkey",
        schema="user_projects",
    )
    op.drop_constraint(
        "scenarios_data_fk_target_profile_id__target_profiles_dict",
        "scenarios_data",
        "foreignkey",
        schema="user_projects",
    )
    op.create_foreign_key(
        "profiles_data_fk_func_zone_type_id__func_zone_type_dict",
        "profiles_data",
        "functional_zone_types_dict",
        ["target_profile_id"],
        ["functional_zone_type_id"],
        source_schema="user_projects",
        referent_schema="public",
    )
    op.create_foreign_key(
        "scenarios_data_fk_func_zone_type_id__func_zone_type_dict",
        "scenarios_data",
        "functional_zone_types_dict",
        ["target_profile_id"],
        ["functional_zone_type_id"],
        source_schema="user_projects",
        referent_schema="public",
    )

    # tables
    op.drop_table("functional_zones_data", schema="user_projects")
    op.drop_table("target_profiles_dict", schema="user_projects")


def downgrade() -> None:
    # tables
    op.create_table(
        "functional_zones_data",
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("functional_zone_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["functional_zone_type_id"],
            ["functional_zone_types_dict.functional_zone_type_id"],
            name=op.f("functional_zones_data_fk_functional_zone_type_id__functional_zone_types_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["user_projects.scenarios_data.scenario_id"],
            name=op.f("functional_zones_data_fk_scenario_id__scenarios_data"),
            ondelete="CASCADE",
        ),
        schema="user_projects",
    )
    op.create_table(
        "target_profiles_dict",
        sa.Column(
            "target_profile_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.target_profile_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.PrimaryKeyConstraint("target_profile_id", name=op.f("target_profiles_dict_pk")),
        schema="user_projects",
    )

    # data
    op.execute(
        sa.text(
            dedent(
                """
                DELETE FROM user_projects.scenarios_data
                WHERE target_profile_id IS NOT NULL;
                """
            )
        )
    )
    op.execute(sa.text(dedent("""DELETE FROM user_projects.profiles_data""")))

    # constraints
    op.drop_constraint(
        "profiles_data_fk_func_zone_type_id__func_zone_type_dict",
        "profiles_data",
        "foreignkey",
        schema="user_projects",
    )
    op.drop_constraint(
        "scenarios_data_fk_func_zone_type_id__func_zone_type_dict",
        "scenarios_data",
        "foreignkey",
        schema="user_projects",
    )
    op.create_foreign_key(
        "profiles_data_fk_target_profile_id__target_profiles_dict",
        "profiles_data",
        "target_profiles_dict",
        ["target_profile_id"],
        ["target_profile_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )
    op.create_foreign_key(
        "scenarios_data_fk_target_profile_id__target_profiles_dict",
        "scenarios_data",
        "target_profiles_dict",
        ["target_profile_id"],
        ["target_profile_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )

    # columns
    op.drop_column("indicators_data", "comment", schema="user_projects")
    op.drop_column("territory_indicators_data", "created_at")
    op.drop_column("territory_indicators_data", "updated_at")
