# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""reg scenarios

Revision ID: 64ddf2ff8424
Revises: 21b28a319b43
Create Date: 2024-10-28 11:56:03.414307

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "64ddf2ff8424"
down_revision: Union[str, None] = "21b28a319b43"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # fix `projects_data` and `projects_territory_data`
    op.add_column(
        "projects_data",
        sa.Column("is_regional", sa.Boolean(), nullable=True),
        schema="user_projects",
    )
    op.execute(
        sa.text(
            dedent(
                """
                UPDATE user_projects.projects_data
                SET is_regional = false
                WHERE is_regional IS NULL;
                """
            )
        )
    )
    op.alter_column("projects_data", "is_regional", nullable=False, schema="user_projects")
    op.add_column(
        "projects_territory_data",
        sa.Column("project_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.execute(
        sa.text(
            dedent(
                """
                UPDATE user_projects.projects_territory_data AS ptd
                SET project_id = pd.project_id
                FROM user_projects.projects_data AS pd
                WHERE ptd.project_territory_id = pd.project_territory_id;
                """
            )
        )
    )
    op.execute(sa.text("""DELETE FROM user_projects.projects_territory_data WHERE project_id IS NULL"""))
    op.alter_column("projects_territory_data", "project_id", nullable=False, schema="user_projects")
    op.create_foreign_key(
        "projects_territory_data_fk_project_id__projects_data",
        "projects_territory_data",
        "projects_data",
        ["project_id"],
        ["project_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
        ondelete="CASCADE",
    )
    op.drop_constraint(
        "projects_data_fk_project_territory_id__projects_territory_data", "projects_data", schema="user_projects"
    )
    op.drop_column("projects_data", "project_territory_id", schema="user_projects")
    op.add_column(
        "projects_data",
        sa.Column("territory_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.execute(
        sa.text(
            dedent(
                """
                UPDATE user_projects.projects_data AS pd
                SET territory_id = (
                    SELECT td.territory_id
                    FROM territories_data AS td
                    JOIN user_projects.projects_territory_data AS ptd ON ST_Intersects(td.geometry, ptd.geometry)
                    WHERE td.level = 2 AND ptd.project_id = pd.project_id
                    LIMIT 1
                );
                """
            )
        )
    )
    op.create_foreign_key(
        "projects_data_fk_territory_id__territories_data",
        "projects_data",
        "territories_data",
        ["territory_id"],
        ["territory_id"],
        source_schema="user_projects",
        referent_schema="public",
    )
    op.alter_column("projects_data", "territory_id", nullable=False, schema="user_projects")
    op.drop_constraint(
        "projects_territory_data_fk_parent_territory_id__project_5a39",
        "projects_territory_data",
        schema="user_projects",
    )
    op.drop_column("projects_territory_data", "parent_territory_id", schema="user_projects")
    op.alter_column(
        "projects_data",
        "description",
        existing_type=sa.String(length=600),
        type_=sa.String(4096),
        schema="user_projects",
    )

    # fix `scenarios_data`
    op.add_column(
        "scenarios_data",
        sa.Column("is_based", sa.Boolean(), nullable=True),
        schema="user_projects",
    )
    op.execute("""UPDATE user_projects.scenarios_data SET is_based = false WHERE is_based IS NULL""")
    op.alter_column("scenarios_data", "is_based", nullable=False, schema="user_projects")
    op.add_column(
        "scenarios_data",
        sa.Column("parent_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "scenarios_data_fk_parent_id__scenarios_data",
        "scenarios_data",
        "scenarios_data",
        ["parent_id"],
        ["scenario_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
        ondelete="CASCADE",
    )

    # fix 'urban_objects_data`
    op.add_column(
        "urban_objects_data",
        sa.Column(
            "public_urban_object_id",
            sa.Integer(),
            nullable=True,
        ),
        schema="user_projects",
    )
    op.create_foreign_key(
        "urban_objects_data_fk_public_urban_object_id__uod",
        "urban_objects_data",
        "urban_objects_data",
        ["public_urban_object_id"],
        ["urban_object_id"],
        source_schema="user_projects",
        referent_schema="public",
        ondelete="CASCADE",
    )
    op.alter_column("urban_objects_data", "physical_object_id", nullable=True, schema="user_projects")
    op.alter_column("urban_objects_data", "object_geometry_id", nullable=True, schema="user_projects")

    # fix `indicators_data`
    op.alter_column(
        "indicators_data", "comment", existing_type=sa.String(length=300), type_=sa.String(2048), schema="user_projects"
    )
    op.drop_constraint("indicators_data_pk", "indicators_data", schema="user_projects")
    op.drop_constraint("indicators_data_fk_scenario_id__scenarios_data", "indicators_data", schema="user_projects")
    op.drop_column("indicators_data", "date_type", schema="user_projects")
    op.drop_column("indicators_data", "date_value", schema="user_projects")
    op.drop_column("indicators_data", "value_type", schema="user_projects")
    op.alter_column("indicators_data", "information_source", nullable=True, schema="user_projects")
    op.add_column(
        "indicators_data",
        sa.Column("territory_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.add_column(
        "indicators_data",
        sa.Column("project_territory_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "indicators_data_fk_territory_id__territories_data",
        "indicators_data",
        "territories_data",
        ["territory_id"],
        ["territory_id"],
        source_schema="user_projects",
        referent_schema="public",
    )
    op.create_foreign_key(
        "indicators_data_fk_project_territory_id__ptd",
        "indicators_data",
        "projects_territory_data",
        ["project_territory_id"],
        ["project_territory_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )
    op.create_foreign_key(
        "indicators_data_fk_scenario_id__scenarios_data",
        "indicators_data",
        "scenarios_data",
        ["scenario_id"],
        ["scenario_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
        ondelete="CASCADE",
    )
    op.add_column(
        "indicators_data",
        sa.Column("indicator_value_id", sa.Integer(), nullable=False),
        schema="user_projects",
    )
    op.create_primary_key("indicators_data_pk", "indicators_data", ["indicator_value_id"], schema="user_projects")


def downgrade() -> None:
    # reverse changes in `indicators_data`
    op.drop_constraint("indicators_data_pk", "indicators_data", schema="user_projects")
    op.drop_constraint("indicators_data_fk_scenario_id__scenarios_data", "indicators_data", schema="user_projects")
    op.drop_column("indicators_data", "indicator_value_id", schema="user_projects")
    op.drop_constraint("indicators_data_fk_project_territory_id__ptd", "indicators_data", schema="user_projects")
    op.drop_constraint("indicators_data_fk_territory_id__territories_data", "indicators_data", schema="user_projects")
    op.drop_column("indicators_data", "project_territory_id", schema="user_projects")
    op.drop_column("indicators_data", "territory_id", schema="user_projects")
    op.execute(sa.text("""DELETE FROM user_projects.indicators_data"""))
    op.alter_column("indicators_data", "information_source", nullable=False, schema="user_projects")
    op.add_column(
        "indicators_data",
        sa.Column("value_type", sa.Enum(name="indicator_value_type", inherit_schema=True), nullable=False),
        schema="user_projects",
    )
    op.add_column("indicators_data", sa.Column("date_value", sa.Date(), nullable=False), schema="user_projects")
    op.add_column(
        "indicators_data",
        sa.Column(
            "date_type",
            sa.Enum(
                "year",
                "half_year",
                "quarter",
                "month",
                "day",
                name="date_field_type",
            ),
            nullable=False,
        ),
        schema="user_projects",
    )
    op.create_foreign_key(
        "indicators_data_fk_scenario_id__scenarios_data",
        "indicators_data",
        "scenarios_data",
        ["scenario_id"],
        ["scenario_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )
    op.create_primary_key(
        "indicators_data_pk",
        "indicators_data",
        ["indicator_id", "scenario_id", "date_type", "date_value", "value_type", "information_source"],
        schema="user_projects",
    )
    op.alter_column(
        "indicators_data", "comment", existing_type=sa.String(length=2048), type_=sa.String(300), schema="user_projects"
    )

    # reverse changes in `urban_objects_data`
    op.execute(
        sa.text(
            """
            DELETE FROM user_projects.urban_objects_data 
            WHERE object_geometry_id IS NULL OR physical_object_id IS NULL
            """
        )
    )
    op.alter_column("urban_objects_data", "object_geometry_id", nullable=False, schema="user_projects")
    op.alter_column("urban_objects_data", "physical_object_id", nullable=False, schema="user_projects")
    op.drop_constraint(
        "urban_objects_data_fk_public_urban_object_id__uod", "urban_objects_data", schema="user_projects"
    )
    op.drop_column("urban_objects_data", "public_urban_object_id", schema="user_projects")

    # reverse changes in `scenarios_data`
    op.drop_constraint("scenarios_data_fk_parent_id__scenarios_data", "scenarios_data", schema="user_projects")
    op.drop_column("scenarios_data", "parent_id", schema="user_projects")
    op.drop_column("scenarios_data", "is_based", schema="user_projects")

    # reverse changes in `projects_territory_data` and `projects_data`
    op.add_column(
        "projects_territory_data",
        sa.Column("parent_territory_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.execute(
        sa.text(
            dedent(
                """
                UPDATE user_projects.projects_territory_data AS ptd
                SET parent_territory_id = (
                    SELECT td.territory_id
                    FROM territories_data AS td
                    WHERE ST_Covers(td.geometry, ptd.geometry)
                    ORDER BY td.level DESC
                    LIMIT 1
                );                
                """
            )
        )
    )
    op.alter_column("projects_territory_data", "parent_territory_id", nullable=False, schema="user_projects")
    op.create_foreign_key(
        "projects_territory_data_fk_parent_territory_id__project_5a39",
        "projects_territory_data",
        "territories_data",
        ["parent_territory_id"],
        ["territory_id"],
        source_schema="user_projects",
        referent_schema="public",
    )
    op.drop_constraint("projects_data_fk_territory_id__territories_data", "projects_data", schema="user_projects")
    op.drop_column("projects_data", "territory_id", schema="user_projects")
    op.add_column(
        "projects_data",
        sa.Column("project_territory_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.execute(
        sa.text(
            dedent(
                """
                UPDATE user_projects.projects_data AS pd
                SET project_territory_id = ptd.project_territory_id
                FROM user_projects.projects_territory_data AS ptd
                WHERE ptd.project_id = pd.project_id;                
                """
            )
        )
    )
    op.create_foreign_key(
        "projects_data_fk_project_territory_id__projects_territory_data",
        "projects_data",
        "projects_territory_data",
        ["project_territory_id"],
        ["project_territory_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
        ondelete="CASCADE",
    )
    op.alter_column("projects_data", "project_territory_id", nullable=False, schema="user_projects")
    op.drop_column("projects_data", "is_regional", schema="user_projects")
    op.drop_constraint(
        "projects_territory_data_fk_project_id__projects_data", "projects_territory_data", schema="user_projects"
    )
    op.drop_column("projects_territory_data", "project_id", schema="user_projects")
    op.alter_column(
        "projects_data",
        "description",
        existing_type=sa.String(length=4096),
        type_=sa.String(600),
        schema="user_projects",
    )
