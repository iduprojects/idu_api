# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""update scenarios data table, create projects_phases_data

Revision ID: 7d75e7a728f1
Revises: 676ef0e8411b
Create Date: 2025-06-16 14:21:49.576063

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7d75e7a728f1"
down_revision: Union[str, None] = "676ef0e8411b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


scenario_phase_enum = sa.Enum(
    "investment",
    "pre_design",
    "design",
    "construction",
    "operation",
    "decommission",
    name="scenario_phase",
    schema="user_projects",
)


def upgrade() -> None:

    # drop columns phase, phase_percentage
    op.drop_column("scenarios_data", "phase", schema="user_projects")
    op.drop_column("scenarios_data", "phase_percentage", schema="user_projects")

    # drop enum scenarios phase
    scenario_phase_enum.drop(op.get_bind(), checkfirst=True)

    # create table projects_phases_data
    op.create_table(
        "projects_phases_data",
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("actual_start_date", sa.Date(), server_default=sa.null()),
        sa.Column("actual_end_date", sa.Date(), server_default=sa.null()),
        sa.Column("planned_start_date", sa.Date(), server_default=sa.null()),
        sa.Column("planned_end_date", sa.Date(), server_default=sa.null()),
        sa.Column("investment", sa.Float(), server_default=sa.text("0"), nullable=False),
        sa.Column("pre_design", sa.Float(), server_default=sa.text("0"), nullable=False),
        sa.Column("design", sa.Float(), server_default=sa.text("0"), nullable=False),
        sa.Column("construction", sa.Float(), server_default=sa.text("0"), nullable=False),
        sa.Column("operation", sa.Float(), server_default=sa.text("0"), nullable=False),
        sa.Column("decommission", sa.Float(), server_default=sa.text("0"), nullable=False),
        sa.PrimaryKeyConstraint("project_id", name=op.f("projects_phases_data_pk")),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["user_projects.projects_data.project_id"],
            name=op.f("projects_phases_data_fk_user_projects_projects_data"),
            ondelete="CASCADE",
        ),
        schema="user_projects",
    )

    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION user_projects.trigger_insert_into_projects_phases_data()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.is_regional = FALSE THEN
                        INSERT INTO user_projects.projects_phases_data (project_id)
                        VALUES (NEW.project_id);
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
			   """
            )
        )
    )

    op.execute(
        sa.text(
            dedent(
                """
	            CREATE TRIGGER insert_line_in_projects_phases_data
	            AFTER INSERT ON user_projects.projects_data
	            FOR EACH ROW
	            EXECUTE FUNCTION user_projects.trigger_insert_into_projects_phases_data();
	            """
            )
        )
    )

    op.execute(
        sa.text(
            dedent(
                """
                INSERT INTO user_projects.projects_phases_data (project_id)
                SELECT project_id
                FROM user_projects.projects_data
                WHERE is_regional = FALSE;
                """
            )
        )
    )


def downgrade() -> None:

    # revert dropping phase, phase_percentage
    scenario_phase_enum.create(op.get_bind(), checkfirst=True)
    op.add_column("scenarios_data", sa.Column("phase", scenario_phase_enum, nullable=True), schema="user_projects")
    op.add_column(
        "scenarios_data", sa.Column("phase_percentage", sa.Float(precision=53), nullable=True), schema="user_projects"
    )

    # revert creating table projects_phases_data
    op.drop_table("projects_phases_data", schema="user_projects")
    op.execute("DROP TRIGGER IF EXISTS insert_line_in_projects_phases_data ON user_projects.projects_data")
    op.execute("DROP FUNCTION IF EXISTS user_projects.trigger_insert_into_projects_phases_data()")
