# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""drop roads

Revision ID: 8a529b789a12
Revises: 4f32c4d6d04b
Create Date: 2024-11-06 14:40:59.276762

"""
from textwrap import dedent
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "8a529b789a12"
down_revision: Union[str, None] = "4f32c4d6d04b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # drop triggers
    for trigger_name, table_name in [
        ("check_line_geometry_correctness_trigger", "public.roads_data"),
        ("check_line_geometry_correctness_trigger", "user_projects.roads_data"),
    ]:
        op.execute(
            sa.text(
                f"""
                DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};
                """
            )
        )

    # drop functions
    op.execute("DROP FUNCTION IF EXISTS public.trigger_validate_line_geometry;")

    # drop tables
    op.drop_table("roads_data", schema="user_projects")
    op.drop_table("roads_data", schema="public")

    # drop types
    op.execute(sa.text("DROP TYPE public.road_type"))
    op.execute(sa.text("DROP TYPE user_projects.road_type"))

    # drop sequences
    op.execute(sa.schema.DropSequence(sa.Sequence("roads_data_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("roads_data_id_seq")))


def downgrade() -> None:
    # add table `public.roads_data`
    op.execute(sa.schema.CreateSequence(sa.Sequence("roads_data_id_seq")))
    op.create_table(
        "roads_data",
        sa.Column("road_id", sa.Integer(), server_default=sa.text("nextval('roads_data_id_seq')"), nullable=False),
        sa.Column("territory_id", sa.Integer(), nullable=False),
        sa.Column(
            "road_type", sa.Enum("federal", "regional", "local", name="road_type"), default="local", nullable=False
        ),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("roads_data_fk_territory_id__territories_data"),
        ),
        sa.PrimaryKeyConstraint("road_id", name=op.f("roads_data_pk")),
    )

    # add table `user_projects.roads_data`
    op.execute(sa.schema.CreateSequence(sa.Sequence("roads_data_id_seq", schema="user_projects")))
    op.create_table(
        "roads_data",
        sa.Column(
            "road_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.roads_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("public_road_id", sa.Integer(), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), default=False, nullable=False),
        sa.Column("road_type", sa.Enum(name="road_type", inherit_schema=True), nullable=True),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=True,
        ),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=True
        ),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["user_projects.scenarios_data.scenario_id"],
            name=op.f("roads_data_fk_scenario_id__scenarios_data"),
        ),
        sa.ForeignKeyConstraint(
            ["public_road_id"],
            ["roads_data.road_id"],
            name=op.f("roads_data_fk_public_road_id__roads_data"),
        ),
        sa.PrimaryKeyConstraint("road_id", name=op.f("projects_roads_data_pk")),
        schema="user_projects",
    )

    # add a trigger to ensure the geometry is either LineString or MultiLineString
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_validate_line_geometry()
                 RETURNS trigger
                 LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF TG_OP = 'UPDATE' AND OLD.geometry = NEW.geometry THEN
                        return NEW;
                    END IF;
                    IF NOT (ST_GeometryType(NEW.geometry) IN ('ST_LineString', 'ST_MultiLineString')) THEN
                        RAISE EXCEPTION 'Invalid geometry type!';
                    END IF;

                    IF NOT ST_IsValid(NEW.geometry) THEN
                        RAISE EXCEPTION 'Invalid geometry!';
                    END IF;

                    IF ST_IsEmpty(NEW.geometry) THEN
                        RAISE EXCEPTION 'Empty geometry!';
                    END IF;

                    RETURN NEW;
                END;
                $function$
                ;
                """
            )
        )
    )

    for trigger_name, table_name, procedure_name in [
        ("check_line_geometry_correctness_trigger", "public.roads_data", "public.trigger_validate_line_geometry"),
        (
            "check_line_geometry_correctness_trigger",
            "user_projects.roads_data",
            "public.trigger_validate_line_geometry",
        ),
    ]:
        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE TRIGGER {trigger_name}
                    BEFORE INSERT OR UPDATE ON {table_name}
                    FOR EACH ROW
                    EXECUTE PROCEDURE {procedure_name}();
                    """
                )
            )
        )
