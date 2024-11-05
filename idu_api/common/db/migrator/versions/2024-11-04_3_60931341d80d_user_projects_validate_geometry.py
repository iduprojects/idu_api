# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""user projects validate geometry

Revision ID: 60931341d80d
Revises: 24a0fcf2e733
Create Date: 2024-11-04 17:24:29.518656

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "60931341d80d"
down_revision: Union[str, None] = "24a0fcf2e733"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add validate geometry trigger for `user_projects` schema
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION user_projects.trigger_set_centre_point()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.centre_point is NULL THEN
                        NEW.centre_point = ST_Centroid(NEW.geometry);
                    END IF;

                    RETURN NEW;
                END;
                $function$;
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION user_projects.trigger_validate_geometry()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF TG_OP = 'UPDATE' AND OLD.geometry = NEW.geometry THEN
                        return NEW;
                    END IF;
                    IF NOT (ST_GeometryType(NEW.geometry)
                    IN ('ST_Point', 'ST_Polygon', 'ST_MultiPolygon', 'ST_LineString', 'ST_MultiLineString')) THEN
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
                $function$;
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION user_projects.trigger_validate_geometry_not_point()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF TG_OP = 'UPDATE' AND OLD.geometry = NEW.geometry THEN
                        return NEW;
                    END IF;
                    IF NOT (ST_GeometryType(NEW.geometry) IN ('ST_Polygon', 'ST_MultiPolygon')) THEN
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
                $function$;
                """
            )
        )
    )
    for trigger_name, table_name, procedure_name in [
        (
            "check_geometry_correctness_trigger",
            "user_projects.object_geometries_data",
            "user_projects.trigger_validate_geometry",
        ),
        (
            "set_center_point_trigger",
            "user_projects.object_geometries_data",
            "user_projects.trigger_set_centre_point",
        ),
        (
            "check_geometry_correctness_trigger",
            "user_projects.profiles_data",
            "user_projects.trigger_validate_geometry_not_point",
        ),
        (
            "check_geometry_correctness_trigger",
            "user_projects.projects_territory_data",
            "user_projects.trigger_validate_geometry_not_point",
        ),
        (
            "set_center_point_trigger",
            "user_projects.projects_territory_data",
            "user_projects.trigger_set_centre_point",
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


def downgrade() -> None:
    # drop triggers
    for trigger_name, table_name in [
        ("check_geometry_correctness_trigger", "user_projects.object_geometries_data"),
        ("set_center_point_trigger", "user_projects.object_geometries_data"),
        ("check_geometry_correctness_trigger", "user_projects.profiles_data"),
        ("check_geometry_correctness_trigger", "user_projects.projects_territory_data"),
        ("set_center_point_trigger", "user_projects.projects_territory_data"),
    ]:
        op.execute(
            sa.text(
                f"""
                DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};
                """
            )
        )

    # drop functions
    op.execute("DROP FUNCTION IF EXISTS user_projects.trigger_set_centre_point;")
    op.execute("DROP FUNCTION IF EXISTS user_projects.trigger_validate_geometry;")
    op.execute("DROP FUNCTION IF EXISTS user_projects.trigger_validate_geometry_not_point;")
