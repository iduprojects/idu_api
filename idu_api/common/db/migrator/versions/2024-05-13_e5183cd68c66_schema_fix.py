# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""schema_fix

Revision ID: e5183cd68c66
Revises: aa0c57f0df82
Create Date: 2024-05-13 08:10:54.665929

"""
from textwrap import dedent
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5183cd68c66"
down_revision: Union[str, None] = "aa0c57f0df82"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # tables

    op.alter_column("functional_zones_data", "territory_id", existing_type=sa.INTEGER(), nullable=False)

    op.execute("ALTER TABLE indicators_dict RENAME name TO name_full")
    op.add_column("indicators_dict", sa.Column("name_short", sa.VARCHAR(length=200), nullable=True))
    op.execute("UPDATE indicators_dict SET name_short = name_full")

    op.drop_constraint("indicators_dict_name_key", "indicators_dict", type_="unique")
    op.create_unique_constraint(op.f("indicators_dict_name_full_key"), "indicators_dict", ["name_full"])

    op.alter_column("service_types_normatives_data", "service_type_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("service_types_normatives_data", "urban_function_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("service_types_normatives_data", "territory_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "territories_data",
        "geometry",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=False,
    )
    op.alter_column(
        "territories_data",
        "centre_point",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        type_=geoalchemy2.types.Geometry(
            geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
        ),
        nullable=False,
    )

    # helper functions

    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_set_centre_point()
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
                CREATE OR REPLACE FUNCTION public.trigger_check_normative_sensefulness()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NOT (
                        NEW.urban_function_id IS NULL AND NEW.service_type_id IS NOT NULL
                        OR
                        NEW.urban_function_id IS NOT NULL AND NEW.service_type_id IS NULL
                    ) THEN
                        RAISE EXCEPTION 'Invalid normative key fields!';
                    END IF;
                    IF NOT (
                        NEW.services_per_1000_normative IS NULL AND NEW.services_capacity_per_1000_normative IS NOT NULL
                        OR
                        NEW.services_per_1000_normative IS NOT NULL AND NEW.services_capacity_per_1000_normative IS NULL
                    ) THEN
                        RAISE EXCEPTION 'Invalid normative values fields!';
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
                CREATE OR REPLACE FUNCTION public.trigger_validate_geometry_not_point()
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
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_validate_geometry()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF TG_OP = 'UPDATE' AND OLD.geometry = NEW.geometry THEN
                        return NEW;
                    END IF;
                    IF NOT (ST_GeometryType(NEW.geometry) IN ('ST_Point', 'ST_Polygon', 'ST_MultiPolygon')) THEN
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
                CREATE OR REPLACE FUNCTION public.trigger_validate_date_value_correctness()
                RETURNS TRIGGER
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.date_type = 'year'::date_field_type THEN
                        IF DATE_TRUNC('year', NEW.date_value) <> NEW.date_type THEN
                            RAISE EXCEPTION 'Invalid year date_value!';
                        END IF;
                    ELSEIF NEW.date_type = 'half_year'::date_field_type THEN
                        IF NOT (EXTRACT(month FROM NEW.date_value) IN (1, 7) AND EXTRACT(day FROM NEW.date_value) = 1) THEN
                            RAISE EXCEPTION 'Invalid half_year date_value!';
                        END IF;
                    ELSEIF NEW.date_type = 'quarter'::date_field_type THEN
                        IF NOT (EXTRACT(month FROM NEW.date_value) IN (1, 4, 7, 10) AND EXTRACT(day FROM NEW.date_value) = 1) THEN
                            RAISE EXCEPTION 'Invalid quarter date_value!';
                        END IF;
                    ELSEIF NEW.date_type = 'month'::date_field_type THEN
                        IF DATE_TRUNC('month', NEW.date_value) <> NEW.date_type THEN
                            RAISE EXCEPTION 'Invalid month date_value!';
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $function$;
                """
            )
        )
    )

    # triggers

    for trigger_name, table_name, procedure_name in [
        (
            "check_geometry_correctness_trigger",
            "public.territories_data",
            "public.trigger_validate_geometry_not_point",
        ),
        (
            "check_geometry_correctness_trigger",
            "public.object_geometries_data",
            "public.trigger_validate_geometry",
        ),
        (
            "set_center_point_trigger_trigger",
            "public.object_geometries_data",
            "public.trigger_set_centre_point",
        ),
        (
            "check_normative_correctness_trigger",
            "public.service_types_normatives_data",
            "public.trigger_check_normative_sensefulness",
        ),
        (
            "check_geometry_correctness_trigger",
            "public.functional_zones_data",
            "public.trigger_validate_geometry_not_point",
        ),
        (
            "check_date_value_correctness",
            "public.territory_indicators_data",
            "public.trigger_validate_date_value_correctness",
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
    # triggers

    for trigger_name, table_name in [
        ("check_geometry_correctness_trigger", "public.territories_data"),
        ("check_geometry_correctness_trigger", "public.object_geometries_data"),
        ("set_center_point_trigger_trigger", "public.object_geometries_data"),
        ("check_normative_correctness_trigger", "public.service_types_normatives_data"),
        ("check_geometry_correctness_trigger", "public.functional_zones_data"),
        ("check_date_value_correctness", "public.territory_indicators_data"),
    ]:
        op.execute(sa.text(f"DROP TRIGGER {trigger_name} ON {table_name}"))

    # functions

    for function_name in [
        "public.trigger_validate_geometry_not_point",
        "public.trigger_validate_geometry",
        "public.trigger_set_centre_point",
        "public.trigger_check_normative_sensefulness",
        "public.trigger_validate_geometry_not_point",
        "public.trigger_validate_date_value_correctness",
    ]:
        op.execute(sa.text(f"DROP FUNCTION {function_name}"))

    # tables

    op.alter_column(
        "territories_data",
        "centre_point",
        existing_type=geoalchemy2.types.Geometry(
            geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
        ),
        type_=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=True,
    )
    op.alter_column(
        "territories_data",
        "geometry",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=True,
    )
    op.alter_column("service_types_normatives_data", "territory_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("service_types_normatives_data", "urban_function_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("service_types_normatives_data", "service_type_id", existing_type=sa.INTEGER(), nullable=True)

    op.drop_constraint(op.f("indicators_dict_name_short_key"), "indicators_dict", type_="unique")
    op.drop_constraint(op.f("indicators_dict_name_full_key"), "indicators_dict", type_="unique")
    op.execute("ALTER TABLE indicators_dict RENAME name_full TO name")
    op.create_unique_constraint("indicators_dict_name_key", "indicators_dict", ["name"])
    op.drop_column("indicators_dict", "name_short", existing_type=sa.VARCHAR(length=200), nullable=True)

    op.alter_column("functional_zones_data", "territory_id", existing_type=sa.INTEGER(), nullable=True)
