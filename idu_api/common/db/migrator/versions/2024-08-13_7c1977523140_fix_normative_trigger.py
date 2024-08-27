# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix normative trigger

Revision ID: 7c1977523140
Revises: 958163828b45
Create Date: 2024-08-13 13:56:54.146830

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7c1977523140"
down_revision: Union[str, None] = "958163828b45"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # delete trigger
    for trigger_name, table_name in [
        ("check_normative_correctness_trigger", "public.service_types_normatives_data"),
    ]:
        op.execute(sa.text(f"DROP TRIGGER {trigger_name} ON {table_name}"))

    # delete function
    for function_name in [
        "public.trigger_check_normative_sensefulness",
    ]:
        op.execute(sa.text(f"DROP FUNCTION {function_name}"))

    # create function
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
                        NEW.radius_availability_meters IS NULL AND NEW.time_availability_minutes IS NOT NULL
                        OR
                        NEW.radius_availability_meters IS NOT NULL AND NEW.time_availability_minutes IS NULL
                    ) THEN
                        RAISE EXCEPTION 'Invalid normative values fields!';
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

    # create trigger
    for trigger_name, table_name, procedure_name in [
        (
            "check_normative_correctness_trigger",
            "public.service_types_normatives_data",
            "public.trigger_check_normative_sensefulness",
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
    # delete trigger
    for trigger_name, table_name in [
        ("check_normative_correctness_trigger", "public.service_types_normatives_data"),
    ]:
        op.execute(sa.text(f"DROP TRIGGER {trigger_name} ON {table_name}"))

    # delete function
    for function_name in [
        "public.trigger_check_normative_sensefulness",
    ]:
        op.execute(sa.text(f"DROP FUNCTION {function_name}"))

    # create function
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

    # create trigger
    for trigger_name, table_name, procedure_name in [
        (
            "check_normative_correctness_trigger",
            "public.service_types_normatives_data",
            "public.trigger_check_normative_sensefulness",
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
