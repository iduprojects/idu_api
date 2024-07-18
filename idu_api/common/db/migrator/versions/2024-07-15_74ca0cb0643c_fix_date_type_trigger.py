# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix date_type trigger

Revision ID: 74ca0cb0643c
Revises: db3b9d3503f4
Create Date: 2024-07-15 14:40:42.267220

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "74ca0cb0643c"
down_revision: Union[str, None] = "db3b9d3503f4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # drop trigger

    for trigger_name, table_name in [
        ("check_date_value_correctness", "public.territory_indicators_data"),
    ]:
        op.execute(sa.text(f"DROP TRIGGER {trigger_name} ON {table_name}"))

    for function_name in [
        "public.trigger_validate_date_value_correctness",
    ]:
        op.execute(sa.text(f"DROP FUNCTION {function_name}"))

    # add trigger

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
                        IF DATE_TRUNC('year', NEW.date_value) <> NEW.date_value THEN
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
                        IF DATE_TRUNC('month', NEW.date_value) <> NEW.date_value THEN
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

    for trigger_name, table_name, procedure_name in [
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
    # drop trigger

    for trigger_name, table_name in [
        ("check_date_value_correctness", "public.territory_indicators_data"),
    ]:
        op.execute(sa.text(f"DROP TRIGGER {trigger_name} ON {table_name}"))

    for function_name in [
        "public.trigger_validate_date_value_correctness",
    ]:
        op.execute(sa.text(f"DROP FUNCTION {function_name}"))

    # add trigger

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

    for trigger_name, table_name, procedure_name in [
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
