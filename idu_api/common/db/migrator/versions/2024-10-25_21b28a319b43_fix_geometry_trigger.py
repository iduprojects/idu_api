# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix geometry trigger

Revision ID: 21b28a319b43
Revises: 5876eee7dce0
Create Date: 2024-10-25 13:38:19.703511

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "21b28a319b43"
down_revision: Union[str, None] = "5876eee7dce0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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


def downgrade() -> None:
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
