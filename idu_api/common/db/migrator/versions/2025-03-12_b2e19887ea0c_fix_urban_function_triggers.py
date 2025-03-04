# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix urban function triggers

Revision ID: b2e19887ea0c
Revises: 973d04fdd152
Create Date: 2025-03-12 19:45:06.971881

"""
from textwrap import dedent
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b2e19887ea0c"
down_revision: Union[str, None] = "973d04fdd152"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # fix `urban_functions_dict` triggers (rename `parent_urban_function_id` to `parent_id`)

    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_urban_function_level_on_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    NEW.level := (
                        SELECT COALESCE(
                            (SELECT level
                            FROM urban_functions_dict
                            WHERE urban_function_id = NEW.parent_id), 0) + 1
                    );
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
                CREATE OR REPLACE FUNCTION public.trigger_update_urban_function_level_on_update()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.level <> OLD.level THEN
                        UPDATE urban_functions_dict 
                        SET level = NEW.level + 1           
                        WHERE parent_id = NEW.urban_function_id;
                    END IF;

                    IF NEW.parent_id <> OLD.parent_id OR 
                    NEW.parent_id IS NULL AND OLD.parent_id IS NOT NULL OR
                    NEW.parent_id IS NOT NULL AND OLD.parent_id IS NULL
                    THEN
                        UPDATE urban_functions_dict 
                        SET level = (
                            SELECT COALESCE(
                                (SELECT level
                                FROM urban_functions_dict
                                WHERE urban_function_id = NEW.parent_id), 0) + 1
                        )                  
                        WHERE urban_function_id = NEW.urban_function_id;
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
                -- Функция для генерации list_label
                CREATE OR REPLACE FUNCTION public.trigger_generate_urban_function_list_label()
                RETURNS TRIGGER AS $$
                DECLARE
                    parent_label TEXT;
                    next_number INT;
                BEGIN
                    -- Если это корневая функция (нет родительской функции)
                    IF NEW.parent_id IS NULL THEN
                        -- Считаем количество корневых функций и формируем list_label для нового элемента
                        NEW.list_label := (SELECT COUNT(*) + 1 FROM urban_functions_dict WHERE parent_id IS NULL)::TEXT;
                    ELSE
                        -- Получаем list_label родительской функции
                        SELECT list_label INTO parent_label
                        FROM urban_functions_dict
                        WHERE urban_function_id = NEW.parent_id;

                        NEW.list_label := parent_label || '.' || (SELECT COUNT(*) + 1 FROM urban_functions_dict WHERE parent_id = NEW.parent_id)::TEXT;
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
                CREATE OR REPLACE FUNCTION public.trigger_update_urban_function_list_label_on_update()
                RETURNS TRIGGER AS $$
                DECLARE
                    parent_label TEXT;
                    new_label TEXT;
                    sibling_label TEXT;
                    position INT := 0;
                BEGIN
                    -- Проверка на изменение list_label
                    IF NEW.list_label <> OLD.list_label THEN
                        -- Перенумеровываем дочерние функции для обновленной
                        FOR sibling_label IN
                            SELECT list_label FROM urban_functions_dict
                            WHERE parent_id = NEW.urban_function_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := NEW.list_label || '.' || position;

                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    END IF;

                    -- Проверка на изменение родительской функции
                    IF NEW.parent_id <> OLD.parent_id OR 
                    NEW.parent_id IS NULL AND OLD.parent_id IS NOT NULL OR
                    NEW.parent_id IS NOT NULL AND OLD.parent_id IS NULL
                    THEN
                        -- Если это корневая функция (нет родительской функции)
                        IF NEW.parent_id IS NULL THEN
                            new_label := (SELECT COUNT(*) FROM urban_functions_dict WHERE parent_id IS NULL)::TEXT;

                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE urban_function_id = NEW.urban_function_id;
                        ELSE
                            SELECT list_label INTO parent_label
                            FROM urban_functions_dict
                            WHERE urban_function_id = NEW.parent_id;

                            new_label := parent_label || '.' || (SELECT COUNT(*) FROM urban_functions_dict WHERE parent_id = NEW.parent_id)::TEXT;

                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE urban_function_id = NEW.urban_function_id;
                        END IF;

                        -- Если старая функция была корневой
                        IF OLD.parent_id IS NULL THEN
                            FOR sibling_label IN
                                SELECT list_label FROM urban_functions_dict
                                WHERE parent_id IS NULL
                                ORDER BY list_label
                            LOOP
                                position := position + 1;
                                new_label := position::TEXT;

                                UPDATE urban_functions_dict
                                SET list_label = new_label
                                WHERE list_label = sibling_label;
                            END LOOP;
                        ELSE
                            -- Получаем label родительской функции
                            SELECT list_label INTO parent_label
                            FROM urban_functions_dict
                            WHERE urban_function_id = OLD.parent_id;
                        END IF;

                        -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                        FOR sibling_label IN
                            SELECT list_label FROM urban_functions_dict
                            WHERE parent_id = OLD.parent_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := parent_label || '.' || position;

                            -- Обновляем list_label для функции на текущем уровне
                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
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
                CREATE OR REPLACE FUNCTION public.trigger_update_urban_function_sibling_labels_on_delete()
                RETURNS TRIGGER AS $$
                DECLARE
                    sibling_label TEXT;
                    new_label TEXT;
                    parent_label TEXT;
                    position INT := 0; -- Инициализируем переменную
                BEGIN
                    -- Если удаленная функция корневая, обновляем функции 1-го уровня
                    IF OLD.parent_id IS NULL THEN
                        FOR sibling_label IN
                            SELECT list_label FROM urban_functions_dict
                            WHERE parent_id IS NULL
                            ORDER BY list_label
                        LOOP
                            position := position + 1;

                            -- Новое значение list_label для корневой функции
                            new_label := position::TEXT;

                            -- Обновляем list_label для корневой функции
                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    ELSE
                        -- Получаем label родительской функции
                        SELECT list_label INTO parent_label
                        FROM urban_functions_dict
                        WHERE urban_function_id = OLD.parent_id;
                    END IF;

                    -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                    FOR sibling_label IN
                        SELECT list_label FROM urban_functions_dict
                        WHERE parent_id = OLD.parent_id
                        ORDER BY list_label
                    LOOP
                        position := position + 1;
                        new_label := parent_label || '.' || position;

                        -- Обновляем list_label для функции на текущем уровне
                        UPDATE urban_functions_dict
                        SET list_label = new_label
                        WHERE list_label = sibling_label;
                    END LOOP;

                    RETURN OLD;
                END;
                $$ LANGUAGE plpgsql;
                """
            )
        )
    )


def downgrade() -> None:
    # revert `urban_functions_dict` triggers
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_urban_function_level_on_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    NEW.level := (
                        SELECT COALESCE(
                            (SELECT level
                            FROM urban_functions_dict
                            WHERE urban_function_id = NEW.parent_urban_function_id), 0) + 1
                    );
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
                CREATE FUNCTION public.trigger_update_urban_function_level_on_update()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.level <> OLD.level THEN
                        UPDATE urban_functions_dict 
                        SET level = NEW.level + 1           
                        WHERE parent_urban_function_id = NEW.urban_function_id;
                    END IF;

                    IF NEW.parent_urban_function_id <> OLD.parent_urban_function_id OR 
                    NEW.parent_urban_function_id IS NULL AND OLD.parent_urban_function_id IS NOT NULL OR
                    NEW.parent_urban_function_id IS NOT NULL AND OLD.parent_urban_function_id IS NULL
                    THEN
                        UPDATE urban_functions_dict 
                        SET level = (
                            SELECT COALESCE(
                                (SELECT level
                                FROM urban_functions_dict
                                WHERE urban_function_id = NEW.parent_urban_function_id), 0) + 1
                        )                  
                        WHERE urban_function_id = NEW.urban_function_id;
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
                -- Функция для генерации list_label
                CREATE FUNCTION public.trigger_generate_urban_function_list_label()
                RETURNS TRIGGER AS $$
                DECLARE
                    parent_label TEXT;
                    next_number INT;
                BEGIN
                    -- Если это корневая функция (нет родительской функции)
                    IF NEW.parent_urban_function_id IS NULL THEN
                        -- Считаем количество корневых функций и формируем list_label для нового элемента
                        NEW.list_label := (SELECT COUNT(*) + 1 FROM urban_functions_dict WHERE parent_urban_function_id IS NULL)::TEXT;
                    ELSE
                        -- Получаем list_label родительской функции
                        SELECT list_label INTO parent_label
                        FROM urban_functions_dict
                        WHERE urban_function_id = NEW.parent_urban_function_id;

                        NEW.list_label := parent_label || '.' || (SELECT COUNT(*) + 1 FROM urban_functions_dict WHERE parent_urban_function_id = NEW.parent_urban_function_id)::TEXT;
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
                CREATE FUNCTION public.trigger_update_urban_function_list_label_on_update()
                RETURNS TRIGGER AS $$
                DECLARE
                    parent_label TEXT;
                    new_label TEXT;
                    sibling_label TEXT;
                    position INT := 0;
                BEGIN
                    -- Проверка на изменение list_label
                    IF NEW.list_label <> OLD.list_label THEN
                        -- Перенумеровываем дочерние функции для обновленной
                        FOR sibling_label IN
                            SELECT list_label FROM urban_functions_dict
                            WHERE parent_urban_function_id = NEW.urban_function_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := NEW.list_label || '.' || position;

                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    END IF;

                    -- Проверка на изменение родительской функции
                    IF NEW.parent_urban_function_id <> OLD.parent_urban_function_id OR 
                    NEW.parent_urban_function_id IS NULL AND OLD.parent_urban_function_id IS NOT NULL OR
                    NEW.parent_urban_function_id IS NOT NULL AND OLD.parent_urban_function_id IS NULL
                    THEN
                        -- Если это корневая функция (нет родительской функции)
                        IF NEW.parent_urban_function_id IS NULL THEN
                            new_label := (SELECT COUNT(*) FROM urban_functions_dict WHERE parent_urban_function_id IS NULL)::TEXT;

                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE urban_function_id = NEW.urban_function_id;
                        ELSE
                            SELECT list_label INTO parent_label
                            FROM urban_functions_dict
                            WHERE urban_function_id = NEW.parent_urban_function_id;

                            new_label := parent_label || '.' || (SELECT COUNT(*) FROM urban_functions_dict WHERE parent_urban_function_id = NEW.parent_urban_function_id)::TEXT;

                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE urban_function_id = NEW.urban_function_id;
                        END IF;

                        -- Если старая функция была корневой
                        IF OLD.parent_urban_function_id IS NULL THEN
                            FOR sibling_label IN
                                SELECT list_label FROM urban_functions_dict
                                WHERE parent_urban_function_id IS NULL
                                ORDER BY list_label
                            LOOP
                                position := position + 1;
                                new_label := position::TEXT;

                                UPDATE urban_functions_dict
                                SET list_label = new_label
                                WHERE list_label = sibling_label;
                            END LOOP;
                        ELSE
                            -- Получаем label родительской функции
                            SELECT list_label INTO parent_label
                            FROM urban_functions_dict
                            WHERE urban_function_id = OLD.parent_urban_function_id;
                        END IF;

                        -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                        FOR sibling_label IN
                            SELECT list_label FROM urban_functions_dict
                            WHERE parent_urban_function_id = OLD.parent_urban_function_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := parent_label || '.' || position;

                            -- Обновляем list_label для функции на текущем уровне
                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
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
                CREATE FUNCTION public.trigger_update_urban_function_sibling_labels_on_delete()
                RETURNS TRIGGER AS $$
                DECLARE
                    sibling_label TEXT;
                    new_label TEXT;
                    parent_label TEXT;
                    position INT := 0; -- Инициализируем переменную
                BEGIN
                    -- Если удаленная функция корневая, обновляем функции 1-го уровня
                    IF OLD.parent_urban_function_id IS NULL THEN
                        FOR sibling_label IN
                            SELECT list_label FROM urban_functions_dict
                            WHERE parent_urban_function_id IS NULL
                            ORDER BY list_label
                        LOOP
                            position := position + 1;

                            -- Новое значение list_label для корневой функции
                            new_label := position::TEXT;

                            -- Обновляем list_label для корневой функции
                            UPDATE urban_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    ELSE
                        -- Получаем label родительской функции
                        SELECT list_label INTO parent_label
                        FROM urban_functions_dict
                        WHERE urban_function_id = OLD.parent_urban_function_id;
                    END IF;

                    -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                    FOR sibling_label IN
                        SELECT list_label FROM urban_functions_dict
                        WHERE parent_urban_function_id = OLD.parent_urban_function_id
                        ORDER BY list_label
                    LOOP
                        position := position + 1;
                        new_label := parent_label || '.' || position;

                        -- Обновляем list_label для функции на текущем уровне
                        UPDATE urban_functions_dict
                        SET list_label = new_label
                        WHERE list_label = sibling_label;
                    END LOOP;

                    RETURN OLD;
                END;
                $$ LANGUAGE plpgsql;
                """
            )
        )
    )
