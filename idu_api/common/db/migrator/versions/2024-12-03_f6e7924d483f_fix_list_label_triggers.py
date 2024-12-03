# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix list label triggers

Revision ID: f6e7924d483f
Revises: ee3927f2af61
Create Date: 2024-12-03 19:36:56.349194

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f6e7924d483f"
down_revision: Union[str, None] = "ee3927f2af61"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # fix physical object function trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_po_function_list_label_on_update()
                 RETURNS trigger
                 LANGUAGE plpgsql
                AS $function$
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
                            SELECT list_label FROM physical_object_functions_dict
                            WHERE parent_id = NEW.physical_object_function_id
                            ORDER BY  
                                (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                 FROM unnest(string_to_array(list_label, '.')) AS part)
                        LOOP
                            position := position + 1;
                            new_label := NEW.list_label || '.' || position;
                
                            UPDATE physical_object_functions_dict
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
                            new_label := (SELECT COUNT(*) FROM physical_object_functions_dict WHERE parent_id IS NULL)::TEXT;
                
                            UPDATE physical_object_functions_dict
                            SET list_label = new_label
                            WHERE physical_object_function_id = NEW.physical_object_function_id;
                        ELSE
                            SELECT list_label INTO parent_label
                            FROM physical_object_functions_dict
                            WHERE physical_object_function_id = NEW.parent_id;
                
                            new_label := parent_label || '.' || (SELECT COUNT(*) FROM physical_object_functions_dict WHERE parent_id = NEW.parent_id)::TEXT;
                
                            UPDATE physical_object_functions_dict
                            SET list_label = new_label
                            WHERE physical_object_function_id = NEW.physical_object_function_id;
                        END IF;
                
                        -- Если старая функция была корневой
                        IF OLD.parent_id IS NULL THEN
                            FOR sibling_label IN
                                SELECT list_label FROM physical_object_functions_dict
                                WHERE parent_id IS NULL
                                ORDER BY  
                                    (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                     FROM unnest(string_to_array(list_label, '.')) AS part)
                            LOOP
                                position := position + 1;
                                new_label := position::TEXT;
                
                                UPDATE physical_object_functions_dict
                                SET list_label = new_label
                                WHERE list_label = sibling_label;
                            END LOOP;
                        ELSE
                            -- Получаем label родительской функции
                            SELECT list_label INTO parent_label
                            FROM physical_object_functions_dict
                            WHERE physical_object_function_id = OLD.parent_id;
                        END IF;
                
                        -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                        FOR sibling_label IN
                            SELECT list_label FROM physical_object_functions_dict
                            WHERE parent_id = OLD.parent_id
                            ORDER BY  
                                (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                 FROM unnest(string_to_array(list_label, '.')) AS part)
                        LOOP
                            position := position + 1;
                            new_label := parent_label || '.' || position;
                
                            -- Обновляем list_label для функции на текущем уровне
                            UPDATE physical_object_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    END IF;
                
                    RETURN NEW;
                END;
                $function$
                ;
                """
            )
        )
    )

    # fix urban function trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_urban_function_list_label_on_update()
                 RETURNS trigger
                 LANGUAGE plpgsql
                AS $function$
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
                            ORDER BY  
                                (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                 FROM unnest(string_to_array(list_label, '.')) AS part)
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
                                ORDER BY  
                                    (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                     FROM unnest(string_to_array(list_label, '.')) AS part)
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
                            ORDER BY  
                                (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                 FROM unnest(string_to_array(list_label, '.')) AS part)
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
                $function$
                ;
                """
            )
        )
    )

    # fix indicators dict trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_indicator_list_label_on_update()
                 RETURNS trigger
                 LANGUAGE plpgsql
                AS $function$
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
                            SELECT list_label FROM indicators_dict
                            WHERE parent_id = NEW.indicator_id
                            ORDER BY  
                                (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                 FROM unnest(string_to_array(list_label, '.')) AS part)
                        LOOP
                            position := position + 1;
                            new_label := NEW.list_label || '.' || position;
                
                            UPDATE indicators_dict
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
                            new_label := (SELECT COUNT(*) FROM indicators_dict WHERE parent_id IS NULL)::TEXT;
                
                            UPDATE indicators_dict
                            SET list_label = new_label
                            WHERE indicator_id = NEW.indicator_id;
                        ELSE
                            SELECT list_label INTO parent_label
                            FROM indicators_dict
                            WHERE indicator_id = NEW.parent_id;
                
                            new_label := parent_label || '.' || (SELECT COUNT(*) FROM indicators_dict WHERE parent_id = NEW.parent_id)::TEXT;
                
                            UPDATE indicators_dict
                            SET list_label = new_label
                            WHERE indicator_id = NEW.indicator_id;
                        END IF;
                
                        -- Если старая функция была корневой
                        IF OLD.parent_id IS NULL THEN
                            FOR sibling_label IN
                                SELECT list_label FROM indicators_dict
                                WHERE parent_id IS NULL
                                ORDER BY  
                                    (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                     FROM unnest(string_to_array(list_label, '.')) AS part)
                            LOOP
                                position := position + 1;
                                new_label := position::TEXT;
                
                                UPDATE indicators_dict
                                SET list_label = new_label
                                WHERE list_label = sibling_label;
                            END LOOP;
                        ELSE
                            -- Получаем label родительской функции
                            SELECT list_label INTO parent_label
                            FROM indicators_dict
                            WHERE indicator_id = OLD.parent_id;
                        END IF;
                
                        -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                        FOR sibling_label IN
                            SELECT list_label FROM indicators_dict
                            WHERE parent_id = OLD.parent_id
                            ORDER BY  
                                (SELECT array_to_string(array_agg(LPAD(part, 4, '0')), '.') 
                                 FROM unnest(string_to_array(list_label, '.')) AS part)
                        LOOP
                            position := position + 1;
                            new_label := parent_label || '.' || position;
                
                            -- Обновляем list_label для функции на текущем уровне
                            UPDATE indicators_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    END IF;
                
                    RETURN NEW;
                END;
                $function$
                ;
                """
            )
        )
    )


def downgrade() -> None:
    # fix physical object function trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_po_function_list_label_on_update()
                 RETURNS trigger
                 LANGUAGE plpgsql
                AS $function$
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
                            SELECT list_label FROM physical_object_functions_dict
                            WHERE parent_id = NEW.physical_object_function_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := NEW.list_label || '.' || position;

                            UPDATE physical_object_functions_dict
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
                            new_label := (SELECT COUNT(*) FROM physical_object_functions_dict WHERE parent_id IS NULL)::TEXT;

                            UPDATE physical_object_functions_dict
                            SET list_label = new_label
                            WHERE physical_object_function_id = NEW.physical_object_function_id;
                        ELSE
                            SELECT list_label INTO parent_label
                            FROM physical_object_functions_dict
                            WHERE physical_object_function_id = NEW.parent_id;

                            new_label := parent_label || '.' || (SELECT COUNT(*) FROM physical_object_functions_dict WHERE parent_id = NEW.parent_id)::TEXT;

                            UPDATE physical_object_functions_dict
                            SET list_label = new_label
                            WHERE physical_object_function_id = NEW.physical_object_function_id;
                        END IF;

                        -- Если старая функция была корневой
                        IF OLD.parent_id IS NULL THEN
                            FOR sibling_label IN
                                SELECT list_label FROM physical_object_functions_dict
                                WHERE parent_id IS NULL
                                ORDER BY list_label
                            LOOP
                                position := position + 1;
                                new_label := position::TEXT;

                                UPDATE physical_object_functions_dict
                                SET list_label = new_label
                                WHERE list_label = sibling_label;
                            END LOOP;
                        ELSE
                            -- Получаем label родительской функции
                            SELECT list_label INTO parent_label
                            FROM physical_object_functions_dict
                            WHERE physical_object_function_id = OLD.parent_id;
                        END IF;

                        -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                        FOR sibling_label IN
                            SELECT list_label FROM physical_object_functions_dict
                            WHERE parent_id = OLD.parent_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := parent_label || '.' || position;

                            -- Обновляем list_label для функции на текущем уровне
                            UPDATE physical_object_functions_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    END IF;

                    RETURN NEW;
                END;
                $function$
                ;
                """
            )
        )
    )

    # fix urban function trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_urban_function_list_label_on_update()
                 RETURNS trigger
                 LANGUAGE plpgsql
                AS $function$
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
                $function$
                ;
                """
            )
        )
    )

    # fix indicators dict trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_indicator_list_label_on_update()
                 RETURNS trigger
                 LANGUAGE plpgsql
                AS $function$
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
                            SELECT list_label FROM indicators_dict
                            WHERE parent_id = NEW.indicator_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := NEW.list_label || '.' || position;

                            UPDATE indicators_dict
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
                            new_label := (SELECT COUNT(*) FROM indicators_dict WHERE parent_id IS NULL)::TEXT;

                            UPDATE indicators_dict
                            SET list_label = new_label
                            WHERE indicator_id = NEW.indicator_id;
                        ELSE
                            SELECT list_label INTO parent_label
                            FROM indicators_dict
                            WHERE indicator_id = NEW.parent_id;

                            new_label := parent_label || '.' || (SELECT COUNT(*) FROM indicators_dict WHERE parent_id = NEW.parent_id)::TEXT;

                            UPDATE indicators_dict
                            SET list_label = new_label
                            WHERE indicator_id = NEW.indicator_id;
                        END IF;

                        -- Если старая функция была корневой
                        IF OLD.parent_id IS NULL THEN
                            FOR sibling_label IN
                                SELECT list_label FROM indicators_dict
                                WHERE parent_id IS NULL
                                ORDER BY list_label
                            LOOP
                                position := position + 1;
                                new_label := position::TEXT;

                                UPDATE indicators_dict
                                SET list_label = new_label
                                WHERE list_label = sibling_label;
                            END LOOP;
                        ELSE
                            -- Получаем label родительской функции
                            SELECT list_label INTO parent_label
                            FROM indicators_dict
                            WHERE indicator_id = OLD.parent_id;
                        END IF;

                        -- Перенумеровываем оставшиеся функции на том же уровне, что и удаленная
                        FOR sibling_label IN
                            SELECT list_label FROM indicators_dict
                            WHERE parent_id = OLD.parent_id
                            ORDER BY list_label
                        LOOP
                            position := position + 1;
                            new_label := parent_label || '.' || position;

                            -- Обновляем list_label для функции на текущем уровне
                            UPDATE indicators_dict
                            SET list_label = new_label
                            WHERE list_label = sibling_label;
                        END LOOP;
                    END IF;

                    RETURN NEW;
                END;
                $function$
                ;
                """
            )
        )
    )
