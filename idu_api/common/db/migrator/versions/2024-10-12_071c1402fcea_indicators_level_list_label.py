# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""indicators level list label

Revision ID: 071c1402fcea
Revises: 8de8ea793205
Create Date: 2024-10-12 15:58:43.705902

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "071c1402fcea"
down_revision: Union[str, None] = "8de8ea793205"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add cascade delete rule for indicators dict parent id
    op.drop_constraint(
        "indicators_dict_fk_parent_id__indicators_dict",
        "indicators_dict",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "indicators_dict_fk_parent_id__indicators_dict",
        "indicators_dict",
        "indicators_dict",
        ["parent_id"],
        ["indicator_id"],
        ondelete="CASCADE",
    )

    # create functions/triggers to auto-set indicators level on insert/update/delete
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_indicator_level_on_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    NEW.level := (
                        SELECT COALESCE(
                            (SELECT level
                            FROM indicators_dict
                            WHERE indicator_id = NEW.parent_id), 0) + 1
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
                CREATE TRIGGER update_indicator_level_on_insert_trigger
                BEFORE INSERT ON public.indicators_dict
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_indicator_level_on_insert();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_indicator_level_on_update()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.level <> OLD.level THEN
                        UPDATE indicators_dict 
                        SET level = NEW.level + 1           
                        WHERE parent_id = NEW.indicator_id;
                    END IF;

                    IF NEW.parent_id <> OLD.parent_id OR 
                    NEW.parent_id IS NULL AND OLD.parent_id IS NOT NULL OR
                    NEW.parent_id IS NOT NULL AND OLD.parent_id IS NULL
                    THEN
                        UPDATE indicators_dict 
                        SET level = (
                            SELECT COALESCE(
                                (SELECT level
                                FROM indicators_dict
                                WHERE indicator_id = NEW.parent_id), 0) + 1
                        )                  
                        WHERE indicator_id = NEW.indicator_id;
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
                CREATE TRIGGER update_indicator_level_on_update_trigger
                AFTER UPDATE ON public.indicators_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_indicator_level_on_update();
                """
            )
        )
    )

    # create functions/triggers to auto-set indicator list_label on insert/update/delete
    op.execute(
        sa.text(
            dedent(
                """
                -- Функция для генерации list_label
                CREATE FUNCTION public.trigger_generate_indicator_list_label()
                RETURNS TRIGGER AS $$
                DECLARE
                    parent_label TEXT;
                    next_number INT;
                BEGIN
                    -- Если это корневая функция (нет родительской функции)
                    IF NEW.parent_id IS NULL THEN
                        -- Считаем количество корневых функций и формируем list_label для нового элемента
                        NEW.list_label := (SELECT COUNT(*) + 1 FROM indicators_dict WHERE parent_id IS NULL)::TEXT;
                    ELSE
                        -- Получаем list_label родительской функции
                        SELECT list_label INTO parent_label
                        FROM indicators_dict
                        WHERE indicator_id = NEW.parent_id;

                        NEW.list_label := parent_label || '.' || (SELECT COUNT(*) + 1 FROM indicators_dict WHERE parent_id = NEW.parent_id)::TEXT;
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
                CREATE TRIGGER update_indicator_list_label_on_insert_trigger
                BEFORE INSERT ON public.indicators_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_generate_indicator_list_label();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_indicator_list_label_on_update()
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
                $$ LANGUAGE plpgsql;
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_indicator_list_label_on_update_trigger
                AFTER UPDATE ON public.indicators_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_indicator_list_label_on_update();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_indicator_sibling_labels_on_delete()
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
                            SELECT list_label FROM indicators_dict
                            WHERE parent_id IS NULL
                            ORDER BY list_label
                        LOOP
                            position := position + 1;

                            -- Новое значение list_label для корневой функции
                            new_label := position::TEXT;

                            -- Обновляем list_label для корневой функции
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

                    RETURN OLD;
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
                CREATE TRIGGER reorder_indicator_sibling_labels_on_delete_trigger
                AFTER DELETE ON public.indicators_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_indicator_sibling_labels_on_delete();
                """
            )
        )
    )


def downgrade() -> None:
    # drop cascade delete rule
    op.drop_constraint(
        "indicators_dict_fk_parent_id__indicators_dict",
        "indicators_dict",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "indicators_dict_fk_parent_id__indicators_dict",
        "indicators_dict",
        "indicators_dict",
        ["parent_id"],
        ["indicator_id"],
    )

    # drop triggers/function to auto-set indicator level
    op.execute("DROP TRIGGER IF EXISTS update_indicator_level_on_update_trigger ON public.indicators_dict")
    op.execute("DROP TRIGGER IF EXISTS update_indicator_level_on_insert_trigger ON public.indicators_dict")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_indicator_level_on_insert")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_indicator_level_on_update")

    # drop triggers/functions to auto-set indicator list label
    op.execute("DROP TRIGGER IF EXISTS reorder_indicator_sibling_labels_on_delete_trigger ON public.indicators_dict")
    op.execute("DROP TRIGGER IF EXISTS update_indicator_list_label_on_update_trigger ON public.indicators_dict")
    op.execute("DROP TRIGGER IF EXISTS update_indicator_list_label_on_insert_trigger ON public.indicators_dict")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_indicator_sibling_labels_on_delete")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_indicator_list_label_on_update")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_generate_indicator_list_label")
