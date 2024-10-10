# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""service types urban functions

Revision ID: a9c7320e2115
Revises: 0a25fb0c815e
Create Date: 2024-10-09 12:45:06.243309

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a9c7320e2115"
down_revision: Union[str, None] = "0a25fb0c815e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add `delete rules` for service types and urban functions
    op.drop_constraint(
        "service_types_normatives_data_fk_service_type_id__servi_225c",
        "service_types_normatives_data",
        type_="foreignkey",
    )
    op.drop_constraint(
        "service_types_normatives_data_fk_urban_function_id__urb_8b61",
        "service_types_normatives_data",
        type_="foreignkey",
    )
    op.drop_constraint(
        "services_data_fk_service_type_id__service_types_dict",
        "services_data",
        type_="foreignkey",
    )
    op.drop_constraint(
        "service_types_dict_fk_urban_function_id__urban_functions_dict",
        "service_types_dict",
        type_="foreignkey",
    )
    op.drop_constraint(
        "urban_functions_dict_fk_parent_urban_function_id__urban_6c14",
        "urban_functions_dict",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "service_types_normatives_data_fk_service_type_id__servi_225c",
        "service_types_normatives_data",
        "service_types_dict",
        ["service_type_id"],
        ["service_type_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "service_types_normatives_data_fk_urban_function_id__urb_8b61",
        "service_types_normatives_data",
        "urban_functions_dict",
        ["urban_function_id"],
        ["urban_function_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "services_data_fk_service_type_id__service_types_dict",
        "services_data",
        "service_types_dict",
        ["service_type_id"],
        ["service_type_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "service_types_dict_fk_urban_function_id__urban_functions_dict",
        "service_types_dict",
        "urban_functions_dict",
        ["urban_function_id"],
        ["urban_function_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "urban_functions_dict_fk_parent_urban_function_id__urban_6c14",
        "urban_functions_dict",
        "urban_functions_dict",
        ["parent_urban_function_id"],
        ["urban_function_id"],
        ondelete="CASCADE",
    )

    # create triggers/function to auto-set urban function level on insert/update
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_urban_function_level_on_insert()
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
                CREATE TRIGGER update_urban_function_level_on_insert_trigger
                BEFORE INSERT ON public.urban_functions_dict
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_urban_function_level_on_insert();
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
                CREATE TRIGGER update_urban_function_level_on_update_trigger
                AFTER UPDATE ON public.urban_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_urban_function_level_on_update();
                """
            )
        )
    )

    # create functions/triggers to auto-set urban_function list_label on insert/update/delete
    op.execute(
        sa.text(
            dedent(
                """
                -- Функция для генерации list_label
                CREATE FUNCTION public.trigger_generate_list_label()
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
                CREATE TRIGGER update_list_label_on_insert_trigger
                BEFORE INSERT ON public.urban_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_generate_list_label();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_list_label_on_update()
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
                CREATE TRIGGER update_list_label_on_update_trigger
                AFTER UPDATE ON public.urban_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_list_label_on_update();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_sibling_labels_on_delete()
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
    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER reorder_sibling_labels_on_delete_trigger
                AFTER DELETE ON public.urban_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_sibling_labels_on_delete();
                """
            )
        )
    )

    #


def downgrade() -> None:
    # drop `delete rules` for service types and urban functions
    op.drop_constraint(
        "service_types_normatives_data_fk_service_type_id__servi_225c",
        "service_types_normatives_data",
        type_="foreignkey",
    )
    op.drop_constraint(
        "service_types_normatives_data_fk_urban_function_id__urb_8b61",
        "service_types_normatives_data",
        type_="foreignkey",
    )
    op.drop_constraint(
        "services_data_fk_service_type_id__service_types_dict",
        "services_data",
        type_="foreignkey",
    )
    op.drop_constraint(
        "service_types_dict_fk_urban_function_id__urban_functions_dict",
        "service_types_dict",
        type_="foreignkey",
    )
    op.drop_constraint(
        "urban_functions_dict_fk_parent_urban_function_id__urban_6c14",
        "urban_functions_dict",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "service_types_normatives_data_fk_service_type_id__servi_225c",
        "service_types_normatives_data",
        "service_types_dict",
        ["service_type_id"],
        ["service_type_id"],
    )
    op.create_foreign_key(
        "service_types_normatives_data_fk_urban_function_id__urb_8b61",
        "service_types_normatives_data",
        "urban_functions_dict",
        ["urban_function_id"],
        ["urban_function_id"],
    )
    op.create_foreign_key(
        "services_data_fk_service_type_id__service_types_dict",
        "services_data",
        "service_types_dict",
        ["service_type_id"],
        ["service_type_id"],
    )
    op.create_foreign_key(
        "service_types_dict_fk_urban_function_id__urban_functions_dict",
        "service_types_dict",
        "urban_functions_dict",
        ["urban_function_id"],
        ["urban_function_id"],
    )
    op.create_foreign_key(
        "urban_functions_dict_fk_parent_urban_function_id__urban_6c14",
        "urban_functions_dict",
        "urban_functions_dict",
        ["parent_urban_function_id"],
        ["urban_function_id"],
    )

    # drop triggers/function to auto-set urban function level
    op.execute("DROP TRIGGER IF EXISTS update_urban_function_level_on_update_trigger ON public.urban_functions_dict")
    op.execute("DROP TRIGGER IF EXISTS update_urban_function_level_on_insert_trigger ON public.urban_functions_dict")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_urban_function_level_on_insert")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_urban_function_level_on_update")

    # drop triggers/functions to auto-set urban function list label
    op.execute("DROP TRIGGER IF EXISTS reorder_sibling_labels_on_delete_trigger ON public.urban_functions_dict")
    op.execute("DROP TRIGGER IF EXISTS update_list_label_on_update_trigger ON public.urban_functions_dict")
    op.execute("DROP TRIGGER IF EXISTS update_list_label_on_insert_trigger ON public.urban_functions_dict")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_sibling_labels_on_delete")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_list_label_on_update")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_generate_list_label")
