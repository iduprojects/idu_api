# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""physical objects functions

Revision ID: fba380fb8c8a
Revises: 4eadb660d27b
Create Date: 2024-10-21 13:34:25.960008

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fba380fb8c8a"
down_revision: Union[str, None] = "4eadb660d27b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create table `physical_object_function_dict`
    op.execute(sa.schema.CreateSequence(sa.Sequence("physical_object_functions_dict_id_seq")))
    op.create_table(
        "physical_object_functions_dict",
        sa.Column(
            "physical_object_function_id",
            sa.Integer(),
            server_default=sa.text("nextval('physical_object_functions_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("level", sa.Integer(), nullable=False),
        sa.Column("list_label", sa.String(length=20), nullable=False),
        sa.Column("code", sa.String(length=50), nullable=False),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["physical_object_functions_dict.physical_object_function_id"],
            name=op.f("physical_object_functions_dict_fk_parent_id__physical_object_functions_dict"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("physical_object_function_id", name=op.f("physical_object_functions_dict_pk")),
        sa.UniqueConstraint("list_label", name=op.f("physical_object_functions_dict_list_label_key")),
        sa.UniqueConstraint("name", name=op.f("physical_object_functions_dict_name_key")),
    )

    # create functions/triggers to auto-set po functions level on insert/update/delete
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_po_function_level_on_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    NEW.level := (
                        SELECT COALESCE(
                            (SELECT level
                            FROM physical_object_functions_dict
                            WHERE physical_object_function_id = NEW.parent_id), 0) + 1
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
                CREATE TRIGGER update_po_function_level_on_insert_trigger
                BEFORE INSERT ON public.physical_object_functions_dict
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_po_function_level_on_insert();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_po_function_level_on_update()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.level <> OLD.level THEN
                        UPDATE physical_object_functions_dict 
                        SET level = NEW.level + 1           
                        WHERE parent_id = NEW.physical_object_function_id;
                    END IF;

                    IF NEW.parent_id <> OLD.parent_id OR 
                    NEW.parent_id IS NULL AND OLD.parent_id IS NOT NULL OR
                    NEW.parent_id IS NOT NULL AND OLD.parent_id IS NULL
                    THEN
                        UPDATE physical_object_functions_dict 
                        SET level = (
                            SELECT COALESCE(
                                (SELECT level
                                FROM physical_object_functions_dict
                                WHERE physical_object_function_id = NEW.parent_id), 0) + 1
                        )                  
                        WHERE physical_object_function_id = NEW.physical_object_function_id;
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
                CREATE TRIGGER update_po_function_level_on_update_trigger
                AFTER UPDATE ON public.physical_object_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_po_function_level_on_update();
                """
            )
        )
    )

    # create functions/triggers to auto-set po_function list_label on insert/update/delete
    op.execute(
        sa.text(
            dedent(
                """
                -- Функция для генерации list_label
                CREATE FUNCTION public.trigger_generate_po_function_list_label()
                RETURNS TRIGGER AS $$
                DECLARE
                    parent_label TEXT;
                    next_number INT;
                BEGIN
                    -- Если это корневая функция (нет родительской функции)
                    IF NEW.parent_id IS NULL THEN
                        -- Считаем количество корневых функций и формируем list_label для нового элемента
                        NEW.list_label := (SELECT COUNT(*) + 1 FROM physical_object_functions_dict WHERE parent_id IS NULL)::TEXT;
                    ELSE
                        -- Получаем list_label родительской функции
                        SELECT list_label INTO parent_label
                        FROM physical_object_functions_dict
                        WHERE physical_object_function_id = NEW.parent_id;

                        NEW.list_label := parent_label || '.' || (SELECT COUNT(*) + 1 FROM physical_object_functions_dict WHERE parent_id = NEW.parent_id)::TEXT;
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
                CREATE TRIGGER update_po_function_list_label_on_insert_trigger
                BEFORE INSERT ON public.physical_object_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_generate_po_function_list_label();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_po_function_list_label_on_update()
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
                $$ LANGUAGE plpgsql;
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_po_function_list_label_on_update_trigger
                AFTER UPDATE ON public.physical_object_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_po_function_list_label_on_update();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_po_function_sibling_labels_on_delete()
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
                            SELECT list_label FROM physical_object_functions_dict
                            WHERE parent_id IS NULL
                            ORDER BY list_label
                        LOOP
                            position := position + 1;

                            -- Новое значение list_label для корневой функции
                            new_label := position::TEXT;

                            -- Обновляем list_label для корневой функции
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
                CREATE TRIGGER reorder_po_function_sibling_labels_on_delete_trigger
                AFTER DELETE ON public.physical_object_functions_dict
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_po_function_sibling_labels_on_delete();
                """
            )
        )
    )

    # add column `physical_object_function_id` to `physical_object_types_dict`
    op.add_column("physical_object_types_dict", sa.Column("physical_object_function_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "po_types_dict_fk_po_function_id__po_functions_dict",
        "physical_object_types_dict",
        "physical_object_functions_dict",
        ["physical_object_function_id"],
        ["physical_object_function_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # drop column `physical_object_function_id` from `physical_object_types_dict`
    op.drop_constraint("po_types_dict_fk_po_function_id__po_functions_dict", "physical_object_types_dict", "foreignkey")
    op.drop_column("physical_object_types_dict", "physical_object_function_id")

    # drop triggers/function to auto-set po_function level
    op.execute(
        "DROP TRIGGER IF EXISTS update_po_function_level_on_update_trigger " "ON public.physical_object_functions_dict"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS update_po_function_level_on_insert_trigger " "ON public.physical_object_functions_dict"
    )
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_po_function_level_on_insert")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_po_function_level_on_update")

    # drop triggers/functions to auto-set po_function list label
    op.execute(
        "DROP TRIGGER IF EXISTS reorder_po_function_sibling_labels_on_delete_trigger "
        "ON public.physical_object_functions_dict"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS update_po_function_list_label_on_update_trigger "
        "ON public.physical_object_functions_dict"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS update_po_function_list_label_on_insert_trigger "
        "ON public.physical_object_functions_dict"
    )
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_po_function_sibling_labels_on_delete")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_po_function_list_label_on_update")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_generate_po_function_list_label")

    # drop table `physical_object_function_dict`
    op.drop_table("physical_object_functions_dict")
    op.execute(sa.schema.DropSequence(sa.Sequence("physical_object_functions_dict_id_seq")))
