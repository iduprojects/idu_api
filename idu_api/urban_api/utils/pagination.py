from typing import Any, Callable, Optional, Sequence, TypeVar

from fastapi_pagination.api import apply_items_transformer, create_page
from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.types import AdditionalData, ItemsTransformer
from fastapi_pagination.utils import verify_params
from pydantic import BaseModel
from sqlakeyset import paging
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import Select
from sqlalchemy.util import greenlet_spawn

from idu_api.urban_api.dto import PageDTO

func: Callable

DTO = TypeVar("DTO")
T = TypeVar("T", bound=BaseModel)


def paginate(
    items: Sequence[DTO],
    total: int,
    params: Optional[AbstractParams] = None,
    transformer: Optional[ItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    """This function transforms list of DTO to pydantic models
    and creates page with list of results, total count and links."""

    params, _ = verify_params(params, "limit-offset", "cursor")

    t_items = apply_items_transformer(items, transformer)

    return create_page(
        t_items,
        total,
        params,
        **(additional_data or {}),
    )


async def paginate_dto(
    conn: AsyncConnection,
    stmt: Select,
    params: Optional[AbstractParams] = None,
    transformer: Optional[ItemsTransformer] = None,
) -> PageDTO[DTO]:
    """This function returns total count of items and list of results by given params."""

    count_query = select(func.count()).select_from(stmt.alias("subquery"))
    total = (await conn.execute(count_query)).scalar()

    _, raw_params = verify_params(params, "limit-offset", "cursor")

    if raw_params.type == "limit-offset":
        stmt = stmt.offset(raw_params.offset).limit(raw_params.limit)
    elif raw_params.type == "cursor":
        if not getattr(stmt, "_order_by_clauses", True):
            raise ValueError("Cursor pagination requires ordering")

        page = await greenlet_spawn(
            paging.select_page,
            conn.sync_connection,
            selectable=stmt,
            per_page=raw_params.size,
            page=raw_params.cursor,
        )

        items = [row._mapping for row in page]
        t_items = apply_items_transformer(items, transformer)

        cursor_data = {
            "previous": page.paging.bookmark_previous if page.paging.has_previous else None,
            "next_": page.paging.bookmark_next if page.paging.has_next else None,
        }

        return PageDTO(total=total, items=t_items, cursor_data=cursor_data)

    result = (await conn.execute(stmt)).mappings().all()
    t_items = apply_items_transformer(result, transformer)

    return PageDTO(total=total, items=t_items)
