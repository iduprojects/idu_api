from math import ceil
from typing import Any, ClassVar, Generic, Optional, Sequence, TypeVar

from fastapi import Query
from fastapi_pagination.bases import AbstractPage, AbstractParams, CursorRawParams, RawParams
from fastapi_pagination.cursor import decode_cursor, encode_cursor
from fastapi_pagination.default import Params as DefaultParams
from fastapi_pagination.links.bases import create_links
from fastapi_pagination.types import Cursor
from pydantic import BaseModel
from typing_extensions import Self

T = TypeVar("T", bound=BaseModel)


class BaseParams(BaseModel, AbstractParams):
    page: int = Query(1, ge=1, alias="page")
    size: int = Query(10, ge=1, alias="page_size")

    def to_raw_params(self) -> RawParams:
        return RawParams(
            limit=self.size if self.size is not None else None,
            offset=self.size * (self.page - 1) if self.page is not None and self.size is not None else None,
        )


class Page(AbstractPage[T], Generic[T]):  # pylint: disable=too-few-public-methods
    count: int
    prev: Optional[str] = None
    next: Optional[str] = None
    results: Sequence[T]

    __params_type__ = BaseParams

    @classmethod
    def create(
        cls,
        items: Sequence[T],
        params: AbstractParams,
        *,
        total: Optional[int] = None,
        **kwargs: Any,
    ) -> Self:

        assert isinstance(params, BaseParams)
        assert total is not None

        params = params or DefaultParams()
        links = create_links(
            first={"page": 1},
            last={"page": ceil(total / params.size) if total > 0 and params.size > 0 else 1},
            next={"page": params.page + 1} if params.page * params.size < total else None,
            prev={"page": params.page - 1} if params.page - 1 >= 1 else None,
        )

        return cls(
            count=total,
            prev=links.prev,
            next=links.next,
            results=items,
            **kwargs,
        )


class CursorParams(BaseModel, AbstractParams):
    cursor: Optional[str] = Query(None, description="Cursor for the next page")
    size: int = Query(10, ge=1, description="Page size", alias="page_size")

    str_cursor: ClassVar[bool] = True

    def to_raw_params(self) -> CursorRawParams:
        return CursorRawParams(
            cursor=decode_cursor(self.cursor, to_str=self.str_cursor),
            size=self.size,
        )


class CursorPage(AbstractPage[T], Generic[T]):  # pylint: disable=too-few-public-methods
    count: int
    prev: Optional[str] = None
    next: Optional[str] = None
    results: Sequence[T]

    __params_type__ = CursorParams

    @classmethod
    def create(
        cls,
        items: Sequence[T],
        params: AbstractParams,
        *,
        total: Optional[int] = None,
        previous: Optional[Cursor] = None,
        next_: Optional[Cursor] = None,
        **kwargs: Any,
    ) -> Self:

        assert isinstance(params, CursorParams)
        assert total is not None

        links = create_links(
            first=None,
            last=None,
            next={"cursor": encode_cursor(next_)} if next_ is not None else None,
            prev={"cursor": encode_cursor(previous)} if previous is not None else None,
        )

        return cls(
            count=total,
            prev=links.prev,
            next=links.next,
            results=items,
            **kwargs,
        )
