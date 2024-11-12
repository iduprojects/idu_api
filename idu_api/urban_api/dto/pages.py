"""DTO for pagination is defined here."""

from dataclasses import dataclass
from typing import Any, Generic, Sequence, TypeVar

T = TypeVar("T")


@dataclass
class PageDTO(Generic[T]):
    total: int
    items: Sequence[T]
    cursor_data: dict[str, Any] | None = None
