from dataclasses import dataclass
from typing import Any, Generic, Optional, Sequence, TypeVar

T = TypeVar("T")


@dataclass
class PageDTO(Generic[T]):
    total: int
    items: Sequence[T]
    cursor_data: Optional[dict[str, Any]] = None
