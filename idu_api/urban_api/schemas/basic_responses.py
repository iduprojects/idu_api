"""Basic response schemas are defined here."""

from typing import Literal

from pydantic import BaseModel


class OkResponse(BaseModel):
    """Response which is returned when request succeeded."""

    status: Literal["ok"] = "ok"
