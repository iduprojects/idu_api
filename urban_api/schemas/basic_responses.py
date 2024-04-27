"""
Basic response schemas are defined here.
"""
from typing import Literal

from pydantic import BaseModel


class OkResponse(BaseModel):
    """
    Response which is returned when request succseeded
    """

    result: Literal["Ok"] = "Ok"
