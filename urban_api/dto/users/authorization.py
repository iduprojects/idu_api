"""
TokensTuple DTO is defined here.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class TokensTuple:
    """
    Tuple of access and refresh tokens
    """

    access: str
    refresh: str
