"""
User DTO is defined here.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class UserDTO:
    """
    User data transfer object
    """

    id: str
    is_active: bool

    def __str__(self) -> str:
        return f"(id={self.id}, is_active: {self.is_active})"
