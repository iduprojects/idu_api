"""User DTO is defined here."""

from dataclasses import dataclass


@dataclass(frozen=True)
class UserDTO:
    """User data transfer object"""

    id: str
    is_superuser: bool

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return self.id
