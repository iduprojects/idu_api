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
    username: str
    email: str
    roles: list[str]
    is_banned: bool

    def __str__(self) -> str:
        return f"(id={self.id}, username: {self.username})"
