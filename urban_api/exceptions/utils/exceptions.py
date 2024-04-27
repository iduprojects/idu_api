# pylint: disable=method-hidden
"""
Top exceptions for a plants backend service are defined here.
"""

from fastapi import status

from urban_api.exceptions import NotesApiError


class JWTDecodeError(NotesApiError):
    """
    Thrown a failed attempt to decode JWT token value, either because a bad format or missing essential keys.
    """

    def __init__(self, token: str):
        super().__init__()
        self.token = token

    def get_status_code(self) -> int:
        """
        Return '401 Unauthorized' status code.
        """
        return status.HTTP_401_UNAUTHORIZED

    def __str__(self) -> str:
        return "JWT decoding decoding error"
