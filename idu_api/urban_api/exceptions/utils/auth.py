"""Exceptions connected with authentication client are defined here."""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class ExpiredToken(IduApiError):
    """Exception to raise when token has expired."""

    def __str__(self) -> str:
        return "Token has expired"

    def get_status_code(self) -> int:
        """
        Return '401 Unauthorized' status code.
        """
        return status.HTTP_401_UNAUTHORIZED


class JWTDecodeError(IduApiError):
    """Exception to raise when token decoding has failed."""

    def __str__(self) -> str:
        return "JWT decoding error"

    def get_status_code(self) -> int:
        """
        Return '401 Unauthorized' status code.
        """
        return status.HTTP_401_UNAUTHORIZED


class InvalidTokenSignature(IduApiError):
    """Exception to raise when validating token by external service has failed."""

    def __str__(self) -> str:
        return "Invalid token signature"

    def get_status_code(self) -> int:
        """
        Return '401 Unauthorized' status code.
        """
        return status.HTTP_401_UNAUTHORIZED
