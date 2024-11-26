"""Exceptions connected with authentication client are defined here."""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class ExpiredToken(IduApiError):
    """Exception to raise when token has expired."""

    def __init__(self, token: str):
        super().__init__()
        self.token = token

    def __str__(self) -> str:
        return f"Token has expired: {self.token}"

    def get_status_code(self) -> int:
        """
        Return '401 Unauthorized' status code.
        """
        return status.HTTP_401_UNAUTHORIZED


class JWTDecodeError(IduApiError):
    """Exception to raise when token decoding has failed."""

    def __init__(self, token: str):
        super().__init__()
        self.token = token

    def __str__(self) -> str:
        return "JWT decoding decoding error"

    def get_status_code(self) -> int:
        """
        Return '401 Unauthorized' status code.
        """
        return status.HTTP_401_UNAUTHORIZED


class InvalidTokenSignature(IduApiError):
    """Exception to raise when validating token by external service has failed."""

    def __init__(self, token: str):
        super().__init__()
        self.token = token

    def __str__(self) -> str:
        return f"Invalid token signature: {self.token}"

    def get_status_code(self) -> int:
        """
        Return '401 Unauthorized' status code.
        """
        return status.HTTP_401_UNAUTHORIZED


class AuthServiceUnavailable(IduApiError):
    """Exception to raise when auth service is unavailable."""

    def __str__(self) -> str:
        return "Error verifying token signature"

    def get_status_code(self) -> int:
        """
        Return '503 SERVICE UNAVAILABLE' status code.
        """
        return status.HTTP_503_SERVICE_UNAVAILABLE
