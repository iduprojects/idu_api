"""Exceptions connected with external services are defined here."""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class ExternalServiceResponseError(IduApiError):
    """Exception to raise when external service returns http error."""

    def __init__(self, service: str, exc: str, exc_code: int):
        super().__init__()
        self.service = service
        self.exc = exc
        self.exc_code = exc_code

    def __str__(self) -> str:
        return f'External service "{self.service}" response error: {self.exc}'

    def get_status_code(self) -> int:
        """
        Return response error status code.
        """
        return self.exc_code


class ExternalServiceUnavailable(IduApiError):
    """Exception to raise when external service is unavailable."""

    def __init__(self, service: str, exc: str):
        super().__init__()
        self.service = service
        self.exc = exc

    def __str__(self) -> str:
        return f'External service "{self.service}" is unavailable: {self.exc}'

    def get_status_code(self) -> int:
        """
        Return '503 SERVICE UNAVAILABLE' status code.
        """
        return status.HTTP_503_SERVICE_UNAVAILABLE
