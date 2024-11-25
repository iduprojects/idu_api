from fastapi import Request
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.urban_api.exceptions import IduApiError
from idu_api.urban_api.utils.auth_client import AuthenticationClient


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Middleware for authenticating requests and adding UserDTO to request.state."""

    def __init__(self, app, auth_client: AuthenticationClient):
        super().__init__(app)
        self.auth_client = auth_client

    async def dispatch(self, request: Request, call_next):
        try:
            authorization = request.headers.get("Authorization")
            if authorization and authorization.startswith("Bearer "):
                token = authorization.split(" ")[1]
                request.state.user = await self.auth_client.get_user_from_token(token)
            else:
                request.state.user = None  # No token, user is unauthenticated
        except IduApiError:
            raise
        except Exception as exc:
            logger.error("Unexpected error in AuthenticationMiddleware: {}", exc)
            raise exc

        return await call_next(request)
