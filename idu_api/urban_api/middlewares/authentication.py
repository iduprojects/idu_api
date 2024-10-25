from fastapi import HTTPException, Request, status
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.urban_api.utils.auth_client import AuthenticationClient


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Middleware for authenticating requests and adding UserDTO to request.state."""

    def __init__(self, app, auth_client: AuthenticationClient):
        super().__init__(app)
        self.auth_client = auth_client

    async def dispatch(self, request: Request, call_next):
        authorization = request.headers.get("Authorization")
        if authorization and authorization.startswith("Bearer "):
            token = authorization.split(" ")[1]
            try:
                # Validate token and get user information
                request.state.user = await self.auth_client.get_user_from_token(token)
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=str(exc),
                    headers={"WWW-Authenticate": "Bearer"},
                ) from exc
        else:
            request.state.user = None  # No token, user is unauthenticated

        return await call_next(request)
