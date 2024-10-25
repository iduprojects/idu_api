"""FastAPI authentication client is defined here."""

import json
from base64 import b64decode
from datetime import datetime

import httpx
from cachetools import TTLCache
from fastapi import HTTPException, Request
from starlette import status

from idu_api.urban_api.dto.users import UserDTO


class AuthenticationClient:

    def __init__(self, cache_size: int, cache_ttl: int, validate_token: int, auth_url: str):
        self._validate_token = validate_token
        self._auth_url = auth_url
        self._cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)

    @staticmethod
    def decode_token(token: str) -> dict:
        """Decode the JWT token without verification to extract payload."""
        try:
            payload = json.loads(b64decode(token.split(".")[1]))
            return payload
        except Exception as exc:
            raise ValueError("Invalid JWT token") from exc

    @staticmethod
    def is_token_expired(payload: dict) -> bool:
        """Check if the JWT token is expired."""
        if "exp" in payload:
            expiration = datetime.utcfromtimestamp(payload["exp"])
            return expiration < datetime.utcnow()
        return False

    def update(
        self,
        cache_size: int | None = None,
        cache_ttl: int | None = None,
        validate_token: int | None = None,
        auth_url: str | None = None,
    ) -> None:
        self._validate_token = validate_token or self._validate_token
        self._auth_url = auth_url or self._auth_url
        self._cache = TTLCache(maxsize=cache_size, ttl=cache_ttl) or self._cache

    async def validate_token_online(self, token: str) -> None:
        """Validate token by calling an external service if needed."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(self._auth_url, headers={"Authorization": f"Bearer {token}"})
            if response.status_code != 200:
                raise ValueError("Invalid token signature")
        except Exception as exc:
            raise ValueError("Error verifying token signature") from exc

    async def get_user_from_token(self, token: str) -> UserDTO:
        """Main method that processes the token and returns UserDTO."""

        cached_user = self._cache.get(token)
        if cached_user:
            return cached_user

        payload = self.decode_token(token)

        # Optionally validate the token online
        if self._validate_token:
            if self.is_token_expired(payload):
                raise ValueError("Token has expired")
            await self.validate_token_online(token)

        user_dto = UserDTO(id=payload.get("sub"), is_active=payload.get("active"))

        self._cache[token] = user_dto

        return user_dto


def user_dependency(request: Request):
    if not request.state.user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return request.state.user
