"""FastAPI authentication client is defined here."""

import base64
import json
from datetime import datetime, timezone

import aiohttp
from aiohttp import ClientConnectorError, ClientResponseError
from cachetools import TTLCache
from fastapi import Request
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.exceptions.utils.auth import (
    ExpiredToken,
    InvalidTokenSignature,
    JWTDecodeError,
)


class AuthenticationClient:

    RETRIES = 3

    def __init__(self, cache_size: int, cache_ttl: int, validate_token: int, auth_url: str):
        self._validate_token = validate_token
        self._auth_url = auth_url
        self._cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)

    @staticmethod
    def decode_token(token: str) -> dict:
        """Decode the JWT token without verification to extract payload."""
        try:
            payload_base64 = token.split(".")[1]
            padded_payload = payload_base64 + "=" * (-len(payload_base64) % 4)
            decoded_payload = base64.urlsafe_b64decode(padded_payload)
            return json.loads(decoded_payload)
        except Exception as exc:
            raise JWTDecodeError() from exc

    @staticmethod
    def is_token_expired(payload: dict) -> bool:
        """Check if the JWT token is expired."""
        if "exp" in payload:
            expiration = datetime.fromtimestamp(payload["exp"], timezone.utc)
            return expiration < datetime.now(timezone.utc)
        return True

    def update(
        self,
        cache_size: int | None = None,
        cache_ttl: int | None = None,
        validate_token: int | None = None,
        auth_url: str | None = None,
    ) -> None:
        self._validate_token = validate_token or self._validate_token
        self._auth_url = auth_url or self._auth_url
        self._cache = (
            TTLCache(maxsize=cache_size, ttl=cache_ttl)
            if cache_size is not None and cache_ttl is not None
            else self._cache
        )

    @retry(stop=stop_after_attempt(RETRIES), wait=wait_fixed(1), retry=retry_if_exception_type(ClientConnectorError))
    async def validate_token_online(self, token: str) -> None:
        """Validate token by calling an external service if needed."""
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    self._auth_url,
                    headers={"Authorization": f"Bearer {token}"},
                    data={"token": token, "token_type_hint": "access_token"},
                )
            response.raise_for_status()
        except ClientResponseError as exc:
            raise InvalidTokenSignature() from exc

    async def get_user_from_token(self, token: str) -> UserDTO:
        """Main method that processes the token and returns UserDTO."""

        cached_user = self._cache.get(token)
        if cached_user:
            return cached_user

        payload = self.decode_token(token)

        # Optionally validate the token online
        if self._validate_token:
            if self.is_token_expired(payload):
                raise ExpiredToken()
            await self.validate_token_online(token)

        user_dto = UserDTO(id=payload.get("sub"), is_superuser=payload.get("is_superuser", False))

        self._cache[token] = user_dto

        return user_dto


def get_user(request: Request):
    return request.state.user if hasattr(request.state, "user") else None
