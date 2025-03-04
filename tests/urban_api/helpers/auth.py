"""All fixtures for authentication tests are defined here."""

import base64
import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

import pytest

__all__ = ["expired_token", "superuser_token", "valid_token"]

SECRET_KEY = "test_secret_key"


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture(scope="session")
def superuser_token() -> str:
    """Valid authentication access JWT token."""
    expiration_time = datetime.now(timezone.utc) + timedelta(hours=1)
    payload = {"sub": "admin", "is_superuser": True, "exp": int(expiration_time.timestamp())}

    token = create_jwt(payload, SECRET_KEY)
    return token


@pytest.fixture(scope="session")
def valid_token() -> str:
    """Valid authentication access JWT token."""
    expiration_time = datetime.now(timezone.utc) + timedelta(hours=1)
    payload = {"sub": "user1", "is_superuser": False, "exp": int(expiration_time.timestamp())}

    token = create_jwt(payload, SECRET_KEY)
    return token


@pytest.fixture(scope="session")
def expired_token() -> str:
    """Expired authentication access JWT token."""
    expiration_time = datetime.now(timezone.utc) - timedelta(hours=1)
    payload = {"sub": "user1", "is_superuser": False, "exp": int(expiration_time.timestamp())}

    token = create_jwt(payload, SECRET_KEY)
    return token


####################################################################################
#                                 Helpers                                          #
####################################################################################


def base64url_encode(data: bytes) -> str:
    """Encodes data in the Base64 URL format."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def create_jwt(payload: dict, secret_key: str) -> str:
    """Creates a JWT token."""
    # JWT header
    header = {"alg": "HS256", "typ": "JWT"}

    # Encoding the header and payload in Base64URL
    header_encoded = base64url_encode(json.dumps(header).encode("utf-8"))
    payload_encoded = base64url_encode(json.dumps(payload).encode("utf-8"))

    # Creating a signature
    message = (header_encoded + "." + payload_encoded).encode("utf-8")
    signature = hmac.new(secret_key.encode("utf-8"), message, hashlib.sha256).digest()
    signature_encoded = base64url_encode(signature)

    # Collecting a JWT token
    jwt_token = header_encoded + "." + payload_encoded + "." + signature_encoded
    return jwt_token
