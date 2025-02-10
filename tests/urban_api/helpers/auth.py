import pytest

__all__ = ["expired_auth_token"]


@pytest.fixture
def expired_auth_token() -> str:
    """Fixture to get expired auth token. Useful when expiration check is disabled in API."""

    return (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiJhZG1pbkB0ZXN0LnJ1IiwiaWF0IjoxNzI5OTIyMTA1LCJleHAiOjE3Mjk5MjM5MDUsImNpdGllc19pZCI6WzEsMiw1LDEwL"
        "DEzLDE0LDE3LDE5LDIwLDIxLDIyLDIzLDI0LDE4LDI1LDI2LDI4LDI5XSwic2NvcGVzIjpbInNlcnZpY2VzLmhpZXJhcmNoeV9vYmplY3"
        "RzOmRhdGFfZWRpdCIsImRhdGEuY2l0eV9vYmplY3RzOmRhdGFfZWRpdCIsInBvcHVsYXRpb24ubGl2aW5nX21vZGVsOmRhdGFfZWRpdCJ"
        "dLCJpc19zdXBlcnVzZXIiOnRydWV9."
        "bpIr04RCQFOLu283dYR6kAGe8eKT1YCaAS-UQtD25Gk"
    )