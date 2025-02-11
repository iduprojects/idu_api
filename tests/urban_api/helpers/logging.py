import pytest
import structlog

__all__ = ["logger"]


@pytest.fixture(scope="session")
def logger():
    logger: structlog.stdlib.BoundLogger = structlog.get_logger("test")
    return logger