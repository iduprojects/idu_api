"""Helper functions are defined here."""

from typing import Literal

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.logic.impl.helpers.utils import UrbanAPIModel


def assert_response(
    response: httpx.Response,
    expected_status: int,
    validation_model: type[UrbanAPIModel],
    error_message: str | None = None,
    result_type: Literal["dict", "list"] = "dict",
) -> None:
    """Validates the API response according to the test case specifications.

    Args:
        response (httpx.Response): The server response to validate the status code.
        expected_status (int): The expected HTTP status code.
        validation_model (UrbanAPIModel): The Pydantic model to validate successful responses.
        error_message (str | None): The expected substring in the error message.
        result_type (str): The expected type of response (dict or list).

    Raises:
        AssertionError: If any of the basic validations fail.
        pytest.fail: If there are Pydantic model validation errors.
    """
    result = response.json()

    # Basic validations applicable to all cases
    assert response.status_code == expected_status, f"Invalid status code: {response.status_code}.\n{result}"
    assert isinstance(result, eval(result_type)), f"Result should be a {result_type}.\n{result}"

    # Logic for successful responses
    if expected_status in (200, 201):
        try:
            if result_type == "dict":
                validation_model(**result)
            else:
                for item in result:
                    validation_model(**item)
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        return

    # Logic for error responses
    assert "detail" in result, "Response should contain a 'detail' field with the error message"
    if error_message is not None:
        assert error_message.lower() in result["detail"].lower(), (
            f"Error message should contain '{error_message}'. " f"Actual message: {result['detail']}"
        )
