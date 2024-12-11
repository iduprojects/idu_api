import io
import json
from copy import deepcopy
from datetime import datetime

import httpx
import pytest
from PIL import Image
from playwright.async_api import async_playwright
from pydantic import BaseModel

from idu_api.urban_api.dto import ProjectDTO
from idu_api.urban_api.logic.impl.helpers import projects_objects
from idu_api.urban_api.schemas import Project, ProjectPatch, ProjectPost, ProjectPut

####################################################################################
#                               Authentication tests                               #
####################################################################################


@pytest.mark.asyncio
async def test_not_authorized_post_request(urban_api_host, project_post_req):
    """Test POST request's status code is 403 when not authorized."""

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=project_post_req)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_invalid_auth_token_post_request(urban_api_host, project_post_req):
    """Test POST request's status code is 401 when token is invalid."""

    headers = {"Authorization": f"Bearer invalid_token"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=project_post_req, headers=headers)

    assert response.status_code == 401


# ####################################################################################
# #                              Default use-case tests                              #
# ####################################################################################


@pytest.mark.asyncio
async def test_get_all_projects(urban_api_host, expired_auth_token):
    """Test GET to return list and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_user_projects(urban_api_host, expired_auth_token):
    """Test GET to return list and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/user_projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_post_project(urban_api_host, expired_auth_token, project_post_req):
    """Test POST to return correct json of project and status code 201."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1", timeout=5000) as client:
        response = await client.post("/projects", json=project_post_req, headers=headers)

    assert response.status_code == 201
    body: dict = response.json()
    assert body["name"] == project_post_req["name"]
    assert body["description"] == project_post_req["description"]
    assert body["public"] is project_post_req["public"]
    assert body.get("project_id") is not None


# ####################################################################################
# #                               Invalid data tests                                 #
# ####################################################################################


@pytest.mark.asyncio
async def test_invalid_data_post_request(urban_api_host, expired_auth_token, project_post_req):
    """Test POST requests' status code is 422 when invalid data is passed."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    invalid_project_territory_info = deepcopy(project_post_req)
    invalid_geometry_type = deepcopy(project_post_req)
    invalid_geometry_coordinates = deepcopy(project_post_req)

    invalid_project_territory_info["territory"] = "invalid"
    invalid_geometry_type["territory"]["geometry"]["type"] = "invalid"
    invalid_geometry_coordinates["territory"]["geometry"]["coordinates"] = "invalid"

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        invalid_project_territory_info_response = await client.post(
            "/projects", json=invalid_project_territory_info, headers=headers
        )
        invalid_geometry_type_response = await client.post("/projects", json=invalid_geometry_type, headers=headers)
        invalid_geometry_coordinates_response = await client.post(
            "/projects", json=invalid_geometry_coordinates, headers=headers
        )

    assert invalid_project_territory_info_response.status_code == 422
    assert invalid_geometry_type_response.status_code == 422
    assert invalid_geometry_coordinates_response.status_code == 422


####################################################################################
#                               E2E Swagger Tests                                  #
####################################################################################


@pytest.mark.asyncio
async def test_swagger_post_project_not_authorized(urban_api_host, expired_auth_token, project_post_req):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(f"{urban_api_host}/api/docs")

        await page.wait_for_selector("div.opblock.opblock-post:has-text('api/v1/projects')")

        endpoint_locator = page.locator("div.opblock.opblock-post:has-text('api/v1/projects')")
        assert await endpoint_locator.is_visible(), "Эндпоинт 'POST /api/v1/projects' не найден"
        await endpoint_locator.click()

        description = page.locator("div.opblock-description-wrapper div.opblock-description div.renderedMarkdown p")
        assert await description.inner_text() == "Add a new project.", "Описание метода не совпадает"

        try_it_out_button = page.locator("button:has-text('Try it out')")
        assert await try_it_out_button.is_visible(), "Кнопка 'Try it out' не найдена"
        await try_it_out_button.click()

        body_textarea = page.locator("textarea.body-param__text")
        assert await body_textarea.is_visible(), "Поле для ввода тела запроса не найдено"
        await body_textarea.fill(json.dumps(project_post_req, indent=4))

        execute_button = page.locator("button:has-text('Execute')")
        assert await execute_button.is_visible(), "Кнопка 'Execute' не найдена"
        await execute_button.click()

        await page.wait_for_selector("table.responses-table.live-responses-table tbody tr.response")

        response_locator = page.locator("table.responses-table.live-responses-table tbody tr.response")
        assert await response_locator.is_visible(), "Ответ не отображается"

        response_status_locator = response_locator.locator(".response-col_status").last
        response_status = await response_status_locator.inner_text()
        response_body_locator = response_locator.locator(".response-col_description pre").first
        response_body = await response_body_locator.inner_text()
        assert response_status[:3] == "403", (
            f"Запрос вернул некорректный статус: {response_status}\n" f"Тело ответа: {response_body}\n"
        )
        assert "Not authenticated" in response_body, "Ответ не содержит ожидаемых данных"

        await browser.close()


@pytest.mark.asyncio
async def test_swagger_post_invalid_project(urban_api_host, expired_auth_token, project_post_req):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(f"{urban_api_host}/api/docs")

        await page.wait_for_selector("button:has-text('Authorize')")

        auth_button = page.locator("button:has-text('Authorize')")
        assert await auth_button.is_visible(), "Кнопка 'Authorize' не найдена"
        await auth_button.click()

        bearer_input = page.locator("#auth-bearer-value")
        assert await bearer_input.is_visible(), "Поле ввода токена не найдено"

        await bearer_input.fill(expired_auth_token)

        authorize_button = page.locator("button:has-text('Authorize')").last
        await authorize_button.click()

        close_button = page.locator("button:has-text('Close')")
        await close_button.click()

        endpoint_locator = page.locator("div.opblock.opblock-post:has-text('api/v1/projects')")
        assert await endpoint_locator.is_visible(), "Эндпоинт 'POST /api/v1/projects' не найден"
        await endpoint_locator.click()

        description = page.locator("div.opblock-description-wrapper div.opblock-description div.renderedMarkdown p")
        assert await description.inner_text() == "Add a new project.", "Описание метода не совпадает"

        try_it_out_button = page.locator("button:has-text('Try it out')")
        assert await try_it_out_button.is_visible(), "Кнопка 'Try it out' не найдена"
        await try_it_out_button.click()

        body_textarea = page.locator("textarea.body-param__text")
        assert await body_textarea.is_visible(), "Поле для ввода тела запроса не найдено"
        project_post_req["territory"] = None
        await body_textarea.fill(json.dumps(project_post_req, indent=4))

        execute_button = page.locator("button:has-text('Execute')")
        assert await execute_button.is_visible(), "Кнопка 'Execute' не найдена"
        await execute_button.click()

        await page.wait_for_selector("table.responses-table.live-responses-table tbody tr.response")

        response_locator = page.locator("table.responses-table.live-responses-table tbody tr.response")
        assert await response_locator.is_visible(), "Ответ не отображается"

        response_status_locator = response_locator.locator(".response-col_status").last
        response_status = await response_status_locator.inner_text()
        response_body_locator = response_locator.locator(".response-col_description pre").first
        response_body = await response_body_locator.inner_text()
        assert response_status == "422", (
            f"Запрос вернул некорректный статус: {response_status}\n" f"Тело ответа: {response_body}\n"
        )
        assert (
            "Input should be a valid dictionary or object to extract fields from" in response_body
        ), "Ответ не содержит ожидаемых данных"

        await browser.close()


@pytest.mark.asyncio
async def test_swagger_post_project(urban_api_host, expired_auth_token, project_post_req):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(f"{urban_api_host}/api/docs")

        await page.wait_for_selector("button:has-text('Authorize')")

        auth_button = page.locator("button:has-text('Authorize')")
        assert await auth_button.is_visible(), "Кнопка 'Authorize' не найдена"
        await auth_button.click()

        bearer_input = page.locator("#auth-bearer-value")
        assert await bearer_input.is_visible(), "Поле ввода токена не найдено"

        await bearer_input.fill(expired_auth_token)

        authorize_button = page.locator("button:has-text('Authorize')").last
        await authorize_button.click()

        close_button = page.locator("button:has-text('Close')")
        await close_button.click()

        endpoint_locator = page.locator("div.opblock.opblock-post:has-text('api/v1/projects')")
        assert await endpoint_locator.is_visible(), "Эндпоинт 'POST /api/v1/projects' не найден"
        await endpoint_locator.click()

        description = page.locator("div.opblock-description-wrapper div.opblock-description div.renderedMarkdown p")
        assert await description.inner_text() == "Add a new project.", "Описание метода не совпадает"

        try_it_out_button = page.locator("button:has-text('Try it out')")
        assert await try_it_out_button.is_visible(), "Кнопка 'Try it out' не найдена"
        await try_it_out_button.click()

        body_textarea = page.locator("textarea.body-param__text")
        assert await body_textarea.is_visible(), "Поле для ввода тела запроса не найдено"
        await body_textarea.fill(json.dumps(project_post_req, indent=4))

        execute_button = page.locator("button:has-text('Execute')")
        assert await execute_button.is_visible(), "Кнопка 'Execute' не найдена"
        await execute_button.click()

        await page.wait_for_selector("table.responses-table.live-responses-table tbody tr.response")

        response_locator = page.locator("table.responses-table.live-responses-table tbody tr.response")
        assert await response_locator.is_visible(), "Ответ не отображается"

        response_status_locator = response_locator.locator(".response-col_status").last
        response_status = await response_status_locator.inner_text()
        response_body_locator = response_locator.locator(".response-col_description pre").first
        response_body = await response_body_locator.inner_text()
        assert response_status == "201", (
            f"Запрос вернул некорректный статус: {response_status}\n" f"Тело ответа: {response_body}\n"
        )
        assert "project_id" in response_body and "name" in response_body, "Ответ не содержит ожидаемых данных"

        await browser.close()


####################################################################################
#                          Unit tests business-logic                               #
####################################################################################


@pytest.mark.asyncio
async def test_add_project_to_db(mock_conn, project_post_req):
    """
    Test add_project_to_db function
    """

    # Arrange
    user_id = "mock_string"
    mocked_project = ProjectPost(**project_post_req)

    # Act
    result = await projects_objects.add_project_to_db(mock_conn, mocked_project, user_id)

    # Asserting
    assert isinstance(result, ProjectDTO)
    assert isinstance(result.project_id, int), "project_id должен быть числом"
    assert isinstance(result.user_id, str), "user_id должен быть строкой"
    assert isinstance(result.properties, dict), "properties должен быть словарем"
    assert isinstance(result.created_at, datetime), "created_at должен быть объектом datetime"
    assert isinstance(result.updated_at, datetime), "updated_at должен быть объектом datetime"
    assert isinstance(Project.from_dto(result), Project), "не удалось собрать pydantic модель из DTO"


@pytest.mark.asyncio
async def test_put_project_to_db(mock_conn, project_put_req):
    """
    Test put_project_to_db function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    mocked_project = ProjectPut(**project_put_req)

    # Act
    result = await projects_objects.put_project_to_db(mock_conn, mocked_project, project_id, user_id)

    # Asserting
    assert isinstance(result, ProjectDTO)
    assert isinstance(result.project_id, int), "project_id должен быть числом"
    assert isinstance(result.user_id, str), "user_id должен быть строкой"
    assert isinstance(result.properties, dict), "properties должен быть словарем"
    assert isinstance(result.created_at, datetime), "created_at должен быть объектом datetime"
    assert isinstance(result.updated_at, datetime), "updated_at должен быть объектом datetime"
    assert isinstance(Project.from_dto(result), Project), "не удалось собрать pydantic модель из DTO"


@pytest.mark.asyncio
async def test_patch_project_to_db(mock_conn, project_patch_req):
    """
    Test patch_project_to_db function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    mocked_project = ProjectPatch(**project_patch_req)

    # Act
    result = await projects_objects.patch_project_to_db(mock_conn, mocked_project, project_id, user_id)

    # Asserting
    assert isinstance(result, ProjectDTO)
    assert isinstance(result.project_id, int), "project_id должен быть числом"
    assert isinstance(result.user_id, str), "user_id должен быть строкой"
    assert isinstance(result.properties, dict), "properties должен быть словарем"
    assert isinstance(result.created_at, datetime), "created_at должен быть объектом datetime"
    assert isinstance(result.updated_at, datetime), "updated_at должен быть объектом datetime"
    assert isinstance(Project.from_dto(result), Project), "не удалось собрать pydantic модель из DTO"


@pytest.mark.asyncio
async def test_delete_project_to_db(mock_conn, mock_minio_client):
    """
    Test delete_project_to_db function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    file_data = io.BytesIO()
    await mock_minio_client.upload_file(file_data, "projects/1/")

    # Act
    result = await projects_objects.delete_project_from_db(mock_conn, project_id, mock_minio_client, user_id)

    # Asserting
    assert isinstance(result, dict)
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_upload_project_image_to_minio_without_error(mock_conn, mock_minio_client):
    """
    Test upload_project_image_to_minio function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    img = Image.new("RGB", (60, 30), color="red")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="PNG")
    img_byte_arr = img_byte_arr.getvalue()

    # Act
    result = await projects_objects.upload_project_image_to_minio(
        mock_conn, mock_minio_client, project_id, user_id, img_byte_arr
    )

    # Asserting
    assert isinstance(result, dict)
    assert "image_url" in result
    assert "preview_url" in result
