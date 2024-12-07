import pytest
from playwright.async_api import async_playwright


@pytest.mark.asyncio
async def test_swagger_ui(urban_api_host):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(f"{urban_api_host}/api/docs")

        title = await page.title()
        assert title == "Digital Territories Platform Data API - Swagger UI", "Неверный заголовок страницы"

        content = await page.content()
        assert "swagger-ui" in content, "Элементы Swagger UI не найдены"

        await page.wait_for_selector(".opblock-summary")

        endpoints = page.locator(".opblock-summary")
        endpoint_count = await endpoints.count()
        assert endpoint_count > 0, "Нет доступных эндпоинтов в Swagger UI"

        first_endpoint = endpoints.nth(0)
        await first_endpoint.click()

        try_it_out_button = page.locator("button:has-text('Try it out')")
        assert await try_it_out_button.is_visible(), "Кнопка 'Try it out' недоступна"
        await try_it_out_button.click()

        execute_button = page.locator("button:has-text('Execute')")
        assert await execute_button.is_visible(), "Кнопка 'Execute' недоступна"
        await execute_button.click()

        response_locator = page.locator(".responses-table").first
        assert await response_locator.is_visible(), "Ответ не отображается"

        response_status_locator = response_locator.locator(".response-col_status").last
        response_status = await response_status_locator.inner_text()
        assert response_status == "200", f"Запрос вернул некорректный статус: {response_status}"

        await browser.close()
