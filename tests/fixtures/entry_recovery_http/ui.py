"""Real published Waterline interaction. No routes or backend replies are mocked."""

import os
from pathlib import Path

from playwright.async_api import async_playwright


async def repair_in_waterline(workflow_id, run_id, case, directory):
    mobile = case == 'required-after'
    theme = 'light' if mobile else 'dark'
    viewport = {'width': 390, 'height': 844} if mobile else {'width': 1440, 'height': 900}
    root = os.environ.get('WATERLINE_URL', 'http://viewer:8080/waterline').rstrip('/')
    selected_path = f'/api/instances/{workflow_id}/runs/{run_id}/repair'
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(args=['--no-sandbox'])
        try:
            page = await browser.new_page(viewport=viewport)
            page.set_default_timeout(5000)
            await page.add_init_script(f"localStorage.setItem('waterline-theme', '{theme}')")
            await page.goto(f'{root}/flows/instances/{workflow_id}/runs/{run_id}', wait_until='domcontentloaded', timeout=8000)
            await page.get_by_role('button', name='Inspect full details', exact=True).click()
            reason = page.get_by_text('workflow_entry_arguments_mismatch', exact=False).first
            await reason.wait_for(state='visible')
            await reason.scroll_into_view_if_needed()
            assert await page.locator('body').evaluate('(body) => body.scrollWidth <= innerWidth + 1')
            await page.screenshot(path=str(Path(directory) / 'waterline-refusal.png'), full_page=True)
            await page.get_by_role('button', name='Repair', exact=True).click()
            await page.get_by_text('Repair run?', exact=True).wait_for(state='visible')
            async with page.expect_response(
                lambda response: response.request.method == 'POST' and response.url.endswith(selected_path),
                timeout=8000,
            ) as captured:
                await page.get_by_role('button', name='Repair run', exact=True).click()
            response = await captured.value
            assert response.status == 200, await response.text()
            result = await response.json()
            return {'url': page.url, 'viewport': viewport, 'theme': theme,
                    'selected_run_repair_url': response.url, 'repair_response': result}
        except Exception as error:
            if 'page' in locals():
                await page.screenshot(path=str(Path(directory) / 'waterline-failure.png'), full_page=True)
                text = await page.locator('body').inner_text()
                (Path(directory) / 'waterline-failure.txt').write_text(str(error)[:1000] + '\n' + text[:6000])
            raise
        finally:
            await browser.close()
