"""Install verification and browser readiness before business admission."""

import asyncio
import hashlib
import json
import urllib.request
from pathlib import Path

import httpx
from playwright.async_api import async_playwright

with urllib.request.urlopen('https://pypi.org/pypi/durable-workflow/2.8.0/json', timeout=20) as response:
    release = json.load(response)
wheel = next(file for file in release['urls'] if file['filename'].endswith('.whl'))
with urllib.request.urlopen(wheel['url'], timeout=20) as response:
    content = response.read()
assert hashlib.sha256(content).hexdigest() == '908267666c35890fce13ab9c8e62a5c93abac78bdffbb0e56423eb3b3e57a77b'
Path('/phase/sdk.whl').write_bytes(content)


async def ready():
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.get('http://api:8080/api/ready')
        assert response.status_code == 200, response.text
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(args=['--no-sandbox'])
        try:
            page = await browser.new_page()
            response = await page.goto('http://viewer:8080/waterline', wait_until='domcontentloaded', timeout=10000)
            assert response.status == 200
        finally:
            await browser.close()
    Path('/phase/setup.ready').write_text('ready')
    print('API, viewer and actual browser ready; no workflow admitted.', flush=True)


asyncio.run(ready())
