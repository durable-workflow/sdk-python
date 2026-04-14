from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    server_url = os.environ.get("DURABLE_WORKFLOW_SERVER_URL", "")
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            if not server_url:
                item.add_marker(pytest.mark.skip(reason="DURABLE_WORKFLOW_SERVER_URL not set"))


@pytest.fixture(scope="session")
def server_url() -> str:
    return os.environ["DURABLE_WORKFLOW_SERVER_URL"]


@pytest.fixture(scope="session")
def server_token() -> str:
    return os.environ.get("DURABLE_WORKFLOW_AUTH_TOKEN", "dev-token-123")
