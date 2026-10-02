"""A killable owner for prepared callback and canonical cleanup qualification."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from tests.integration.test_prepared_local_activity import TrackingPreparedClient, prepared_worker


async def main() -> None:
    queue, owner, mode, trace = sys.argv[1:]
    if mode not in {"hold", "finish"}:
        raise ValueError("unknown prepared qualification mode")
    os.environ["DW_PREPARED_FIXTURE_MODE"] = mode
    async with TrackingPreparedClient(
        os.environ["DURABLE_WORKFLOW_SERVER_URL"],
        token=os.environ.get("DURABLE_WORKFLOW_AUTH_TOKEN", "test-token"), namespace="default", trace_path=Path(trace),
    ) as client:
        await prepared_worker(client, queue, worker_id=owner).run()


if __name__ == "__main__":
    asyncio.run(main())
