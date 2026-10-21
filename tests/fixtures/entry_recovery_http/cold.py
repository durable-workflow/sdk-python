import asyncio
import json
import sys

from durable_workflow import Client


async def main():
    workflow_id, run_id = sys.argv[1:]
    async with Client('http://api:8080', token='general221-fixture') as client:
        handle = client.get_workflow_handle(workflow_id, run_id=run_id, workflow_type='general221.entry')
        result = await handle.result(timeout=3)
        assert result == {'amount': 7, 'typed': [9223372036854775807, None, 'Київ']}, result
        print(json.dumps({'run_id': run_id, 'typed_result': 'exact'}))


asyncio.run(main())
