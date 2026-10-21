import asyncio
import json
import os
import signal
import time
from pathlib import Path

from durable_workflow import Client, Worker
from definition import WaitingWorkflow

TRACE = Path(os.environ['TRACE'])
STOP = asyncio.Event()


def trace(record):
    with TRACE.open('a') as stream:
        stream.write(json.dumps({'time': time.monotonic(), **record}, ensure_ascii=False) + '\n')


class ObservedClient(Client):
    async def register_worker(self, **kwargs):
        result = await super().register_worker(**kwargs)
        trace({'kind': 'registered', 'worker_id': kwargs.get('worker_id')})
        return result

    async def poll_workflow_task(self, **kwargs):
        gate = Path(os.environ['GATE'])
        while not gate.exists():
            if STOP.is_set():
                return None
            await asyncio.sleep(0.02)
        task = await super().poll_workflow_task(**kwargs)
        trace({'kind': 'poll', 'task': task})
        return task

    async def complete_workflow_task(self, **kwargs):
        response = await super().complete_workflow_task(**kwargs)
        trace({'kind': 'completed_task', 'request': kwargs, 'response': response})
        return response

    async def fail_workflow_task(self, **kwargs):
        response = await super().fail_workflow_task(**kwargs)
        trace({'kind': 'failed_task', 'request': kwargs, 'response': response})
        return response


async def main():
    async with ObservedClient('http://api:8080', token='general221-worker') as client:
        worker = Worker(client, task_queue=os.environ['TASK_QUEUE'], workflows=[WaitingWorkflow],
                        worker_id=os.environ['WORKER_ID'], poll_timeout=1, heartbeat_interval=1)
        loop = asyncio.get_running_loop()

        def shutdown():
            STOP.set()
            asyncio.create_task(worker.stop())

        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, shutdown)
        await worker.run()


asyncio.run(main())
