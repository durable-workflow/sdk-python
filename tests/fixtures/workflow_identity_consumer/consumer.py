"""Normally installed identity acceptance against a real published Server."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.metadata
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import durable_workflow
from durable_workflow import Client, Worker, activity, workflow

STATE = Path('/state')
URL = os.environ['SERVER_URL']
COHORT = os.environ['IDENTITY_COHORT']


def verify_install():
    dist = importlib.metadata.distribution('durable-workflow')
    assert dist.version == os.environ['IDENTITY_SDK_VERSION']
    assert '/opt/venv/' in str(Path(durable_workflow.__file__).resolve())
    hashes = []
    for entry in dist.files or []:
        if entry.hash is None:
            continue
        assert entry.hash.mode == 'sha256'
        path = Path(dist.locate_file(entry))
        digest = base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest()).decode().rstrip('=')
        assert digest == entry.hash.value, str(entry)
        hashes.append(str(entry))
    sources = list(Path('/expected-source').rglob('*.py'))
    installed = Path(durable_workflow.__file__).parent
    for source in sources:
        assert source.read_bytes() == (installed / source.relative_to('/expected-source')).read_bytes(), str(source)
    return {'version': dist.version, 'hashed_record_entries': len(hashes), 'matched_source_files': len(sources)}


@activity.defn(name='qualification.identity-echo')
def echo(value):
    return value


@workflow.defn(name='qualification.workflow-identity')
class IdentityWorkflow:
    def run(self, ctx, again=False, prior=None):
        if again:
            yield ctx.continue_as_new(False, [ctx.workflow_id, ctx.run_id])
            return None
        before = [ctx.workflow_id, ctx.run_id]
        value = yield ctx.schedule_activity('qualification.identity-echo', [before + [9007199254740993, None, 'λ']])
        return {'before': before, 'after': [ctx.workflow_id, ctx.run_id], 'prior': prior, 'echo': value}


class ObservedClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.registered = asyncio.Event()

    async def register_worker(self, **kwargs):
        reply = await super().register_worker(**kwargs)
        self.registered.set()
        return reply


@asynccontextmanager
async def node(namespace, queue, role):
    async with ObservedClient(URL, token='identity-worker', namespace=namespace) as client:
        worker = Worker(
            client, task_queue=queue,
            workflows=[IdentityWorkflow] if role != 'activity' else [],
            activities=[echo] if role == 'activity' else [],
            worker_id=f'{COHORT}-{namespace}-{queue}-{role}',
            poll_timeout=1, heartbeat_interval=1,
        )
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(client.registered.wait(), 8)
            yield
        finally:
            await worker.stop()
            await asyncio.wait_for(task, 5)


async def wait_event(client, workflow_id, event_type):
    while True:
        desc = await client.describe_workflow(workflow_id)
        history = await client.get_history(workflow_id, desc.run_id)
        if any(event['event_type'] == event_type for event in history['events']):
            return desc.run_id, history['events']
        await asyncio.sleep(0.1)


async def original(namespace, workflow_id, again):
    started = time.monotonic()
    queue = 'identity-' + workflow_id
    async with Client(URL, token='identity-client', namespace=namespace) as client:
        handle = await client.start_workflow(
            workflow_type='qualification.workflow-identity', task_queue=queue,
            workflow_id=workflow_id, input=[again],
            run_timeout_seconds=30, execution_timeout_seconds=30,
        )
        original_run_id = handle.run_id
        async with node(namespace, queue, 'first'):
            final_run_id, _ = await wait_event(client, workflow_id, 'ActivityScheduled')
        async with node(namespace, queue, 'activity'):
            await wait_event(client, workflow_id, 'ActivityCompleted')
        async with node(namespace, queue, 'cold'):
            result = await client.get_workflow_handle(workflow_id, run_id=final_run_id).result(timeout=20)
        expected = [workflow_id, final_run_id]
        assert result == {
            'before': expected, 'after': expected,
            'prior': [workflow_id, original_run_id] if again else None,
            'echo': expected + [9007199254740993, None, 'λ'],
        }, result
        assert (final_run_id != original_run_id) is again
        original_history = (await client.get_history(workflow_id, original_run_id))['events']
        assert any(e['event_type'] == 'WorkflowContinuedAsNew' for e in original_history) is again
        history = (await client.get_history(workflow_id, final_run_id))['events']
        assert sum(e['event_type'] == 'ActivityStarted' for e in history) == 1
        assert sum(e['event_type'] == 'WorkflowCompleted' for e in history) == 1
        elapsed = time.monotonic() - started
        assert elapsed < 30
        return {'namespace': namespace, 'workflow_id': workflow_id, 'original_run_id': original_run_id,
                'run_id': final_run_id, 'continued': again, 'result': result, 'elapsed_seconds': elapsed,
                'history': history, 'original_history': original_history}


async def main():
    installation = verify_install()
    stage = sys.argv[1]
    path = STATE / 'originals.json'
    if stage == 'execute':
        async with Client(URL, token='identity-admin') as admin:
            await admin.create_namespace(name='identity-named')
        originals = []
        path.write_text('[]')
        for namespace in ['default', 'identity-named']:
            prefix = '' if namespace == 'default' else 'named-'
            for suffix, again in [('0', False), ('17', False), ('017', False), ('continue', True)]:
                result = await asyncio.wait_for(original(namespace, prefix + suffix, again), 30)
                originals.append(result)
                path.write_text(json.dumps(originals, ensure_ascii=False, indent=2))
                print(json.dumps({'workflow_id': result['workflow_id'], 'namespace': namespace,
                                  'elapsed_seconds': result['elapsed_seconds'], 'passed': True}), flush=True)
    elif stage == 'cold':
        originals = json.loads(path.read_text())
        assert len(originals) == 8
        for record in originals:
            async with Client(URL, token='identity-client', namespace=record['namespace']) as client:
                handle = client.get_workflow_handle(record['workflow_id'], run_id=record['run_id'])
                assert await handle.result(timeout=5) == record['result']
                assert (await handle.get_history())['events'] == record['history']
                assert (await client.get_history(record['workflow_id'], record['original_run_id']))['events'] == record['original_history']
    else:
        raise ValueError('Choose execute or cold.')
    assert verify_install() == installation
    print(json.dumps({'stage': stage, 'installation': installation, 'originals': len(originals)}), flush=True)


asyncio.run(main())
