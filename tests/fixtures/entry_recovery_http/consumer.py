import asyncio
import hashlib
import importlib.metadata
import json
import os
import signal
import sys
import time
from pathlib import Path

import httpx
from durable_workflow import Client
from ui import repair_in_waterline

ROOT = Path('/phase/consumer')
BASE = '''import os
from pathlib import Path
from durable_workflow import workflow
@workflow.defn(name="general221.entry")
class WaitingWorkflow:
    def __init__(self):
        self.ready = False
    @workflow.signal("ready")
    def receive(self):
        self.ready = True
    def run(self, ctx, amount):
{body}
'''
WAIT = '        yield ctx.wait_condition(lambda: self.ready, key="ready")\n'
RETURN = '        return {"amount": amount, "typed": [9223372036854775807, None, "Київ"]}\n'
BODY_MARKER = '        Path(os.environ["BODY_TRACE"]).write_text("entered")\n'


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


async def until(check, label, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError(label)


async def main():
    assert importlib.metadata.version('durable-workflow') == '2.8.0'
    case, cohort = sys.argv[1:]
    identity = cohort + '-' + case
    directory = ROOT / identity
    directory.mkdir(parents=True)
    module = directory / 'definition.py'
    after_wait = case in ('required-after', 'body-after')
    old_source = BASE.format(body=BODY_MARKER + (WAIT if after_wait else '') + RETURN)
    new_source = old_source
    if case.startswith('required-'):
        new_source = new_source.replace('ctx, amount)', 'ctx, amount, required)')
    elif case.startswith('body-'):
        new_source = new_source.replace(RETURN, '        raise TypeError("application-body-control")\n')
    elif case == 'optional':
        new_source = new_source.replace('ctx, amount)', 'ctx, amount, extra=None)')
    else:
        raise AssertionError(case)
    module.write_text(old_source)
    processes = []
    fixed = os.environ.get('EXPECTED_BINDING_REFUSAL') == '1'

    async def worker(which, enabled):
        gate, trace = directory / (which + '.gate'), directory / (which + '.jsonl')
        if enabled:
            gate.touch()
        env = {**os.environ, 'TASK_QUEUE': 'q-' + identity, 'WORKER_ID': identity + '-' + which,
               'TRACE': str(trace), 'GATE': str(gate),
               'BODY_TRACE': str(directory / (which + '.entered')),
               'PYTHONPATH': str(directory)}
        log = (directory / (which + '.log')).open('w')
        process = await asyncio.create_subprocess_exec(sys.executable, '/phase/worker.py', env=env,
                    stdout=log, stderr=asyncio.subprocess.STDOUT)
        processes.append((process, log))

        async def registered():
            assert process.returncode is None, (case, which, process.returncode)
            return any(row['kind'] == 'registered' for row in records(trace))

        await until(registered, 'registration ' + which)
        return process, trace

    async def stop(process):
        if process.returncode is None:
            process.send_signal(signal.SIGTERM)
        code = await asyncio.wait_for(process.wait(), 8)
        assert code == 0, (case, 'worker exit', code)

    headers = {'Authorization': 'Bearer general221-fixture', 'X-Namespace': 'default',
               'X-Durable-Workflow-Control-Plane-Version': '2'}
    try:
        async with httpx.AsyncClient(base_url='http://api:8080', headers=headers, timeout=5) as http, Client('http://api:8080', token='general221-fixture') as client:
            async def get(path):
                response = await http.get(path)
                response.raise_for_status()
                return response.json()

            old, old_trace = await worker('old', after_wait)
            handle = await client.start_workflow(workflow_type='general221.entry', workflow_id='original-' + identity,
                        task_queue='q-' + identity, input=[7], run_timeout_seconds=30, execution_timeout_seconds=60)
            route = '/api/workflows/' + handle.workflow_id
            history_route = route + '/runs/' + handle.run_id + '/history'
            if after_wait:
                async def waiting():
                    return any(row['kind'] == 'completed_task' for row in records(old_trace))
                await until(waiting, 'original committed wait')
            before = await get(route)
            prefix = await get(history_route)
            (directory / 'original.json').write_text(json.dumps({'case': case, 'before': before, 'prefix': prefix}))
            await stop(old)
            module.write_text(new_source)
            new, new_trace = await worker('new', True)
            if after_wait:
                await handle.signal('ready', [])

            async def terminal_or_refused():
                state = await get(route)
                return state if state['status'] in ('completed', 'failed') or any(row['kind'] == 'failed_task' for row in records(new_trace)) else None

            after = await until(terminal_or_refused, 'terminal or refused observation')
            await stop(new)
            history = await get(history_route)
            assert after['run_id'] == before['run_id'] == handle.run_id
            for key in ('run_deadline_at', 'execution_deadline_at'):
                assert before[key] == after[key] and after[key], (key, before, after)
            assert prefix['events'] == history['events'][:len(prefix['events'])]
            refusal, debug, repair, waterline = None, None, None, None
            if fixed and case.startswith('required-'):
                refusal = after
                assert not after['is_terminal'], after
                failed = [row for row in records(new_trace) if row['kind'] == 'failed_task']
                assert len(failed) == 1, failed
                assert failed[0]['request']['failure_type'] == 'WorkflowEntryArgumentsMismatchError', failed
                assert failed[0]['request']['failure_reason'] == 'workflow_entry_arguments_mismatch', failed
                assert not (directory / 'new.entered').exists(), 'incompatible entry executed application code'
                assert 'workflow_entry_arguments_mismatch' in failed[0]['request']['message']
                assert not any(row['kind'] == 'completed_task' for row in records(new_trace))
                assert not any(row['event_type'] in ('WorkflowFailed', 'WorkflowCompleted') for row in history['events'])
                debug = await get(route + '/runs/' + handle.run_id + '/debug')
                assert 'WorkflowEntryArgumentsMismatchError' in json.dumps(debug), debug
                assert '"replay_blocked": true' in json.dumps(debug), debug
                module.write_text(old_source)
                recovery, recovery_trace = await worker('recovery', True)
                waterline = await repair_in_waterline(handle.workflow_id, handle.run_id, case, directory)
                repair = waterline['repair_response']
                result = await handle.result(timeout=12)
                assert result == {'amount': 7, 'typed': [9223372036854775807, None, 'Київ']}, result
                await stop(recovery)
                after = await get(route)
                history = await get(history_route)
                assert after['run_id'] == handle.run_id and after['status'] == 'completed'
                assert sum(row['event_type'] == 'WorkflowCompleted' for row in history['events']) == 1
                for key in ('run_deadline_at', 'execution_deadline_at'):
                    assert before[key] == after[key]
                assert prefix['events'] == history['events'][:len(prefix['events'])]
            elif case == 'optional':
                assert (directory / 'new.entered').exists()
                assert after['status'] == 'completed', after
                assert await handle.result(timeout=2) == {'amount': 7, 'typed': [9223372036854775807, None, 'Київ']}
            else:
                assert (directory / 'new.entered').exists(), 'business failure must execute the body'
                assert after['status'] == 'failed', after
                assert any(row['event_type'] == 'WorkflowFailed' for row in history['events']), history
            evidence = {'case': case, 'cohort': cohort, 'sdk': importlib.metadata.version('durable-workflow'),
                        'before': before, 'after': after, 'prefix': prefix, 'history': history,
                        'old_source_sha256': hashlib.sha256(old_source.encode()).hexdigest(),
                        'new_source_sha256': hashlib.sha256(new_source.encode()).hexdigest(),
                        'new_trace': records(new_trace), 'refusal': refusal, 'debug': debug, 'repair': repair,
                        'new_body_entered': (directory / 'new.entered').exists(),
                        'waterline': waterline,
                        'worker_exits': [p.returncode for p, _ in processes]}
            (directory / 'evidence.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
            print(json.dumps({'case': case, 'cohort': cohort, 'run_id': handle.run_id, 'status': after['status'],
                              'events': len(history['events']), 'worker_exits': evidence['worker_exits']}), flush=True)
    finally:
        for process, log in processes:
            if process.returncode is None:
                process.send_signal(signal.SIGTERM)
                try:
                    await asyncio.wait_for(process.wait(), 8)
                except TimeoutError:
                    process.kill()
                    await process.wait()
            log.close()


asyncio.run(asyncio.wait_for(main(), timeout=30))
