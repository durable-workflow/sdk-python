import datetime
import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path

root = Path('/phase')
path = root / 'data/database.sqlite'
files = [p for p in (path, Path(str(path) + '-wal')) if p.exists()]
hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
copy = Path('/audit')
for source in files:
    shutil.copyfile(source, copy / source.name)
db = sqlite3.connect('file:' + str(copy / path.name) + '?mode=ro', uri=True)
db.row_factory = sqlite3.Row
db.execute('PRAGMA query_only=ON')
results = []
for evidence_path in sorted((root / 'consumer').glob('*/evidence.json')):
    evidence = json.loads(evidence_path.read_text())
    run_id = evidence['after']['run_id']
    run = dict(db.execute('SELECT * FROM workflow_runs WHERE id=?', (run_id,)).fetchone())
    tasks = [dict(row) for row in db.execute(
        'SELECT id,status,attempt_count,repair_count FROM workflow_tasks WHERE workflow_run_id=? ORDER BY id', (run_id,),
    )]
    history = [dict(row) for row in db.execute(
        'SELECT sequence,event_type FROM workflow_history_events WHERE workflow_run_id=? ORDER BY sequence', (run_id,),
    )]
    assert run['status'] == evidence['after']['status']
    assert [(r['sequence'], r['event_type']) for r in history] == [
        (r['sequence'], r['event_type']) for r in evidence['history']['events']
    ]
    assert evidence['prefix']['events'] == evidence['history']['events'][:len(evidence['prefix']['events'])]
    started = datetime.datetime.fromisoformat(run['started_at'])
    assert (datetime.datetime.fromisoformat(run['run_deadline_at']) - started).total_seconds() == 30
    assert (datetime.datetime.fromisoformat(run['execution_deadline_at']) - started).total_seconds() == 60
    assert started.microsecond != 0
    if run['status'] == 'completed':
        assert all(task['status'] == 'completed' for task in tasks), tasks
    else:
        assert all(task['status'] in ('completed', 'failed') for task in tasks), tasks
        assert sum(row['event_type'] == 'WorkflowFailed' for row in history) == 1
    repaired = evidence.get('refusal') is not None
    assert sum(task['repair_count'] for task in tasks) == int(repaired), tasks
    assert sum(task['attempt_count'] - 1 for task in tasks) == int(repaired), tasks
    assert sum(row['event_type'] == 'WorkflowCompleted' for row in history) == int(run['status'] == 'completed')
    if repaired:
        assert not evidence['refusal']['is_terminal']
        assert not any(row['event_type'] == 'WorkflowFailed' for row in history)
    assert all(code == 0 for code in evidence['worker_exits'])
    results.append({'case': evidence['case'], 'cohort': evidence['cohort'], 'run_id': run_id,
                    'status': run['status'], 'tasks': len(tasks), 'repairs': int(repaired),
                    'events': len(history), 'worker_exits': evidence['worker_exits'],
                    'run_deadline_at': run['run_deadline_at'], 'execution_deadline_at': run['execution_deadline_at']})
assert len(results) == 2, results
failed_originals = []
for original_path in sorted((root / 'consumer').glob('*/original.json')):
    if (original_path.parent / 'evidence.json').exists():
        continue
    original = json.loads(original_path.read_text())
    run_id = original['before']['run_id']
    run = db.execute('SELECT status FROM workflow_runs WHERE id=?', (run_id,)).fetchone()
    tasks = [dict(row) for row in db.execute(
        'SELECT status,attempt_count,repair_count FROM workflow_tasks WHERE workflow_run_id=?', (run_id,),
    )]
    history = [row[0] for row in db.execute(
        'SELECT event_type FROM workflow_history_events WHERE workflow_run_id=? ORDER BY sequence', (run_id,),
    )]
    assert run['status'] == 'pending', (run_id, dict(run))
    assert tasks == [{'status': 'failed', 'attempt_count': 1, 'repair_count': 0}], tasks
    assert history == ['StartAccepted', 'WorkflowStarted'], history
    assert (original_path.parent / 'waterline-failure.txt').exists()
    failed_originals.append({'run_id': run_id, 'status': run['status'], 'repairs': 0, 'events': len(history)})
assert db.execute('SELECT COUNT(*) FROM workflow_runs').fetchone()[0] == len(results) + len(failed_originals)
assert sum(row['status'] == 'completed' for row in results) == 2
assert sum(row['status'] == 'failed' for row in results) == 0
assert db.execute('SELECT COUNT(*) FROM workflow_worker_registrations').fetchone()[0] == 0
db.close()
assert hashes == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
assert hashes == {p.name: hashlib.sha256((copy / p.name).read_bytes()).hexdigest() for p in files}
print(json.dumps({'sdk': '2.8.0', 'server': '2.10.1', 'waterline': '2.12.1', 'cases': results,
                  'waterline_asset_source': os.environ.get('WATERLINE_ASSET_SOURCE', 'published'),
                  'failed_originals': failed_originals,
                  'events': sum(r['events'] for r in results),
                  'tasks': sum(r['tasks'] for r in results), 'repairs': sum(r['repairs'] for r in results),
                  'workers': sum(len(r['worker_exits']) for r in results), 'database_and_wal_unchanged': True,
                  'source_hashes': hashes}, indent=2))
