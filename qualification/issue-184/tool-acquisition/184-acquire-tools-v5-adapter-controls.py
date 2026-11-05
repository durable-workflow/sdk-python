"""Synthetic preallocated-pool adapter controls; no acquisition or tool execution."""
from __future__ import annotations

from contextlib import contextmanager
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
from unittest.mock import patch

sys.dont_write_bytecode=True
BASE=Path('/tmp/sdk-python-handoff')
REF='https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-9999999999'


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


owner=load('trusted_v5_owner_adapter',BASE/'184-acquire-tools-v5.py')
guard=load('trusted_reviewed_guard_custody',BASE/'184-acquisition-custody-guardian-v5-proposal.py')
assert hashlib.sha256((BASE/'184-acquisition-custody-guardian-v5-proposal.py').read_bytes()).hexdigest()== \
    '4c92d889d89dbef2502ef5e9bc923d00c0314821b42aa77d4c0ee92080aba41e'
results=[]


def check(name,operation):
    try:operation()
    except BaseException as error:
        results.append({'id':name,'outcome':'fail','type':type(error).__name__,'detail':str(error)[:160]})
    else:results.append({'id':name,'outcome':'pass'})


def refuses(operation,code):
    try:operation()
    except owner.Refusal as error:assert error.code==code,(error.code,code)
    else:raise AssertionError('required refusal absent: '+code)


@contextmanager
def fixture():
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-adapter-controls-') as temporary:
        parent=Path(temporary)
        custody=guard.ArchiveCustody(parent,'sdk-python-184-tools-guardian-14adc40b-9999999999',['one.whl','two.whl'])
        ids={'.':list(custody.root_id),'wheelhouse':list(custody.wheelhouse_id)}
        for name in ('metadata','events'):
            (custody.path/name).mkdir(mode=0o700)
            st=(custody.path/name).stat();ids[name]=[st.st_dev,st.st_ino]
        borrowed={name:custody.duplicate_for_owner(name) for name in custody.archive_nodes}
        proof={'pool':str(custody.path),'ready':True,'started_monotonic':time.monotonic(),
               'started_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
               'dir_ids':ids,
               'archive_fds':dict(borrowed)}
        with patch.object(owner,'POOL_BASE',parent):item=owner.Acquisition(REF)
        try:yield item,custody,proof
        finally:
            for directory in (custody.path,custody.path/'wheelhouse',custody.path/'metadata',custody.path/'events'):
                if directory.exists() and not directory.is_symlink():directory.chmod(0o700)
            for fd in borrowed.values():os.close(fd)
            custody.close()


def prepare(item,proof):
    item.attach_guarded_pool(proof)
    item.prepare_pool([{'filename':'one.whl'},{'filename':'two.whl'}])


def positive_handoff_write_seal():
    with fixture() as (item,custody,proof):
        prepare(item,proof)
        assert item.created and item.guard_attached and item.archive_opened==2
        for name,raw in [('one.whl',b'first synthetic bytes'),('two.whl',b'second synthetic bytes')]:
            identity=custody.archive_nodes[name]['identity']
            item.write('wheelhouse/'+name,raw)
            assert (custody.node(name).st_dev,custody.node(name).st_ino)==identity
            assert os.pread(custody.archive_nodes[name]['fd'],len(raw),0)==raw
        assert item.archive_opened==item.archive_written==2
        assert len(item.check_pool())==2
        item.seal();assert len(item.check_pool())==2


def wrong_proof(change,code):
    with fixture() as (item,custody,proof):
        change(proof)
        refuses(lambda:item.attach_guarded_pool(proof),code)
        assert not item.guard_attached and not item.requests
        assert all(custody.node(name).st_size==0 for name in custody.archive_nodes)


def network_without_guard_refused():
    with fixture() as (item,custody,proof), \
         patch.object(owner,'VERIFIED_SOURCE_SHA256','0'*64),patch.dict(os.environ,owner.MINIMAL_ENV,clear=True), \
         patch.object(owner,'read_regular',side_effect=AssertionError('must refuse before reading inputs')):
        refuses(item.run,'INDEPENDENT_GUARDIAN_CUSTODY_REQUIRED_BEFORE_NETWORK')
        assert not item.requests


def duplicate_attach_refused():
    with fixture() as (item,custody,proof):
        item.attach_guarded_pool(proof)
        refuses(lambda:item.attach_guarded_pool(proof),'GUARDIAN_ALREADY_ATTACHED')


def full_filename_set_required():
    with fixture() as (item,custody,proof):
        item.attach_guarded_pool(proof)
        refuses(lambda:item.prepare_pool([{'filename':'one.whl'}]),'COMPLETE_GUARDIAN_ARCHIVE_CUSTODY_REQUIRED')
        assert not list((item.pool/'metadata').iterdir())


def fdopen_failure_closes_duplicate_cleans_all_preallocated():
    with fixture() as (item,custody,proof):
        prepare(item,proof)
        duplicate=[]
        def refuse_wrap(fd,mode):duplicate.append(fd);raise OSError('synthetic wrap failure')
        with patch.object(owner.os,'fdopen',side_effect=refuse_wrap):
            try:item.write('wheelhouse/one.whl',b'partial')
            except OSError:pass
            else:raise AssertionError('fdopen failure not exercised')
        assert len(duplicate)==1
        try:os.fstat(duplicate[0])
        except OSError:pass
        else:raise AssertionError('duplicate fd remains open')
        result=item.failure_result(OSError('synthetic wrap failure'))
        assert result['archive_cleanup']['complete'] and not list((item.pool/'wheelhouse').iterdir())
        assert result['archive_nodes']=={'opened':2,'written':0,'removed':2,'remaining_owned':0}
        assert custody.cleanup_archives()['complete']


def late_failure_cleans_unused_preallocations():
    with fixture() as (item,custody,proof):
        prepare(item,proof);item.write('wheelhouse/one.whl',b'first synthetic bytes')
        result=item.failure_result(owner.Refusal('synthetic late refusal'))
        assert result['archive_cleanup']['complete'] and result['archive_nodes']['removed']==2
        assert not list((item.pool/'wheelhouse').iterdir()) and custody.cleanup_archives()['complete']


def replaced_archive_not_written():
    with fixture() as (item,custody,proof):
        prepare(item,proof)
        path=item.pool/'wheelhouse/one.whl'
        path.rename(item.pool/'retained-original')
        path.write_bytes(b'unknown node')
        refuses(lambda:item.write('wheelhouse/one.whl',b'never written'),
                'GUARDED_ARCHIVE_NAME_CHANGED_BEFORE_WRITE')
        assert path.read_bytes()==b'unknown node'
        assert os.fstat(custody.archive_nodes['one.whl']['fd']).st_size==0


def wrong_descriptor_refused():
    with fixture() as (item,custody,proof):
        foreign=item.pool.parent/'foreign'
        foreign.write_bytes(b'foreign untouched')
        fd=os.open(foreign,os.O_RDONLY)
        try:
            proof['archive_fds']['one.whl']=fd
            refuses(lambda:item.attach_guarded_pool(proof),'GUARDIAN_ARCHIVE_DESCRIPTOR_IDENTITY_CHANGED')
            assert foreign.read_bytes()==b'foreign untouched'
        finally:os.close(fd)


with patch.object(socket,'create_connection',side_effect=AssertionError('NO_NETWORK_IN_ADAPTER_CONTROLS')):
    check('adopt-preallocated-nodes-write-with-same-inodes-seal',positive_handoff_write_seal)
    check('network-requires-independent-guardian-before-inputs',network_without_guard_refused)
    check('missing-readiness-refused',lambda:wrong_proof(lambda p:p.update(ready=False),'GUARDIAN_POOL_PROOF_MISMATCH'))
    check('wrong-root-path-refused',lambda:wrong_proof(lambda p:p.update(pool='/synthetic/foreign'),'GUARDIAN_POOL_PROOF_MISMATCH'))
    check('changed-root-inode-refused',lambda:wrong_proof(lambda p:p['dir_ids']['.'].__setitem__(1,0),'GUARDIAN_DIRECTORY_IDENTITY_CHANGED'))
    check('changed-wheelhouse-inode-refused',lambda:wrong_proof(lambda p:p['dir_ids']['wheelhouse'].__setitem__(1,0),'GUARDIAN_DIRECTORY_IDENTITY_CHANGED'))
    check('incomplete-descriptor-set-refused',lambda:wrong_proof(lambda p:p['archive_fds'].pop('one.whl'),'GUARDIAN_ARCHIVE_SET_CHANGED'))
    check('duplicate-descriptors-refused',lambda:wrong_proof(lambda p:p['archive_fds'].update({'two.whl':p['archive_fds']['one.whl']}),'GUARDIAN_DESCRIPTOR_SET_INVALID'))
    check('expired-anchor-refused',lambda:wrong_proof(lambda p:p.update(started_monotonic=time.monotonic()-271),'GUARDIAN_START_DEADLINE_INVALID'))
    check('changed-wall-clock-anchor-refused',lambda:wrong_proof(lambda p:p.update(started_utc='2020-01-01T00:00:00+00:00'),'GUARDIAN_WALL_CLOCK_ANCHOR_CHANGED'))
    check('duplicate-adoption-refused',duplicate_attach_refused)
    check('selected-six-filename-closure-before-directory-creation',full_filename_set_required)
    check('guarded-fdopen-failure-closes-duplicate-cleans-used-and-unused',fdopen_failure_closes_duplicate_cleans_all_preallocated)
    check('late-failure-cleans-earlier-and-unused-preallocations',late_failure_cleans_unused_preallocations)
    check('unknown-replacement-inode-never-written',replaced_archive_not_written)
    check('foreign-fd-never-adopted-or-written',wrong_descriptor_refused)

report={'scope':'synthetic owner adapter, not integrated acquisition or launcher',
        'owner_sha256':hashlib.sha256((BASE/'184-acquire-tools-v5.py').read_bytes()).hexdigest(),
        'guardian_sha256':hashlib.sha256((BASE/'184-acquisition-custody-guardian-v5-proposal.py').read_bytes()).hexdigest(),
        'cases':len(results),'passed':sum(row['outcome']=='pass' for row in results),
        'failed':sum(row['outcome']=='fail' for row in results),'results':results,
        'real_network_requests':0,'downloaded_wheel_imports_or_installs':0,
        'terminal_acceptance_and_exact_launcher_pending':True}
report_path=BASE/'184-acquire-tools-v5-adapter-controls-result.json'
report_bytes=(json.dumps(report,sort_keys=True,indent=2)+'\n').encode()
if report_path.exists():
    if report_path.read_bytes()!=report_bytes:
        print(json.dumps(report,indent=2))
        raise SystemExit('CONTROL_RESULT_COLLISION; prior record preserved')
else:
    descriptor=os.open(report_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400)
    with os.fdopen(descriptor,'wb') as stream:
        assert stream.write(report_bytes)==len(report_bytes)
print(json.dumps(report,indent=2))
raise SystemExit(bool(report['failed']))
