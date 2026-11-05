"""Narrow predecessor countercontrols; no data acquisition or admitted start."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from unittest.mock import patch

sys.dont_write_bytecode = True
BASE = Path('/tmp/sdk-python-handoff')
REF = 'https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-9999999999'
results = []


def load(name, version):
    path = BASE / f'184-acquisition-custody-guardian-{version}-proposal.py'
    raw = path.read_bytes()
    if version == 'v2':
        assert hashlib.sha256(raw).hexdigest() == 'afc5f198de12d4b26520957bc563ff0122f8535838a1cbc64c40272c5f3f7f70'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


old = load('trusted_frozen_v2_countercontrol', 'v2')
new = load('trusted_v5_correction_control', 'v5')
archive_predecessor = load('trusted_frozen_v4_archive_countercontrol', 'v4')
assert hashlib.sha256((BASE/'184-acquisition-custody-guardian-v4-proposal.py').read_bytes()).hexdigest() == 'd52bb389127cb81b5b0303b3a593ee1af74f8c4835793d67f08d0bf182e2101c'
linked_predecessor = load('trusted_frozen_v3_link_countercontrol', 'v3')
assert hashlib.sha256((BASE/'184-acquisition-custody-guardian-v3-proposal.py').read_bytes()).hexdigest() == '79ddc82b0095872885aceb943cbf9daa1eb3a05048bc08f9ee9cd093511de187'
inputs = {key: 'a' * 64 for key in new.INPUT_FIELDS}
authority = dict(decision=REF, selected_source=new.SELECTED_SOURCE, inputs=inputs,
                 environment=new.MINIMAL_ENV, host='lab4', total_seconds=300, io_seconds=270,
                 retries=0, survivor_policy='retain-custody-and-escalate',
                 pool_parent='/home/lab4/fleet/tmp', launcher_sha256='b' * 64, cpu_affinity=[0, 1],
                 retention_deadline_utc='2099-01-01T00:00:00Z')


def bound(module, raw):
    return module.bind_authority(raw, hashlib.sha256(raw).hexdigest(), REF, inputs)


def parser_refuses(raw, predecessor=True):
    if predecessor:
        assert bound(old, raw)['decision'] == REF, 'predecessor acceptance not reproduced'
    try:
        bound(new, raw)
    except (ValueError, UnicodeError):
        return
    raise AssertionError('ambiguous authority accepted')


def sha_refuses():
    try: new.bind_authority(encoded(authority), '0' * 64, REF, inputs)
    except new.Refusal as error: assert str(error) == 'AUTHORITY_BYTES_CHANGED'
    else: raise AssertionError('changed authority SHA accepted')


def encoded(value):
    return json.dumps(value, separators=(',', ':')).encode()


def mutate(key, value):
    row = copy.deepcopy(authority); row[key] = value
    return encoded(row)


def allocation(module, stage, unknown=False):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-guardian-tree-controls-') as temp:
        parent = Path(temp); leaf = 'sdk-python-184-tools-guardian-source-control'
        root = parent / leaf
        custody = None
        real_mkdir = os.mkdir
        def mkdir(path, mode=0o777, *, dir_fd=None):
            if path == 'wheelhouse' and stage == 'after-root':
                if unknown: (root / 'unknown-root-member').write_bytes(b'untouched')
                raise OSError('synthetic second mkdir refusal')
            return real_mkdir(path, mode, dir_fd=dir_fd)
        real_open = os.open
        def opened(path, flags, mode=0o777, *, dir_fd=None):
            if path == 'one.whl' and stage == 'after-wheelhouse':
                raise OSError('synthetic first archive refusal')
            return real_open(path, flags, mode, dir_fd=dir_fd)
        try:
            with patch.object(module.os, 'mkdir', side_effect=mkdir), patch.object(module.os, 'open', side_effect=opened):
                try: custody = module.ArchiveCustody(parent, leaf, ['one.whl'])
                except module.Refusal as error:
                    if module is new:
                        assert isinstance(error, new.AllocationRefusal)
                        custody = error.custody
                        assert error.cleanup['complete'] is (not unknown)
                        assert error.cleanup['outer_owner_custody_required'] is unknown
                        if unknown:
                            assert root.exists() and (root / 'unknown-root-member').read_bytes() == b'untouched'
                            assert os.fstat(custody.parent_fd).st_uid == os.geteuid()
                        else: assert not root.exists()
                    else: raise
                except OSError:
                    assert module is old and root.exists(), 'predecessor residue not reproduced'
                else:
                    assert stage == 'normal'
                    if unknown: (root / 'unknown-root-member').write_bytes(b'untouched')
                    result = custody.cleanup()
                    if module is old:
                        assert result['complete'] and root.exists() and (root / 'wheelhouse').exists()
                    elif unknown:
                        assert not result['complete'] and result['outer_owner_custody_required']
                        assert (root / 'unknown-root-member').read_bytes() == b'untouched'
                        assert not (root / 'wheelhouse').exists()
                    else:
                        assert result['complete'] and not result['outer_owner_custody_required']
                        assert not root.exists(), 'TemporaryDirectory must not conceal retained directories'
        finally:
            if custody is not None: custody.close()


def removed_root_with_tracked_nodes(module, scenario):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-linked-descendant-control-') as temp:
        parent=Path(temp);custody=module.ArchiveCustody(parent,
            'sdk-python-184-tools-guardian-linked-control',['one.whl'])
        extra_fd=None
        try:
            archive=custody.path/'wheelhouse'/'one.whl';archive.write_bytes(b'original synthetic bytes')
            if scenario=='wheelhouse':
                (custody.path/'wheelhouse').rename(parent/'moved-wheelhouse')
                retained=parent/'moved-wheelhouse'/'one.whl'
            else:
                if scenario=='archive':
                    archive.rename(parent/'moved-archive.whl');retained=parent/'moved-archive.whl'
                else:archive.unlink();retained=None
                (custody.path/'wheelhouse').rmdir()
                if scenario=='extra-directory':
                    (custody.path/'metadata').mkdir()
                    extra_fd=os.open(custody.path/'metadata',os.O_RDONLY|os.O_DIRECTORY)
                    st=os.fstat(extra_fd)
                    custody.dir_ids={'metadata':[st.st_dev,st.st_ino]}
                    custody.directory_fds={'metadata':extra_fd}
                    (custody.path/'metadata').rename(parent/'moved-metadata')
            custody.path.rmdir()
            assert os.fstat(custody.root_fd).st_nlink==0
            result=custody.cleanup()
            if scenario=='fully-unlinked':
                assert module is new and result['complete'] and result['all_unlinked']
                assert not result['outer_owner_custody_required']
                assert not result['retained_or_unproven_inodes']
            elif module is linked_predecessor:
                assert result['complete'] and not result['outer_owner_custody_required'], 'old false completion not reproduced'
            else:
                assert not result['complete'] and result['outer_owner_custody_required']
                assert result['refusal_code']=='TRACKED_INODES_STILL_LINKED_OR_UNPROVEN'
                expected='wheelhouse' if scenario=='wheelhouse' else 'archive:one.whl' if scenario=='archive' else 'metadata'
                assert expected in result['retained_or_unproven_inodes']
            if retained is not None:assert retained.read_bytes()==b'original synthetic bytes'
            if scenario=='extra-directory':assert (parent/'moved-metadata').is_dir()
        finally:
            if extra_fd is not None:os.close(extra_fd)
            custody.close()


def allocation_identity_gap(module):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-allocation-identity-control-') as temp:
        parent=Path(temp);root=parent/'sdk-python-184-tools-guardian-identity-control'
        real_stat=os.stat;faulted=False;held=None
        def stat_before_identity(path,*args,**kwargs):
            nonlocal faulted
            if path=='wheelhouse' and not faulted:
                faulted=True
                (root/'wheelhouse').rename(parent/'moved-before-identity');root.rmdir()
                raise OSError('synthetic identity read refusal')
            return real_stat(path,*args,**kwargs)
        try:
            with patch.object(module.os,'stat',side_effect=stat_before_identity):
                try:module.ArchiveCustody(parent,root.name,['one.whl'])
                except module.AllocationRefusal as error:held=error.custody;result=error.cleanup
                else:raise AssertionError('allocation identity fault absent')
            assert faulted and (parent/'moved-before-identity').is_dir()
            if module is linked_predecessor:
                assert result['complete'] and not result['outer_owner_custody_required']
            else:
                assert not result['complete'] and result['outer_owner_custody_required']
                assert result['refusal_code']=='TRACKED_INODES_STILL_LINKED_OR_UNPROVEN'
                assert 'wheelhouse' in result['retained_or_unproven_inodes']
        finally:
            if held is not None:held.close()


def archive_before_identity(module,fully_unlinked=False,remove_root=True):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-raw-archive-control-') as temp:
        parent=Path(temp);root=parent/'sdk-python-184-tools-guardian-raw-archive-control'
        opened=[];real_open=os.open;real_fstat=os.fstat;held=None;faulted=False
        moved=parent/'moved-before-archive-identity.whl'
        def exclusive_open(path,*args,**kwargs):
            fd=real_open(path,*args,**kwargs)
            if path=='one.whl':opened.append(fd)
            return fd
        def identity_read(fd):
            nonlocal faulted
            if fd in opened and not faulted:
                faulted=True
                os.write(fd,b'synthetic allocated bytes')
                (root/'wheelhouse'/'one.whl').rename(moved)
                if fully_unlinked:moved.unlink()
                if remove_root:(root/'wheelhouse').rmdir();root.rmdir()
                raise OSError('synthetic archive identity refusal')
            return real_fstat(fd)
        try:
            with patch.object(module.os,'open',side_effect=exclusive_open),patch.object(module.os,'fstat',side_effect=identity_read):
                try:module.ArchiveCustody(parent,root.name,['one.whl'])
                except module.AllocationRefusal as error:held=error.custody;result=error.cleanup
                else:raise AssertionError('archive identity fault absent')
            assert faulted and len(opened)==1
            if module is archive_predecessor:
                assert not held.archive_nodes and result['complete'] and not result['outer_owner_custody_required']
                assert moved.read_bytes()==b'synthetic allocated bytes' and moved.stat().st_nlink==1
            else:
                assert held.archive_nodes['one.whl']=={'fd':opened[0],'identity':None}
                if fully_unlinked:
                    assert result['complete'] and result['all_unlinked'] and not result['outer_owner_custody_required']
                    assert real_fstat(opened[0]).st_nlink==0 and not root.exists()
                else:
                    assert not result['complete'] and result['outer_owner_custody_required']
                    assert result['refusal_code']=='TRACKED_INODES_STILL_LINKED_OR_UNPROVEN'
                    assert 'archive:one.whl' in result['retained_or_unproven_inodes']
                    assert moved.read_bytes()==b'synthetic allocated bytes' and real_fstat(opened[0]).st_nlink==1
        finally:
            if held is not None:held.close()


def check(name, operation):
    try: operation()
    except BaseException as error:
        results.append({'id': name, 'outcome': 'fail', 'type': type(error).__name__, 'detail': str(error)[:250]})
    else: results.append({'id': name, 'outcome': 'pass'})


with patch.object(socket, 'create_connection', side_effect=AssertionError('NO_NETWORK_IN_SOURCE_CONTROLS')):
    check('v4-false-complete-opened-archive-before-identity',lambda:archive_before_identity(archive_predecessor))
    check('v5-retains-opened-archive-before-identity',lambda:archive_before_identity(new))
    check('v5-fully-unlinked-partial-archive-root-absent-positive',lambda:archive_before_identity(new,True,True))
    check('v5-fully-unlinked-partial-archive-root-present-positive',lambda:archive_before_identity(new,True,False))
    check('predecessor-false-complete-allocation-before-child-identity',lambda:allocation_identity_gap(linked_predecessor))
    check('successor-retains-allocation-before-child-identity',lambda:allocation_identity_gap(new))
    for scenario in ('wheelhouse','archive','extra-directory'):
        for module,label in ((linked_predecessor,'predecessor-false-complete'),(new,'successor-retains')):
            check(label+'-removed-root-linked-'+scenario,
                  lambda module=module,scenario=scenario:removed_root_with_tracked_nodes(module,scenario))
    check('fully-unlinked-root-children-and-archives-completes',lambda:removed_root_with_tracked_nodes(new,'fully-unlinked'))
    check('exact-schema-positive', lambda: bound(new, encoded(authority)))
    for name, prefix in [('identical', '"decision":"' + REF + '",'),
                         ('conflicting', '"decision":"earlier-conflict",'),
                         ('escaped', '"decis\\u0069on":"earlier-conflict",')]:
        check('old-accept-new-refuse-duplicate-' + name,
              lambda prefix=prefix: parser_refuses(b'{' + prefix.encode() + encoded(authority)[1:]))
    row = copy.deepcopy(authority); row['unreviewed_future_semantics'] = {}
    check('old-accept-new-refuse-extra-top-field', lambda: parser_refuses(encoded(row)))
    for key in ('survivor_policy', 'pool_parent', 'launcher_sha256', 'cpu_affinity', 'retention_deadline_utc'):
        missing = copy.deepcopy(authority); del missing[key]
        check('old-accept-new-refuse-missing-' + key, lambda missing=missing: parser_refuses(encoded(missing)))
    for key, nested in [('inputs', inputs), ('environment', new.MINIMAL_ENV)]:
        first = next(iter(nested)); duplicated = encoded(authority).replace(
            ('"' + key + '":{').encode(), ('"' + key + '":{"' + first + '":"conflict",').encode(), 1)
        check('old-accept-new-refuse-nested-duplicate-' + key, lambda raw=duplicated: parser_refuses(raw))
        for extra in (True, False):
            value = dict(nested)
            if extra: value['unknown'] = 'x'
            else: del value[first]
            check('refuse-nested-schema-' + key + ('-extra' if extra else '-missing'),
                  lambda key=key, value=value: parser_refuses(mutate(key, value), predecessor=False))
    for key in ('total_seconds', 'io_seconds', 'retries'):
        check('old-accept-new-refuse-float-' + key, lambda key=key: parser_refuses(mutate(key, float(authority[key]))))
        for kind, value in [('bool', False), ('string', str(authority[key])), ('nan', float('nan')),
                            ('positive-infinity', float('inf')), ('negative-infinity', -float('inf'))]:
            check('refuse-' + key + '-' + kind,
                  lambda key=key, value=value: parser_refuses(mutate(key, value), predecessor=(key == 'retries' and value is False)))
    for value in ([], [True], [0, 0], [0.0], ['0'], [0, 1, 2]):
        check('refuse-cpu-schema-' + repr(value), lambda value=value: parser_refuses(mutate('cpu_affinity', value)))
    check('nonfinite-at-unexpected-depth-refuses', lambda: parser_refuses(
        encoded(authority).replace(b'"cpu_affinity":[0,1]', b'"cpu_affinity":[NaN]')))
    check('invalid-utf8-refuses', lambda: parser_refuses(b'\xff', predecessor=False))
    check('sha-check-preserved', sha_refuses)
    for stage, unknown in [('normal', False), ('normal', True), ('after-root', False),
                           ('after-wheelhouse', False), ('after-root', True)]:
        for module, label in ((old, 'predecessor'), (new, 'successor')):
            check(label + '-tree-' + stage + ('-unknown' if unknown else ''),
                  lambda module=module, stage=stage, unknown=unknown: allocation(module, stage, unknown))

report = {'scope': 'guardian authority/root-tree predecessor countercontrols only; no admitted data API',
          'source_sha256': hashlib.sha256((BASE / '184-acquisition-custody-guardian-v5-proposal.py').read_bytes()).hexdigest(),
          'predecessor_sha256': 'afc5f198de12d4b26520957bc563ff0122f8535838a1cbc64c40272c5f3f7f70',
          'cases': len(results), 'passed': sum(row['outcome'] == 'pass' for row in results),
          'failed': sum(row['outcome'] == 'fail' for row in results), 'results': results,
          'real_network_requests': 0, 'selected_archives_acquired': 0, 'new_general_decision': None}
raw = (json.dumps(report, sort_keys=True, indent=2) + '\n').encode()
path = BASE / '184-acquisition-guardian-v5-controls-result.json'
if path.exists():
    if path.read_bytes() != raw:
        print(json.dumps(report, indent=2)); raise SystemExit('CONTROL_RESULT_COLLISION; prior result retained')
else:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    with os.fdopen(fd, 'wb') as stream: assert stream.write(raw) == len(raw)
print(json.dumps(report, indent=2)); raise SystemExit(bool(report['failed']))
