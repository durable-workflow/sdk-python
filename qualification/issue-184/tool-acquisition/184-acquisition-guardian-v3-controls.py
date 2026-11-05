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
new = load('trusted_v3_correction_control', 'v3')
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


def check(name, operation):
    try: operation()
    except BaseException as error:
        results.append({'id': name, 'outcome': 'fail', 'type': type(error).__name__, 'detail': str(error)[:250]})
    else: results.append({'id': name, 'outcome': 'pass'})


with patch.object(socket, 'create_connection', side_effect=AssertionError('NO_NETWORK_IN_SOURCE_CONTROLS')):
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
          'source_sha256': hashlib.sha256((BASE / '184-acquisition-custody-guardian-v3-proposal.py').read_bytes()).hexdigest(),
          'predecessor_sha256': 'afc5f198de12d4b26520957bc563ff0122f8535838a1cbc64c40272c5f3f7f70',
          'cases': len(results), 'passed': sum(row['outcome'] == 'pass' for row in results),
          'failed': sum(row['outcome'] == 'fail' for row in results), 'results': results,
          'real_network_requests': 0, 'selected_archives_acquired': 0, 'new_general_decision': None}
raw = (json.dumps(report, sort_keys=True, indent=2) + '\n').encode()
path = BASE / '184-acquisition-guardian-v3-controls-result.json'
if path.exists():
    if path.read_bytes() != raw:
        print(json.dumps(report, indent=2)); raise SystemExit('CONTROL_RESULT_COLLISION; prior result retained')
else:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    with os.fdopen(fd, 'wb') as stream: assert stream.write(raw) == len(raw)
print(json.dumps(report, indent=2)); raise SystemExit(bool(report['failed']))
