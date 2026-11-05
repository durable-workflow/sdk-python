"""Synthetic source-only controls: no network, selected archives or tool imports."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import ast
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
import subprocess
import tempfile
import urllib.error
import urllib.request
from unittest.mock import patch
import zipfile

sys.dont_write_bytecode = True
BASE = Path('/tmp/sdk-python-handoff')
FAILED = Path('/tmp/sdk-python-184-tools-14adc40b-20261105T035512Z-6d90ccfb71')
NEW_REF = 'https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-9999999999'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


old = load('original_acquisition_source_only', BASE / '184-acquire-tools.py')
new = load('successor_acquisition_source_only', BASE / '184-acquire-tools-v5.py')
bootstrap = load('successor_bootstrap_source_only', BASE / '184-acquire-tools-v5-bootstrap.py')
assert hashlib.sha256((BASE / '184-acquire-tools.py').read_bytes()).hexdigest() \
    == 'e0b8955c8c26c68333e1439c4eb195ef803d710d68f4a4b297b3dd17fcfd7c54'
assert hashlib.sha256((BASE / '184-acquire-tools-v5.py').read_bytes()).hexdigest() == bootstrap.OWNER_SHA
selection = json.loads((FAILED / 'selection.json').read_text())
tools = {row['name']: row for row in selection['tools']}
authorities = {name: json.loads((FAILED / 'metadata' / (name + '.json')).read_text()) for name in tools}
results = []
fake_calls = 0


def check(label, body):
    try:
        body()
    except BaseException as error:
        results.append({'id': label, 'outcome': 'fail', 'error': type(error).__name__, 'detail': str(error)[:240]})
    else:
        results.append({'id': label, 'outcome': 'pass'})


def refuses(body, code):
    try:
        body()
    except new.Refusal as error:
        assert error.code == code, (error.code, code)
    else:
        raise AssertionError('expected refusal ' + code)


def synthetic_wheel(name='build', *, single_quotes=False, requirements=None, change=None, nested=False):
    tool = deepcopy(tools[name])
    authority = deepcopy(authorities[name])
    reqs = list(authority['info'].get('requires_dist') or []) if requirements is None else requirements
    headers = ['Metadata-Version: 2.4', 'Name: ' + tool['name'], 'Version: ' + tool['version'],
               'Requires-Python: ' + tool['requires_python']]
    headers += ['Requires-Dist: ' + (row.replace('"', "'") if single_quotes else row) for row in reqs]
    metadata = '\n'.join(headers) + '\n\nSynthetic header-only ZIP; not an acquired tool archive.'
    wheel = 'Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
    members = {tool['name'].replace('-', '_') + '-' + tool['version'] + '.dist-info/METADATA': metadata,
               tool['name'].replace('-', '_') + '-' + tool['version'] + '.dist-info/WHEEL': wheel}
    if nested:
        members['build/_vendor/other-1.dist-info/METADATA'] = 'Name: other\nVersion: 1\n'
    if change:
        change(members, tool, authority)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w') as archive:
        for path, text in members.items():
            archive.writestr(path, text)
    raw = stream.getvalue()
    tool['bytes'], tool['sha256'] = len(raw), hashlib.sha256(raw).hexdigest()
    return raw, tool, authority


@contextmanager
def owner():
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-source-controls-') as temporary:
        item = new.Acquisition(NEW_REF)
        item.pool = Path(temporary) / 'synthetic-pool'
        item.pool.mkdir()
        item.created = True
        for name in ('metadata', 'wheelhouse', 'events'):
            (item.pool / name).mkdir()
        for name in ('.', 'metadata', 'wheelhouse', 'events'):
            st = (item.pool if name == '.' else item.pool / name).stat()
            item.dir_ids[name] = (st.st_dev, st.st_ino)
        try:
            yield item
        finally:
            item.pool.chmod(0o700)
            for name in ('metadata', 'wheelhouse', 'events'):
                (item.pool / name).chmod(0o700)


class Response:
    def __init__(self, body=b'{}', *, status=200, length='auto', encoding='identity', url=None, partial=False):
        self.status = status
        self.headers = {'Content-Encoding': encoding}
        if length is not None:
            self.headers['Content-Length'] = str(len(body)) if length == 'auto' else length
        self.body, self.url, self.partial = io.BytesIO(body), url, partial
        self.reads = 0
    def __enter__(self): return self
    def __exit__(self, *args): self.body.close()
    def geturl(self): return self.url
    def read(self, count):
        self.reads += 1
        if self.partial and self.reads > 1:
            raise TimeoutError('synthetic only')
        return self.body.read(count)


class Opener:
    def __init__(self, response=None, error=None):
        self.response, self.error, self.calls = response, error, 0
    def open(self, request, timeout):
        global fake_calls
        self.calls += 1
        fake_calls += 1
        if self.error:
            raise self.error
        if self.response.url is None:
            self.response.url = request.full_url
        return self.response


def old_quote_control():
    raw, tool, authority = synthetic_wheel(single_quotes=True)
    try:
        old.Acquisition.__new__(old.Acquisition).inspect_wheel(raw, tool, authority)
    except ValueError as error:
        assert str(error) == 'wheel dependency metadata mismatch'
    else:
        raise AssertionError('original did not expose the equivalent-marker defect')
    with owner() as item:
        result = item.inspect_wheel(raw, tool, authority)
        assert result['version'] == tool['version']


def metadata_mutation(mutator, code):
    authority = deepcopy(authorities['build'])
    mutator(authority)
    refuses(lambda: new.validate_authority(authority, tools['build']), code)


def transport_control(response, code=None, *, kind='metadata', size=None):
    with owner() as item:
        item.opener = Opener(response)
        url = tools['build']['metadata_source' if kind == 'metadata' else 'url']
        if code:
            refuses(lambda: item.get(url, 64, kind, size), code)
            record = json.loads((item.pool / 'events/01-refusal.json').read_text())
            assert record['refusal_code'] == code and item.opener.calls == 1
        else:
            assert item.get(url, 64, kind, size) == b'{}'
            assert json.loads((item.pool / 'events/01-complete.json').read_text())['body_sha256'] \
                == hashlib.sha256(b'{}').hexdigest()
            assert item.opener.calls == 1


def body_removed_after_semantic_refusal():
    def changed(members, tool, authority):
        key = next(key for key in members if key.endswith('/METADATA'))
        members[key] = members[key].replace('Version: 1.6.1', 'Version: 9.9')
    raw, tool, authority = synthetic_wheel(change=changed)
    with owner() as item:
        refuses(lambda: item.retain_and_inspect(raw, tool, authority), 'WHEEL_NAME_VERSION_MISMATCH')
        saved = item.pool / 'wheelhouse' / tool['filename']
        assert not saved.exists() and not item.archive_nodes
        assert item.archive_used == 0 and item.archive_removed == 1
        assert item.archive_facts[-1]['zip_started'] and not item.archive_facts[-1]['zip_complete']


def bad_hash_not_retained():
    raw, tool, authority = synthetic_wheel()
    tool['sha256'] = '0' * 64
    with owner() as item:
        refuses(lambda: item.retain_and_inspect(raw, tool, authority), 'ARCHIVE_BYTES_HASH_MISMATCH')
        assert not list((item.pool / 'wheelhouse').iterdir())


def assert_zero_archives(item, result):
    assert result['outcome'] == 'fail' and result['archive_cleanup']['complete']
    assert result['archive_nodes']['remaining_owned'] == 0
    assert not item.archive_nodes and not list((item.pool / 'wheelhouse').iterdir())
    assert item.archive_used == 0


def late_failure_removes_earlier_archives():
    with owner() as item:
        for name in ('build', 'packaging'):
            raw, tool, authority = synthetic_wheel(name)
            item.retain_and_inspect(raw, tool, authority)
        result = item.failure_result(new.Refusal('synthetic late refusal'))
        assert_zero_archives(item, result)
        assert result['archive_nodes'] == {'opened': 2, 'written': 2, 'removed': 2, 'remaining_owned': 0}
        assert result['failure_record_written'] and result['custody_seal_complete']
        saved = json.loads((item.pool / 'failure.json').read_text())
        assert saved['refusal_code'] == 'SYNTHETIC_LATE_REFUSAL'
        assert saved['archive_cleanup']['complete'] and len(item.check_pool()) == 1


def malformed_zip_cleanup():
    raw, tool, authority = synthetic_wheel()
    raw = b'synthetic invalid ZIP'
    tool.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    with owner() as item:
        try:
            item.retain_and_inspect(raw, tool, authority)
        except zipfile.BadZipFile as error:
            result = item.failure_result(error)
        else: raise AssertionError('malformed ZIP accepted')
        assert result['refusal_code'] == 'EXCEPTION_BADZIPFILE'
        assert_zero_archives(item, result)
        assert result['archive_facts'][0]['zip_started']
        assert not result['archive_facts'][0]['zip_complete']


def special_zip_cleanup():
    import stat
    raw, tool, authority = synthetic_wheel()
    body = io.BytesIO(raw)
    with zipfile.ZipFile(body, 'a') as archive:
        link = zipfile.ZipInfo('synthetic-link')
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(link, 'never-followed')
    raw = body.getvalue()
    tool.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    with owner() as item:
        refuses(lambda: item.retain_and_inspect(raw, tool, authority), 'ZIP_SPECIAL_LINK_MEMBER')
        result = item.failure_result(new.Refusal('ZIP special/link member'))
        assert_zero_archives(item, result)


def partial_archive_write_cleanup():
    raw, tool, authority = synthetic_wheel()
    original_fdopen = new.os.fdopen
    class ShortWrite:
        def __init__(self, fd): self.stream = original_fdopen(fd, 'wb')
        def __enter__(self): return self
        def __exit__(self, *args): self.stream.close()
        def close(self): self.stream.close()
        def fileno(self): return self.stream.fileno()
        def write(self, value): return self.stream.write(value[:3])
    with owner() as item:
        with patch.object(new.os, 'fdopen', side_effect=lambda fd, mode: ShortWrite(fd)):
            refuses(lambda: item.retain_and_inspect(raw, tool, authority), 'SHORT_OUTPUT_WRITE')
        result = item.failure_result(new.Refusal('short output write'))
        assert_zero_archives(item, result)
        assert result['archive_nodes'] == {'opened': 1, 'written': 0, 'removed': 1, 'remaining_owned': 0}


def failure_record_collision_cleanup():
    raw, tool, authority = synthetic_wheel()
    with owner() as item:
        item.retain_and_inspect(raw, tool, authority)
        item.write('failure.json', b'original collision evidence')
        result = item.failure_result(new.Refusal('primary remains primary'))
        assert_zero_archives(item, result)
        assert result['refusal_code'] == 'PRIMARY_REMAINS_PRIMARY'
        assert not result['failure_record_written']
        assert result['failure_record_refusal_code'] == 'POOL_OUTPUT_BOUND_OR_COLLISION'
        assert (item.pool / 'failure.json').read_bytes() == b'original collision evidence'


def untracked_failure_record_collision_cleanup():
    raw, tool, authority = synthetic_wheel()
    with owner() as item:
        item.retain_and_inspect(raw, tool, authority)
        (item.pool / 'failure.json').write_bytes(b'unknown collision evidence')
        result = item.failure_result(new.Refusal('primary remains primary'))
        assert_zero_archives(item, result)
        assert result['refusal_code'] == 'PRIMARY_REMAINS_PRIMARY'
        assert not result['failure_record_written']
        assert result['failure_record_refusal_code'] == 'EXCEPTION_FILEEXISTSERROR'
        assert (item.pool / 'failure.json').read_bytes() == b'unknown collision evidence'


def v2_semantic_refusal_countercontrol():
    previous = load('unexecuted_v2_source_only', BASE / '184-acquire-tools-v2.py')
    raw, tool, authority = synthetic_wheel()
    authority['info']['requires_dist'] = ['different>=1']
    with owner() as item:
        prior = previous.Acquisition(NEW_REF)
        prior.pool, prior.created, prior.dir_ids = item.pool, True, item.dir_ids
        try:
            prior.retain_and_inspect(raw, tool, authority)
        except previous.Refusal as error:
            assert error.code == 'WHEEL_DEPENDENCY_METADATA_MISMATCH'
        else: raise AssertionError('semantic refusal required')
        saved = item.pool / 'wheelhouse' / tool['filename']
        assert saved.read_bytes() == raw, 'countercontrol: v2 retained exactly one refused archive'
    with owner() as item:
        refuses(lambda: item.retain_and_inspect(raw, tool, authority), 'WHEEL_DEPENDENCY_METADATA_MISMATCH')
        assert not list((item.pool / 'wheelhouse').iterdir()) and item.archive_removed == 1


def redirect_failure_cleans_prior_archive():
    raw, tool, authority = synthetic_wheel()
    with owner() as item:
        item.retain_and_inspect(raw, tool, authority)
        refusal = new.Refusal('redirect refused no second request')
        refusal.http_status = 302
        item.opener = Opener(error=refusal)
        try:
            item.get(tools['build']['url'], 64, 'archive', 2)
        except new.Refusal as error:
            result = item.failure_result(error)
        else: raise AssertionError('redirect accepted')
        assert result['refusal_code'] == 'REDIRECT_REFUSED_NO_SECOND_REQUEST'
        record = json.loads((item.pool / 'events/01-refusal.json').read_text())
        assert record['http_status'] == 302 and item.opener.calls == 1
        assert_zero_archives(item, result)


def fdopen_failure_countercontrol():
    previous = load('held_v3_source_only', BASE / '184-acquire-tools-v3.py')
    for controller, expect_closed, expect_empty in ((previous, False, False), (new, True, True)):
        with owner() as scope:
            item = controller.Acquisition(NEW_REF)
            item.pool, item.created, item.dir_ids = scope.pool, True, scope.dir_ids
            descriptors = []
            def failed_wrap(fd, mode):
                descriptors.append(fd)
                raise OSError('synthetic fdopen boundary only')
            with patch.object(controller.os, 'fdopen', side_effect=failed_wrap):
                try: item.write('wheelhouse/probe.whl', b'synthetic only')
                except OSError: pass
                else: raise AssertionError('fdopen fault did not fire')
            assert len(descriptors) == 1
            try:
                os.fstat(descriptors[0])
            except OSError:
                closed = True
            else:
                closed = False
                os.close(descriptors[0])  # Control alone closes the reproduced predecessor leak.
            assert closed is expect_closed
            result = item.failure_result(OSError('synthetic fdopen boundary only'))
            archive = item.pool / 'wheelhouse/probe.whl'
            assert (not archive.exists()) is expect_empty
            assert result['archive_cleanup']['complete'] is expect_empty
            if expect_empty:
                assert_zero_archives(item, result)
                assert result['archive_nodes'] == {'opened': 1, 'written': 0, 'removed': 1, 'remaining_owned': 0}
            else:
                assert result['archive_nodes']['remaining_owned'] == 0 and archive.stat().st_size == 0


SENTINELS = {
    'GH_TOKEN': 'synthetic-gh', 'GITHUB_TOKEN': 'synthetic-github',
    'SSH_AUTH_SOCK': '/synthetic/agent', 'SSH_ASKPASS': '/synthetic/askpass',
    'AWS_ACCESS_KEY_ID': 'synthetic-aws', 'AWS_SECRET_ACCESS_KEY': 'synthetic-aws-secret',
    'GOOGLE_APPLICATION_CREDENTIALS': '/synthetic/gcp', 'AZURE_CLIENT_SECRET': 'synthetic-azure',
    'HTTP_PROXY': 'http://synthetic.invalid', 'HTTPS_PROXY': 'http://synthetic.invalid',
    'ALL_PROXY': 'http://synthetic.invalid', 'NO_PROXY': '*',
    'HOME': '/synthetic/home', 'SSL_CERT_FILE': '/synthetic/ca', 'PYTHONPATH': '/synthetic/python',
}


def bootstrap_environment_countercontrol():
    previous = load('held_v3_bootstrap_source_only', BASE / '184-acquire-tools-v3-bootstrap.py')
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-bootstrap-env-control-') as temporary:
        source = Path(temporary) / 'trusted-synthetic-owner.py'
        # Only names, never even sentinel values, leave the synthetic owner.
        raw = b'import os\nENV_SEEN = dict(os.environ)\ndef run_once(admission): pass\ndef run_guarded_once(admission, proof): return {}\n'
        source.write_bytes(raw)
        for launcher, expected_clean in ((previous, False), (bootstrap, True)):
            observed = []
            real_exec = exec
            def record_exec(code, namespace):
                real_exec(code, namespace)
                observed.append(namespace['ENV_SEEN'])
            with patch.dict(os.environ, SENTINELS, clear=False), \
                 patch.object(launcher, 'OWNER_PATH', source), \
                 patch.object(launcher, 'OWNER_SHA', hashlib.sha256(raw).hexdigest()), \
                 patch.dict(launcher.__dict__, {'exec': record_exec}):
                launcher.launch(NEW_REF)
            assert len(observed) == 1
            if expected_clean:
                assert observed[0] == new.MINIMAL_ENV == bootstrap.MINIMAL_ENV
                assert not set(SENTINELS) & set(observed[0])
            else:
                assert all(observed[0].get(key) == value for key, value in SENTINELS.items())


def pinned_bundle_without_child_control():
    ca = new.read_regular(new.CA_SOURCE, 300000)
    with patch.dict(os.environ, SENTINELS, clear=False), \
         patch.object(subprocess, 'run', side_effect=AssertionError('NO_CHILD_FOR_PINNED_CA')) as child:
        proof = new.verify_ca_bundle(ca)
        assert proof['sha256'] == new.CA_SHA and proof['certificates'] == 118 and proof['blocked_subjects'] == 0
        refuses(lambda: new.verify_ca_bundle(ca+b'changed'), 'EXISTING_CLEAN_CA_PROVENANCE_CHANGED')
        assert child.call_count == 0


def owner_has_no_child_launch_api():
    tree=ast.parse((BASE/'184-acquire-tools-v5.py').read_bytes())
    for node in ast.walk(tree):
        if isinstance(node,ast.Import):assert all(alias.name!='subprocess' for alias in node.names)
        if isinstance(node,ast.ImportFrom):assert node.module!='subprocess'
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name):
            assert not (node.func.value.id=='os' and (node.func.attr in {'system','popen','fork','forkpty','posix_spawn','posix_spawnp'}
                        or node.func.attr.startswith(('exec','spawn'))))


def ambient_owner_environment_refuses():
    with patch.object(new, 'VERIFIED_SOURCE_SHA256', '0' * 64), patch.dict(os.environ, SENTINELS, clear=False):
        item = new.Acquisition(NEW_REF)
        refuses(item.run, 'CLOSED_OWNER_ENVIRONMENT_REQUIRED')
        assert not item.created and not item.requests


def cleanup_refuses_unknown_replacement():
    raw, tool, authority = synthetic_wheel()
    with owner() as item:
        item.retain_and_inspect(raw, tool, authority)
        p = item.pool / 'wheelhouse' / tool['filename']
        p.rename(item.pool / 'original-owned-node')
        p.write_bytes(b'unknown replacement')
        result = item.failure_result(new.Refusal('primary remains primary'))
        assert result['refusal_code'] == 'PRIMARY_REMAINS_PRIMARY'
        assert not result['archive_cleanup']['complete']
        assert result['archive_cleanup']['refusal_code'] == 'CLEANUP_ARCHIVE_IDENTITY_COLLISION'
        assert p.read_bytes() == b'unknown replacement'


def cleanup_refuses_replaced_directory():
    raw, tool, authority = synthetic_wheel()
    with owner() as item:
        item.retain_and_inspect(raw, tool, authority)
        (item.pool / 'wheelhouse').rename(item.pool / 'original-wheelhouse')
        (item.pool / 'wheelhouse').mkdir()
        result = item.failure_result(new.Refusal('primary remains primary'))
        assert not result['archive_cleanup']['complete']
        assert result['archive_cleanup']['refusal_code'] == 'CLEANUP_ARCHIVE_DIRECTORY_CHANGED'
        assert (item.pool / 'original-wheelhouse' / tool['filename']).read_bytes() == raw


def uncreated_pool_never_mutated():
    with owner() as item:
        item.created = False
        before = sorted(p.name for p in item.pool.iterdir())
        result = item.failure_result(FileExistsError('synthetic existing pool'))
        assert result['archive_cleanup']['complete'] and not result['failure_record_written']
        assert sorted(p.name for p in item.pool.iterdir()) == before
        assert not (item.pool / 'failure.json').exists()


def transport_failure_cleans_prior_archive(response, code, *, size=None):
    raw, tool, authority = synthetic_wheel()
    with owner() as item:
        item.retain_and_inspect(raw, tool, authority)
        item.opener = Opener(response)
        try:
            item.get(tools['build']['url'], 64, 'archive', size or 2)
        except BaseException as error:
            result = item.failure_result(error)
        else: raise AssertionError('transport refusal required')
        assert result['refusal_code'] == code and item.opener.calls == 1
        assert len(result['http_requests']) == 1
        assert_zero_archives(item, result)


def wrong_archive_tuple_cleans_prior_archive(change_size):
    raw, tool, authority = synthetic_wheel()
    with owner() as item:
        item.retain_and_inspect(raw, tool, authority)
        raw, tool, authority = synthetic_wheel('packaging')
        if change_size: tool['bytes'] += 1
        else: tool['sha256'] = '0' * 64
        try:
            item.retain_and_inspect(raw, tool, authority)
        except new.Refusal as error:
            result = item.failure_result(error)
        else: raise AssertionError('wrong tuple accepted')
        assert result['refusal_code'] == 'ARCHIVE_BYTES_HASH_MISMATCH'
        assert_zero_archives(item, result)


def graph_change():
    changed = (authorities['build']['info']['requires_dist'] or []) + ['unselected-tool>=1']
    raw, tool, authority = synthetic_wheel(requirements=changed)
    authority['info']['requires_dist'] = changed
    with owner() as item:
        refuses(lambda: item.inspect_wheel(raw, tool, authority), 'APPROVED_ACTIVE_DEPENDENCY_GRAPH_CHANGED')


def unsafe_zip():
    raw, tool, authority = synthetic_wheel(change=lambda members, tool, authority: members.update({'../escape': 'data'}))
    with owner() as item:
        refuses(lambda: item.inspect_wheel(raw, tool, authority), 'UNSAFE_ZIP_MEMBER')


def partial_response():
    with owner() as item:
        item.opener = Opener(Response(b'abc', length=None, partial=True))
        try:
            item.get(tools['build']['metadata_source'], 64, 'metadata')
        except TimeoutError:
            record = json.loads((item.pool / 'events/01-refusal.json').read_text())
            assert record['refusal_code'] == 'EXCEPTION_TIMEOUTERROR'
            assert record['stage'] == 'body-reading' and record['bytes_received'] == 3
            assert item.requests[-1]['partial_body_sha256'] == hashlib.sha256(b'abc').hexdigest()
            assert item.opener.calls == 1
        else: raise AssertionError('partial response did not fail')


def diagnostic_write_preserves_primary():
    with owner() as item:
        item.opener = Opener(Response(length='99999'))
        write = item.write
        def refused_event(name, raw):
            if name.endswith('-refusal.json'):
                raise OSError('synthetic log failure')
            write(name, raw)
        item.write = refused_event
        refuses(lambda: item.get(tools['build']['metadata_source'], 64, 'metadata'), 'HTTP_BODY_EXCEEDS_BOUND')
        assert item.requests[-1]['diagnostic_write_failed'] is True


def outside_output():
    with owner() as item:
        refuses(lambda: item.write('../outside', b'bad'), 'INVALID_OUTPUT_PATH')
        assert not (item.pool.parent / 'outside').exists()


def changed_directory():
    with owner() as item:
        (item.pool / 'metadata').rename(item.pool / 'original-metadata')
        (item.pool / 'metadata').mkdir()
        refuses(lambda: item.write('metadata/probe.json', b'{}'), 'POOL_SUBDIRECTORY_CHANGED')
        assert not (item.pool / 'metadata/probe.json').exists()


def output_collision():
    with owner() as item:
        item.write('once.json', b'one')
        refuses(lambda: item.write('once.json', b'two'), 'POOL_OUTPUT_BOUND_OR_COLLISION')
        assert (item.pool / 'once.json').read_bytes() == b'one'


def preserve_failure_reserve():
    with owner() as item:
        item.used = new.POOL_CAP - 65536
        refuses(lambda: item.write('over-bound.json', b'x'), 'POOL_OUTPUT_BOUND_OR_COLLISION')
        item.write('failure.json', b'{}')
        assert (item.pool / 'failure.json').read_bytes() == b'{}'


def extra_empty_directory_refuses():
    with owner() as item:
        (item.pool / 'extra-empty-directory').mkdir()
        refuses(item.check_pool, 'EXTRA_OR_MISSING_POOL_OUTPUT')


def changed_written_file_refuses():
    with owner() as item:
        item.write('written.json', b'one')
        p = item.pool / 'written.json'
        p.chmod(0o600)
        p.write_bytes(b'two')
        p.chmod(0o400)
        refuses(item.check_pool, 'WRITTEN_POOL_BYTES_CHANGED')


def complete_synthetic_pool_checks_and_seals():
    with owner() as item:
        item.write('written.json', b'one')
        assert len(item.check_pool()) == 1
        item.seal()
        assert len(item.check_pool()) == 1 and item.pool.stat().st_mode & 0o777 == 0o500
        item.pool.chmod(0o700)
        for name in ('metadata', 'wheelhouse', 'events'):
            (item.pool / name).chmod(0o700)


def old_admission_refuses_before_pool():
    refuses(lambda: new.Acquisition(new.OLD_ADMISSION), 'NEW_ADMISSION_REQUIRED_OLD_ATTEMPT_CANNOT_RETRY')


def missing_bootstrap_refuses_before_pool():
    item = new.Acquisition(NEW_REF)
    refuses(item.run, 'VERIFIED_SOURCE_BOOTSTRAP_REQUIRED')
    assert not item.created


def authority_refuses_before_request(url):
    with owner() as item:
        item.opener = Opener(Response())
        refuses(lambda: item.get(url, 64, 'metadata'), 'UNEXPECTED_PUBLIC_AUTHORITY')
        assert not item.requests and item.opener.calls == 0


def redirect_has_status_and_no_second_request():
    error = new.Refusal('redirect refused no second request')
    error.http_status = 302
    with owner() as item:
        item.opener = Opener(error=error)
        refuses(lambda: item.get(tools['build']['metadata_source'], 64, 'metadata'),
                'REDIRECT_REFUSED_NO_SECOND_REQUEST')
        record = json.loads((item.pool / 'events/01-refusal.json').read_text())
        assert record['http_status'] == 302 and item.opener.calls == 1
    refuses(lambda: new.NoRedirect().redirect_request(None, None, 302, None, {}, 'https://example.invalid'),
            'REDIRECT_REFUSED_NO_SECOND_REQUEST')


def HTTP_error_has_status():
    error = urllib.error.HTTPError(tools['build']['metadata_source'], 404, 'synthetic', {}, None)
    with owner() as item:
        item.opener = Opener(error=error)
        try:
            item.get(tools['build']['metadata_source'], 64, 'metadata')
        except urllib.error.HTTPError:
            record = json.loads((item.pool / 'events/01-refusal.json').read_text())
            assert record['http_status'] == 404 and record['refusal_code'] == 'EXCEPTION_HTTPERROR'
            assert item.opener.calls == 1
        else: raise AssertionError('HTTP failure accepted')


def bootstrap_validates_without_execution():
    raw, _ = bootstrap.verified_source(bootstrap.OWNER_PATH, bootstrap.OWNER_SHA)
    assert hashlib.sha256(raw).hexdigest() == bootstrap.OWNER_SHA


def bootstrap_changed_source_refuses():
    try:
        bootstrap.verified_source(bootstrap.OWNER_PATH, '0' * 64)
    except ValueError as error:
        assert str(error) == 'BOOTSTRAP_SOURCE_HASH_MISMATCH'
    else: raise AssertionError('changed source accepted')


def bootstrap_symlink_refuses():
    with tempfile.TemporaryDirectory(prefix='sdk-python-bootstrap-control-') as temporary:
        p = Path(temporary) / 'source-link'
        p.symlink_to(bootstrap.OWNER_PATH)
        try:
            bootstrap.verified_source(p, bootstrap.OWNER_SHA)
        except OSError:
            pass
        else: raise AssertionError('symlink source accepted')


def bootstrap_hardlink_refuses():
    import os
    with tempfile.TemporaryDirectory(prefix='sdk-python-bootstrap-control-') as temporary:
        p = Path(temporary) / 'synthetic-source'
        p.write_bytes(b'never executed')
        os.link(p, Path(temporary) / 'second-link')
        try:
            bootstrap.verified_source(p, hashlib.sha256(p.read_bytes()).hexdigest())
        except ValueError as error:
            assert str(error) == 'BOOTSTRAP_SOURCE_TYPE_OR_SIZE'
        else: raise AssertionError('shared source inode accepted')


def bootstrap_old_admission_refuses_before_source_read():
    with patch.object(bootstrap, 'verified_source', side_effect=AssertionError('must not read/execute source')):
        try:
            bootstrap.launch(bootstrap.OLD_ADMISSION)
        except ValueError as error:
            assert str(error) == 'NEW_GENERAL_ADMISSION_REQUIRED_OLD_ATTEMPT_CANNOT_RETRY'
        else: raise AssertionError('old admission reused')


with patch.object(socket, 'create_connection', side_effect=AssertionError('NO_NETWORK_IN_SOURCE_CONTROLS')), \
     patch.object(urllib.request, 'urlopen', side_effect=AssertionError('NO_NETWORK_IN_SOURCE_CONTROLS')):
    check('old-refuses-equivalent-marker-quotes-new-accepts', old_quote_control)
    for name in tools:
        check('retained-authority-tuple-' + name, lambda name=name: new.validate_authority(authorities[name], tools[name]))
        def headers_for_name(name=name):
            raw, tool, authority = synthetic_wheel(name, single_quotes=True)
            with owner() as item:
                assert item.inspect_wheel(raw, tool, authority)['name'] == name
        check('authority-derived-synthetic-headers-' + name, headers_for_name)
    for label, mutator, code in [
        ('version', lambda a: a['info'].update(version='changed'), 'VERSIONED_METADATA_IDENTITY_CHANGED'),
        ('project', lambda a: a['info'].update(name='changed'), 'VERSIONED_METADATA_IDENTITY_CHANGED'),
        ('python', lambda a: a['info'].update(requires_python='>=9'), 'VERSIONED_METADATA_IDENTITY_CHANGED'),
        ('URL', lambda a: a['urls'][0].update(url='https://example.invalid/tool'), 'VERSIONED_ARCHIVE_TUPLE_YANK_STATE_CHANGED'),
        ('size', lambda a: a['urls'][0].update(size=1), 'VERSIONED_ARCHIVE_TUPLE_YANK_STATE_CHANGED'),
        ('bool-size', lambda a: a['urls'][0].update(size=True), 'VERSIONED_ARCHIVE_TUPLE_YANK_STATE_CHANGED'),
        ('hash', lambda a: a['urls'][0]['digests'].update(sha256='0'*64), 'VERSIONED_ARCHIVE_TUPLE_YANK_STATE_CHANGED'),
        ('yanked', lambda a: a['urls'][0].update(yanked=True), 'VERSIONED_ARCHIVE_TUPLE_YANK_STATE_CHANGED'),
        ('unknown-yank', lambda a: a['urls'][0].pop('yanked'), 'VERSIONED_ARCHIVE_TUPLE_YANK_STATE_CHANGED'),
        ('string-yank', lambda a: a['urls'][0].update(yanked='false'), 'VERSIONED_ARCHIVE_TUPLE_YANK_STATE_CHANGED'),
        ('duplicate', lambda a: a['urls'].append(deepcopy(a['urls'][0])), 'VERSIONED_METADATA_ARCHIVE_CARDINALITY'),
    ]:
        check('metadata-refuses-' + label, lambda m=mutator, c=code: metadata_mutation(m, c))
    check('single-request-receipt-and-body-hash', lambda: transport_control(Response()))
    check('oversize-header-stable-refusal', lambda: transport_control(Response(length='99999'), 'HTTP_BODY_EXCEEDS_BOUND'))
    check('encoded-response-stable-refusal', lambda: transport_control(Response(encoding='gzip'), 'ENCODED_BODY_REFUSED'))
    check('changed-response-URL', lambda: transport_control(Response(url='https://example.invalid'), 'UNEXPECTED_HTTP_RESULT'))
    check('archive-missing-length', lambda: transport_control(Response(length=None), 'ARCHIVE_HTTP_SIZE_CHANGED', kind='archive', size=2))
    check('archive-changed-length', lambda: transport_control(Response(length='3'), 'ARCHIVE_HTTP_SIZE_CHANGED', kind='archive', size=2))
    check('partial-body-count-hash-refusal-ledger', partial_response)
    check('diagnostic-failure-preserves-primary-code', diagnostic_write_preserves_primary)
    check('zero-archive-after-semantic-refusal', body_removed_after_semantic_refusal)
    check('hash-refusal-does-not-retain-unverified-candidate', bad_hash_not_retained)
    check('late-refusal-removes-all-new-archives', late_failure_removes_earlier_archives)
    check('malformed-ZIP-removes-new-archive', malformed_zip_cleanup)
    check('special-ZIP-member-removes-new-archive', special_zip_cleanup)
    check('partial-archive-write-tracked-and-removed', partial_archive_write_cleanup)
    check('failure-record-collision-preserves-primary-and-zero-archives', failure_record_collision_cleanup)
    check('unknown-failure-record-collision-preserved-and-zero-archives', untracked_failure_record_collision_cleanup)
    check('v2-retains-semantic-refused-archive-v3-removes-countercontrol', v2_semantic_refusal_countercontrol)
    check('redirect-refusal-ledger-and-zero-archives', redirect_failure_cleans_prior_archive)
    check('v3-fdopen-leaks-archive-and-fd-v4-closes-and-removes', fdopen_failure_countercontrol)
    check('v3-bootstrap-inherits-sentinels-v4-closed-owner-env', bootstrap_environment_countercontrol)
    check('exact-reviewed-CA-reused-without-child-changed-bytes-refuse', pinned_bundle_without_child_control)
    check('guarded-owner-has-no-child-launch-surface',owner_has_no_child_launch_api)
    check('owner-refuses-ambient-env-before-pool-or-network', ambient_owner_environment_refuses)
    check('cleanup-identity-collision-preserves-unknown-node-nonpass', cleanup_refuses_unknown_replacement)
    check('cleanup-directory-collision-preserves-unknown-tree-nonpass', cleanup_refuses_replaced_directory)
    check('uncreated-pool-collision-never-mutates-existing-pool', uncreated_pool_never_mutated)
    for label, response, code in [
        ('missing-Length', Response(length=None), 'ARCHIVE_HTTP_SIZE_CHANGED'),
        ('changed-Length', Response(length='3'), 'ARCHIVE_HTTP_SIZE_CHANGED'),
        ('final-URL', Response(url='https://example.invalid'), 'UNEXPECTED_HTTP_RESULT'),
        ('encoding', Response(encoding='gzip'), 'ENCODED_BODY_REFUSED'),
        ('timeout', Response(b'abc', length='3', partial=True), 'EXCEPTION_TIMEOUTERROR'),
    ]:
        check('transport-refusal-zero-archives-' + label,
              lambda response=response, code=code: transport_failure_cleans_prior_archive(
                  response, code, size=3 if label == 'timeout' else 2))
    check('wrong-size-removes-earlier-new-archive', lambda: wrong_archive_tuple_cleans_prior_archive(True))
    check('wrong-hash-removes-earlier-new-archive', lambda: wrong_archive_tuple_cleans_prior_archive(False))
    check('changed-active-graph-refuses', graph_change)
    check('unsafe-ZIP-path-refuses', unsafe_zip)
    check('parent-output-refuses', outside_output)
    check('changed-directory-inode-refuses', changed_directory)
    check('exclusive-output-collision-refuses', output_collision)
    check('failure-record-reserve-survives-pool-bound', preserve_failure_reserve)
    check('empty-extra-directory-refuses', extra_empty_directory_refuses)
    check('written-file-mutation-refuses', changed_written_file_refuses)
    check('complete-synthetic-pool-custody-and-seal', complete_synthetic_pool_checks_and_seals)
    check('old-admission-cannot-retry', old_admission_refuses_before_pool)
    check('missing-hash-bootstrap-refuses-before-pool', missing_bootstrap_refuses_before_pool)
    check('HTTP-scheme-refuses-before-request', lambda: authority_refuses_before_request('http://pypi.org/'))
    check('foreign-origin-refuses-before-request', lambda: authority_refuses_before_request('https://example.invalid/'))
    check('URL-userinfo-refuses-before-request', lambda: authority_refuses_before_request('https://synthetic:synthetic@pypi.org/'))
    check('redirect-status-custody-no-second-request', redirect_has_status_and_no_second_request)
    check('HTTP-error-status-custody', HTTP_error_has_status)
    check('body-shorter-than-header-refuses', lambda: transport_control(Response(length='3'), 'HTTP_BODY_LENGTH_MISMATCH'))
    check('bootstrap-validates-owned-bytes-without-execution', bootstrap_validates_without_execution)
    check('bootstrap-changed-source-refuses', bootstrap_changed_source_refuses)
    check('bootstrap-symlink-source-refuses', bootstrap_symlink_refuses)
    check('bootstrap-shared-inode-source-refuses', bootstrap_hardlink_refuses)
    check('bootstrap-old-admission-refuses-before-source-read', bootstrap_old_admission_refuses_before_source_read)
    for name, text, expected in [
        ('numeric-python-order', "foo; python_version >= '3.9'", True),
        ('old-Python-dependency-excluded', 'foo; python_full_version < "3.10.2"', False),
        ('Windows-dependency-excluded', "foo; os_name == 'nt'", False),
        ('extra-dependency-excluded', "foo; extra == 'core'", False),
    ]:
        check('marker-' + name, lambda text=text, expected=expected: (_ for _ in ()).throw(AssertionError('marker result'))
              if new.marker_active(text) is not expected else None)
    check('marker-code-is-not-executed', lambda: refuses(
        lambda: new.marker_active("foo; extra == 'test' and __import__('os').system('false')"), 'UNSUPPORTED_MARKER_SYNTAX'))
    check('patch-sensitive-marker-refuses', lambda: refuses(
        lambda: new.marker_active("foo; python_full_version >= '3.13.1'"), 'PATCH_SENSITIVE_MARKER_REQUIRES_EXACT_RUNTIME_VERSION'))
    check('unsupported-marker-variable-refuses', lambda: refuses(
        lambda: new.marker_active("foo; unknown == 'yes'"), 'UNSUPPORTED_MARKER_VARIABLE'))

report = {'scope': 'synthetic source controls, not archive acquisition/SDK/runtime qualification',
          'original_source_sha256': hashlib.sha256((BASE / '184-acquire-tools.py').read_bytes()).hexdigest(),
          'successor_source_sha256': bootstrap.OWNER_SHA,
          'bootstrap_sha256': hashlib.sha256((BASE / '184-acquire-tools-v5-bootstrap.py').read_bytes()).hexdigest(),
          'cases': len(results), 'passed': sum(row['outcome'] == 'pass' for row in results),
          'failed': sum(row['outcome'] == 'fail' for row in results), 'results': results,
          'simulated_transport_calls': fake_calls, 'real_network_requests': 0,
          'downloaded_wheel_imports_or_installs': 0, 'actual_failed_archive_cause_established': False}
(BASE / '184-acquire-tools-v5-controls-result.json').write_text(json.dumps(report, sort_keys=True, indent=2)+'\n')
print(json.dumps(report, indent=2))
raise SystemExit(bool(report['failed']))
