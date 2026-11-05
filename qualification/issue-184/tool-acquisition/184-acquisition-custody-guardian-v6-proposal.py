"""Source-only custody primitive. No acquisition launcher or acceptance path exists here.

The guardian preallocates archive nodes before handing any writable descriptor
to an owner, so owner death cannot leave an unregistered new archive. A future
owner adapter and exact authority/descriptor handoff require separate review.
"""
from __future__ import annotations

from contextlib import ExitStack
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import time

MINIMAL_ENV = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'TZ': 'UTC'}
OLD_DECISION = 'https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-6649046802'
SELECTED_SOURCE = '14adc40b00b68cc8a3e1ae5647a885a42f273650'
ARCHIVE_CAP = 3 * 1024**2


class Refusal(ValueError):
    pass


class AllocationRefusal(Refusal):
    """Partial construction returns custody instead of discarding live handles."""
    def __init__(self, custody, cleanup, cause):
        super().__init__('ALLOCATION_REFUSED')
        self.custody, self.cleanup = custody, cleanup
        self.cause_type = type(cause).__name__


def require(value: bool, code: str) -> None:
    if not value:
        raise Refusal(code)


AUTHORITY_FIELDS = {'decision', 'selected_source', 'inputs', 'environment', 'host',
                    'total_seconds', 'io_seconds', 'retries', 'survivor_policy',
                    'pool_parent', 'launcher_sha256', 'cpu_affinity'}
AUTHORITY_FIELDS.add('retention_deadline_utc')
INPUT_FIELDS = {'owner', 'bootstrap', 'guardian', 'terminal', 'selection', 'interpreter', 'library', 'ca'}


def unique_pairs(values):
    result = {}
    for key, value in values:
        require(key not in result, 'DUPLICATE_AUTHORITY_KEY')
        result[key] = value
    return result


def reject_constant(value):
    raise Refusal('NONFINITE_AUTHORITY_NUMBER')


def decode_authority(raw: bytes) -> dict:
    authority = json.loads(raw.decode('utf-8'), object_pairs_hook=unique_pairs, parse_constant=reject_constant)
    require(type(authority) is dict and set(authority) == AUTHORITY_FIELDS, 'EXACT_AUTHORITY_SCHEMA_REQUIRED')
    require(type(authority['inputs']) is dict and set(authority['inputs']) == INPUT_FIELDS
            and all(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value)
                    for value in authority['inputs'].values()), 'EXACT_INPUT_SCHEMA_REQUIRED')
    require(type(authority['environment']) is dict and set(authority['environment']) == set(MINIMAL_ENV)
            and all(type(value) is str for value in authority['environment'].values()),
            'EXACT_ENVIRONMENT_SCHEMA_REQUIRED')
    require(all(type(authority[key]) is int for key in ('total_seconds', 'io_seconds', 'retries')),
            'INTEGER_AUTHORITY_LIMITS_REQUIRED')
    require(all(type(authority[key]) is str for key in ('decision', 'selected_source', 'host',
            'survivor_policy', 'pool_parent', 'launcher_sha256'))
            and re.fullmatch(r'[0-9a-f]{64}', authority['launcher_sha256']), 'EXACT_AUTHORITY_VALUE_TYPES_REQUIRED')
    require(type(authority['retention_deadline_utc']) is str
            and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z',
                             authority['retention_deadline_utc']), 'EXACT_RETENTION_DEADLINE_REQUIRED')
    affinity = authority['cpu_affinity']
    require(type(affinity) is list and 1 <= len(affinity) <= 2
            and all(type(cpu) is int and cpu >= 0 for cpu in affinity)
            and len(set(affinity)) == len(affinity), 'EXACT_CPU_SCHEMA_REQUIRED')
    return authority


def bind_authority(raw: bytes, expected_sha: str, expected_decision: str, expected_inputs: dict) -> dict:
    """Expected values must come from the separately reviewed exact future invocation."""
    require(len(raw) <= 32768 and hashlib.sha256(raw).hexdigest() == expected_sha, 'AUTHORITY_BYTES_CHANGED')
    authority = decode_authority(raw)
    require(type(expected_inputs) is dict and set(expected_inputs) == INPUT_FIELDS,
            'REVIEWED_INPUT_SCHEMA_REQUIRED')
    require(isinstance(expected_decision, str) and expected_decision != OLD_DECISION
            and re.fullmatch(r'https://github\.com/durable-workflow/sdk-python/issues/184#issuecomment-[1-9][0-9]{5,17}', expected_decision)
            and authority.get('decision') == expected_decision, 'EXACT_NEW_DECISION_REQUIRED')
    require(authority.get('selected_source') == SELECTED_SOURCE
            and authority.get('inputs') == expected_inputs, 'EXACT_INPUT_AUTHORITY_CHANGED')
    require(authority.get('environment') == MINIMAL_ENV and authority.get('host') == 'lab4'
            and authority.get('total_seconds') == 300 and authority.get('io_seconds') == 270
            and authority.get('retries') == 0, 'OPERATION_BOUNDARY_CHANGED')
    require(authority['survivor_policy'] == 'retain-custody-and-escalate'
            and authority['pool_parent'] == '/home/lab4/fleet/tmp', 'EXACT_CUSTODY_POLICY_REQUIRED')
    return authority


class ArchiveCustody:
    """Owns only a newly exclusive root and exact preallocated archive inodes."""
    def __init__(self, parent: Path, leaf: str, filenames: list[str]):
        require(re.fullmatch(r'sdk-python-184-tools-guardian-[a-zA-Z0-9-]{1,96}', leaf) is not None,
                'INVALID_EXCLUSIVE_ROOT_NAME')
        require(1 <= len(filenames) <= 6 and len(set(filenames)) == len(filenames)
                and all(re.fullmatch(r'[a-zA-Z0-9_.-]{1,160}\.whl', name) for name in filenames),
                'INVALID_ARCHIVE_NAMES')
        self.path = parent / leaf
        self.stack = ExitStack()
        self.closed_owned_fds = set()
        self.archive_nodes: dict[str, dict] = {}
        self.root_id = None
        self.wheelhouse_id = None
        self.created = False
        self.allocated_directory_names = set()
        self.root_fd = None
        self.wheelhouse_fd = None
        # Directory descriptors cannot be wrapped in streams; keep explicit close custody.
        self.parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            self.stack.callback(self.close_owned_fd, self.parent_fd)
            st = os.fstat(self.parent_fd)
            require(st.st_uid == os.geteuid() and not st.st_mode & 0o022, 'UNSAFE_POOL_PARENT')
            os.mkdir(leaf, 0o700, dir_fd=self.parent_fd)
            self.created = True
            st = os.stat(leaf, dir_fd=self.parent_fd, follow_symlinks=False)
            self.root_id = (st.st_dev, st.st_ino)
            self.root_fd = os.open(leaf, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.parent_fd)
            self.stack.callback(self.close_owned_fd, self.root_fd)
            st = os.fstat(self.root_fd)
            require((st.st_dev, st.st_ino) == self.root_id, 'ROOT_ALLOCATION_COLLISION')
            os.mkdir('wheelhouse', 0o700, dir_fd=self.root_fd)
            self.allocated_directory_names.add('wheelhouse')
            st = os.stat('wheelhouse', dir_fd=self.root_fd, follow_symlinks=False)
            self.wheelhouse_id = (st.st_dev, st.st_ino)
            self.wheelhouse_fd = os.open('wheelhouse', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.root_fd)
            self.stack.callback(self.close_owned_fd, self.wheelhouse_fd)
            st = os.fstat(self.wheelhouse_fd)
            require((st.st_dev, st.st_ino) == self.wheelhouse_id, 'WHEELHOUSE_ALLOCATION_COLLISION')
            for name in filenames:
                fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600,
                             dir_fd=self.wheelhouse_fd)
                # Raw descriptor custody starts before callback/identity reads.
                # Pending identity never permits handoff or named-node removal.
                self.archive_nodes[name] = {'fd': fd, 'identity': None}
                self.stack.callback(self.close_owned_fd, fd)
                st = os.fstat(fd)
                self.archive_nodes[name]['identity'] = (st.st_dev, st.st_ino)
        except BaseException as error:
            try:
                cleanup = self.cleanup() if self.created else {
                    'complete': True, 'exclusive_root_created': False, 'outer_owner_custody_required': False}
            except BaseException as cleanup_error:
                cleanup = {'complete': False, 'outer_owner_custody_required': True,
                           'refusal_code': type(cleanup_error).__name__}
            # Even an incomplete constructor returns the exact object/handles.
            # The caller must retain it until the reported tree custody closes.
            raise AllocationRefusal(self, cleanup, error) from error

    def scope(self) -> None:
        try:
            st = os.stat(self.path.name, dir_fd=self.parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            raise Refusal('ROOT_SCOPE_MISSING') from None
        require(stat.S_ISDIR(st.st_mode) and st.st_uid == os.geteuid()
                and (st.st_dev, st.st_ino) == self.root_id, 'ROOT_SCOPE_COLLISION')
        try:
            st = os.stat('wheelhouse', dir_fd=self.root_fd, follow_symlinks=False)
        except FileNotFoundError:
            raise Refusal('WHEELHOUSE_SCOPE_MISSING') from None
        require(stat.S_ISDIR(st.st_mode) and st.st_uid == os.geteuid()
                and (st.st_dev, st.st_ino) == self.wheelhouse_id, 'WHEELHOUSE_SCOPE_COLLISION')

    def node(self, name: str) -> os.stat_result:
        self.scope()
        expected = self.archive_nodes[name]
        st = os.stat(name, dir_fd=self.wheelhouse_fd, follow_symlinks=False)
        require(expected['identity'] is not None, 'ARCHIVE_IDENTITY_UNPROVEN')
        require(stat.S_ISREG(st.st_mode) and st.st_uid == os.geteuid() and st.st_nlink == 1
                and (st.st_dev, st.st_ino) == expected['identity'], 'ARCHIVE_SCOPE_COLLISION')
        descriptor = os.fstat(expected['fd'])
        require((descriptor.st_dev, descriptor.st_ino) == expected['identity'], 'ARCHIVE_FD_CHANGED')
        return st

    def duplicate_for_owner(self, name: str) -> int:
        """Only valid after a future ready/authority handshake; no owner is launched here."""
        self.node(name)
        return os.dup(self.archive_nodes[name]['fd'])

    def verify_archive_bytes(self, expected: dict[str, dict]) -> list[dict]:
        """Raw tuple check only. This does not accept tool metadata or an acquisition."""
        require(set(expected) == set(self.archive_nodes), 'INCOMPLETE_ARCHIVE_SET')
        self.scope()
        require(set(os.listdir(self.wheelhouse_fd)) == set(expected), 'EXTRA_OR_MISSING_ARCHIVE_NODE')
        rows = []
        for name, wanted in expected.items():
            st = self.node(name)
            require(type(wanted.get('bytes')) is int and 0 < wanted['bytes'] <= 2 * 1024**2
                    and st.st_size == wanted['bytes'] and stat.S_IMODE(st.st_mode) == 0o400,
                    'ARCHIVE_SIZE_OR_MODE_CHANGED')
            fd = self.archive_nodes[name]['fd']
            raw = os.pread(fd, wanted['bytes'] + 1, 0)
            after = os.fstat(fd)
            require((st.st_size, st.st_mtime_ns, st.st_ctime_ns)
                    == (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    and len(raw) == st.st_size and hashlib.sha256(raw).hexdigest() == wanted['sha256'],
                    'ARCHIVE_BYTES_CHANGED')
            rows.append({'filename': name, 'bytes': len(raw), 'sha256': wanted['sha256']})
        require(sum(row['bytes'] for row in rows) <= ARCHIVE_CAP, 'ARCHIVE_TOTAL_BOUND')
        return rows

    def cleanup_archives(self) -> dict:
        """Only the archive namespace; never claims root-tree completion."""
        result = {'complete': False, 'removed': [], 'already_absent': [], 'collisions': []}
        self.scope()
        os.fchmod(self.wheelhouse_fd, 0o700)
        for name in self.archive_nodes:
            try:
                self.node(name)
            except FileNotFoundError:
                if os.fstat(self.archive_nodes[name]['fd']).st_nlink == 0:
                    result['already_absent'].append(name)
                else:
                    result['collisions'].append({'filename': name, 'code': 'OWNED_NODE_STILL_LINKED'})
            except Refusal as error:
                result['collisions'].append({'filename': name, 'code': str(error)})
            else:
                os.unlink(name, dir_fd=self.wheelhouse_fd)
                result['removed'].append(name)
        result['remaining_names'] = sorted(os.listdir(self.wheelhouse_fd))
        self.scope()
        result['complete'] = not result['remaining_names'] and not result['collisions']
        return result

    def unlink_state(self) -> dict:
        """Root absence cannot stand in for unlink proof of tracked descendants."""
        directories = {'.': (self.root_fd, self.root_id)}
        if self.wheelhouse_id is not None or 'wheelhouse' in self.allocated_directory_names:
            directories['wheelhouse'] = (self.wheelhouse_fd, self.wheelhouse_id)
        for name, identity in getattr(self, 'dir_ids', {}).items():
            if name not in directories:
                directories[name] = (getattr(self, 'directory_fds', {}).get(name), tuple(identity))
        for name in self.allocated_directory_names:
            directories.setdefault(name, (None, None))
        tracked = [(name, fd, identity, stat.S_ISDIR, False) for name, (fd, identity) in directories.items()
                   ]
        tracked += [('archive:' + name, row['fd'], row['identity'], stat.S_ISREG, True)
                    for name, row in self.archive_nodes.items()]
        retained = []
        for name, fd, identity, right_type, raw_archive in tracked:
            if fd in self.closed_owned_fds:
                retained.append(name)
                continue
            try:
                st = os.fstat(fd)
                proven = (right_type(st.st_mode) and st.st_uid == os.geteuid()
                          and ((raw_archive and identity is None) or (st.st_dev, st.st_ino) == identity)
                          and st.st_nlink == 0)
            except (OSError, TypeError):
                proven = False
            if not proven:
                retained.append(name)
        return {'all_unlinked': bool(self.root_id is not None) and not retained,
                'retained_or_unproven_inodes': sorted(retained)}

    def finish_unlinked(self, result: dict) -> dict:
        state = self.unlink_state()
        result.update(state)
        result['complete'] = state['all_unlinked']
        result['outer_owner_custody_required'] = not result['complete']
        if not result['complete']:
            result['refusal_code'] = 'TRACKED_INODES_STILL_LINKED_OR_UNPROVEN'
        return result

    def cleanup(self) -> dict:
        """Close the whole allocated tree; unknown members retain explicit custody."""
        result = {'complete': False, 'removed_directories': [], 'collisions': [],
                  'outer_owner_custody_required': True}
        if not self.created:
            result.update(complete=True, outer_owner_custody_required=False)
            return result
        try:
            named = os.stat(self.path.name, dir_fd=self.parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            # A vanished name is completion only if the exact created inode is
            # also unlinked; replacement/move is retained uncertain custody.
            return self.finish_unlinked(result)
        if not (stat.S_ISDIR(named.st_mode) and named.st_uid == os.geteuid()
                and (named.st_dev, named.st_ino) == self.root_id):
            result['collisions'].append('ROOT_SCOPE_COLLISION')
            return result
        if self.root_fd is None:
            self.root_fd = os.open(self.path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                   dir_fd=self.parent_fd)
            self.stack.callback(self.close_owned_fd, self.root_fd)
        current = os.fstat(self.root_fd)
        require((current.st_dev, current.st_ino) == self.root_id, 'ROOT_FD_COLLISION')
        os.fchmod(self.root_fd, 0o700)
        root_names = set(os.listdir(self.root_fd))
        result['unknown_root_names'] = sorted(root_names - ({'wheelhouse'} if self.wheelhouse_id else set()))
        if self.wheelhouse_id is not None:
            try:
                st = os.stat('wheelhouse', dir_fd=self.root_fd, follow_symlinks=False)
            except FileNotFoundError:
                if self.wheelhouse_fd is None or os.fstat(self.wheelhouse_fd).st_nlink != 0:
                    result['collisions'].append('WHEELHOUSE_NAME_MISSING_OR_MOVED')
            else:
                if not (stat.S_ISDIR(st.st_mode) and st.st_uid == os.geteuid()
                        and (st.st_dev, st.st_ino) == self.wheelhouse_id):
                    result['collisions'].append('WHEELHOUSE_SCOPE_COLLISION')
                else:
                    if self.wheelhouse_fd is None:
                        self.wheelhouse_fd = os.open('wheelhouse', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                                   dir_fd=self.root_fd)
                        self.stack.callback(self.close_owned_fd, self.wheelhouse_fd)
                    archives = self.cleanup_archives()
                    result['archive_cleanup'] = archives
                    if archives['complete']:
                        self.scope()
                        os.rmdir('wheelhouse', dir_fd=self.root_fd)
                        result['removed_directories'].append('wheelhouse')
                    else:
                        result['collisions'].append('ARCHIVE_NAMESPACE_RETAINED')
        result['remaining_root_names'] = sorted(os.listdir(self.root_fd))
        if not result['remaining_root_names'] and not result['collisions']:
            st = os.stat(self.path.name, dir_fd=self.parent_fd, follow_symlinks=False)
            require((st.st_dev, st.st_ino) == self.root_id, 'ROOT_REMOVAL_COLLISION')
            os.rmdir(self.path.name, dir_fd=self.parent_fd)
            result['removed_directories'].append('.')
            return self.finish_unlinked(result)
        result['outer_owner_custody_required'] = not result['complete']
        return result

    def close_owned_fd(self, fd: int) -> None:
        """Callbacks and explicit closure share one descriptor lifetime ledger."""
        if fd in self.closed_owned_fds:
            return
        try:
            os.close(fd)
        except OSError as error:
            if error.errno != errno.EBADF:
                raise
        self.closed_owned_fds.add(fd)

    def close(self) -> None:
        # Close the newest raw allocation first even if its callback was never
        # registered. Closing handles neither deletes nor accepts retained data.
        descriptors = [row['fd'] for row in reversed(list(self.archive_nodes.values()))]
        descriptors += list(reversed(list(getattr(self, 'directory_fds', {}).values())))
        descriptors += [self.wheelhouse_fd, self.root_fd, self.parent_fd]
        try:
            for fd in descriptors:
                if type(fd) is int:
                    self.close_owned_fd(fd)
        finally:
            self.stack.close()


def watch_owner_eof(custody: ArchiveCustody, owner_pipe: int, deadline: float, ready_pipe: int,
                    owner_pidfd: int, expected_pid: int, cleanup_ceiling: float) -> dict:
    """Independent process primitive; deadline is anchored before any future allocation.

    No success/preserve message is accepted. Until reviewed owner integration and
    terminal acceptance exist, this prototype ALWAYS cleans up on EOF/deadline.
    """
    require(dict(os.environ) == MINIMAL_ENV, 'CLOSED_GUARDIAN_ENV_REQUIRED')
    require(time.monotonic() < deadline <= time.monotonic() + 300, 'INVALID_ABSOLUTE_DEADLINE')
    require(deadline < cleanup_ceiling <= deadline + 30, 'INVALID_CLEANUP_CEILING')
    require(os.readlink(f'/proc/self/fd/{owner_pidfd}') == 'anon_inode:[pidfd]', 'OWNER_PIDFD_REQUIRED')
    info = Path(f'/proc/self/fdinfo/{owner_pidfd}').read_text()
    require(re.search(r'^Pid:\s*' + re.escape(str(expected_pid)) + r'\s*$', info, re.M) is not None
            and expected_pid > 1 and expected_pid != os.getpid(), 'EXACT_OWNER_PIDFD_REQUIRED')
    custody.scope()
    os.write(ready_pipe, b'READY\n')
    os.close(ready_pipe)
    reason = 'ABSOLUTE_DEADLINE'
    while time.monotonic() < deadline:
        available, _, _ = select.select([owner_pipe], [], [], max(0, deadline - time.monotonic()))
        if available:
            data = os.read(owner_pipe, 1)
            reason = 'OWNER_EOF' if not data else 'UNSUPPORTED_OWNER_MESSAGE'
            break
    # A pidfd refers to the created process, so PID reuse cannot retarget this stop.
    if not select.select([owner_pidfd], [], [], 0)[0]:
        try:
            signal.pidfd_send_signal(owner_pidfd, signal.SIGKILL)
        except ProcessLookupError:
            pass
    owner_absent = bool(select.select([owner_pidfd], [], [], max(0, min(5, cleanup_ceiling - time.monotonic())))[0])
    if not owner_absent:
        # A surviving process may retain writable descriptors. Keep its named
        # nodes in custody for the outer owner; unlinking them is not completion.
        result = {'complete': False, 'deferred_until_exact_owner_exit': True,
                  'refusal_code': 'OWNER_EXIT_UNCONFIRMED',
                  'retained_owned_names': sorted(custody.archive_nodes)}
    else:
        try:
            result = custody.cleanup()
        except BaseException as error:
            result = {'complete': False, 'refusal_code': str(error) if isinstance(error, Refusal)
                      else 'EXCEPTION_' + type(error).__name__.upper()}
    within_ceiling = time.monotonic() < cleanup_ceiling
    terminal_complete = owner_absent and bool(result['complete']) and within_ceiling
    return {'reason': reason, 'archive_cleanup': result, 'owner_process_exited': owner_absent,
            'within_cleanup_ceiling': within_ceiling,
            'terminal_cleanup_complete': terminal_complete,
            'outer_owner_custody_required': not terminal_complete,
            'acquisition_accepted': False}


if __name__ == '__main__':
    raise SystemExit('SOURCE_PROPOSAL_ONLY; owner adapter, launcher, acceptance and exact new authority are pending')
