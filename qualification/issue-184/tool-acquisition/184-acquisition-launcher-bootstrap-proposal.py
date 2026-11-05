"""Unexecuted reviewed inline bootstrap; no decision is selected by this source.

A future invocation must embed these exact reviewed bytes under env -i and the
absolute pinned interpreter with -I -S. Hash/decision arguments bind that separate
General decision; a syntactically valid comment URL does not grant permission.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import stat
import sys
import time
from types import ModuleType

ENV={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8','LC_ALL':'C.UTF-8','TZ':'UTC'}
SESSION=Path('/tmp/sdk-python-handoff/184-acquisition-session-proposal.py')
OLD='https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-6649046802'


def require(value,code):
    if not value:raise ValueError(code)


def pairs(values):
    result={}
    for key,value in values:
        require(key not in result,'DUPLICATE_AUTHORITY_KEY');result[key]=value
    return result


def reject_constant(value):
    raise ValueError('NONFINITE_AUTHORITY_NUMBER')


def parse_authority(raw):
    authority=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_constant=reject_constant)
    require(type(authority) is dict and set(authority)=={
        'decision','selected_source','inputs','environment','host','total_seconds','io_seconds',
        'retries','survivor_policy','pool_parent','launcher_sha256','cpu_affinity',
        'retention_deadline_utc'},'EXACT_AUTHORITY_SCHEMA_REQUIRED')
    require(type(authority['inputs']) is dict and set(authority['inputs'])=={
        'owner','bootstrap','guardian','terminal','selection','interpreter','library','ca'}
        and all(type(value) is str and re.fullmatch(r'[0-9a-f]{64}',value)
                for value in authority['inputs'].values()),'EXACT_INPUT_SCHEMA_REQUIRED')
    require(type(authority['environment']) is dict and authority['environment']==ENV
            and all(type(value) is str for value in authority['environment'].values()),'EXACT_ENVIRONMENT_SCHEMA_REQUIRED')
    require(all(type(authority[key]) is int for key in ('total_seconds','io_seconds','retries')),
            'INTEGER_AUTHORITY_LIMITS_REQUIRED')
    require(all(type(authority[key]) is str for key in ('decision','selected_source','host',
        'survivor_policy','pool_parent','launcher_sha256','retention_deadline_utc')),'EXACT_AUTHORITY_VALUE_TYPES_REQUIRED')
    return authority


def read_verified(path,expected,cap):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as stream:
        before=os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid() and before.st_nlink==1
                and not before.st_mode&0o022 and before.st_size<=cap,'BOOTSTRAP_INPUT_CUSTODY_CHANGED')
        raw=stream.read(cap+1);after=os.fstat(stream.fileno())
        stamp=lambda st:(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)
        require(stamp(before)==stamp(after) and len(raw)==before.st_size
                and hashlib.sha256(raw).hexdigest()==expected,'BOOTSTRAP_INPUT_BYTES_CHANGED')
    require(stamp(os.stat(path,follow_symlinks=False))==stamp(before),'BOOTSTRAP_INPUT_PATH_CHANGED')
    return raw


def configure_resources(affinity):
    require(isinstance(affinity,list) and 1<=len(affinity)<=2
            and all(type(cpu) is int and cpu>=0 for cpu in affinity)
            and len(set(affinity))==len(affinity) and set(affinity)<=os.sched_getaffinity(0),
            'EXACT_AVAILABLE_CPU_AFFINITY_REQUIRED')
    os.sched_setaffinity(0,set(affinity))
    resource.setrlimit(resource.RLIMIT_AS,(128*1024**2,384*1024**2))
    resource.setrlimit(resource.RLIMIT_CPU,(30,60))
    resource.setrlimit(resource.RLIMIT_NOFILE,(128,128))
    resource.setrlimit(resource.RLIMIT_FSIZE,(4*1024**2,4*1024**2))
    signal.signal(signal.SIGCHLD,signal.SIG_DFL)


def launch(authority_path,expected_authority_sha,expected_decision,expected_session_sha):
    started=time.monotonic();started_utc=dt.datetime.now(dt.timezone.utc).isoformat()
    require(dict(os.environ)==ENV and sys.flags.isolated==1 and sys.flags.no_site==1,
            'ENV_I_AND_ISOLATED_NO_SITE_REQUIRED_BEFORE_STARTUP')
    require(isinstance(expected_decision,str) and expected_decision!=OLD
            and re.fullmatch(r'https://github\.com/durable-workflow/sdk-python/issues/184#issuecomment-[1-9][0-9]{5,17}',expected_decision),
            'EXACT_NEW_GENERAL_DECISION_REQUIRED')
    raw=read_verified(Path(authority_path),expected_authority_sha,32768)
    authority=parse_authority(raw)
    require(authority.get('decision')==expected_decision
            and authority.get('launcher_sha256')==expected_session_sha,'EXACT_AUTHORITY_BINDING_CHANGED')
    code_raw=read_verified(SESSION,expected_session_sha,65536)
    configure_resources(authority.get('cpu_affinity'))
    module=ModuleType('reviewed_guarded_acquisition_session');module.__file__=str(SESSION)
    exec(compile(code_raw,'<verified-reviewed-acquisition-session>','exec'),module.__dict__)
    session=module.start(authority_path,expected_authority_sha,expected_decision,expected_session_sha,
                         started,started_utc)
    anchor=dt.datetime.fromisoformat(started_utc)
    print(json.dumps({'phase':'SESSION_CREATED','controller_pid':os.getpid(),'guardian_pid':session.pid,
                      'pool':session.expected_pool,'started_at_utc':started_utc,
                      'active_cutoff_utc':(anchor+dt.timedelta(seconds=270)).isoformat(),
                      'cleanup_ceiling_utc':(anchor+dt.timedelta(seconds=300)).isoformat(),
                      'authority_sha256':expected_authority_sha,'decision':expected_decision}),flush=True)
    result=session.wait()
    # A survivor/incomplete record retains the live Session object and all outer
    # descriptor/process handles. The caller must escalate; it cannot finalize.
    return session,result


def run_and_report(authority_path,expected_authority_sha,expected_decision,expected_session_sha):
    """The only proposed invocation wrapper; retain custody until proved quiescent.

    CUSTODY_REQUIRED must be escalated immediately by the invoking agent/operator.
    No automatic data retry, admission expansion or hidden work occurs here.
    Unknown ownership can require explicit operator stop/manual reconciliation.
    """
    session,result=launch(authority_path,expected_authority_sha,expected_decision,expected_session_sha)
    print(json.dumps({'phase':'RESULT','record':result}),flush=True)
    if not session.finished:
        print(json.dumps({'phase':'CUSTODY_REQUIRED','controller_pid':os.getpid(),'guardian_pid':session.pid,
                          'owner_pid':session.owner_pid,'pool':session.expected_pool,
                          'terminal_cleanup_complete':False,'acquisition_accepted':False}),flush=True)
    while not session.finished:
        # This is the separately proposed failed-operation custody keeper, not
        # acquisition work. Its policy must be explicitly admitted in authority.
        later=session.reconcile_retained()
        if later is not None:
            result=later
            if session.finished:print(json.dumps({'phase':'POST_BOUND_QUIESCENT_NONPASS','record':later}),flush=True)
        if not session.finished:time.sleep(1)
    return result


if __name__=='__main__':
    raise SystemExit('UNEXECUTED_INLINE_SOURCE_PROPOSAL; no authority or new General decision selected')
