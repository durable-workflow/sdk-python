"""Local bootstrap refusal/resource controls. No acquisition or admitted start."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from unittest.mock import patch

sys.dont_write_bytecode=True
BASE=Path('/tmp/sdk-python-handoff')
SOURCE=BASE/'184-acquisition-launcher-bootstrap-proposal.py'
spec=importlib.util.spec_from_file_location('trusted_launcher_bootstrap_control',SOURCE)
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
REF='https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-9999999999'
results=[]


def refuses(operation,code):
    try:operation()
    except ValueError as error:assert str(error)==code,(str(error),code)
    else:raise AssertionError('required refusal absent: '+code)


def read_control(mode):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-bootstrap-controls-') as temporary:
        p=Path(temporary)/'source';raw=b'trusted synthetic source bytes';p.write_bytes(raw)
        expected=hashlib.sha256(raw).hexdigest()
        if mode=='positive':assert b.read_verified(p,expected,1024)==raw
        elif mode=='hash':refuses(lambda:b.read_verified(p,'0'*64,1024),'BOOTSTRAP_INPUT_BYTES_CHANGED')
        elif mode=='bound':refuses(lambda:b.read_verified(p,expected,1),'BOOTSTRAP_INPUT_CUSTODY_CHANGED')
        elif mode=='hardlink':
            os.link(p,Path(temporary)/'shared');refuses(lambda:b.read_verified(p,expected,1024),'BOOTSTRAP_INPUT_CUSTODY_CHANGED')
        elif mode=='symlink':
            link=Path(temporary)/'link';link.symlink_to(p)
            try:b.read_verified(link,expected,1024)
            except OSError:pass
            else:raise AssertionError('symlink accepted')


def env_gate():
    with patch.dict(os.environ,{'GH_TOKEN':'synthetic'},clear=False), \
         patch.object(b,'read_verified',side_effect=AssertionError('must refuse before reading authority')):
        refuses(lambda:b.launch('/synthetic','0'*64,REF,'0'*64),
                'ENV_I_AND_ISOLATED_NO_SITE_REQUIRED_BEFORE_STARTUP')


def affinity_refusal(value):
    with patch.object(b.os,'sched_setaffinity',side_effect=AssertionError('must refuse before mutation')):
        refuses(lambda:b.configure_resources(value),'EXACT_AVAILABLE_CPU_AFFINITY_REQUIRED')


def check(name,operation):
    try:operation()
    except BaseException as error:results.append({'id':name,'outcome':'fail','type':type(error).__name__,'detail':str(error)[:160]})
    else:results.append({'id':name,'outcome':'pass'})


with patch.object(socket,'create_connection',side_effect=AssertionError('NO_NETWORK_IN_BOOTSTRAP_CONTROLS')):
    for mode in ('positive','hash','bound','hardlink','symlink'):
        check('verified-inline-source-'+mode,lambda mode=mode:read_control(mode))
    check('ambient-credentials-or-wrong-startup-refuse-before-inputs',env_gate)
    for name,value in [('empty',[]),('too-many',[0,1,2]),('bool',[True]),('duplicate',[0,0]),('unavailable',[max(os.sched_getaffinity(0))+1])]:
        check('affinity-'+name+'-refuses-before-mutation',lambda value=value:affinity_refusal(value))
    check('duplicate-authority-key-refused',lambda:refuses(lambda:b.pairs([('decision','one'),('decision','two')]),'DUPLICATE_AUTHORITY_KEY'))

report={'scope':'bootstrap file/refusal controls; no admitted start or selected archive access',
        'source_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),'cases':len(results),
        'passed':sum(row['outcome']=='pass' for row in results),'failed':sum(row['outcome']=='fail' for row in results),
        'results':results,'real_network_requests':0,'selected_archives_acquired':0,
        'downloaded_wheel_imports_or_installs':0,'new_general_decision':None}
(BASE/'184-acquisition-launcher-bootstrap-controls-result.json').write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
print(json.dumps(report,indent=2));raise SystemExit(bool(report['failed']))
