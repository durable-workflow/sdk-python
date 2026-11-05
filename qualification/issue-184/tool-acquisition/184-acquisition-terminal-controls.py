"""Synthetic six-tool terminal readback; no network/downloaded-wheel imports/SDK."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import datetime as dt
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
from unittest.mock import patch
import zipfile

sys.dont_write_bytecode=True
BASE=Path('/tmp/sdk-python-handoff')
FAILED=Path('/tmp/sdk-python-184-tools-14adc40b-20261105T035512Z-6d90ccfb71')
REF='https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-9999999999'


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module);return module


o=load('trusted_owner_terminal_control',BASE/'184-acquire-tools-v5.py')
g=load('trusted_guard_terminal_control',BASE/'184-acquisition-custody-guardian-v2-proposal.py')
t=load('trusted_terminal_readback_control',BASE/'184-acquisition-terminal-proposal.py')
OWNER_SHA=hashlib.sha256((BASE/'184-acquire-tools-v5.py').read_bytes()).hexdigest()
results=[]


def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return (json.dumps(value,sort_keys=True,indent=2)+'\n').encode()


def synthetic_selection():
    selection=json.loads((FAILED/'selection.json').read_text())
    bodies={};authorities={}
    for tool in selection['tools']:
        authority=json.loads((FAILED/'metadata'/(tool['name']+'.json')).read_text())
        metadata=f"Metadata-Version: 2.4\nName: {tool['name']}\nVersion: {tool['version']}\nRequires-Python: {tool['requires_python']}\n"
        metadata+=''.join('Requires-Dist: '+text+'\n' for text in authority['info'].get('requires_dist') or [])
        body=io.BytesIO();root=tool['name'].replace('-','_')+'-'+tool['version']+'.dist-info/'
        with zipfile.ZipFile(body,'w') as archive:
            archive.writestr(root+'METADATA',metadata)
            archive.writestr(root+'WHEEL','Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n')
        raw=body.getvalue();tool['bytes']=len(raw);tool['sha256']=sha(raw)
        for row in authority['urls']:
            if row.get('filename')==tool['filename']:
                row['size']=len(raw);row['digests']['sha256']=sha(raw)
        bodies[tool['filename']]=raw;authorities[tool['name']]=authority
    return selection,bodies,authorities


@contextmanager
def candidate_pool():
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-terminal-controls-') as temporary:
        selection,bodies,authorities=synthetic_selection();raw_selection=encoded(selection)
        parent=Path(temporary)
        lease=g.ArchiveCustody(parent,'sdk-python-184-tools-guardian-9999999999',[tool['filename'] for tool in selection['tools']])
        borrowed={name:lease.duplicate_for_owner(name) for name in lease.archive_nodes}
        proof={'pool':str(lease.path),'ready':True,'started_monotonic':time.monotonic(),
               'started_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
               'dir_ids':{'.':list(lease.root_id),'wheelhouse':list(lease.wheelhouse_id)},'archive_fds':borrowed}
        with patch.object(o,'POOL_BASE',parent),patch.object(o,'SELECTION_SHA',sha(raw_selection)), \
             patch.object(o,'ARCHIVE_BYTES',sum(tool['bytes'] for tool in selection['tools'])), \
             patch.object(o,'CA_SHA',sha(b'synthetic trust fixture')):
            item=o.Acquisition(REF);item.attach_guarded_pool(proof);item.prepare_pool(selection['tools'])
            item.write('selection.json',raw_selection);item.write('ca.pem',b'synthetic trust fixture')
            item.write('launch.json',encoded({'admission':REF,'source':o.SOURCE,
                       'verified_acquisition_source_sha256':OWNER_SHA,'credentials_proxy_default_trust':False}))
            metadata=[];requests=[]
            for tool in selection['tools']:
                item.write('metadata/'+tool['name']+'.json',encoded(authorities[tool['name']]))
            for tool in selection['tools']:
                metadata.append(item.retain_and_inspect(bodies[tool['filename']],tool,authorities[tool['name']]))
            for kind in ('metadata','archive'):
                for tool in selection['tools']:
                    number=len(requests)+1;url=tool['metadata_source' if kind=='metadata' else 'url']
                    body=encoded(authorities[tool['name']]) if kind=='metadata' else bodies[tool['filename']]
                    before={'number':number,'kind':kind,'url':url,'attempt':1,'stage':'before-open'}
                    complete=dict(before,stage='body-complete',bytes_received=len(body),body_sha256=sha(body))
                    prefix=f'events/{number:02d}-'
                    item.write(prefix+'request.json',encoded(before))
                    item.write(prefix+'headers.json',encoded({'number':number,'status':200,'same_selected_url':True,
                               'content_length':len(body),'content_encoding':'identity'}))
                    item.write(prefix+'complete.json',encoded(complete));requests.append(complete)
            item.write('wheel-metadata.json',encoded({'environment':'CPython3.13 Linux amd64; no extras','tools':metadata,
                       'actual_requires_dist_matches_versioned_authority':True,'active_dependency_edges_match_approved_metadata_selection':True}))
            item.write('acquisition-observation.json',encoded({'status':'collected-before-final-custody-check','retries':0,
                       'import_install_container_SDK_build':False,'archive_bytes':item.archive_used,'http_requests':requests}))
            inventory_raw=encoded({'source':o.SOURCE,'entries':item.check_pool(),'inventory_excludes_self':True})
            item.write('inventory.json',inventory_raw);item.seal()
            candidate={'outcome':'candidate','pool':str(item.pool),'source':o.SOURCE,'files':len(item.files),
                       'whole_pool_bytes':item.used,'archive_bytes':item.archive_used,'inventory_sha256':sha(inventory_raw)}
            context={'lease':lease,'candidate':candidate,'selection':raw_selection,'owner':item}
            try:yield context
            finally:
                for directory in (lease.path,lease.path/'wheelhouse',lease.path/'metadata',lease.path/'events'):
                    directory.chmod(0o700)
                for fd in borrowed.values():os.close(fd)
                lease.close()


def validate(context,**overrides):
    args={'custody':context['lease'],'candidate':context['candidate'],'selection_raw':context['selection'],
          'owner_module':o,'owner_source_sha':OWNER_SHA,'admission':REF,'owner_exited':True,
          'owner_exit_code':0,'cleanup_ceiling':time.monotonic()+10}
    args.update(overrides);return t.validate_candidate(**args)


def changed_file(context,name,raw):
    path=context['lease'].path/name;path.chmod(0o600);path.write_bytes(raw);path.chmod(0o400)


def positive():
    with candidate_pool() as context:
        result=validate(context)
        assert result['status']=='validated-candidate-for-outer-readback' and len(result['archives'])==6
        assert len(result['semantic_tools'])==6 and not result['acquisition_accepted']
        assert result['launcher_and_new_general_decision_pending']


def refusal_case(mutate=None,override=None,code=None):
    with candidate_pool() as context:
        if mutate:mutate(context)
        try:validate(context,**(override or {}))
        except ValueError as error:
            actual=getattr(error,'code',str(error))
            if code is not None:assert actual==code,(actual,code)
        else:raise AssertionError('mutated candidate accepted')


def extra_file(context):
    path=context['lease'].path;path.chmod(0o700)
    (path/'unexpected').write_bytes(b'unknown');(path/'unexpected').chmod(0o400);path.chmod(0o500)
    context['candidate']['files']+=1;context['candidate']['whole_pool_bytes']+=7


def inventory_bool(context):
    raw=json.loads((context['lease'].path/'inventory.json').read_text())
    raw['entries'][0]['links']=True
    changed_file(context,'inventory.json',encoded(raw));context['candidate']['inventory_sha256']=sha(encoded(raw))
    context['candidate']['whole_pool_bytes']=sum(p.stat().st_size for p in context['lease'].path.rglob('*') if p.is_file())


def check(name,operation):
    try:operation()
    except BaseException as error:results.append({'id':name,'outcome':'fail','type':type(error).__name__,'detail':str(error)[:180]})
    else:results.append({'id':name,'outcome':'pass'})


with patch.object(socket,'create_connection',side_effect=AssertionError('NO_NETWORK_IN_TERMINAL_CONTROLS')):
    check('six-synthetic-header-tuples-semantic-and-ledger-readback',positive)
    check('live-owner-cannot-enter-readback',lambda:refusal_case(override={'owner_exited':False},code='EXACT_OWNER_SUCCESSFUL_EXIT_REQUIRED'))
    check('failed-owner-cannot-preserve',lambda:refusal_case(override={'owner_exit_code':1},code='EXACT_OWNER_SUCCESSFUL_EXIT_REQUIRED'))
    check('bool-exitcode-cannot-masquerade-as-zero',lambda:refusal_case(override={'owner_exit_code':False},code='EXACT_OWNER_SUCCESSFUL_EXIT_REQUIRED'))
    check('expired-terminal-ceiling-refused',lambda:refusal_case(override={'cleanup_ceiling':time.monotonic()-1},code='TERMINAL_DEADLINE_EXCEEDED'))
    check('owner-pass-marker-not-independent-candidate',lambda:refusal_case(lambda c:c['candidate'].update(outcome='pass'),code='OWNER_CANDIDATE_MISMATCH'))
    check('candidate-file-count-mismatch-refused',lambda:refusal_case(lambda c:c['candidate'].update(files=0),code='CANDIDATE_COUNTS_CHANGED'))
    check('candidate-byte-count-mismatch-refused',lambda:refusal_case(lambda c:c['candidate'].update(whole_pool_bytes=0),code='CANDIDATE_COUNTS_CHANGED'))
    check('candidate-inventory-hash-mismatch-refused',lambda:refusal_case(lambda c:c['candidate'].update(inventory_sha256='0'*64),code='CANDIDATE_INVENTORY_CHANGED'))
    check('changed-selected-source-bytes-refused',lambda:refusal_case(override={'selection_raw':b'changed'},code='SELECTION_AUTHORITY_CHANGED'))
    check('changed-trust-file-refused',lambda:refusal_case(lambda c:changed_file(c,'ca.pem',b'changed')))
    check('changed-launch-admission-refused',lambda:refusal_case(lambda c:changed_file(c,'launch.json',encoded({'admission':'changed'}))))
    check('changed-wheel-body-refused',lambda:refusal_case(lambda c:changed_file(c,'wheelhouse/'+next(iter(c['lease'].archive_nodes)),b'changed')))
    check('unknown-extra-output-refused',lambda:refusal_case(extra_file,code='OWNER_INVENTORY_INCOMPLETE_OR_EXTRA'))
    check('bool-inventory-link-count-refused',lambda:refusal_case(inventory_bool,code='OWNER_INVENTORY_BYTES_OR_IDENTITY_CHANGED'))
    check('different-exact-admission-refused',lambda:refusal_case(override={'admission':'different'},code='LAUNCH_AUTHORITY_CHANGED'))

report={'scope':'synthetic six-tool terminal readback, not actual input acquisition or launcher',
        'source_sha256':sha((BASE/'184-acquisition-terminal-proposal.py').read_bytes()),'cases':len(results),
        'passed':sum(row['outcome']=='pass' for row in results),'failed':sum(row['outcome']=='fail' for row in results),
        'results':results,'real_network_requests':0,'selected_archives_acquired':0,'downloaded_wheel_imports_or_installs':0,
        'new_general_decision':None,'launcher_pending':True}
(BASE/'184-acquisition-terminal-controls-result.json').write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
print(json.dumps(report,indent=2));raise SystemExit(bool(report['failed']))
