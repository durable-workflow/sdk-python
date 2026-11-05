"""Synthetic private role protocol; never calls the admitted-data start API."""
from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from types import SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode=True
BASE=Path('/tmp/sdk-python-handoff')
REF='https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-9999999999'


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module);return module


s=load('trusted_session_protocol_control',BASE/'184-acquisition-session-proposal.py')
g=load('trusted_guard_protocol_control',BASE/'184-acquisition-custody-guardian-v2-proposal.py')
t=load('trusted_terminal_protocol_control',BASE/'184-acquisition-terminal-proposal.py')
o=load('trusted_owner_protocol_control',BASE/'184-acquire-tools-v5.py')
selection=json.loads((BASE/'184-immutable-build-tool-input-proposal.json').read_text())
filenames=[tool['filename'] for tool in selection['tools']]
metadata=[tool['name']+'.json' for tool in selection['tools']]
results=[]


class SyntheticBootstrap:
    def __init__(self,mode):self.mode=mode
    def launch(self,decision,proof):
        assert dict(os.environ)==s.MINIMAL_ENV and decision==REF
        if self.mode=='fail':raise RuntimeError('synthetic failure only')
        if self.mode in {'block','abort','guardian-crash'}:
            os.write(next(iter(proof['archive_fds'].values())),b'synthetic partial')
            signal.pause()
        if self.mode=='survivor':
            os.write(next(iter(proof['archive_fds'].values())),b'synthetic retained partial')
            time.sleep(0.8)
        for fd in proof['archive_fds'].values():
            os.write(fd,b'synthetic protocol bytes');os.fchmod(fd,0o400);os.close(fd)
        for name in ('metadata','wheelhouse','events'):(Path(proof['pool'])/name).chmod(0o500)
        Path(proof['pool']).chmod(0o500)
        return {'outcome':'pass' if self.mode=='bad-marker' else 'candidate','pool':proof['pool'],
                'source':s.SOURCE,'owner_environment_names':sorted(os.environ)}


def fake_terminal(mode):
    def validate(*args):
        if mode=='readback-refusal':raise t.Refusal('SYNTHETIC_READBACK_REFUSED')
        assert args[-3] is True and args[-2]==0
        return {'status':'synthetic-protocol-only','acquisition_accepted':False}
    return SimpleNamespace(Refusal=t.Refusal,validate_candidate=validate,snapshot=t.snapshot)


class Response:
    def __init__(self,url,raw):
        self.status=200;self.url=url;self.body=io.BytesIO(raw)
        self.headers={'Content-Length':str(len(raw)),'Content-Encoding':'identity'}
    def __enter__(self):return self
    def __exit__(self,*args):self.body.close()
    def geturl(self):return self.url
    def read(self,count):return self.body.read(count)


def integrated_closure(parent,mode):
    selected=json.loads((BASE/'184-immutable-build-tool-input-proposal.json').read_text())
    bodies={};archive_urls=set()
    failed=Path('/tmp/sdk-python-184-tools-14adc40b-20261105T035512Z-6d90ccfb71')
    for tool in selected['tools']:
        authority=json.loads((failed/'metadata'/(tool['name']+'.json')).read_text())
        metadata=f"Metadata-Version: 2.4\nName: {tool['name']}\nVersion: {tool['version']}\nRequires-Python: {tool['requires_python']}\n"
        metadata+=''.join('Requires-Dist: '+value+'\n' for value in authority['info'].get('requires_dist') or [])
        buffer=io.BytesIO();root=tool['name'].replace('-','_')+'-'+tool['version']+'.dist-info/'
        with zipfile.ZipFile(buffer,'w') as archive:
            archive.writestr(root+'METADATA',metadata)
            archive.writestr(root+'WHEEL','Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n')
        raw=buffer.getvalue();tool['bytes']=len(raw);tool['sha256']=hashlib.sha256(raw).hexdigest()
        for row in authority['urls']:
            if row['filename']==tool['filename']:row['size']=len(raw);row['digests']['sha256']=tool['sha256']
        bodies[tool['metadata_source']]=json.dumps(authority,sort_keys=True).encode()
        bodies[tool['url']]=raw;archive_urls.add(tool['url'])
    raw=json.dumps(selected,sort_keys=True).encode()
    fixture=parent/'synthetic-selection.json';fixture.write_bytes(raw);fixture.chmod(0o400)
    real_owner=load('trusted_owner_synthetic_data_closure',BASE/'184-acquire-tools-v5.py')
    real_owner.SELECTION=fixture;real_owner.SELECTION_SHA=hashlib.sha256(raw).hexdigest()
    real_owner.ARCHIVE_BYTES=sum(tool['bytes'] for tool in selected['tools']);real_owner.POOL_BASE=parent
    real_owner.VERIFIED_SOURCE_SHA256=s.PINNED['owner']
    class Opener:
        def open(self,request,timeout):
            if mode=='integrated-timeout' and request.full_url in archive_urls:raise TimeoutError('synthetic transport only')
            assert request.full_url in bodies
            return Response(request.full_url,bodies[request.full_url])
    class Bootstrap:
        def launch(self,decision,proof):
            with patch.object(urllib.request,'build_opener',return_value=Opener()), \
                 patch.object(real_owner.Acquisition,'cached',return_value=(None,None)):
                return real_owner.run_guarded_once(decision,proof)
    return real_owner,Bootstrap(),t


def cleanup_directory(path):
    for directory in [path,*[p for p in path.rglob('*') if p.is_dir() and not p.is_symlink()]]:
        directory.chmod(0o700)


def run_protocol(mode,disconnect=None):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-session-controls-') as temporary:
        parent=Path(temporary);outer,inner=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
        started=time.monotonic();ceiling=started+3
        authority={'decision':REF};authority_raw=s.encoded({'scope':'synthetic protocol only','decision':REF})
        modules=(g,o,SyntheticBootstrap(mode),fake_terminal(mode))
        if mode.startswith('integrated-'):
            real_owner,bootstrap,terminal=integrated_closure(parent,mode);modules=(g,real_owner,bootstrap,terminal)
        child=os.fork()
        if child==0:
            try:
                outer.close();os.environ.clear();os.environ.update(s.MINIMAL_ENV)
                s.POOL_PARENT=parent
                if mode=='survivor':
                    real_stop=s.stop_owner;calls=[0]
                    def stop_with_one_survivor(role,ceiling):
                        calls[0]+=1
                        if calls[0]==1:return {'owner_exited':False,'owner_exit_code':None}
                        return real_stop(role,ceiling)
                    s.stop_owner=stop_with_one_survivor
                code=s.guardian_job(inner,authority_raw,authority,started,dt.datetime.now(dt.timezone.utc).isoformat(),
                    modules,bounds=(0.05 if mode=='survivor' else 1 if mode.startswith('integrated-') else 0.35,3))
                inner.close();os._exit(code)
            except BaseException:os._exit(3)
        inner.close();pidfd=os.pidfd_open(child);session=None;received=[];owner_pidfd=None;reaped=False
        expected_pool=parent/'sdk-python-184-tools-guardian-14adc40b-9999999999'
        try:
            if disconnect:
                outer.settimeout(2)
                message,fds=s.recv_frame(outer);received+=fds
                assert message['type']=='REGISTERED' and len(fds)==11
                if disconnect=='owner':
                    s.send_frame(outer,{'type':'ARM'})
                    message,fds=s.recv_frame(outer);received+=fds
                    assert message['type']=='OWNER' and len(fds)==1;owner_pidfd=fds[0]
                outer.close()
                assert select.select([pidfd],[],[],max(0,ceiling-time.monotonic()))[0]
                _,status=os.waitpid(child,0);reaped=True
                assert os.waitstatus_to_exitcode(status)==1
                if owner_pidfd is not None:assert select.select([owner_pidfd],[],[],0)[0]
                assert not list((expected_pool/'wheelhouse').iterdir())
                assert (expected_pool/'guardian-result.json').exists()
            else:
                session=s.Session(outer,child,pidfd,g,modules[3],ceiling,expected_pool,filenames,metadata,authority_raw)
                if mode=='abort':
                    assert session.receive() is None and session.receive() is None
                    message=session.abort('SYNTHETIC_EXPLICIT_ABORT')
                elif mode=='guardian-crash':
                    assert session.receive() is None and session.receive() is None
                    signal.pidfd_send_signal(session.pidfd,signal.SIGKILL)
                    message=session.wait()
                else:message=session.wait()
                reaped=session.guardian_reaped
                result=message['result'];assert not result['acquisition_accepted']
                if mode=='survivor':
                    assert result['status']=='SURVIVOR_CUSTODY_REQUIRED' and result['outer_owner_custody_required']
                    assert not result['terminal_cleanup_complete'] and not session.finished
                    assert not select.select([session.owner_pidfd],[],[],0)[0]
                    assert all(os.fstat(row['fd']).st_nlink==1 for row in session.custody.archive_nodes.values())
                    message=session.wait();result=message['result'];reaped=session.guardian_reaped
                    assert message['type']=='DEFERRED_CLEANUP' and result['terminal_cleanup_complete']
                    assert session.finished and not list((expected_pool/'wheelhouse').iterdir())
                elif mode in {'positive','integrated-positive'}:
                    assert result['status']=='COLLECTED_FOR_OUTER_READBACK' and session.finished
                    assert len(list((expected_pool/'wheelhouse').iterdir()))==6
                    assert (expected_pool/'operation-authority.json').read_bytes()==authority_raw
                else:
                    assert result['status']=='REFUSED',result
                    assert result['terminal_cleanup_complete'] and result['retained_custody_sealed']
                    assert not list((expected_pool/'wheelhouse').iterdir())
                    assert session.finished
                assert select.select([pidfd],[],[],0)[0] if not session.finished else True
        finally:
            if not reaped:
                try:os.kill(child,signal.SIGKILL)
                except ProcessLookupError:pass
                try:os.waitpid(child,0)
                except ChildProcessError:pass
            if session is not None:session.stack.close()
            else:
                outer.close()
                for fd in received:os.close(fd)
                os.close(pidfd)
            cleanup_directory(parent)


def frame_control(raw,code):
    one,two=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
    try:
        one.send(raw)
        try:s.recv_frame(two)
        except (ValueError,s.Refusal) as error:
            if code is not None:assert str(error)==code,(str(error),code)
        else:raise AssertionError('invalid frame accepted')
    finally:one.close();two.close()


def fd_frame_roundtrip():
    one,two=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
    read,write=os.pipe();received=[]
    try:
        s.send_frame(one,{'type':'synthetic'},[read])
        value,received=s.recv_frame(two);assert value=={'type':'synthetic'} and len(received)==1
        os.write(write,b'G');assert os.read(received[0],1)==b'G'
    finally:
        one.close();two.close();os.close(read);os.close(write)
        for fd in received:os.close(fd)


def actual_closed_startup():
    # Only trusted local stdlib/controller source, not a data operation or tool wheel.
    cpus=sorted(os.sched_getaffinity(0))[:2]
    code="import pathlib,types; b=pathlib.Path('/tmp/sdk-python-handoff/184-acquisition-launcher-bootstrap-proposal.py'); bootstrap=types.ModuleType('resource_source_control'); exec(compile(b.read_bytes(),str(b),'exec'),bootstrap.__dict__); bootstrap.configure_resources("+repr(cpus)+"); p=pathlib.Path('/tmp/sdk-python-handoff/184-acquisition-session-proposal.py'); m=types.ModuleType('source_startup_control'); m.__file__=str(p); exec(compile(p.read_bytes(),str(p),'exec'),m.__dict__); m.verify_startup(); print('CLOSED_STARTUP_OK')"
    run=subprocess.run([s.INTERPRETER,'-I','-S','-c',code],env=s.MINIMAL_ENV,close_fds=True,
                       capture_output=True,timeout=5,check=True)
    assert run.stdout==b'CLOSED_STARTUP_OK\n'


def live_start_refuses_no_decision():
    with patch.object(s,'verify_startup'),patch.object(s,'checked_bytes',side_effect=AssertionError('must not read authority')):
        try:s.start('/synthetic/absent','0'*64,None,'0'*64,time.monotonic(),dt.datetime.now(dt.timezone.utc).isoformat())
        except s.Refusal as error:assert str(error)=='UNFROZEN_OR_UNADMITTED_PACKET'
        else:raise AssertionError('missing admission accepted')


def check(name,operation):
    try:operation()
    except BaseException as error:results.append({'id':name,'outcome':'fail','type':type(error).__name__,'detail':str(error)[:600]})
    else:results.append({'id':name,'outcome':'pass'})


with patch.object(socket,'create_connection',side_effect=AssertionError('NO_NETWORK_IN_SESSION_CONTROLS')):
    check('synthetic-owner-exit-independent-check-and-outer-readback',lambda:run_protocol('positive'))
    check('owner-failure-is-refusal-zero-archives',lambda:run_protocol('fail'))
    check('active-deadline-stops-and-reaps-exact-owner',lambda:run_protocol('block'))
    check('candidate-marker-required-before-preserve',lambda:run_protocol('bad-marker'))
    check('independent-readback-refusal-cleans-archives',lambda:run_protocol('readback-refusal'))
    check('explicit-outer-abort-stops-owner-cleans-archives',lambda:run_protocol('abort'))
    check('guardian-crash-outer-stops-exact-owner-cleans-archives',lambda:run_protocol('guardian-crash'))
    check('survivor-keeps-all-custody-until-confirmed-deferred-exit',lambda:run_protocol('survivor'))
    check('actual-owner-and-terminal-compose-with-synthetic-six-tool-closure',lambda:run_protocol('integrated-positive'))
    check('actual-owner-transport-timeout-refuses-integrated-preserve',lambda:run_protocol('integrated-timeout'))
    check('outer-disappears-before-arming-no-owner-work',lambda:run_protocol('positive','register'))
    check('outer-disappears-with-blocked-owner-stops-before-work',lambda:run_protocol('positive','owner'))
    check('descriptor-frame-roundtrip-preserves-scoped-handle',fd_frame_roundtrip)
    check('oversized-wire-frame-refused',lambda:frame_control(b'X'*(s.FRAME_CAP+1),'FRAME_EOF_OR_TRUNCATION'))
    check('duplicate-frame-key-refused',lambda:frame_control(b'{"type":"one","type":"two"}','DUPLICATE_FRAME_KEY'))
    check('non-object-frame-refused',lambda:frame_control(b'[]','FRAME_OBJECT_REQUIRED'))
    check('actual-root-owned-interpreter-clean-env-I-S-startup',actual_closed_startup)
    check('missing-new-decision-refuses-before-authority-or-fork',live_start_refuses_no_decision)

report={'scope':'synthetic role-protocol controls; start/admitted-data API never called with a decision',
        'source_sha256':hashlib.sha256((BASE/'184-acquisition-session-proposal.py').read_bytes()).hexdigest(),
        'cases':len(results),'passed':sum(row['outcome']=='pass' for row in results),
        'failed':sum(row['outcome']=='fail' for row in results),'results':results,
        'real_network_requests':0,'selected_archives_acquired':0,'downloaded_wheel_imports_or_installs':0,
        'new_general_decision':None,'acquisition_accepted':False}
(BASE/'184-acquisition-session-controls-result.json').write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
print(json.dumps(report,indent=2));raise SystemExit(bool(report['failed']))
