"""Synthetic private role protocol; never calls the admitted-data start API."""
from __future__ import annotations

import datetime as dt
import ast
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
import traceback
import urllib.request
import zipfile
from types import ModuleType,SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode=True
BASE=Path('/tmp/sdk-python-handoff')
REF='https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-9999999999'


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module);return module


s=load('trusted_session_protocol_control',BASE/'184-acquisition-session-proposal.py')
g=load('trusted_guard_protocol_control',BASE/'184-acquisition-custody-guardian-v7-proposal.py')
historical=BASE/'control-inputs/historical-source/session-v6.source'
assert hashlib.sha256(historical.read_bytes()).hexdigest()=='e721bba90c8cf7fae9eede4b3a2df2b7abb25b59f84b62f8ee5be5b9ee381e89'
predecessor=ModuleType('trusted_session_v6_predecessor');predecessor.__file__=str(historical)
exec(compile(historical.read_bytes(),str(historical),'exec'),predecessor.__dict__)
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
        signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.alarm(5)
        if self.mode=='fail':raise RuntimeError('synthetic failure only')
        if self.mode in {'block','abort','guardian-crash','late-reconcile'}:
            os.write(next(iter(proof['archive_fds'].values())),b'synthetic partial')
            signal.pause()
        if self.mode in {'survivor','stop-refusal'}:
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
    failed=BASE/'control-inputs'
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


def run_protocol(mode,disconnect=None,module=s):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-session-controls-') as temporary:
        parent=Path(temporary);outer,inner=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
        started=time.monotonic();ceiling=started+3
        authority={'decision':REF};authority_raw=s.encoded({'scope':'synthetic protocol only','decision':REF})
        expected_pool=parent/'sdk-python-184-tools-guardian-14adc40b-9999999999'
        modules=(g,o,SyntheticBootstrap(mode),fake_terminal(mode))
        if mode.startswith('integrated-'):
            real_owner,bootstrap,terminal=integrated_closure(parent,mode);modules=(g,real_owner,bootstrap,terminal)
        child=os.fork()
        if child==0:
            try:
                outer.close();os.environ.clear();os.environ.update(s.MINIMAL_ENV)
                module.POOL_PARENT=parent
                if mode.startswith('pre-registration-'):
                    real_open=os.open;real_fstat=os.fstat;real_send=module.send_frame;opened=set()
                    def prereg_open(path,flags,*args,**kwargs):
                        if path==filenames[0] and flags==os.O_RDONLY|os.O_NOFOLLOW:
                            if mode=='pre-registration-open':raise OSError('synthetic readonly open failure')
                            fd=real_open(path,flags,*args,**kwargs);opened.add(fd);return fd
                        return real_open(path,flags,*args,**kwargs)
                    def prereg_fstat(fd):
                        if fd in opened and mode=='pre-registration-fstat':raise OSError('synthetic readonly identity failure')
                        return real_fstat(fd)
                    def prereg_send(channel,value,fds=()):
                        if value.get('type')=='REGISTERED':
                            if mode=='pre-registration-unknown':(expected_pool/'unknown-root-member').write_bytes(b'unknown must stay')
                            if mode=='pre-registration-send-after':real_send(channel,value,fds)
                            if mode in {'pre-registration-send','pre-registration-send-after','pre-registration-unknown'}:
                                raise OSError('synthetic registration send failure')
                        return real_send(channel,value,fds)
                    module.os.open=prereg_open;module.os.fstat=prereg_fstat;module.send_frame=prereg_send
                if mode.startswith('alloc-'):
                    real_mkdir=os.mkdir;real_open=os.open
                    def allocated_mkdir(path,mode_bits=0o777,*,dir_fd=None):
                        if (path=='wheelhouse' and mode in {'alloc-after-root','alloc-unknown'}) or (path=='events' and mode=='alloc-after-metadata'):
                            if mode=='alloc-unknown':(expected_pool/'unknown-root-member').write_bytes(b'synthetic unknown stays')
                            raise OSError('synthetic allocation refusal')
                        return real_mkdir(path,mode_bits,dir_fd=dir_fd)
                    def allocated_open(path,flags,mode_bits=0o777,*,dir_fd=None):
                        if path==filenames[0] and mode=='alloc-after-wheelhouse':raise OSError('synthetic archive allocation refusal')
                        return real_open(path,flags,mode_bits,dir_fd=dir_fd)
                    s.os.mkdir=allocated_mkdir;s.os.open=allocated_open
                if mode=='pidfd-fail':
                    def failed_pidfd(pid):raise OSError('synthetic pidfd prerequisite refusal')
                    s.os.pidfd_open=failed_pidfd
                if mode=='stop-refusal':
                    def refused_stop(*args):raise PermissionError(1,'synthetic stop refusal')
                    s.signal.pidfd_send_signal=refused_stop
                if mode=='survivor':
                    real_stop=s.stop_owner;calls=[0]
                    def stop_with_one_survivor(role,ceiling):
                        calls[0]+=1
                        if calls[0]==1:return {'owner_exited':False,'owner_exit_code':None}
                        return real_stop(role,ceiling)
                    s.stop_owner=stop_with_one_survivor
                code=module.guardian_job(inner,authority_raw,authority,started,dt.datetime.now(dt.timezone.utc).isoformat(),
                    modules,bounds=(0.05 if mode in {'survivor','stop-refusal'} else 1 if mode.startswith('integrated-') else 0.35,3))
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
                if disconnect=='register':
                    assert not expected_pool.exists()
                    assert all(os.fstat(fd).st_nlink==0 for fd in received[1:])
                else:
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
                elif mode=='late-reconcile':
                    assert session.receive() is None and session.receive() is None
                    session.ceiling=time.monotonic()+0.02
                    first=session.wait()
                    assert not first['result']['terminal_cleanup_complete'] and not session.finished
                    assert select.select([session.pidfd],[],[],2)[0]
                    message=session.reconcile_retained()
                else:message=session.wait()
                reaped=session.guardian_reaped
                result=message['result'];assert not result['acquisition_accepted']
                if mode=='pre-registration-unknown':
                    assert result['status']=='PRE_ADOPTION_CUSTODY_REQUIRED',result
                    assert not session.finished and not result['terminal_cleanup_complete']
                    assert not select.select([pidfd],[],[],0)[0]
                    assert session.custody is None and session.owner_pidfd is None
                    assert (expected_pool/'unknown-root-member').read_bytes()==b'unknown must stay'
                elif mode.startswith('pre-registration-'):
                    assert session.owner_pidfd is None and not result['acquisition_accepted']
                    if module is predecessor:
                        assert not session.finished and result['outer_owner_custody_required'],result
                        assert result['status'] in {'REFUSED','OUTER_CUSTODY_REQUIRED'}
                        assert session.custody is None and expected_pool.exists()
                        assert select.select([pidfd],[],[],max(0,ceiling-time.monotonic()))[0]
                        assert not list((expected_pool/'wheelhouse').iterdir())
                    else:
                        assert result['status']=='PRE_ADOPTION_REMOVED' and result['terminal_cleanup_complete'],result
                        assert result['owner_was_allocated'] is False and not expected_pool.exists() and session.finished
                elif mode=='alloc-unknown':
                    assert not session.finished and result['outer_owner_custody_required']
                    assert not result['terminal_cleanup_complete'] and not result.get('retained_forensic_custody_ready')
                    assert not select.select([pidfd],[],[],0)[0], 'partial allocation guardian lost custody'
                    assert (expected_pool/'unknown-root-member').read_bytes()==b'synthetic unknown stays'
                elif mode.startswith('alloc-'):
                    assert result['status']=='PRE_ADOPTION_REMOVED' and result['terminal_cleanup_complete']
                    assert result['allocation_cleanup']['complete'] and session.finished
                    assert not expected_pool.exists(), 'outer independently verifies allocation root absence'
                elif mode=='late-reconcile':
                    assert result['quiescent_after_failed_bound'] and not result['terminal_cleanup_complete']
                    assert not result['acquisition_accepted'] and session.finished
                    assert not list((expected_pool/'wheelhouse').iterdir())
                elif mode=='survivor':
                    assert result['status']=='SURVIVOR_CUSTODY_REQUIRED' and result['outer_owner_custody_required']
                    assert not result['terminal_cleanup_complete'] and not session.finished
                    assert not select.select([session.owner_pidfd],[],[],0)[0]
                    assert all(os.fstat(row['fd']).st_nlink==1 for row in session.custody.archive_nodes.values())
                    message=session.wait();result=message['result'];reaped=session.guardian_reaped
                    assert message['type']=='DEFERRED_CLEANUP' and result['retained_forensic_custody_ready'] and not result['terminal_cleanup_complete']
                    assert session.finished and not list((expected_pool/'wheelhouse').iterdir())
                elif mode in {'positive','integrated-positive'}:
                    assert result['status']=='COLLECTED_FOR_OUTER_READBACK' and session.finished
                    assert len(list((expected_pool/'wheelhouse').iterdir()))==6
                    assert (expected_pool/'operation-authority.json').read_bytes()==authority_raw
                else:
                    assert result['status']=='REFUSED',result
                    assert not result['terminal_cleanup_complete'] and result['retained_custody_sealed']
                    assert result['outer_owner_custody_required'] and result['retention_owner']=='sdk-python'
                    assert result['retained_tree_inventory_verified'] and result['root_tree_retained']
                    assert not list((expected_pool/'wheelhouse').iterdir())
                    assert session.finished
                    if mode=='pidfd-fail':
                        assert result['stop_reason']=='OWNER_PIDFD_PREREQUISITE_FAILED'
                        assert result['owner_process_exited'] and not result['owner_pidfd_available']
                    if mode=='stop-refusal':assert result['stop_refusal_code']=='PIDFD_STOP_ERRNO_1'
                if not session.finished and mode not in {'alloc-unknown','pre-registration-unknown'}:
                    assert select.select([pidfd],[],[],0)[0]
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


def unreported_live_owner_keeps_custody():
    """A separate private pipe gives only this test the missing actor's ground truth."""
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-unreported-owner-control-') as temporary:
        parent=Path(temporary);pool=parent/'sdk-python-184-tools-guardian-14adc40b-9999999999'
        outer,inner=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
        side_read,side_write=os.pipe();child=os.fork()
        if child==0:
            try:
                outer.close();os.close(side_read);os.environ.clear();os.environ.update(s.MINIMAL_ENV)
                custody=s.prepare_tree(g,parent,pool.name,filenames);custody.metadata_names=set(metadata)
                proof={'pool':str(pool),'ready':True,'started_monotonic':time.monotonic(),
                       'started_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'dir_ids':custody.dir_ids}
                fds=[custody.parent_fd,*[custody.directory_fds[d] for d in ('.','wheelhouse','metadata','events')],
                     *[custody.archive_nodes[name]['fd'] for name in filenames]]
                s.send_frame(inner,{'type':'REGISTERED','proof':proof,'filenames':filenames,'metadata_names':sorted(metadata)},fds)
                message,received=s.recv_frame(inner);assert message=={'type':'ARM'} and not received
                hidden=os.fork()
                if hidden==0:
                    inner.close();os.close(side_write)
                    signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.alarm(5)
                    os.write(next(iter(custody.archive_nodes.values()))['fd'],b'synthetic live holder')
                    signal.pause();os._exit(2)
                os.write(side_write,str(hidden).encode());os.close(side_write)
                # Fault boundary: guardian exits without publishing OWNER/pidfd.
                custody.close();inner.close();os._exit(0)
            except BaseException:os._exit(3)
        inner.close();os.close(side_write)
        guardian_pidfd=os.pidfd_open(child)
        session=s.Session(outer,child,guardian_pidfd,g,t,time.monotonic()+5,pool,filenames,metadata,b'{}')
        unknown_pidfd=None
        try:
            assert session.receive() is None
            hidden=int(os.read(side_read,64));unknown_pidfd=os.pidfd_open(hidden)
            message=session.wait();result=message['result']
            assert result['status']=='OUTER_CUSTODY_REQUIRED' and result['guardian_process_exited']
            assert not result['owner_process_exited'] and not result['terminal_cleanup_complete']
            assert session.owner_pidfd is None and not session.finished
            assert not select.select([unknown_pidfd],[],[],0)[0]
            assert len(list((pool/'wheelhouse').iterdir()))==6
            assert all(os.fstat(row['fd']).st_nlink==1 for row in session.custody.archive_nodes.values())
            # Only the independent test knows/reconciles the missing exact handle.
            signal.pidfd_send_signal(unknown_pidfd,signal.SIGKILL)
            assert select.select([unknown_pidfd],[],[],2)[0]
            session.owner_pidfd=os.dup(unknown_pidfd);session.owner_pid=hidden
            session.stack.callback(os.close,session.owner_pidfd)
            final=session.abort('SYNTHETIC_INDEPENDENT_HANDLE_RECONCILIATION')
            assert final['result']['retained_tree_inventory_verified'] and session.finished
            assert not final['result']['terminal_cleanup_complete'] and final['result']['outer_owner_custody_required']
            assert not list((pool/'wheelhouse').iterdir())
        finally:
            if unknown_pidfd is not None:
                if not select.select([unknown_pidfd],[],[],0)[0]:signal.pidfd_send_signal(unknown_pidfd,signal.SIGKILL)
                assert select.select([unknown_pidfd],[],[],2)[0];os.close(unknown_pidfd)
            if not session.guardian_reaped:
                signal.pidfd_send_signal(guardian_pidfd,signal.SIGKILL);os.waitpid(child,0)
            session.stack.close();os.close(side_read);cleanup_directory(parent)


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
    code="import pathlib,types,signal; b=pathlib.Path('/tmp/sdk-python-handoff/184-acquisition-launcher-bootstrap-proposal.py'); bootstrap=types.ModuleType('resource_source_control'); exec(compile(b.read_bytes(),str(b),'exec'),bootstrap.__dict__); signal.signal(signal.SIGCHLD,signal.SIG_IGN); bootstrap.configure_resources("+repr(cpus)+"); p=pathlib.Path('/tmp/sdk-python-handoff/184-acquisition-session-proposal.py'); m=types.ModuleType('source_startup_control'); m.__file__=str(p); exec(compile(p.read_bytes(),str(p),'exec'),m.__dict__); m.verify_startup(); print('CLOSED_STARTUP_OK')"
    run=subprocess.run([s.INTERPRETER,'-I','-S','-c',code],env=s.MINIMAL_ENV,close_fds=True,
                       capture_output=True,timeout=5,check=True)
    assert run.stdout==b'CLOSED_STARTUP_OK\n'


def live_start_refuses_no_decision():
    with patch.object(s,'verify_startup'),patch.object(s,'checked_bytes',side_effect=AssertionError('must not read authority')):
        try:s.start('/synthetic/absent','0'*64,None,'0'*64,time.monotonic(),dt.datetime.now(dt.timezone.utc).isoformat())
        except s.Refusal as error:assert str(error)=='UNFROZEN_OR_UNADMITTED_PACKET'
        else:raise AssertionError('missing admission accepted')


def retention_boundary(seconds,accepted):
    now=1800000000
    value=dt.datetime.fromtimestamp(now+seconds,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:s.verify_retention_deadline(value,now)
    except s.Refusal as error:
        assert not accepted and str(error)=='BOUNDED_FUTURE_RETENTION_REQUIRED'
    else:assert accepted


def preparation_callback_failure_closes(directory):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-extra-dir-close-control-') as temporary:
        parent=Path(temporary);opened=[];held=None;real_open=os.open;real_callback=g.ExitStack.callback
        def allocated_open(path,*args,**kwargs):
            fd=real_open(path,*args,**kwargs)
            if path==directory:opened.append(fd)
            return fd
        def callback(stack,function,*args,**kwargs):
            if args and args[0] in opened:raise MemoryError('synthetic extra-directory callback failure')
            return real_callback(stack,function,*args,**kwargs)
        try:
            with patch.object(s.os,'open',side_effect=allocated_open),patch.object(g.ExitStack,'callback',new=callback):
                try:s.prepare_tree(g,parent,'sdk-python-184-tools-guardian-extra-dir-close',filenames)
                except g.AllocationRefusal as error:held=error.custody;cleanup=error.cleanup
                else:raise AssertionError('directory callback failure not exercised')
            assert len(opened)==1 and held.directory_fds[directory]==opened[0]
            assert cleanup['complete'] and not held.path.exists()
            held.close();held.close()
            try:os.fstat(opened[0])
            except OSError as error:assert error.errno==9
            else:raise AssertionError('extra directory descriptor leaked')
        finally:
            if held is not None:held.close()


def directory_before_ledger(module,directory,interruption):
    tree=ast.parse(Path(module.__file__).read_text())
    insertion=next(node for node in ast.walk(tree) if isinstance(node,ast.Assign)
        and any(isinstance(target,ast.Subscript) and isinstance(target.value,ast.Attribute)
                and target.value.attr=='directory_fds' for target in node.targets))
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-directory-pre-ledger-') as temporary:
        parent=Path(temporary);held=None;opened=[]
        def interrupt(frame,event,arg):
            if event=='line' and frame.f_code.co_filename==module.__file__ and frame.f_lineno==insertion.lineno and frame.f_locals['name']==directory:
                opened.append(frame.f_locals['fd']);raise interruption('synthetic pre-ledger directory interruption')
            return interrupt
        try:
            sys.settrace(interrupt)
            try:module.prepare_tree(g,parent,'sdk-python-184-tools-guardian-directory-pre-ledger',filenames)
            except g.AllocationRefusal as error:held=error.custody
            finally:sys.settrace(None)
            assert len(opened)==1
            held.close();held.close()
            if module is predecessor:
                assert os.fstat(opened[0]).st_nlink==0, 'predecessor untracked directory FD not reproduced'
                os.close(opened.pop())
            else:
                try:os.fstat(opened[0])
                except OSError as error:assert error.errno==9
                else:raise AssertionError('pre-ledger directory descriptor leaked')
                opened.clear()
            assert not held.path.exists()
        finally:
            sys.settrace(None)
            if held is not None:held.close()
            for fd in opened:
                try:os.close(fd)
                except OSError:pass


def readonly_before_ledger(interruption):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-readonly-pre-ledger-') as temporary:
        custody=s.prepare_tree(g,Path(temporary),'sdk-python-184-tools-guardian-readonly-pre-ledger',filenames)
        opened=[];real_open=os.open
        class RefusedList:
            def append(self,fd):raise interruption('synthetic read-only list insertion interruption')
        def opened_fd(*args,**kwargs):
            fd=real_open(*args,**kwargs);opened.append(fd);return fd
        try:
            with patch.object(s.os,'open',side_effect=opened_fd):
                try:s.append_readonly_archive(custody,filenames[0],RefusedList())
                except interruption:pass
                else:raise AssertionError('read-only insertion failure not exercised')
            assert len(opened)==1
            try:os.fstat(opened[0])
            except OSError as error:assert error.errno==9
            else:raise AssertionError('pre-ledger read-only FD leaked')
            assert s.remove_unadopted_tree(custody)['complete'] and not custody.path.exists()
        finally:custody.close()


def guardian_startup_failure(mode):
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-guardian-setup-') as temporary:
        parent=Path(temporary);pool=parent/'not-allocated';marker=parent/'job-started';session=None
        def job(channel):
            marker.write_bytes(b'synthetic source fixture only')
            signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.alarm(3)
            signal.pause();return 1
        try:
            target='pidfd_open' if mode=='pidfd' else 'Session'
            owner=s.os if mode=='pidfd' else s
            with patch.object(owner,target,side_effect=OSError('synthetic guardian startup prerequisite failure')):
                session=s.spawn_guardian_session(job,g,t,time.monotonic()+2,pool,filenames,metadata,b'{}')
            result=session.wait()['result']
            assert session.finished and result['status']=='GUARDIAN_SETUP_REFUSED'
            assert result['guardian_process_exited'] and result['handles_closed'] and result['terminal_cleanup_complete']
            assert not result['allocation_gate_sent'] and not result['pool_was_allocated'] and not result['owner_was_allocated']
            assert not marker.exists() and not pool.exists() and not result['acquisition_accepted']
            try:os.waitpid(session.pid,os.WNOHANG)
            except ChildProcessError:pass
            else:raise AssertionError('exact setup guardian not reaped')
        finally:
            if session is not None and not session.stop['guardian_reaped']:
                os.kill(session.pid,signal.SIGKILL);os.waitpid(session.pid,0)


def predecessor_guardian_pidfd_failure():
    # Compile only the exact frozen start() fork suffix, retaining its error path;
    # substitute guardian_job with an inert marker fixture, never call start().
    tree=ast.parse(historical.read_text());start=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='start')
    index=next(i for i,node in enumerate(start.body) if isinstance(node,ast.Assign)
               and any(isinstance(target,ast.Tuple) for target in node.targets))
    function=ast.FunctionDef(name='frozen_guardian_fork_suffix',args=ast.arguments(posonlyargs=[],args=[],kwonlyargs=[],kw_defaults=[],defaults=[]),
                             body=start.body[index:],decorator_list=[])
    wrapper=ast.fix_missing_locations(ast.Module(body=[function],type_ignores=[]))
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-v6-guardian-pidfd-') as temporary:
        parent=Path(temporary);marker=parent/'unowned-job-started';created=[];channels=[]
        real_fork=os.fork;real_pair=socket.socketpair
        def fork():
            pid=real_fork()
            if pid:created.append(pid)
            return pid
        def pair(*args):
            pair=real_pair(*args);channels.extend(pair);return pair
        def job(*args):
            marker.write_bytes(b'predecessor allocation could run without outer pidfd')
            signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.alarm(3);signal.pause();return 1
        env=dict(predecessor.__dict__,POOL_PARENT=parent,raw=b'{}',authority={'decision':REF},
                 started=time.monotonic(),started_utc='synthetic',guard=g,owner=o,bootstrap=None,terminal=t,
                 selection=selection,guardian_job=job)
        exec(compile(wrapper,str(historical)+'#exact-start-fork-suffix','exec'),env)
        try:
            with patch.object(predecessor.os,'fork',side_effect=fork),patch.object(predecessor.socket,'socketpair',side_effect=pair), \
                 patch.object(predecessor.os,'pidfd_open',side_effect=OSError('synthetic outer pidfd failure')):
                try:env['frozen_guardian_fork_suffix']()
                except OSError:pass
                else:raise AssertionError('predecessor pidfd refusal missing')
            assert len(created)==1
            deadline=time.monotonic()+1
            while not marker.exists() and time.monotonic()<deadline:time.sleep(0.01)
            assert marker.exists() and os.waitpid(created[0],os.WNOHANG)==(0,0), 'predecessor lost live guardian not reproduced'
        finally:
            for pid in created:
                try:os.kill(pid,signal.SIGKILL)
                except ProcessLookupError:pass
                try:os.waitpid(pid,0)
                except ChildProcessError:pass
            for channel in channels:channel.close()


def guardian_gate_positive():
    with tempfile.TemporaryDirectory(prefix='sdk-python-184-guardian-gate-') as temporary:
        parent=Path(temporary);pool=parent/'never-allocated';marker=parent/'gate-received'
        def job(channel):
            marker.write_bytes(b'exact start gate arrived')
            s.send_frame(channel,{'type':'TERMINAL','result':{
                'status':'PRE_ADOPTION_REMOVED','owner_was_allocated':False,'outer_adoption_acknowledged':False,
                'allocation_cleanup':{'complete':True},'terminal_cleanup_complete':True,'acquisition_accepted':False}})
            return 1
        session=s.spawn_guardian_session(job,g,t,time.monotonic()+2,pool,filenames,metadata,b'{}')
        try:
            assert not marker.exists() and session.needs_guardian_start
            result=session.wait()['result']
            assert marker.exists() and session.finished and session.guardian_reaped and result['terminal_cleanup_complete']
            assert not result['acquisition_accepted'] and not pool.exists()
        finally:
            if not session.guardian_reaped:
                os.kill(session.pid,signal.SIGKILL);os.waitpid(session.pid,0)
            session.stack.close()


def check(name,operation):
    try:operation()
    except BaseException as error:results.append({'id':name,'outcome':'fail','type':type(error).__name__,'detail':str(error)[:600],
        'traceback':traceback.format_exc(limit=4)[-2000:]})
    else:results.append({'id':name,'outcome':'pass'})


baseline_fd_count=len(os.listdir('/proc/self/fd'))
with patch.object(socket,'create_connection',side_effect=AssertionError('NO_NETWORK_IN_SESSION_CONTROLS')):
    check('v6-guardian-pidfd-failure-loses-live-job-predecessor',predecessor_guardian_pidfd_failure)
    for failure in ('pidfd','constructor'):
        check('guardian-'+failure+'-failure-before-allocation-stops-reaps-reserved-child',lambda failure=failure:guardian_startup_failure(failure))
    check('guardian-allocation-waits-for-returned-outer-session-start-gate',guardian_gate_positive)
    for interruption in (MemoryError,KeyboardInterrupt):
        check('readonly-pre-ledger-'+interruption.__name__+'-closes-local-fd',lambda interruption=interruption:readonly_before_ledger(interruption))
        for directory in ('metadata','events'):
            for module,label in ((predecessor,'v6-leak'),(s,'successor-closes')):
                check(label+'-'+directory+'-immediate-pre-ledger-'+interruption.__name__,lambda module=module,directory=directory,interruption=interruption:directory_before_ledger(module,directory,interruption))
    for mode in ('pre-registration-open','pre-registration-fstat','pre-registration-send','pre-registration-send-after'):
        for module,label in ((predecessor,'v6-unowned-retention'),(s,'successor-no-owner-tree-removed')):
            check(label+'-'+mode,lambda module=module,mode=mode:run_protocol(mode,module=module))
    check('unknown-pre-registration-tree-keeps-exact-guardian-custody',lambda:run_protocol('pre-registration-unknown'))
    for directory in ('metadata','events'):
        check(directory+'-callback-failure-closes-recorded-directory-fd',lambda directory=directory:preparation_callback_failure_closes(directory))
    for seconds,accepted in [(0,False),(300,False),(301,True),(86400,True),(86401,False)]:
        check('retention-deadline-boundary-'+str(seconds),lambda seconds=seconds,accepted=accepted:retention_boundary(seconds,accepted))
    for mode in ('alloc-after-root','alloc-after-wheelhouse','alloc-after-metadata','alloc-unknown'):
        check(mode+'-root-custody-is-not-discarded',lambda mode=mode:run_protocol(mode))
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
    check('post-fork-pidfd-failure-exits-reserved-child-before-cleanup',lambda:run_protocol('pidfd-fail'))
    check('unreported-live-owner-is-unknown-retains-custody',unreported_live_owner_keeps_custody)
    check('stop-permission-refusal-still-requires-confirmed-exit',lambda:run_protocol('stop-refusal'))
    check('late-quiescence-releases-custody-without-late-pass',lambda:run_protocol('late-reconcile'))
    check('outer-disappears-before-arming-no-owner-work',lambda:run_protocol('positive','register'))
    check('outer-disappears-with-blocked-owner-stops-before-work',lambda:run_protocol('positive','owner'))
    check('descriptor-frame-roundtrip-preserves-scoped-handle',fd_frame_roundtrip)
    check('oversized-wire-frame-refused',lambda:frame_control(b'X'*(s.FRAME_CAP+1),'FRAME_EOF_OR_TRUNCATION'))
    check('duplicate-frame-key-refused',lambda:frame_control(b'{"type":"one","type":"two"}','DUPLICATE_FRAME_KEY'))
    check('non-object-frame-refused',lambda:frame_control(b'[]','FRAME_OBJECT_REQUIRED'))
    check('actual-root-owned-interpreter-clean-env-I-S-startup',actual_closed_startup)
    check('missing-new-decision-refuses-before-authority-or-fork',live_start_refuses_no_decision)
    check('all-control-children-reaped',lambda:s.require(not Path('/proc/self/task/'+str(os.getpid())+'/children').read_text().strip(),
                                                      'CONTROL_CHILD_REMAINS'))
    check('all-control-descriptors-closed',lambda:s.require(len(os.listdir('/proc/self/fd'))==baseline_fd_count,
                                                           'CONTROL_DESCRIPTOR_REMAINS'))

report={'scope':'synthetic role-protocol controls; start/admitted-data API never called with a decision',
        'source_sha256':hashlib.sha256((BASE/'184-acquisition-session-proposal.py').read_bytes()).hexdigest(),
        'cases':len(results),'passed':sum(row['outcome']=='pass' for row in results),
        'failed':sum(row['outcome']=='fail' for row in results),'results':results,
        'real_network_requests':0,'selected_archives_acquired':0,'downloaded_wheel_imports_or_installs':0,
        'new_general_decision':None,'acquisition_accepted':False}
report_path=BASE/'184-acquisition-session-controls-result.json'
report_bytes=(json.dumps(report,sort_keys=True,indent=2)+'\n').encode()
if report_path.exists():
    if report_path.read_bytes()!=report_bytes:
        print(json.dumps(report,indent=2))
        raise SystemExit('CONTROL_RESULT_COLLISION; prior record preserved')
else:
    descriptor=os.open(report_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400)
    with os.fdopen(descriptor,'wb') as stream:
        assert stream.write(report_bytes)==len(report_bytes)
print(json.dumps(report,indent=2));raise SystemExit(bool(report['failed']))
