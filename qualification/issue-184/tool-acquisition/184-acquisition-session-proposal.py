"""Unexecuted isolated guardian/owner session. No CLI invocation is admitted.

The public start function requires exact reviewed authority bytes/decision and
closed startup. A valid-looking URL alone is not authorization. In a survivor
state the session retains descriptor/process custody and cannot finalize.
"""
from __future__ import annotations

from array import array
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import resource
import re
import select
import signal
import socket
import stat
import sys
import time
from types import ModuleType

BASE=Path('/tmp/sdk-python-handoff')
POOL_PARENT=Path('/home/lab4/fleet/tmp')
MINIMAL_ENV={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8','LC_ALL':'C.UTF-8','TZ':'UTC'}
SOURCE='14adc40b00b68cc8a3e1ae5647a885a42f273650'
INTERPRETER='/usr/bin/python3.14'
INTERPRETER_SHA='52e0a13e60a981d8c4b6478be2ba5176f69da07948a056bf49cf6f077e30cb41'
LIBRARY='/usr/lib/x86_64-linux-gnu/libpython3.14.so.1.0'
LIBRARY_SHA='982cb3768d1d8b00126255ee2785bccf8360c68b197fb3dec95858ab56195f87'
PINNED={'owner': '234e099b5208aaafca379d3e5535cf44b3158a327764eec0ef4e12afcfc45ffb', 'bootstrap': '12679c567452dd4cdda7dbd789ac600ef0b5455a1225c95cf59d066fda472569', 'guardian': 'afc5f198de12d4b26520957bc563ff0122f8535838a1cbc64c40272c5f3f7f70', 'terminal': 'd050992a68c09db741fa4a0afda234c60e30a3412441b5dfce16be5e26efedce', 'selection': 'e9a7d639325b866569cdab83b81a2949437209a6bca91c881c2e365fdc573a1c', 'interpreter': '52e0a13e60a981d8c4b6478be2ba5176f69da07948a056bf49cf6f077e30cb41', 'library': '982cb3768d1d8b00126255ee2785bccf8360c68b197fb3dec95858ab56195f87', 'ca': '2ef52ad56b4803e264045c915e55c236cbfa045b84b81efefc218a19eb44bd2b'}
FRAME_CAP=65536
POOL_CAP=4*1024**2


class Refusal(ValueError):pass


def require(value,code):
    if not value:raise Refusal(code)


def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return (json.dumps(value,sort_keys=True,separators=(',',':'))+'\n').encode()


def pairs(values):
    result={}
    for key,value in values:
        require(key not in result,'DUPLICATE_FRAME_KEY');result[key]=value
    return result


def checked_bytes(path,expected,cap,uid):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        before=os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_uid==uid and before.st_nlink==1
                and not before.st_mode&0o022 and before.st_size<=cap,'INPUT_CUSTODY_CHANGED')
        raw=stream.read(cap+1);after=os.fstat(stream.fileno())
        require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)
                ==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
                and len(raw)==before.st_size and sha(raw)==expected,'INPUT_BYTES_CHANGED')
    named=os.stat(path,follow_symlinks=False)
    require((named.st_dev,named.st_ino,named.st_mtime_ns,named.st_ctime_ns)
            ==(before.st_dev,before.st_ino,before.st_mtime_ns,before.st_ctime_ns),'INPUT_PATH_CHANGED')
    return raw


def module(name,filename,expected):
    raw=checked_bytes(BASE/filename,expected,65536,os.geteuid())
    result=ModuleType(name);result.__file__=str(BASE/filename)
    result.VERIFIED_SOURCE_SHA256=expected
    exec(compile(raw,'<verified-'+name+'>','exec'),result.__dict__)
    return result


def verify_startup():
    require(dict(os.environ)==MINIMAL_ENV and sys.flags.isolated==1 and sys.flags.no_site==1,
            'CLOSED_ISOLATED_NO_SITE_STARTUP_REQUIRED')
    require(Path(sys.executable).resolve()==Path(INTERPRETER),'EXACT_INTERPRETER_REQUIRED')
    require(1<=len(os.sched_getaffinity(0))<=2,'BOUNDED_CPU_AFFINITY_REQUIRED')
    require(resource.getrlimit(resource.RLIMIT_AS)==(128*1024**2,384*1024**2)
            and resource.getrlimit(resource.RLIMIT_CPU)==(30,60)
            and resource.getrlimit(resource.RLIMIT_NOFILE)==(128,128)
            and resource.getrlimit(resource.RLIMIT_FSIZE)==(POOL_CAP,POOL_CAP),'EXACT_STARTUP_LIMITS_REQUIRED')
    checked_bytes(Path(INTERPRETER),INTERPRETER_SHA,8*1024**2,0)
    checked_bytes(Path(LIBRARY),LIBRARY_SHA,9*1024**2,0)


def send_frame(channel,value,fds=()):
    raw=encoded(value);require(len(raw)<=FRAME_CAP and len(fds)<=16,'FRAME_BOUND')
    ancillary=[] if not fds else [(socket.SOL_SOCKET,socket.SCM_RIGHTS,array('i',fds))]
    require(channel.sendmsg([raw],ancillary)==len(raw),'SHORT_FRAME_WRITE')


def recv_frame(channel):
    raw,ancillary,flags,_=channel.recvmsg(FRAME_CAP+1,socket.CMSG_SPACE(16*array('i').itemsize))
    received=[]
    try:
        for level,kind,data in ancillary:
            require(level==socket.SOL_SOCKET and kind==socket.SCM_RIGHTS,'UNKNOWN_FRAME_ANCILLARY')
            values=array('i');values.frombytes(data[:len(data)-len(data)%values.itemsize]);received.extend(values)
        require(raw and len(raw)<=FRAME_CAP and not flags&(socket.MSG_TRUNC|socket.MSG_CTRUNC),
                'FRAME_EOF_OR_TRUNCATION')
        value=json.loads(raw,object_pairs_hook=pairs)
        require(isinstance(value,dict),'FRAME_OBJECT_REQUIRED')
        return value,received
    except BaseException:
        for fd in received:os.close(fd)
        raise


def prepare_tree(guard,parent,leaf,filenames):
    custody=guard.ArchiveCustody(parent,leaf,filenames)
    custody.dir_ids={'.':list(custody.root_id),'wheelhouse':list(custody.wheelhouse_id)}
    custody.directory_fds={'.':custody.root_fd,'wheelhouse':custody.wheelhouse_fd}
    try:
        for name in ('metadata','events'):
            os.mkdir(name,0o700,dir_fd=custody.root_fd)
            fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=custody.root_fd)
            custody.stack.callback(os.close,fd)
            st=os.fstat(fd);custody.dir_ids[name]=[st.st_dev,st.st_ino];custody.directory_fds[name]=fd
        return custody
    except BaseException:
        custody.cleanup();custody.close();raise


def progress_bound(custody):
    custody.scope();total=0;count=0
    for directory in ('.','metadata','wheelhouse','events'):
        parent=custody.directory_fds[directory]
        st=os.fstat(parent)
        require([st.st_dev,st.st_ino]==custody.dir_ids[directory],'PROGRESS_DIRECTORY_CHANGED')
        if directory!='.':
            named=os.stat(directory,dir_fd=custody.root_fd,follow_symlinks=False)
            require([named.st_dev,named.st_ino]==custody.dir_ids[directory],'PROGRESS_DIRECTORY_CHANGED')
        names=set(os.listdir(parent))
        if directory=='.':names-={'metadata','wheelhouse','events'}
        require(len(names)<=128,'PROGRESS_FILE_COUNT_BOUND')
        for name in names:
            st=os.stat(name,dir_fd=parent,follow_symlinks=False)
            require(stat.S_ISREG(st.st_mode) and st.st_uid==os.geteuid() and st.st_nlink==1,
                    'PROGRESS_FILE_CUSTODY_CHANGED')
            total+=st.st_size;count+=1
            if directory=='wheelhouse':
                require(name in custody.archive_nodes and st.st_size<=2*1024**2
                        and (st.st_dev,st.st_ino)==custody.archive_nodes[name]['identity'],
                        'PROGRESS_ARCHIVE_BOUND_OR_IDENTITY_CHANGED')
    require(count<=128 and total<=POOL_CAP,'PROGRESS_POOL_BOUND')
    total+=sum(os.fstat(row['fd']).st_size for row in custody.archive_nodes.values()
               if os.fstat(row['fd']).st_nlink==0)
    require(total<=POOL_CAP,'PROGRESS_POOL_BOUND')
    require(sum(os.fstat(row['fd']).st_size for row in custody.archive_nodes.values())<=3*1024**2,
            'PROGRESS_ARCHIVE_TOTAL_BOUND')
    return total


def spawn_owner(bootstrap,decision,proof,custody):
    status_read,status_write=os.pipe2(os.O_CLOEXEC)
    gate_read,gate_write=os.pipe2(os.O_CLOEXEC)
    borrowed={name:custody.duplicate_for_owner(name) for name in custody.archive_nodes}
    proof=dict(proof,archive_fds=borrowed)
    pid=os.fork()
    if pid==0:
        try:
            keep={status_write,gate_read,*borrowed.values()}
            for item in Path('/proc/self/fd').iterdir():
                fd=int(item.name)
                if fd>=3 and fd not in keep:
                    try:os.close(fd)
                    except OSError:pass
            devnull=os.open('/dev/null',os.O_RDWR)
            for fd in (0,1,2):os.dup2(devnull,fd)
            if devnull not in keep and devnull>2:os.close(devnull)
            os.environ.clear();os.environ.update(MINIMAL_ENV);os.chdir('/')
            resource.setrlimit(resource.RLIMIT_AS,(384*1024**2,384*1024**2))
            resource.setrlimit(resource.RLIMIT_CPU,(60,60))
            resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
            resource.setrlimit(resource.RLIMIT_FSIZE,(POOL_CAP,POOL_CAP))
            require(os.read(gate_read,1)==b'G','OWNER_NOT_ARMED')
            os.close(gate_read)
            result=bootstrap.launch(decision,proof)
        except BaseException as error:
            result={'outcome':'fail','refusal_code':'OWNER_'+type(error).__name__.upper()}
        try:
            raw=encoded(result)
            require(len(raw)<=32768,'OWNER_RESULT_BOUND')
            while raw:
                written=os.write(status_write,raw);require(written>0,'SHORT_OWNER_RESULT_WRITE');raw=raw[written:]
        finally:os._exit(0 if result.get('outcome')=='candidate' else 1)
    os.close(status_write);os.close(gate_read)
    for fd in borrowed.values():os.close(fd)
    os.set_blocking(status_read,False)
    return {'pid':pid,'pidfd':os.pidfd_open(pid),'status':status_read,'gate':gate_write}


def stop_owner(role,ceiling):
    if not select.select([role['pidfd']],[],[],0)[0]:
        try:signal.pidfd_send_signal(role['pidfd'],signal.SIGKILL)
        except ProcessLookupError:pass
    exited=bool(select.select([role['pidfd']],[],[],max(0,min(5,ceiling-time.monotonic())))[0])
    if not exited:return {'owner_exited':False,'owner_exit_code':None}
    _,status=os.waitpid(role['pid'],0)
    return {'owner_exited':True,'owner_exit_code':os.waitstatus_to_exitcode(status)}


def record_terminal(custody,result,authority_raw,label='guardian-result.json'):
    require(label in {'guardian-result.json','outer-result.json'},'INVALID_TERMINAL_RECORD_NAME')
    custody.scope();used=progress_bound(custody)
    additions={'operation-authority.json':authority_raw,label:encoded(result)}
    try:
        fd=os.open('operation-authority.json',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=custody.root_fd)
    except FileNotFoundError:pass
    else:
        with os.fdopen(fd,'rb') as stream:
            st=os.fstat(stream.fileno())
            require(stat.S_ISREG(st.st_mode) and st.st_uid==os.geteuid() and st.st_nlink==1
                    and st.st_size==len(authority_raw) and stream.read(32769)==authority_raw,
                    'EXISTING_OPERATION_AUTHORITY_COLLISION')
        del additions['operation-authority.json']
    require(used+sum(len(raw) for raw in additions.values())<=POOL_CAP,'TERMINAL_RECORD_POOL_BOUND')
    os.fchmod(custody.root_fd,0o700)
    try:
        for name,raw in additions.items():
            fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400,dir_fd=custody.root_fd)
            with os.fdopen(fd,'wb') as stream:
                require(stream.write(raw)==len(raw),'SHORT_TERMINAL_RECORD_WRITE');stream.flush();os.fsync(stream.fileno())
    finally:os.fchmod(custody.root_fd,0o500)


def seal_retained(custody):
    """Seal known new forensic nodes after exit; unknown replacements stay untouched."""
    custody.scope();complete=True
    fixed={'selection.json','ca.pem','launch.json','wheel-metadata.json','acquisition-observation.json',
           'inventory.json','failure.json','operation-authority.json','guardian-result.json','outer-result.json'}
    for directory,parent in custody.directory_fds.items():
        st=os.fstat(parent)
        require([st.st_dev,st.st_ino]==custody.dir_ids[directory],'SEAL_DIRECTORY_CHANGED')
        names=set(os.listdir(parent))
        if directory=='.':names-={'metadata','wheelhouse','events'}
        for name in names:
            known=(name in fixed if directory=='.' else name in custody.metadata_names if directory=='metadata'
                   else name in custody.archive_nodes if directory=='wheelhouse'
                   else bool(re.fullmatch(r'(?:0[1-9]|1[0-2])-(?:request|headers|complete|refusal)\.json',name)))
            if not known:complete=False;continue
            try:
                if directory=='wheelhouse':custody.node(name)
                fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
                try:
                    node=os.fstat(fd)
                    require(stat.S_ISREG(node.st_mode) and node.st_uid==os.geteuid() and node.st_nlink==1,
                            'SEAL_FILE_CUSTODY_CHANGED')
                    named=os.stat(name,dir_fd=parent,follow_symlinks=False)
                    require((named.st_dev,named.st_ino)==(node.st_dev,node.st_ino),'SEAL_FILE_IDENTITY_CHANGED')
                    os.fchmod(fd,0o400)
                finally:os.close(fd)
            except (OSError,ValueError):complete=False
        os.fchmod(parent,0o500)
    custody.scope();return complete


def cleanup_after_exit(custody,stop,reason,ceiling):
    if not stop['owner_exited']:
        return {'status':'SURVIVOR_CUSTODY_REQUIRED','stop_reason':reason,'owner_process_exited':False,
                'archive_cleanup':{'complete':False,'refusal_code':'OWNER_EXIT_UNCONFIRMED'},
                'terminal_cleanup_complete':False,'outer_owner_custody_required':True,
                'retained_owned_names':sorted(custody.archive_nodes),'acquisition_accepted':False}
    try:cleanup=custody.cleanup()
    except BaseException as error:
        cleanup={'complete':False,'refusal_code':str(error) if isinstance(error,ValueError) else type(error).__name__}
    try:sealed=seal_retained(custody)
    except BaseException:sealed=False
    within=time.monotonic()<ceiling
    return {'status':'REFUSED','stop_reason':reason,'owner_process_exited':True,
            'owner_exit_code':stop['owner_exit_code'],'archive_cleanup':cleanup,
            'within_cleanup_ceiling':within,'retained_custody_sealed':sealed,
            'terminal_cleanup_complete':bool(cleanup['complete']) and sealed and within,'acquisition_accepted':False}


def guardian_job(channel,authority_raw,authority,started,started_utc,modules,bounds=(270,300)):
    guard,owner,bootstrap,terminal=modules
    require(0<bounds[0]<bounds[1]<=300,'PRIVATE_PROTOCOL_TEST_BOUNDS_INVALID')
    ceiling=started+bounds[1];active=started+bounds[0]
    selection_raw=checked_bytes(owner.SELECTION,owner.SELECTION_SHA,32768,os.geteuid())
    selection=json.loads(selection_raw,object_pairs_hook=pairs)
    filenames=[tool['filename'] for tool in selection['tools']]
    leaf='sdk-python-184-tools-guardian-14adc40b-'+authority['decision'].rsplit('-',1)[-1]
    custody=None;role=None;stop=None;owner_result=None
    try:
        custody=prepare_tree(guard,POOL_PARENT,leaf,filenames)
        custody.metadata_names={tool['name']+'.json' for tool in selection['tools']}
        proof={'pool':str(custody.path),'ready':True,'started_monotonic':started,'started_utc':started_utc,
               'dir_ids':custody.dir_ids}
        directory_order=['.','wheelhouse','metadata','events']
        fds=[custody.parent_fd,*[custody.directory_fds[name] for name in directory_order],
             *[custody.archive_nodes[name]['fd'] for name in filenames]]
        readonly=[]
        try:
            for name in filenames:
                fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=custody.wheelhouse_fd)
                readonly.append(fd)
                st=os.fstat(fd)
                require((st.st_dev,st.st_ino)==custody.archive_nodes[name]['identity'],
                        'OUTER_ARCHIVE_DESCRIPTOR_CHANGED')
            send_frame(channel,{'type':'REGISTERED','proof':proof,'filenames':filenames,
                               'metadata_names':sorted(custody.metadata_names)},fds[:5]+readonly)
        finally:
            for fd in readonly:os.close(fd)
        channel.settimeout(max(0.001,min(10,active-time.monotonic())))
        message,received=recv_frame(channel)
        require(not received and message=={'type':'ARM'},'GUARDIAN_ARM_MISMATCH')
        role=spawn_owner(bootstrap,authority['decision'],proof,custody)
        send_frame(channel,{'type':'OWNER','pid':role['pid']},[role['pidfd']])
        message,received=recv_frame(channel)
        require(not received and message=={'type':'RUN'},'OWNER_RUN_MISMATCH')
        os.write(role['gate'],b'G');os.close(role['gate']);role['gate']=-1
        channel.settimeout(None)
        raw=bytearray();reason=None
        while time.monotonic()<active:
            progress_bound(custody)
            available,_,_=select.select([channel,role['status'],role['pidfd']],[],[],max(0,min(0.2,active-time.monotonic())))
            if channel in available:
                reason='OUTER_CONTROL_CLOSED_OR_ABORTED';break
            if role['status'] in available:
                chunk=os.read(role['status'],32769-len(raw));raw.extend(chunk)
                require(len(raw)<=32768,'OWNER_RESULT_BOUND')
            if role['pidfd'] in available:
                while True:
                    try:chunk=os.read(role['status'],32769-len(raw))
                    except BlockingIOError:break
                    if not chunk:break
                    raw.extend(chunk);require(len(raw)<=32768,'OWNER_RESULT_BOUND')
                stop=stop_owner(role,ceiling)
                result=json.loads(raw,object_pairs_hook=pairs)
                owner_result=result
                require(stop['owner_exited'] and stop['owner_exit_code']==0 and result.get('outcome')=='candidate',
                        'OWNER_DID_NOT_COMPLETE')
                readback=terminal.validate_candidate(custody,result,selection_raw,owner,PINNED['owner'],
                    authority['decision'],stop['owner_exited'],stop['owner_exit_code'],ceiling)
                terminal_record={'status':'COLLECTED_FOR_OUTER_READBACK','readback':readback,
                                 'owner_process_exited':True,'owner_exit_code':0,'acquisition_accepted':False}
                record_terminal(custody,terminal_record,authority_raw)
                snapshot=terminal.snapshot(custody)
                public={name:{key:value for key,value in row.items() if key!='raw'} for name,row in snapshot.items()}
                send_frame(channel,{'type':'TERMINAL','result':terminal_record,'inventory':public})
                return 0
        if reason is None:reason='ABSOLUTE_ACTIVE_DEADLINE'
        raise Refusal(reason)
    except BaseException as error:
        code=getattr(error,'code',None)
        if not isinstance(code,str):
            code=str(error) if isinstance(error,(Refusal,guard.Refusal,terminal.Refusal)) else ''
        reason=code if re.fullmatch(r'[A-Z0-9_]{1,128}',code) else 'GUARDIAN_'+type(error).__name__.upper()
        if role is not None and stop is None:stop=stop_owner(role,ceiling)
        if custody is None:
            result={'status':'SETUP_REFUSED','stop_reason':reason,'terminal_cleanup_complete':False,'acquisition_accepted':False}
        elif role is None:
            result=cleanup_after_exit(custody,{'owner_exited':True,'owner_exit_code':None},reason,ceiling)
            result['owner_was_allocated']=False;result['owner_process_exited']=None
        else:result=cleanup_after_exit(custody,stop,reason,ceiling)
        if isinstance(owner_result,dict):result['owner_result']=owner_result
        if custody is not None and (role is None or stop['owner_exited']):
            try:
                record_terminal(custody,result,authority_raw);result['retained_terminal_record_written']=True
            except BaseException:
                result['retained_terminal_record_written']=False;result['terminal_cleanup_complete']=False
        try:send_frame(channel,{'type':'TERMINAL','result':result})
        except BaseException:pass
        if role is not None and not stop['owner_exited']:
            # Custody only: no work/retry or acceptance beyond the failed bound.
            # The separately admitted survivor policy must allow this keeper;
            # the outer session already holds duplicate inode and pidfd handles.
            while not select.select([role['pidfd']],[],[],60)[0]:pass
            stop=stop_owner(role,time.monotonic()+1)
            final=cleanup_after_exit(custody,stop,'DEFERRED_EXACT_OWNER_EXIT',ceiling)
            try:record_terminal(custody,final,authority_raw)
            except BaseException:final['terminal_cleanup_complete']=False
            try:send_frame(channel,{'type':'DEFERRED_CLEANUP','result':final})
            except BaseException:pass
        return 1
    finally:
        if role is not None:
            for name in ('status','gate','pidfd'):
                if role[name]>=0:os.close(role[name])
        if custody is not None:custody.close()


class Session:
    """Outer owner retains handles until process exit AND scoped terminal proof."""
    def __init__(self,channel,pid,pidfd,guard,terminal,ceiling,expected_pool,expected_names,expected_metadata,authority_raw=None):
        self.channel,self.pid,self.pidfd=channel,pid,pidfd
        self.guard,self.terminal,self.ceiling=guard,terminal,ceiling
        self.expected_pool,self.expected_names=str(expected_pool),list(expected_names)
        self.expected_metadata=set(expected_metadata)
        self.authority_raw=authority_raw
        self.custody=None;self.owner_pidfd=None;self.owner_pid=None
        self.stack=ExitStack();self.stack.callback(channel.close);self.stack.callback(os.close,pidfd)
        self.finished=False;self.guardian_reaped=False

    def receive(self):
        self.channel.settimeout(max(0.001,self.ceiling-time.monotonic()))
        message,fds=recv_frame(self.channel)
        adopted=False
        try:
            if message.get('type')=='REGISTERED':
                require(self.custody is None,'DUPLICATE_POOL_REGISTRATION')
                names=message['filenames'];require(len(names)==6 and len(fds)==11,'POOL_DESCRIPTOR_HANDOFF_CHANGED')
                proof=message['proof']
                require(proof.get('pool')==self.expected_pool and names==self.expected_names
                        and set(message.get('metadata_names',[]))==self.expected_metadata
                        and set(proof.get('dir_ids',{}))=={'.','wheelhouse','metadata','events'},
                        'EXPECTED_POOL_HANDOFF_CHANGED')
                actual_parent=os.stat(Path(self.expected_pool).parent,follow_symlinks=False)
                handed_parent=os.fstat(fds[0])
                require((actual_parent.st_dev,actual_parent.st_ino)==(handed_parent.st_dev,handed_parent.st_ino),
                        'EXPECTED_PARENT_DESCRIPTOR_CHANGED')
                lease=self.guard.ArchiveCustody.__new__(self.guard.ArchiveCustody)
                lease.path=Path(proof['pool']);lease.parent_fd=fds[0];lease.root_fd=fds[1];lease.wheelhouse_fd=fds[2]
                lease.dir_ids=proof['dir_ids'];lease.root_id=tuple(lease.dir_ids['.']);lease.wheelhouse_id=tuple(lease.dir_ids['wheelhouse'])
                lease.directory_fds=dict(zip(['.','wheelhouse','metadata','events'],fds[1:5]))
                lease.archive_nodes={name:{'fd':fd,'identity':(os.fstat(fd).st_dev,os.fstat(fd).st_ino)} for name,fd in zip(names,fds[5:])}
                lease.metadata_names=self.expected_metadata
                lease.scope();progress_bound(lease)
                for name in names:require(lease.node(name).st_size==0,'ARCHIVE_NOT_EMPTY_AT_HANDOFF')
                self.custody=lease
                for fd in fds:self.stack.callback(os.close,fd)
                adopted=True;send_frame(self.channel,{'type':'ARM'})
            elif message.get('type')=='OWNER':
                require(self.custody is not None and self.owner_pidfd is None and len(fds)==1,'OWNER_DESCRIPTOR_HANDOFF_CHANGED')
                require(os.readlink(f'/proc/self/fd/{fds[0]}')=='anon_inode:[pidfd]','EXACT_OWNER_PIDFD_REQUIRED')
                info=Path(f'/proc/self/fdinfo/{fds[0]}').read_text()
                require(('Pid:\t'+str(message['pid'])+'\n') in info,'EXACT_OWNER_PIDFD_REQUIRED')
                self.owner_pidfd=fds[0];self.owner_pid=message['pid'];self.stack.callback(os.close,fds[0]);adopted=True
                send_frame(self.channel,{'type':'RUN'})
            elif message.get('type') in {'TERMINAL','DEFERRED_CLEANUP'}:
                require(not fds,'UNEXPECTED_TERMINAL_DESCRIPTORS')
                result=message['result']
                if result.get('status')=='SURVIVOR_CUSTODY_REQUIRED':return message
                require(select.select([self.pidfd],[],[],max(0,self.ceiling-time.monotonic()))[0],
                        'GUARDIAN_EXIT_UNCONFIRMED')
                _,status=os.waitpid(self.pid,0)
                self.guardian_reaped=True
                if result.get('status')=='COLLECTED_FOR_OUTER_READBACK':
                    require(os.waitstatus_to_exitcode(status)==0 and self.owner_pidfd is not None
                            and select.select([self.owner_pidfd],[],[],0)[0],'TERMINAL_PROCESS_EXIT_MISMATCH')
                    current=self.terminal.snapshot(self.custody)
                    public={name:{key:value for key,value in row.items() if key!='raw'} for name,row in current.items()}
                    require(public==message.get('inventory'),'OUTER_INVENTORY_READBACK_CHANGED')
                elif not result.get('terminal_cleanup_complete',False):
                    result['outer_owner_custody_required']=True;return message
                self.finished=True;self.stack.close();return message
            else:raise Refusal('UNEXPECTED_SESSION_MESSAGE')
            return None
        finally:
            if not adopted:
                for fd in fds:os.close(fd)

    def wait(self):
        try:
            while not self.finished:
                result=self.receive()
                if result is not None:return result
        except BaseException as error:
            code=str(error) if isinstance(error,Refusal) else ''
            return self.abort('OUTER_'+(code if re.fullmatch(r'[A-Z0-9_]{1,128}',code) else type(error).__name__.upper()))

    def abort(self,reason):
        """Keep custody if either writer/guardian exit or exact cleanup is unproved."""
        try:send_frame(self.channel,{'type':'ABORT'})
        except BaseException:pass
        guardian_gone=bool(select.select([self.pidfd],[],[],max(0,self.ceiling-time.monotonic()))[0])
        owner_gone=self.owner_pidfd is not None and bool(select.select([self.owner_pidfd],[],[],0)[0])
        if guardian_gone and not owner_gone:
            try:signal.pidfd_send_signal(self.owner_pidfd,signal.SIGKILL)
            except ProcessLookupError:pass
            owner_gone=bool(select.select([self.owner_pidfd],[],[],max(0,min(5,self.ceiling-time.monotonic())))[0])
        if guardian_gone and not self.guardian_reaped:
            os.waitpid(self.pid,0);self.guardian_reaped=True
        if guardian_gone and owner_gone and self.custody is not None:
            result=cleanup_after_exit(self.custody,{'owner_exited':True,'owner_exit_code':None},reason,self.ceiling)
            if self.authority_raw is not None:
                try:record_terminal(self.custody,result,self.authority_raw,'outer-result.json')
                except BaseException:result['terminal_cleanup_complete']=False
            if result['terminal_cleanup_complete']:
                self.finished=True;self.stack.close()
            else:result['outer_owner_custody_required']=True
        else:
            result={'status':'OUTER_CUSTODY_REQUIRED','stop_reason':reason,'terminal_cleanup_complete':False,
                    'owner_process_exited':owner_gone,'guardian_process_exited':guardian_gone,
                    'outer_owner_custody_required':True,'acquisition_accepted':False}
        return {'type':'TERMINAL','result':result}


def start(authority_path,expected_sha,expected_decision,expected_session_sha,started,started_utc):
    """Proposal API only; caller must have the separate exact General admission."""
    verify_startup()
    require(PINNED and expected_decision is not None,'UNFROZEN_OR_UNADMITTED_PACKET')
    checked_bytes(Path(__file__),expected_session_sha,65536,os.geteuid())
    raw=checked_bytes(Path(authority_path),expected_sha,32768,os.geteuid())
    json.loads(raw,object_pairs_hook=pairs)
    guard=module('reviewed_guardian','184-acquisition-custody-guardian-v2-proposal.py',PINNED['guardian'])
    authority=guard.bind_authority(raw,expected_sha,expected_decision,PINNED)
    require(authority.get('survivor_policy')=='retain-custody-and-escalate'
            and authority.get('pool_parent')==str(POOL_PARENT)
            and authority.get('launcher_sha256')==expected_session_sha
            and isinstance(authority.get('cpu_affinity'),list)
            and all(type(cpu) is int for cpu in authority['cpu_affinity'])
            and set(authority['cpu_affinity'])==os.sched_getaffinity(0),'OUTER_CUSTODY_POLICY_REQUIRED')
    require(set(authority)=={'decision','selected_source','inputs','environment','host','total_seconds','io_seconds',
                            'retries','survivor_policy','pool_parent','launcher_sha256','cpu_affinity'},'UNKNOWN_AUTHORITY_FIELD')
    owner=module('reviewed_guarded_owner','184-acquire-tools-v5.py',PINNED['owner'])
    bootstrap=module('reviewed_owner_bootstrap','184-acquire-tools-v5-bootstrap.py',PINNED['bootstrap'])
    terminal=module('reviewed_terminal','184-acquisition-terminal-proposal.py',PINNED['terminal'])
    require(bootstrap.OWNER_SHA==PINNED['owner'],'BOOTSTRAP_OWNER_AUTHORITY_CHANGED')
    selection_raw=checked_bytes(owner.SELECTION,owner.SELECTION_SHA,32768,os.geteuid())
    selection=json.loads(selection_raw,object_pairs_hook=pairs)
    checked_bytes(owner.CA_SOURCE,owner.CA_SHA,300000,os.geteuid())
    require(type(started) in (int,float) and 0<started<=time.monotonic()<started+270,'BOOTSTRAP_TOTAL_DEADLINE_EXCEEDED')
    outer,inner=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
    pid=os.fork()
    if pid==0:
        outer.close();os.setsid()
        resource.setrlimit(resource.RLIMIT_AS,(128*1024**2,384*1024**2))
        resource.setrlimit(resource.RLIMIT_CPU,(30,60))
        resource.setrlimit(resource.RLIMIT_FSIZE,(POOL_CAP,POOL_CAP))
        try:code=guardian_job(inner,raw,authority,started,started_utc,(guard,owner,bootstrap,terminal))
        except BaseException:code=2
        finally:inner.close()
        os._exit(code)
    inner.close()
    expected_pool=POOL_PARENT/('sdk-python-184-tools-guardian-14adc40b-'+authority['decision'].rsplit('-',1)[-1])
    return Session(outer,pid,os.pidfd_open(pid),guard,terminal,started+300,
                   expected_pool,[tool['filename'] for tool in selection['tools']],
                   [tool['name']+'.json' for tool in selection['tools']],raw)


if __name__=='__main__':
    raise SystemExit('UNEXECUTED_SOURCE_PROPOSAL; exact authority/launcher review and new General admission required')
