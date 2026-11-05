"""Unexecuted independent candidate readback; no launcher or acquisition admission."""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import PurePosixPath
import re
import stat
import time


class Refusal(ValueError):
    pass


def require(value, code):
    if not value:raise Refusal(code)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def unique_pairs(pairs):
    result={}
    for key,value in pairs:
        require(key not in result,'DUPLICATE_JSON_KEY')
        result[key]=value
    return result


def parsed(raw):return json.loads(raw,object_pairs_hook=unique_pairs)


def snapshot(custody, cap=4*1024**2):
    """Closed tree, pinned archive identities and stable no-follow regular-file reads."""
    custody.scope()
    rows={}
    with ExitStack() as stack:
        root=custody.root_fd
        st=os.fstat(root)
        require(stat.S_IMODE(st.st_mode)==0o500,'ROOT_NOT_SEALED')
        children=set(os.listdir(root))
        directories={'metadata','wheelhouse','events'}
        require(directories<=children,'MISSING_POOL_DIRECTORY')
        for directory in ('.','metadata','wheelhouse','events'):
            parent=root
            names=children-directories
            if directory!='.':
                parent=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=root)
                stack.callback(os.close,parent)
                st=os.fstat(parent)
                require(st.st_uid==os.geteuid() and stat.S_IMODE(st.st_mode)==0o500,'DIRECTORY_NOT_SEALED')
                names=set(os.listdir(parent))
            require(len(names)<=128,'FILE_COUNT_BOUND')
            for leaf in sorted(names):
                require(isinstance(leaf,str) and leaf not in {'.','..'} and '/' not in leaf
                        and '\\' not in leaf and not any(ord(c)<32 for c in leaf),'UNSAFE_POOL_LEAF')
                name=leaf if directory=='.' else directory+'/'+leaf
                fd=os.open(leaf,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
                with os.fdopen(fd,'rb') as stream:
                    before=os.fstat(stream.fileno())
                    require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid()
                            and before.st_nlink==1 and stat.S_IMODE(before.st_mode)==0o400
                            and 0<=before.st_size<=cap,'POOL_FILE_CUSTODY_CHANGED')
                    raw=stream.read(cap+1)
                    after=os.fstat(stream.fileno())
                    require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)
                            ==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
                            and len(raw)==before.st_size,'POOL_FILE_CHANGED_DURING_READ')
                if directory=='wheelhouse':
                    require(leaf in custody.archive_nodes
                            and (before.st_dev,before.st_ino)==custody.archive_nodes[leaf]['identity'],
                            'ARCHIVE_IDENTITY_CHANGED')
                rows[name]={'bytes':len(raw),'sha256':sha(raw),'device':before.st_dev,
                            'inode':before.st_ino,'mode':'0400','links':1,'raw':raw}
        require(len(rows)<=128 and sum(row['bytes'] for row in rows.values())<=cap,'WHOLE_POOL_BOUND')
    custody.scope()
    return rows


def validate_candidate(custody, candidate, selection_raw, owner_module, owner_source_sha,
                       admission, owner_exited, owner_exit_code, cleanup_ceiling):
    """Caller must establish exact pidfd exit/code from its created child before calling."""
    require(owner_exited is True and type(owner_exit_code) is int and owner_exit_code==0,
            'EXACT_OWNER_SUCCESSFUL_EXIT_REQUIRED')
    require(time.monotonic()<cleanup_ceiling,'TERMINAL_DEADLINE_EXCEEDED')
    require(isinstance(candidate,dict) and candidate.get('outcome')=='candidate'
            and candidate.get('pool')==str(custody.path) and candidate.get('source')==owner_module.SOURCE,
            'OWNER_CANDIDATE_MISMATCH')
    require(sha(selection_raw)==owner_module.SELECTION_SHA,'SELECTION_AUTHORITY_CHANGED')
    selection=parsed(selection_raw)
    tools=selection['tools']
    require(selection.get('source')==owner_module.SOURCE and len(tools)==6
            and len({tool['filename'] for tool in tools})==6
            and sum(tool['bytes'] for tool in tools)==owner_module.ARCHIVE_BYTES,
            'SELECTED_SIX_TOOL_CLOSURE_CHANGED')
    expected={tool['filename']:{'bytes':tool['bytes'],'sha256':tool['sha256']} for tool in tools}
    archive_rows=custody.verify_archive_bytes(expected)
    rows=snapshot(custody)
    require(type(candidate.get('files')) is int and candidate['files']==len(rows)
            and type(candidate.get('whole_pool_bytes')) is int
            and candidate['whole_pool_bytes']==sum(row['bytes'] for row in rows.values())
            and type(candidate.get('archive_bytes')) is int
            and candidate['archive_bytes']==owner_module.ARCHIVE_BYTES,'CANDIDATE_COUNTS_CHANGED')
    fixed={'selection.json','ca.pem','launch.json','wheel-metadata.json',
           'acquisition-observation.json','inventory.json'}
    expected_names=fixed|{'wheelhouse/'+tool['filename'] for tool in tools}|{'metadata/'+tool['name']+'.json' for tool in tools}
    require(fixed<=set(rows),'MISSING_TERMINAL_RECORD')
    require(rows['selection.json']['raw']==selection_raw and rows['ca.pem']['sha256']==owner_module.CA_SHA,
            'SELECTION_OR_TRUST_CHANGED')
    inventory_raw=rows['inventory.json']['raw']
    require(candidate.get('inventory_sha256')==sha(inventory_raw),'CANDIDATE_INVENTORY_CHANGED')
    inventory=parsed(inventory_raw)
    require(inventory.get('source')==owner_module.SOURCE and inventory.get('inventory_excludes_self') is True
            and isinstance(inventory.get('entries'),list),'OWNER_INVENTORY_SHAPE_CHANGED')
    recorded={}
    for row in inventory['entries']:
        name=row.get('path')
        require(isinstance(name,str) and str(PurePosixPath(name))==name
                and not PurePosixPath(name).is_absolute() and '..' not in PurePosixPath(name).parts
                and name not in recorded,'OWNER_INVENTORY_PATH_CHANGED')
        recorded[name]=row
    require(set(recorded)==set(rows)-{'inventory.json'},'OWNER_INVENTORY_INCOMPLETE_OR_EXTRA')
    for name,row in recorded.items():
        require(all(type(row.get(key)) is type(rows[name][key]) and row[key]==rows[name][key]
                    for key in ('bytes','sha256','device','inode','mode','links')),
                'OWNER_INVENTORY_BYTES_OR_IDENTITY_CHANGED')
    launch=parsed(rows['launch.json']['raw'])
    require(launch.get('admission')==admission and launch.get('source')==owner_module.SOURCE
            and launch.get('verified_acquisition_source_sha256')==owner_source_sha
            and launch.get('credentials_proxy_default_trust') is False,'LAUNCH_AUTHORITY_CHANGED')
    observation=parsed(rows['acquisition-observation.json']['raw'])
    require(observation.get('status')=='collected-before-final-custody-check'
            and type(observation.get('retries')) is int and observation['retries']==0
            and observation.get('import_install_container_SDK_build') is False
            and observation.get('archive_bytes')==owner_module.ARCHIVE_BYTES,'OBSERVATION_BOUNDARY_CHANGED')
    requests=observation.get('http_requests')
    require(isinstance(requests,list) and 6<=len(requests)<=12,'REQUEST_LEDGER_COUNT_CHANGED')
    selected_metadata={tool['metadata_source']:tool for tool in tools}
    selected_archives={tool['url']:tool for tool in tools}
    seen_metadata=set();seen_archives=set()
    for number,request in enumerate(requests,1):
        kind,url=request.get('kind'),request.get('url')
        require(type(request.get('number')) is int and request['number']==number
                and type(request.get('attempt')) is int and request['attempt']==1
                and request.get('stage')=='body-complete','REQUEST_LEDGER_PHASE_CHANGED')
        require((number<=6 and kind=='metadata' and url==tools[number-1]['metadata_source'])
                or (number>6 and kind=='archive'),'REQUEST_ORDER_CHANGED')
        if kind=='metadata':
            require(url in selected_metadata and url not in seen_metadata,'METADATA_REQUEST_CHANGED_OR_RETRIED')
            seen_metadata.add(url);tool=selected_metadata[url];body=rows['metadata/'+tool['name']+'.json']['raw']
        else:
            require(kind=='archive' and url in selected_archives and url not in seen_archives,
                    'ARCHIVE_REQUEST_CHANGED_OR_RETRIED')
            seen_archives.add(url);tool=selected_archives[url];body=rows['wheelhouse/'+tool['filename']]['raw']
        require(request.get('bytes_received')==len(body) and request.get('body_sha256')==sha(body),
                'REQUEST_BODY_CUSTODY_CHANGED')
        prefix=f'events/{number:02d}-'
        expected_names|={prefix+'request.json',prefix+'headers.json',prefix+'complete.json'}
        require(all(prefix+suffix in rows for suffix in ('request.json','headers.json','complete.json')),
                'REQUEST_EVENT_RECORD_MISSING')
        before=parsed(rows[prefix+'request.json']['raw'])
        headers=parsed(rows[prefix+'headers.json']['raw'])
        complete=parsed(rows[prefix+'complete.json']['raw'])
        require(before=={'number':number,'kind':kind,'url':url,'attempt':1,'stage':'before-open'}
                and complete==request,'REQUEST_EVENT_PHASE_LINK_CHANGED')
        require(headers.get('number')==number and headers.get('status')==200
                and headers.get('same_selected_url') is True and headers.get('content_encoding')=='identity',
                'HTTP_RESPONSE_SHAPE_CHANGED')
        if kind=='archive':require(headers.get('content_length')==tool['bytes'],'ARCHIVE_RESPONSE_SIZE_CHANGED')
    require(seen_metadata==set(selected_metadata),'SIX_FRESH_METADATA_REQUESTS_REQUIRED')
    require(set(rows)==expected_names,'EXTRA_OR_MISSING_TERMINAL_OUTPUT')
    semantic=[]
    inspector=owner_module.Acquisition.__new__(owner_module.Acquisition)
    for tool in tools:
        authority=parsed(rows['metadata/'+tool['name']+'.json']['raw'])
        owner_module.validate_authority(authority,tool)
        semantic.append(inspector.inspect_wheel(rows['wheelhouse/'+tool['filename']]['raw'],tool,authority))
    metadata=parsed(rows['wheel-metadata.json']['raw'])
    require(metadata.get('environment')=='CPython3.13 Linux amd64; no extras'
            and metadata.get('actual_requires_dist_matches_versioned_authority') is True
            and metadata.get('active_dependency_edges_match_approved_metadata_selection') is True
            and isinstance(metadata.get('tools'),list) and len(metadata['tools'])==6,'METADATA_RESULT_BOUNDARY_CHANGED')
    for actual,record in zip(semantic,metadata['tools']):
        require({key:value for key,value in record.items() if key!='cache_source'}==actual,
                'METADATA_SEMANTIC_READBACK_CHANGED')
    second=snapshot(custody)
    require({name:{key:value for key,value in row.items() if key!='raw'} for name,row in rows.items()}
            =={name:{key:value for key,value in row.items() if key!='raw'} for name,row in second.items()},
            'POOL_CHANGED_DURING_TERMINAL_READBACK')
    require(time.monotonic()<cleanup_ceiling,'TERMINAL_DEADLINE_EXCEEDED')
    return {'status':'validated-candidate-for-outer-readback','files':len(rows),
            'whole_pool_bytes':sum(row['bytes'] for row in rows.values()),'archives':archive_rows,
            'semantic_tools':semantic,'inventory_sha256':sha(inventory_raw),
            'acquisition_accepted':False,'launcher_and_new_general_decision_pending':True}


if __name__=='__main__':
    raise SystemExit('SOURCE_PROPOSAL_ONLY; no launcher or new acquisition admission')
