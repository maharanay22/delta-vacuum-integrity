"""Read-only original-history exposure audit. No Spark, VACUUM, aging writes or regeneration.
Usage: bundled-python -B audit.py --output NEW_DIR [--research ORIGINAL_ROOT]
Replay: bundled-python -B audit.py --output NEW_DIR --replay PREVIOUS_OUTPUT/evidence_snapshot.json
Outputs never overwrite existing directories. UNKNOWN means evidence is absent, not zero.
"""
import argparse, bisect, collections, csv, hashlib, json, os, platform, sys, uuid, zipfile
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import unquote
sys.dont_write_bytecode=True
H=3600000
Z85='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#'
CLASSES=['F1','F3','F4']
IMPLS=['deltars-1.6.6','deltars-main','deltars-main+4858','spark-4.4.1']

def digest(data): return hashlib.sha256(data).hexdigest()
def jsonout(p,obj): p.write_text(json.dumps(obj,indent=2,default=str),encoding='utf-8')
def normalize(p): return unquote(p).replace('\\','/')
def dvfile(d):
    if not d or d['storageType']=='i': return None
    if d['storageType']=='p': return normalize(d['pathOrInlineDv'])
    assert d['storageType']=='u'
    text=d['pathOrInlineDv']; encoded=text[-20:]; raw=b''
    for i in range(0,20,5):
        n=0
        for ch in encoded[i:i+5]: n=n*85+Z85.index(ch)
        raw+=n.to_bytes(4,'big')
    return ((text[:-20]+'/') if text[:-20] else '')+'deletion_vector_'+str(uuid.UUID(bytes=raw))+'.bin'
def identity(a):
    d=a.get('deletionVector')
    return (normalize(a['path']),json.dumps(d,sort_keys=True) if d else '')
def emit(path,rows):
    cols=list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=cols); writer.writeheader()
        writer.writerows({k:json.dumps(v,separators=(',',':')) if isinstance(v,(list,dict)) else 'unknown' if v is None else v for k,v in r.items()} for r in rows)

def gather(research,out,discovery):
    import pyarrow.parquet as pq
    sources={}; snapshot={'histories':{},'rows':[],'sources':sources,'archives':[],'work_checks':[]}
    def track(p,role):
        p=Path(p); data=p.read_bytes(); st=p.stat()
        sources[str(p)]={'role':role,'sha256':digest(data),'size':len(data),'mtime_ns':st.st_mtime_ns,'mtime_utc':datetime.fromtimestamp(st.st_mtime,timezone.utc).isoformat()}
        return data
    for f in list((research/'code'/'harness').glob('*.py'))+list((research/'code'/'experiments').glob('*.py'))+[research/'code'/'experiments'/'run_all.sh',research/'code'/'README.md',research/'code'/'results'/'environment.json',research/'code'/'results'/'runs.csv',research/'code'/'results'/'summary.md']:
        data=track(f,'original_code_or_derived_result')
        target=out/'reference_sources'/f.relative_to(research); target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(data)
    for file in sorted((research/'code'/'results').glob('runs_*.jsonl')):
        data=track(file,'original_matrix_results')
        target=out/'reference_sources'/'code'/'results'/file.name; target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(data)
        for line_no,line in enumerate(data.decode('utf-8').splitlines(),1):
            if line.strip(): snapshot['rows'].append(dict(json.loads(line),source_file=str(file),source_line=line_no))
    for family in ['nockpt','ckpt']:
        for seed in range(100):
            if seed%10==0: print('Reading original history',family,seed,flush=True)
            hist=research/'data'/'histories'/family/f'seed_{seed}'
            m=hist/'meta.json'
            if not m.exists(): snapshot['histories'][f'{family}/{seed}']={'missing':True}; continue
            data=track(m,'original_history_metadata'); meta=json.loads(data)
            table=hist/'table'; commits=[]; checkpoints=[]
            for p in sorted((table/'_delta_log').glob('*.json')):
                if p.stem.isdigit():
                    data=track(p,'original_history_json_commit')
                    commits.append({'version':int(p.stem),'actions':[json.loads(x) for x in data.decode().splitlines() if x]})
            for p in sorted((table/'_delta_log').glob('*.checkpoint.parquet')):
                track(p,'original_history_checkpoint')
                parquet=pq.read_table(p,columns=['add','remove'])
                checkpoints.append({'version':int(p.name[:20]),'path':str(p),'rows':parquet.to_pylist()})
            inventory={}
            for p in sorted(table.rglob('*')):
                if p.is_file():
                    rel=p.relative_to(table).as_posix()
                    if str(p) not in sources: track(p,'original_history_physical_file')
                    inventory[rel]=sources[str(p)]
            snapshot['histories'][f'{family}/{seed}']={'meta':meta,'commits':commits,'checkpoints':checkpoints,'inventory':inventory,'source_dir':str(hist),'meta_sha256':sources[str(m)]['sha256']}
    # Archived datasets are compared by content, not by filename alone. No archive extraction.
    print('Original histories read; checking archive provenance',flush=True)
    if discovery and discovery.exists():
        disc=json.loads(discovery.read_text())
        snapshot['discovery']=disc
        for entry in disc['search']:
            if entry.get('type')!='zip': continue
            path=Path(entry['path']); track(path,'searched_backup_archive')
            with zipfile.ZipFile(path) as z:
                records=[]
                for name in z.namelist():
                    if name.endswith('/') or not (name.endswith('meta.json') or '/_delta_log/' in name or ('runs_' in name and name.endswith('.jsonl')) or name.endswith('/runs.csv')): continue
                    data=z.read(name); h=digest(data)
                    matches=[p for p,s in sources.items() if s['sha256']==h and s['role'] not in ['searched_backup_archive']]
                    records.append({'member':name,'size':len(data),'sha256':h,'matches_original_sources':matches,'zip_timestamp_no_timezone':z.getinfo(name).date_time})
                snapshot['archives'].append({'path':str(path),'sha256':sources[str(path)]['sha256'],'members':records})
    # Retained work directories: inspect original history-prefix JSON and infer the run's now_ms.
    print('Checking retained original work directories',flush=True)
    for r in snapshot['rows']:
        w=research/'data'/'work'/r['impl']/f"{r['family']}_{r['seed']}_{r['mode']}_{r['retention_h']}"
        if not w.exists(): continue
        hist=snapshot['histories'].get(f"{r['family']}/{r['seed']}",{})
        meta=hist.get('meta'); entry={'run_key':[r[k] for k in ['impl','family','seed','mode','retention_h']],'path':str(w),'json_checks':[],'inferred_now_ms':None,'mtime_checks':[]}
        if meta:
            candidates=[]
            for commit in hist['commits']:
                p=w/'_delta_log'/f"{commit['version']:020d}.json"
                if not p.exists(): entry['json_checks'].append({'version':commit['version'],'missing':True}); continue
                actions=[json.loads(x) for x in track(p,'retained_post_vacuum_log').decode().splitlines() if x]
                info=next((a['commitInfo'] for a in actions if 'commitInfo' in a),{})
                if 'timestamp' in info: candidates.append(info['timestamp']+int(meta['offsets_hours'][commit['version']]*H))
                entry['json_checks'].append({'version':commit['version'],'actions':actions})
            if candidates and len(set(candidates))==1: entry['inferred_now_ms']=candidates[0]
            for p in w.rglob('*'):
                if p.is_file() and not p.relative_to(w).as_posix().startswith('_delta_log/'):
                    st=p.stat(); rel=p.relative_to(w).as_posix()
                    entry['mtime_checks'].append({'path':rel,'mtime_ms':int(st.st_mtime*1000),'size':st.st_size})
        snapshot['work_checks'].append(entry)
    jsonout(out/'evidence_snapshot.json',snapshot)
    # Content checks across all consumed sources; size/mtime checked too, hash repeated on changed stat.
    changes=[]
    for name,st in sources.items():
        p=Path(name); after=p.stat()
        if after.st_size!=st['size'] or after.st_mtime_ns!=st['mtime_ns']:
            changes.append({'path':name,'sha256_after':digest(p.read_bytes()),'before':st})
    jsonout(out/'source_integrity.json',{'checked_sources':len(sources),'changed_sources':changes,'note':'All source files opened read-only; stats verified at end, hashes recorded at read time.'})
    assert not changes
    return snapshot

def analyze(s,out):
    histfacts={}; factrows=[]; issues=[]
    for key,h in s['histories'].items():
        if h.get('missing'): continue
        meta=h['meta']; commits=h['commits']; times=[v['commit_ts'] for v in meta['versions']]; offsets=meta['offsets_hours']
        assert len(times)==len(commits)==len(offsets),key
        def mapped(t,times=times,offsets=offsets):
            i=min(bisect.bisect_left(times,int(t)),len(times)-1)
            return -int(offsets[i]*H)-(times[i]-int(t))
        active={}; removes=[]; states=[]; rawtimestamps=[]
        for c in commits:
            for action in c['actions']:
                if 'commitInfo' in action:
                    ci=action['commitInfo']; rawtimestamps.append(ci.get('inCommitTimestamp') or ci.get('timestamp'))
                elif 'add' in action: active[identity(action['add'])]=action['add']
                elif 'remove' in action:
                    a=action['remove']; active.pop(identity(a),None); removes.append(dict(a,_version=c['version']))
            states.append(list(active.values()))
        assert times==rawtimestamps,(key,'commit timestamps disagree')
        physical=set(meta['orig_mtimes_ms'])
        visible={p for p in h['inventory'] if not p.startswith('_delta_log/')}
        mtimes={p:mapped(t) for p,t in meta['orig_mtimes_ms'].items() if not p.startswith('_delta_log/')}
        assert visible==set(mtimes),(key,'physical inventory differs from saved generation inventory')
        original_mtime_diffs=[p for p in visible if abs(int(h['inventory'][p]['mtime_ns']/1e6)-meta['orig_mtimes_ms'][p])>1]
        live_data={normalize(a['path']) for a in active.values()}
        live_dv={dvfile(a.get('deletionVector')) for a in active.values()}-{None}
        all_paths=set().union(*(set(normalize(a['path']) for a in st) | ({dvfile(a.get('deletionVector')) for a in st}-{None}) for st in states))
        assert all_paths<=visible,(key,'referenced physical file missing')
        # Latest checkpoint + later JSON tombstones must not lose relevant removes.
        cp_removes=[]; checkpoint_mismatch=[]
        if h['checkpoints']:
            cp=max(h['checkpoints'],key=lambda x:x['version'])
            cp_removes=[row['remove'] for row in cp['rows'] if row.get('remove')]
            cp_removes+= [a for a in removes if a['_version']>cp['version']]
        else: cp_removes=removes
        facts={}
        for retention in [0,168]:
            cutoff=-retention*H
            tombstones=collections.defaultdict(list)
            for a in removes: tombstones[normalize(a['path'])].append(a)
            exp=lambda a:mapped(a['deletionTimestamp'])<cutoff if a.get('deletionTimestamp') is not None else None
            mixed={p for p,refs in tombstones.items() if any(exp(a) is True for a in refs) and any(exp(a) is False for a in refs)}
            tomb_dvs={dvfile(a.get('deletionVector')) for a in removes if exp(a) is False}-{None}
            inactive_tomb_dvs=tomb_dvs-live_dv
            cp_unexpired={(normalize(a['path']),dvfile(a.get('deletionVector')),a.get('deletionTimestamp')) for a in cp_removes if exp(a) is False}
            log_unexpired={(normalize(a['path']),dvfile(a.get('deletionVector')),a.get('deletionTimestamp')) for a in removes if exp(a) is False}
            cp_missing=log_unexpired-cp_unexpired
            cp_tombstones=collections.defaultdict(list)
            for a in cp_removes: cp_tombstones[normalize(a['path'])].append(a)
            cp_mixed={p for p,refs in cp_tombstones.items() if any(exp(a) is True for a in refs) and any(exp(a) is False for a in refs)}
            cp_tomb_dvs={dvfile(a.get('deletionVector')) for a in cp_removes if exp(a) is False}-{None}
            protected=[v for v in range(len(states)) if v==len(states)-1 or -int(offsets[v+1]*H)>=cutoff]
            needed=set().union(*({normalize(a['path']) for a in states[v]} | ({dvfile(a.get('deletionVector')) for a in states[v]}-{None}) for v in protected))
            sets={'F1':live_dv,'F3':mixed,'F4':inactive_tomb_dvs}
            per={}
            for cls,structural in sets.items():
                eligible={p for p in structural if p in mtimes and mtimes[p]<cutoff}
                # F3 is a tombstone-only data-path failure. An active data path has an
                # independent live-add keep reference (a deletion there is F2, not F3).
                visible=cp_mixed-live_data if cls=='F3' else cp_tomb_dvs-live_dv if cls=='F4' else live_dv
                protected_eligible=eligible & needed & visible
                per[cls]={'structural':sorted(structural),'eligible':sorted(eligible),'protected_eligible':sorted(protected_eligible),'independent_active_keep_paths':sorted(eligible & live_data) if cls=='F3' else [],'checkpoint_visible_paths':sorted(visible)}
                for p in sorted(structural):
                    refs=tombstones.get(p,[]) if cls=='F3' else [a for a in removes if dvfile(a.get('deletionVector'))==p]
                    factrows.append({'family':meta['family'],'seed':meta['seed'],'retention_h':retention,'class':cls,'path':p,'live_in_latest':p in live_data or p in live_dv,'exists_in_original':p in h['inventory'],'aged_mtime_relative_to_run_now_ms':mtimes.get(p),'cutoff_relative_to_run_now_ms':cutoff,'old_enough':p in eligible,'protected_snapshot_reference':p in needed,'class_specific_exposure':p in protected_eligible,'checkpoint_visible_class_candidate':p in visible,'remove_versions':[a['_version'] for a in refs],'remove_ages_hours':[-mapped(a['deletionTimestamp'])/H if a.get('deletionTimestamp') is not None else None for a in refs],'physical_sha256':h['inventory'].get(p,{}).get('sha256')})
            facts[retention]={'classes':per,'needed':needed,'protected':protected,'cp_missing_unexpired':len(cp_missing)}
        histfacts[key]={'facts':facts,'states':states,'all_paths':all_paths,'mtimes':mtimes,'active':active,'mapped':mapped,'meta':meta,'original_mtime_diffs':original_mtime_diffs}
        if original_mtime_diffs: issues.append({'history':key,'issue':'current original mtimes differ from saved metadata; original metadata used','files':original_mtime_diffs})
    rows=s['rows']; keys=[tuple(r[k] for k in ['impl','family','seed','mode','retention_h']) for r in rows]
    work_lookup={tuple(w['run_key']):w for w in s['work_checks']}
    assert len(rows)==3200 and len(set(keys))==3200,'Matrix duplicate or missing rows'
    expected={(i,f,seed,m,t) for i in IMPLS for f in ['nockpt','ckpt'] for seed in range(100) for m in ['full','lite'] for t in [0,168]}
    assert set(keys)==expected
    per_run=[]; joins=[]
    for r in rows:
        hkey=f"{r['family']}/{r['seed']}"; hf=histfacts.get(hkey); run_id=f"{r['impl']}/{r['family']}_{r['seed']}_{r['mode']}_{r['retention_h']}"
        base={k:r[k] for k in ['impl','family','seed','mode','retention_h']}
        base.update(run_id=run_id,source_file=r['source_file'],source_line=r['source_line'],source_sha256=s['sources'][r['source_file']]['sha256'],history_source=s['histories'].get(hkey,{}).get('source_dir'),history_meta_sha256=s['histories'].get(hkey,{}).get('meta_sha256'),vacuum_error=r.get('error',''))
        archived_cutoffs={d['cutoff'] for d in r.get('wrong_detail',{}).values() if 'cutoff' in d}
        assert len(archived_cutoffs)<=1
        work=work_lookup.get(tuple(r[k] for k in ['impl','family','seed','mode','retention_h']))
        inferred_now=work.get('inferred_now_ms') if work else None
        cutoff=next(iter(archived_cutoffs)) if archived_cutoffs else inferred_now-r['retention_h']*H if inferred_now is not None else None
        base.update(historical_cutoff_ms=cutoff,historical_run_now_ms=cutoff+r['retention_h']*H if cutoff is not None else None,absolute_time_source='archived_wrong_detail' if archived_cutoffs else 'retained_aged_commit_plus_saved_offset' if cutoff is not None else 'unknown; relative ages reconstructible')
        if not hf:
            for cls in CLASSES: per_run.append(dict(base,failure_class=cls,structural_present=None,age_eligible=None,exposed=None,selected=None,deleted=None,read_failure_cooccurred=None,unknown_reason='original history missing'))
            continue
        meta=hf['meta']; facts=hf['facts'][r['retention_h']]
        mismatch=[]
        if r.get('n_versions')!=len(meta['versions']): mismatch.append('version_count')
        if r.get('files_before')!=len(hf['mtimes']): mismatch.append('physical_file_count')
        if r.get('n_protected')!=len(facts['protected']): mismatch.append('protected_version_count')
        if r.get('has_checkpoint')!=bool(s['histories'][hkey]['checkpoints']): mismatch.append('checkpoint_flag')
        observed=set(r.get('plan',[])) | set(r.get('deleted',{})) | set(r.get('wrongly_retained',{}))
        if observed-set(hf['mtimes']): mismatch.append('observed_path_not_in_original')
        if set(r.get('plan_unsafe_oracle',[]))!=set(r.get('plan',[])) & facts['needed']: mismatch.append('needed_set_vs_archived_oracle')
        if set(r.get('wrongly_deleted',{}))!=set(r.get('deleted',{})) & facts['needed']: mismatch.append('deleted_needed_set_vs_archived_oracle')
        joins.append({'run_id':run_id,'mismatches':mismatch,'observed_paths_matched':len(observed & set(hf['mtimes'])),'path_evidence_present':bool(observed)})
        for cls in CLASSES:
            fs=facts['classes'][cls]; structural=set(fs['structural']); eligible=set(fs['eligible']); protected=set(fs['protected_eligible'])
            plan_known='checker_flagged' in r and not r.get('error','').startswith('dry_run:')
            del_known='deleted' in r
            selected=protected & set(r.get('plan',[])); deleted=protected & set(r.get('deleted',{}))
            failed_versions=[x['v'] for x in r.get('reads',[]) if x.get('protected') and not x.get('ok')]
            affected_versions=[]; exact_error_paths=[]
            for v in failed_versions:
                refs={normalize(a['path']) for a in hf['states'][v]} | ({dvfile(a.get('deletionVector')) for a in hf['states'][v]}-{None})
                if deleted & refs: affected_versions.append(v)
                err=next(x.get('error','') for x in r['reads'] if x['v']==v)
                exact_error_paths+= [p for p in deleted if p in normalize(err)]
            item=dict(base,failure_class=cls,structural_present=int(bool(structural)),structural_files=len(structural),age_eligible=int(bool(eligible)),age_eligible_files=len(eligible),exposed=int(bool(protected)),exposed_files=len(protected),mode_direct_mechanism_applicable=int(cls=='F3' or r['mode']=='full'),selected=int(bool(selected)) if plan_known else None,selected_files=len(selected) if plan_known else None,deleted=int(bool(deleted)) if del_known else None,deleted_files=len(deleted) if del_known else None,read_failure_cooccurred=int(bool(affected_versions)) if 'reads' in r else None,failed_read_exact_path=int(bool(exact_error_paths)) if 'reads' in r else None,affected_failed_versions=affected_versions,successful_incorrect_protected_reads=[x['v'] for x in r.get('reads',[]) if x.get('protected') and x.get('ok') and not x.get('same')],structural_paths=sorted(structural),eligible_paths=sorted(eligible),selected_paths=sorted(selected) if plan_known else None,deleted_paths=sorted(deleted) if del_known else None,exact_error_paths=sorted(set(exact_error_paths)),history_join_mismatches=mismatch,checkpoint_unexpired_refs_missing=facts['cp_missing_unexpired'],unknown_reason='')
            item.update(mechanism_exposed=int(bool(protected) and (cls=='F3' or r['mode']=='full')),exposed_paths=sorted(protected),independent_active_keep_paths=fs['independent_active_keep_paths'])
            if mismatch:
                for k in ['structural_present','structural_files','age_eligible','age_eligible_files','exposed','exposed_files','mechanism_exposed','selected','selected_files','deleted','deleted_files','read_failure_cooccurred','failed_read_exact_path']: item[k]=None
                item['unknown_reason']='history/outcome join mismatch: '+','.join(mismatch)
            per_run.append(item)
    summary=[]
    # Explicit combined-family and combined-configuration rows prevent seed double counting.
    for impl in IMPLS+['ALL']:
        for mode in ['full','lite','ALL']:
            for retention in [0,168,'ALL']:
                for family in ['nockpt','ckpt','ALL']:
                    for cls in CLASSES:
                        g=[r for r in per_run if (impl=='ALL' or r['impl']==impl) and (mode=='ALL' or r['mode']==mode) and (retention=='ALL' or r['retention_h']==retention) and (family=='ALL' or r['family']==family) and r['failure_class']==cls]
                        item={'impl':impl,'mode':mode,'retention_h':retention,'family':family,'failure_class':cls,'matrix_runs':len(g),'unique_numeric_seeds':len({r['seed'] for r in g}),'unique_family_seed_histories':len({(r['family'],r['seed']) for r in g})}
                        for stage in ['structural_present','age_eligible','exposed','mechanism_exposed','selected','deleted','read_failure_cooccurred','failed_read_exact_path']:
                            known=[r for r in g if r[stage] is not None]; yes=[r for r in known if r[stage]]
                            item.update({stage+'_known_denominator':len(known),stage+'_runs':len(yes),stage+'_unknown_runs':len(g)-len(known),stage+'_unique_numeric_seeds':len({r['seed'] for r in yes}),stage+'_unique_family_seed_histories':len({(r['family'],r['seed']) for r in yes})})
                        summary.append(item)
    seeds=[]
    for impl in IMPLS:
        for mode in ['full','lite']:
            for ret in [0,168]:
                for cls in CLASSES:
                    for seed in range(100):
                        g=[r for r in per_run if (r['impl'],r['mode'],r['retention_h'],r['failure_class'],r['seed'])==(impl,mode,ret,cls,seed)]
                        item={'impl':impl,'mode':mode,'retention_h':ret,'failure_class':cls,'seed':seed,'family_run_denominator':len(g)}
                        for stage in ['structural_present','age_eligible','exposed','mechanism_exposed','selected','deleted','read_failure_cooccurred']:
                            item[stage+'_family_runs']=sum(r[stage] or 0 for r in g); item[stage+'_unknown_family_runs']=sum(r[stage] is None for r in g)
                        item['exposed_families']=[r['family'] for r in g if r['exposed']]
                        seeds.append(item)
    # Confirm retained work transformations without touching file times.
    work_checks=[]
    for w in s['work_checks']:
        impl,fam,seed,mode,ret=w['run_key']; hf=histfacts[f'{fam}/{seed}']; now=w['inferred_now_ms']; mismatches=[]
        if now is None: mismatches.append('inconsistent_or_missing_inferred_clock')
        else:
            original=s['histories'][f'{fam}/{seed}']['commits']
            for chk in w['json_checks']:
                if chk.get('missing'): mismatches.append('missing_original_commit'); continue
                transformed=json.loads(json.dumps(original[chk['version']]['actions']))
                for a in transformed:
                    for kind,fields in {'commitInfo':['timestamp','inCommitTimestamp'],'add':['modificationTime'],'remove':['deletionTimestamp']}.items():
                        for field in fields:
                            if kind in a and a[kind].get(field) is not None: a[kind][field]=now+hf['mapped'](a[kind][field])
                if transformed!=chk['actions']: mismatches.append('aged_commit_content')
            for m in w['mtime_checks']:
                if m['path'] in hf['mtimes'] and abs(m['mtime_ms']-(now+hf['mtimes'][m['path']]))>1: mismatches.append('aged_surviving_file_mtime')
        work_checks.append({'run_key':w['run_key'],'source_dir':w['path'],'inferred_run_now_ms':now,'inferred_cutoff_ms':now-ret*H if now is not None else None,'mismatches':sorted(set(mismatches)),'checked_surviving_physical_files':len(w['mtime_checks'])})
    emit(out/'Pass10_Per_Run_Exposure.csv',per_run); emit(out/'Pass10_Per_Seed_Exposure.csv',seeds); emit(out/'Pass10_Exposure_Summary.csv',summary)
    emit(out/'Pass10_File_Level_Preconditions.csv',factrows)
    jsonout(out/'history_join_checks.json',joins); jsonout(out/'retained_work_checks.json',work_checks); jsonout(out/'issues.json',issues)
    jsonout(out/'analysis_tables.json',{'summary':summary,'per_run':per_run,'per_seed':seeds})
    jsonout(out/'audit_totals.json',{'matrix_rows':len(rows),'class_run_rows':len(per_run),'original_histories':len(histfacts),'original_versions':sum(len(h['states']) for h in histfacts.values()),'checkpoint_files':sum(len(h['checkpoints']) for h in s['histories'].values() if not h.get('missing')),'history_join_mismatch_runs':sum(bool(j['mismatches']) for j in joins),'retained_work_dirs':len(work_checks),'retained_work_mismatch_dirs':sum(bool(w['mismatches']) for w in work_checks),'mtime_issue_histories':len(issues),'overall':[r for r in summary if r['impl']=='ALL' and r['family']=='ALL' and r['mode']=='ALL'],'full_retention_by_impl':[r for r in summary if r['impl']!='ALL' and r['family']=='ALL' and r['mode']=='full']})
    return summary

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--research',type=Path,default=Path.cwd()/'original_research'); p.add_argument('--output',type=Path,required=True); p.add_argument('--replay',type=Path); p.add_argument('--discovery',type=Path,default=Path(__file__).parent/'discovery.json'); a=p.parse_args()
    out=a.output.resolve(); research=a.research.resolve()
    forbidden=[research,(Path.cwd()/'original_validation').resolve()]
    assert all(not out.is_relative_to(x) for x in forbidden),'Output must be outside original research'
    out.mkdir(parents=True,exist_ok=False)
    if a.replay:
        if a.replay.suffix=='.gz':
            import gzip
            snapshot=json.loads(gzip.decompress(a.replay.read_bytes()).decode('utf-8'))
        else: snapshot=json.loads(a.replay.read_text(encoding='utf-8'))
    else: p.error('Publication copy supports offline --replay only; original live acquisition is retained privately')
    analyze(snapshot,out)
    jsonout(out/'analysis_environment.json',{'python':sys.version,'platform':platform.platform(),'utc':datetime.now(timezone.utc).isoformat(),'mode':'read-only replay of archived original logs, not regenerated data','script_sha256':digest(Path(__file__).read_bytes())})
    print(json.dumps(json.loads((out/'audit_totals.json').read_text()),indent=2))
