"""Bind a bounded local restart exercise to freshly verified archive events.

A persistent checkpoint test is separate from crash durability, source transaction
completeness, and restoration after loss of this VM.
"""
from datetime import datetime,timezone
from collections import defaultdict
from offset_checkpoint import position,archive_coordinate
from audit_core import lsn,image_key,archive_images


class SourcePositions:
    """Bind the archive's missing command_id to unique retained SQL CDC images."""
    def __init__(self,source,policy,direct):
        self.policy,self.direct=policy,direct;self.rows=defaultdict(list)
        for table,rows in source.get('tables',{}).items():
            for row in rows:
                self.rows[image_key(table,row['pk'],row['operation'],row['commit_lsn'],row['change_lsn'])].append(row)

    def for_event(self,event):
        command=None
        if self.direct:
            commands=set()
            for key in archive_images(event,self.policy):
                rows=self.rows[key]
                if len(rows)!=1: raise ValueError('Command-id source identity missing or ambiguous')
                command=rows[0].get('command_id')
                if type(command) is not int or command<0: raise ValueError('Source command id unavailable')
                commands.add(command)
            if len(commands)!=1: raise ValueError('Update images disagree on command id')
            command=next(iter(commands))
        return position({**event['source'],'command_id':command})


class ResumeAudit:
    def __init__(self,bundle,source=None,policy=None):
        self.bundle=bundle;self.positions=set();self.probes={};self.snapshot_count=0
        current=bundle.get('current') if bundle else None
        self.binding=SourcePositions(source or {},policy or {},bool(current and current['checkpoint'].get('command_id') is not None))
        self.binding_errors=0;self.bound_events=set()
        exercise=bundle.get('exercise') if bundle else None
        self.ids=set(exercise.get('probe_ids',{}).values()) if exercise else set()

    def observe(self,event):
        if event['op']=='r': self.snapshot_count+=1;return
        src=event['source'];coordinate=archive_coordinate(src)
        current=(self.bundle.get('current') or {}).get('checkpoint') if self.bundle else None
        exercise=self.bundle.get('exercise') if self.bundle else None
        points=[current]
        if exercise: points.extend(exercise.get(name,{}).get('checkpoint') for name in ['before','after'])
        matches=[point for point in points if point and archive_coordinate(point)==coordinate]
        row=event['before'] if event['op']=='d' else event['after']
        probe=str(row['ProbeID']).lower() if src['schema']+'.'+src['table']=='lab.CdcProbe' else None
        if not matches and probe not in self.ids: return
        try: bound=self.binding.for_event(event)
        except (ValueError,KeyError,TypeError): self.binding_errors+=1;return
        self.bound_events.add((src['schema'],src['table'],event['op'],bound))
        for point in matches:
            if position(point)==bound: self.positions.add(bound)
        if probe in self.ids:
            value=(event['op'],row['Payload'],bound)
            self.probes.setdefault(probe,set()).add(value)

    def report(self,source,policy,now=None):
        result={'contract':'cdc-persistent-resume/v1','verified':False,'purge_authorized':False,
                'scope':'Graceful local container recreation and persistent files; no crash/power-loss, off-VM restore or transaction-fence certification'}
        if not self.bundle or not self.bundle.get('current'):
            return result|{'reason':'current_checkpoint_unavailable'}
        current=self.bundle['current'];exercise=self.bundle.get('exercise')
        result['checkpoint']=current['checkpoint'];result['exercise_sha256']=self.bundle.get('exercise_sha256')
        if not exercise: return result|{'reason':'local_resume_exercise_missing'}
        try:
            if self.binding_errors: raise ValueError('Source command-id binding failed')
            if exercise['contract']!='cdc-local-resume-exercise/v1' or not exercise['passed']:
                raise ValueError('Exercise failed')
            age=((now or datetime.now(timezone.utc))-datetime.fromisoformat(exercise['completed_at'])).total_seconds()
            if not 0<=age<=policy['checkpoint_exercise_max_age_hours']*3600: raise ValueError('Exercise is not recent')
            before,after=exercise['before'],exercise['after']
            modes={item['checkpoint'].get('command_id') is not None for item in [before,after,current]}
            if len(modes)!=1: raise ValueError('Source query mode changed across the exercise')
            for name in ['image_id','config_sha256','state_directory','source_container_id']:
                if not before[name]==after[name]==current[name]: raise ValueError('Environment binding differs')
            if before['container_id']==after['container_id'] or current['container_id']!=after['container_id']:
                raise ValueError('Container recreation binding differs')
            if not before['files']['schema-history.dat']==after['files']['schema-history.dat']==current['files']['schema-history.dat']:
                raise ValueError('Schema history changed since the exercise')
            for item in [before,after,current]:
                if item['checkpoint']['partition']!=current['checkpoint']['partition'] or item['checkpoint']['namespace']!=current['checkpoint']['namespace']:
                    raise ValueError('Offset partition changed')
                if position(item['checkpoint']) not in self.positions: raise ValueError('Checkpoint not in verified archive')
            if not position(before['checkpoint'])<position(after['checkpoint'])<=position(current['checkpoint']):
                raise ValueError('Checkpoint did not advance across recreation')
            lower=lsn(policy['snapshot_commit_lsn']);upper=lsn(source['upper_lsn'])
            commit=position(current['checkpoint'])[0]
            if not lower<commit<=upper or any(c['start_lsn'] is None or lsn(c['start_lsn'])>commit for c in source['capture_instances']):
                raise ValueError('Current resume position is outside observed retained history')
            if not exercise['state_fsynced_while_stopped'] or not exercise['mounts_preserved']:
                raise ValueError('Persistent state evidence missing')
            if not exercise['snapshot_records_before']==exercise['snapshot_records_after']==self.snapshot_count>0:
                raise ValueError('Snapshot archive count changed')
            if set(exercise['probe_ids'])!={'before','while_stopped','after'} or len(self.ids)!=3:
                raise ValueError('Probe lifecycle is incomplete')
            bounds={}
            for label,probe in exercise['probe_ids'].items():
                events=self.probes.get(probe,set())
                if len(events)!=3 or {e[0] for e in events}!={'c','u','d'}:
                    raise ValueError('Probe events missing, conflicting or duplicated at new positions')
                if any(value!=('before' if op=='c' else 'after') for op,value,point in events):
                    raise ValueError('Probe values differ')
                ordered={op:point for op,value,point in events}
                if not ordered['c']<ordered['u']<ordered['d']: raise ValueError('Probe order differs')
                bounds[label]=(ordered['c'],ordered['d'])
            if not (bounds['before'][1]<=position(before['checkpoint'])<bounds['while_stopped'][0]
                    and bounds['while_stopped'][1]<bounds['after'][0]
                    and bounds['after'][1]<=position(after['checkpoint'])):
                raise ValueError('Resume checkpoints do not bound the probe exercise')
        except (ValueError,KeyError,TypeError):
            return result|{'reason':'resume_evidence_failed_revalidation'}
        return result|{'verified':True,'probe_cycles_verified':3,'checkpoint_in_verified_archive':True,
                       'query_mode':'direct' if self.binding.direct else 'function',
                       'source_command_ids_bound_events':len(self.bound_events) if self.binding.direct else 0}
