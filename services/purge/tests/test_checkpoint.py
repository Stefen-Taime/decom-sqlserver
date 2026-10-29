import base64
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from offset_checkpoint import decode,Reader,position
from resume_audit import ResumeAudit

FIXTURES=json.loads((Path(__file__).parent/'fixtures/offsets.json').read_text())


class OffsetParserTests(unittest.TestCase):
    def test_real_java_serialization_decodes_exact_coordinates(self):
        result=decode(base64.b64decode(FIXTURES['valid']))
        self.assertEqual(result['namespace'],'debezium-engine')
        self.assertEqual(result['commit_lsn'],'00000001000000200001')
        self.assertEqual(result['event_serial_no'],2)
        self.assertIsNone(result['command_id'])

    def test_all_truncations_fail_instead_of_becoming_an_empty_checkpoint(self):
        data=base64.b64decode(FIXTURES['valid'])
        for end in range(len(data)):
            with self.subTest(end=end),self.assertRaises(ValueError): decode(data[:end])

    def test_arbitrary_objects_null_and_duplicate_keys_are_rejected(self):
        for name in ['object','string','null','duplicate','wrong_partition','snapshot','duplicate_json','extra_field']:
            with self.subTest(name=name),self.assertRaises(ValueError): decode(base64.b64decode(FIXTURES[name]))

    def test_trailing_or_oversized_content_is_rejected(self):
        for data in [base64.b64decode(FIXTURES['valid'])+b'x',b'0'*(1024*1024+1)]:
            with self.assertRaises(ValueError): decode(data)

    def test_byte_array_references_are_data_not_executable_objects(self):
        entries=Reader(base64.b64decode(FIXTURES['shared'])).read()
        self.assertEqual(entries[0][0],entries[0][1])

    def test_direct_mode_orders_command_before_change_lsn(self):
        first=decode(base64.b64decode(FIXTURES['valid']));first['command_id']=1
        second=deepcopy(first);second['command_id']=2;second['change_lsn']='00000001:00000001:0001'
        self.assertGreater(position(second),position(first))


NOW=datetime(2026,10,29,16,tzinfo=timezone.utc)


def fixture():
    def coord(n): return f'00000001:000000{n:02x}:0001'
    def state(n,container):
        return {'container_id':container,'image_id':'image','config_sha256':'config','state_directory':'disk',
            'source_container_id':'source','checkpoint':{'namespace':'engine','partition':{'server':'wwi','database':'WideWorldImporters'},
                'commit_lsn':coord(n),'change_lsn':coord(n),'event_serial_no':1,'command_id':None},
            'files':{'schema-history.dat':{'sha256':'history','bytes':42},'offsets.dat':{'sha256':str(n),'bytes':42}}}
    ids={name:f'00000000-0000-0000-0000-00000000000{i}' for i,name in enumerate(['before','while_stopped','after'])}
    exercise={'contract':'cdc-local-resume-exercise/v1','passed':True,'completed_at':NOW.isoformat(),
              'before':state(3,'old'),'after':state(9,'new'),'probe_ids':ids,
              'state_fsynced_while_stopped':True,'mounts_preserved':True,
              'snapshot_records_before':1,'snapshot_records_after':1}
    bundle={'current':state(9,'new'),'exercise':exercise,'exercise_sha256':'report'}
    events=[{'op':'r'}]
    for i,probe in enumerate(ids.values()):
        for j,op in enumerate(['c','u','d']):
            row={'ProbeID':probe,'Payload':'before' if op=='c' else 'after'};n=i*3+j+1
            events.append({'op':op,'before':row if op in {'u','d'} else None,'after':row,
                'source':{'schema':'lab','table':'CdcProbe','commit_lsn':coord(n),'change_lsn':coord(n),'event_serial_no':1}})
    source={'upper_lsn':coord(9),'capture_instances':[{'start_lsn':coord(0)}]}
    policy={'snapshot_commit_lsn':coord(0),'checkpoint_exercise_max_age_hours':24}
    return bundle,events,source,policy


def check(bundle,events,source,policy):
    audit=ResumeAudit(bundle,source,policy)
    for event in events: audit.observe(event)
    return audit.report(source,policy,NOW)


class ResumeEvidenceTests(unittest.TestCase):
    def test_recreated_connector_and_three_complete_probes_verify(self):
        report=check(*fixture())
        self.assertTrue(report['verified']);self.assertFalse(report['purge_authorized'])

    def test_missing_checkpoint_or_exercise_is_unknown(self):
        bundle,events,source,policy=fixture()
        for missing in [{},{'current':bundle['current'],'exercise':None}]:
            self.assertFalse(check(missing,events,source,policy)['verified'])

    def test_missing_probe_or_new_snapshot_fails(self):
        bundle,events,source,policy=fixture()
        for changed in [events[:-1],events+[{'op':'r'}]]:
            self.assertFalse(check(bundle,changed,source,policy)['verified'])

    def test_exact_archive_redelivery_is_idempotent(self):
        bundle,events,source,policy=fixture()
        self.assertTrue(check(bundle,events+events[1:],source,policy)['verified'])

    def test_stale_changed_or_unsupported_checkpoint_fails(self):
        for mutation in ['stale','image','config','history','source','container','direct','unknown_position','no_fsync']:
            bundle,events,source,policy=fixture()
            if mutation=='stale': bundle['exercise']['completed_at']=(NOW-timedelta(hours=25)).isoformat()
            elif mutation=='history': bundle['current']['files']['schema-history.dat']['sha256']='changed'
            elif mutation=='direct': bundle['current']['checkpoint']['command_id']=1
            elif mutation=='unknown_position': bundle['current']['checkpoint']['event_serial_no']=99
            elif mutation=='no_fsync': bundle['exercise']['state_fsynced_while_stopped']=False
            else: bundle['current'][{'image':'image_id','config':'config_sha256','source':'source_container_id','container':'container_id'}[mutation]]='changed'
            with self.subTest(mutation=mutation): self.assertFalse(check(bundle,events,source,policy)['verified'])

    def test_source_retention_or_upper_bound_cannot_be_ignored(self):
        for mutation in ['retention','upper']:
            bundle,events,source,policy=fixture()
            if mutation=='retention': source['capture_instances'][0]['start_lsn']='00000001:00000010:0001'
            else: source['upper_lsn']='00000001:00000002:0001'
            with self.subTest(mutation=mutation): self.assertFalse(check(bundle,events,source,policy)['verified'])

    def test_wrong_probe_values_fail(self):
        bundle,events,source,policy=fixture();events[-1]['before']['Payload']='wrong'
        self.assertFalse(check(bundle,events,source,policy)['verified'])


def direct_fixture():
    bundle,events,source,policy=fixture()
    for item in [bundle['exercise']['before'],bundle['exercise']['after'],bundle['current']]:
        item['checkpoint']['command_id']=3
    table='lab.CdcProbe';policy['tables']={table:{'primary_key':'ProbeID'}}
    rows=[]
    for event in events[1:]:
        op=event['op'];row=event['after'];metadata=event['source']
        for operation in {'c':[2],'u':[3,4],'d':[1]}[op]:
            rows.append({'pk':row['ProbeID'],'operation':operation,'commit_lsn':metadata['commit_lsn'],
                'change_lsn':metadata['change_lsn'],'command_id':{'c':1,'u':2,'d':3}[op]})
    source['tables']={table:rows}
    return bundle,events,source,policy


class DirectResumeTests(unittest.TestCase):
    def test_binds_every_probe_and_checkpoint_to_source_commands(self):
        result=check(*direct_fixture())
        self.assertTrue(result['verified']);self.assertEqual(result['query_mode'],'direct')
        self.assertEqual(result['source_command_ids_bound_events'],9)

    def test_wrong_checkpoint_command_does_not_match_same_lsn_and_serial(self):
        bundle,events,source,policy=direct_fixture();bundle['current']['checkpoint']['command_id']=4
        self.assertFalse(check(bundle,events,source,policy)['verified'])

    def test_missing_ambiguous_or_differing_update_command_blocks(self):
        for change in ['missing','duplicate','update']:
            bundle,events,source,policy=direct_fixture();rows=source['tables']['lab.CdcProbe']
            if change=='missing': rows.pop()
            elif change=='duplicate': rows.append(deepcopy(rows[-1]))
            else: rows[2]['command_id']=9
            with self.subTest(change=change): self.assertFalse(check(bundle,events,source,policy)['verified'])

    def test_query_mode_change_cannot_be_certified(self):
        bundle,events,source,policy=direct_fixture();bundle['exercise']['before']['checkpoint']['command_id']=None
        self.assertFalse(check(bundle,events,source,policy)['verified'])


if __name__=='__main__': unittest.main()
