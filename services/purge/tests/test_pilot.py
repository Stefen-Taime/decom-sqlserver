from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from pilot_core import plan,execution_sql,verify_after,TABLE,CAPTURE


def lsn(n): return f'0x{n:020x}'


def fixture():
    def cycle(identity,start):
        return [{'commit_lsn':lsn(start+i),'change_lsn':lsn(start+i),'command_id':1,
                 'operation':op,'update_mask':'0x03','pk':identity,
                 'row_data':{'ProbeID':identity,'Payload':'before' if op in {2,3} else 'after'}}
                for i,op in [(0,2),(1,3),(1,4),(2,1)]]
    anchor='00000000-0000-0000-0000-000000000002'
    rows=cycle('00000000-0000-0000-0000-000000000001',10)+cycle(anchor,30)
    capture={'capture_instance':CAPTURE,'table_name':TABLE,'object_id':123,'source_object_id':456,
             'start_lsn':lsn(1),'create_date':'2026-10-01'}
    source={'upper_lsn':lsn(33),'tables':{TABLE:rows,'Sales.Orders':[{'unchanged':True}]},
            'captured_columns':{TABLE:['ProbeID','Payload']},'capture_instances':[capture,
                {'capture_instance':'Sales_Orders','table_name':'Sales.Orders','start_lsn':lsn(1)}]}
    metadata={'capture':capture,'effective_lsn':lsn(20),'live_probe_rows':0,
              'cleanup_jobs':[{'enabled':0}],'candidate_count':4,'candidates':deepcopy(rows[:4]),
              'candidate_sql_sha256':'a'*64,'youngest_candidate_age_seconds':901}
    checkpoint={'checkpoint':{'partition':{'server':'wwi','database':'WideWorldImporters'},'commit_lsn':lsn(32)}}
    return source,metadata,checkpoint,anchor


class PilotSafetyTests(unittest.TestCase):
    def test_complete_old_cycle_is_selected_and_fresh_anchor_retained(self):
        result=plan(*fixture())
        self.assertEqual((result['candidate_count'],result['candidate_cycles'],result['retained_rows']),(4,1,4))

    def test_checkpoint_transaction_itself_cannot_be_purged(self):
        for boundary in [32,33,1]:
            args=list(fixture());args[1]['effective_lsn']=lsn(boundary)
            with self.subTest(boundary=boundary),self.assertRaises(ValueError): plan(*args)

    def test_recent_active_or_wrong_scope_blocks(self):
        for mutation in ['age','live','job','table','partition','count','value','command']:
            source,meta,point,anchor=fixture()
            if mutation=='age': meta['youngest_candidate_age_seconds']=899
            elif mutation=='live': meta['live_probe_rows']=1
            elif mutation=='job': meta['cleanup_jobs'][0]['enabled']=1
            elif mutation=='table': meta['capture']['table_name']='Sales.Orders'
            elif mutation=='partition': point['checkpoint']['partition']['server']='elsewhere'
            elif mutation=='count': meta['candidate_count']=201
            elif mutation=='value': meta['candidates'][0]['row_data']['Payload']='other'
            elif mutation=='command': meta['candidates'][0]['command_id']=9
            with self.subTest(mutation=mutation),self.assertRaises(ValueError): plan(source,meta,point,anchor)

    def test_complete_lifecycle_required_even_when_sql_count_agrees(self):
        source,meta,point,anchor=fixture()
        source['tables'][TABLE].pop(3);meta['candidates'].pop();meta['candidate_count']=3
        with self.assertRaisesRegex(ValueError,'complete'): plan(source,meta,point,anchor)

    def test_boundary_cannot_split_probe_lifecycle(self):
        source,meta,point,anchor=fixture();meta['effective_lsn']=lsn(12)
        meta['candidates'].pop();meta['candidate_count']=3
        with self.assertRaisesRegex(ValueError,'splits'): plan(source,meta,point,anchor)

    def test_arbitrary_payload_cannot_be_reclassified_as_technical(self):
        source,meta,point,anchor=fixture()
        source['tables'][TABLE][0]['row_data']['Payload']='business-like'
        meta['candidates']=deepcopy(source['tables'][TABLE][:4])
        with self.assertRaisesRegex(ValueError,'known technical fixture'): plan(source,meta,point,anchor)

    def test_missing_anchor_blocks(self):
        source,meta,point,anchor=fixture();source['tables'][TABLE]=source['tables'][TABLE][:4]
        with self.assertRaises(ValueError): plan(source,meta,point,anchor)

    def test_executor_rejects_business_table_and_unbounded_or_injected_plan(self):
        approved=plan(*fixture())
        for field,value in [('table','Sales.Orders'),('capture_instance','Sales_Orders'),('candidate_count',201),
                            ('candidate_count',True),('candidate_sql_sha256',"x';DROP TABLE x;--")]:
            changed=deepcopy(approved);changed[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError): execution_sql(changed)

    def test_reconciliation_detects_wrong_deletion_and_metadata_change(self):
        before,meta,point,anchor=fixture();approved=plan(before,meta,point,anchor)
        after=deepcopy(before);after['tables'][TABLE]=after['tables'][TABLE][4:]
        after['capture_instances'][0]['start_lsn']=lsn(20)
        self.assertEqual(verify_after(before,after,approved)['exact_planned_images_removed'],4)
        for mutation in ['business','retained','capture','partial','upper']:
            changed=deepcopy(after)
            if mutation=='business': changed['tables']['Sales.Orders']=[]
            elif mutation=='retained': changed['tables'][TABLE].pop()
            elif mutation=='capture': changed['capture_instances'][1]['start_lsn']=lsn(20)
            elif mutation=='partial': changed['tables'][TABLE].append(before['tables'][TABLE][0])
            elif mutation=='upper': changed['upper_lsn']=lsn(34)
            with self.subTest(mutation=mutation),self.assertRaises(ValueError): verify_after(before,changed,approved)


if __name__=='__main__': unittest.main()
