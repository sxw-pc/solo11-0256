import json,tempfile,unittest
from pathlib import Path
import layercourt
from layercourt.store import connect,run,replay,one
ROOT=Path(__file__).resolve().parents[1]
class Business(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'db.sqlite';self.db=connect(str(self.path))
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def steps(self):return json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
 def test_shared_layer_is_protected_until_both_claims_release(self):
  out=[run(self.db,x['command'],x['input']) for x in self.steps()]
  self.assertEqual(out[3]['decision'],'layer_shared');self.assertEqual(out[7]['decision'],'layer_protected_or_missing')
  self.assertEqual(out[9]['decision'],'release_unproven');self.assertEqual(out[11]['decision'],'planned')
  self.assertEqual(out[12]['decision'],'delete_failed');self.assertEqual(out[13]['decision'],'space_reclaimed')
  self.assertTrue(replay(str(self.path))['verified'])
 def test_new_reference_blocks_planned_delete(self):
  for x in self.steps()[:5]:run(self.db,x['command'],x['input'])
  plan={'event_id':'p','op':'eviction-plan','id':'p','node':'node-a','layer':'layer-1','needed':50,'at':5}
  self.assertEqual(run(self.db,'eviction',plan)['decision'],'planned')
  run(self.db,'retention',self.steps()[5]['input'])
  self.assertEqual(run(self.db,'eviction',{'event_id':'del','op':'delete-reconcile','id':'del','plan':'p','layer':'layer-1','at':6,'success':True})['decision'],'new_reference_blocks_delete')
 def test_conflicting_digest_size_rejected(self):
  for x in self.steps()[:4]:run(self.db,x['command'],x['input'])
  wrong=dict(self.steps()[3]['input'],event_id='wrong',id='layer-3',size=101)
  self.assertEqual(run(self.db,'manifest',wrong)['decision'],'digest_size_conflict')
 def test_revoked_manifest_keeps_existing_offline_lease_until_expiry(self):
  p=self.steps()
  for x in p[:7]:run(self.db,x['command'],x['input'])
  run(self.db,'manifest',p[8]['input'])
  run(self.db,'retention',p[10]['input'])
  plan=dict(p[11]['input'],event_id='early',id='early',at=15)
  self.assertEqual(run(self.db,'eviction',plan)['decision'],'layer_protected_or_missing')
  self.assertEqual(one(self.db,'SELECT status FROM retention_leases WHERE id=?',('lease-1',))['status'],'frozen')
  self.assertEqual(one(self.db,'SELECT termination_receipt FROM lease_claims WHERE id=?',('release-2',))['termination_receipt'],'term')
 def test_old_pod_uid_cannot_release_new_generation_reference(self):
  p=self.steps()
  for x in p[:6]:run(self.db,x['command'],x['input'])
  new=dict(p[5]['input'],event_id='new-pod',id='ref-2',uid='uid-2',generation=2)
  self.assertEqual(run(self.db,'retention',new)['decision'],'reference_active')
  old=dict(p[10]['input'],event_id='old-release',id='old-release',ref='ref-2')
  self.assertEqual(run(self.db,'retention',old)['decision'],'release_unproven')
  self.assertEqual(one(self.db,'SELECT status FROM workload_refs WHERE id=?',('ref-2',))['status'],'active')
 def test_layer_too_small_cannot_satisfy_capacity_shortfall(self):
  p=self.steps()
  for x in p[:5]:run(self.db,x['command'],x['input'])
  plan=dict(p[11]['input'],event_id='large',id='large',needed=101)
  self.assertEqual(run(self.db,'eviction',plan)['decision'],'layer_protected_or_missing')
 def test_failed_delete_preserves_cache_and_plan(self):
  p=self.steps()
  for x in p[:13]:run(self.db,x['command'],x['input'])
  self.assertEqual(one(self.db,'SELECT status FROM node_caches WHERE node=? AND layer=?',('node-a','layer-1'))['status'],'present')
  self.assertEqual(one(self.db,'SELECT status FROM eviction_plans WHERE id=?',('plan-2',))['status'],'planned')
 def test_unapproved_offline_retention_is_denied(self):
  p=self.steps()
  for x in p[:4]:run(self.db,x['command'],x['input'])
  lease=dict(p[6]['input'],event_id='unsigned',id='unsigned',approver='')
  self.assertEqual(run(self.db,'retention',lease)['decision'],'lease_denied')
  self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM retention_leases')['n'],0)
 def test_new_pod_generation_does_not_remove_old_unique_layer_protection(self):
  p=self.steps()
  for x in p[:6]:run(self.db,x['command'],x['input'])
  unique={'event_id':'unique','op':'layer-deduplicate','id':'unique-layer','manifest':'manifest-1','arch':'amd64','digest':'sha:unique','size':60}
  run(self.db,'manifest',unique)
  run(self.db,'eviction',{'event_id':'cache-unique','op':'cache-record','node':'node-a','layer':'unique-layer'})
  new=dict(p[5]['input'],event_id='new-pod',id='ref-2',uid='uid-2',manifest='manifest-2',generation=2)
  run(self.db,'retention',new)
  self.assertEqual(one(self.db,'SELECT status FROM workload_refs WHERE id=?',('ref-1',))['status'],'active')
  self.assertEqual(run(self.db,'eviction',{'event_id':'evict-unique','op':'eviction-plan','id':'evict-unique','node':'node-a','layer':'unique-layer','needed':50,'at':12})['decision'],'layer_protected_or_missing')
 def test_batch_capacity_counts_only_successful_distinct_layer_deletes(self):
  p=self.steps()
  for x in p[:5]:run(self.db,x['command'],x['input'])
  run(self.db,'manifest',{'event_id':'extra','op':'layer-deduplicate','id':'layer-extra','manifest':'manifest-1','arch':'amd64','digest':'sha:extra','size':60})
  run(self.db,'eviction',{'event_id':'cache-extra','op':'cache-record','node':'node-a','layer':'layer-extra'})
  plan=run(self.db,'eviction',{'event_id':'batch','op':'eviction-batch-plan','id':'batch-1','node':'node-a','needed':150,'at':5})
  self.assertEqual(plan['decision'],'batch_planned');self.assertEqual(set(plan['layers']),{'layer-1','layer-extra'})
  first=run(self.db,'eviction',{'event_id':'del-first','op':'delete-reconcile','id':'del-first','plan':'batch-1','layer':'layer-1','at':6,'success':True})
  self.assertEqual(first,{'decision':'partial_progress','bytes':100})
  second=run(self.db,'eviction',{'event_id':'del-second','op':'delete-reconcile','id':'del-second','plan':'batch-1','layer':'layer-extra','at':6,'success':True})
  self.assertEqual(second,{'decision':'space_reclaimed','bytes':160})
  self.assertEqual(one(self.db,'SELECT status FROM batch_plans WHERE id=?',('batch-1',))['status'],'complete')
 def test_offline_lease_requires_full_node_cache_closure(self):
  p=self.steps()
  for x in p[:4]:run(self.db,x['command'],x['input'])
  lease=dict(p[6]['input'],event_id='early-lease',id='early-lease')
  self.assertEqual(run(self.db,'retention',lease)['decision'],'offline_closure_missing')
  run(self.db,'eviction',p[4]['input'])
  self.assertEqual(run(self.db,'retention',dict(lease,event_id='later-lease',id='later-lease'))['decision'],'lease_active')
 def test_committed_manifest_cannot_grow_layer_closure(self):
  p=self.steps()
  for x in p[:6]:run(self.db,x['command'],x['input'])
  extra={'event_id':'late-layer','op':'layer-deduplicate','id':'late-layer','manifest':'manifest-1','arch':'amd64','digest':'sha:late','size':20}
  self.assertEqual(run(self.db,'manifest',extra)['decision'],'manifest_closure_committed')
 def test_parallel_delete_plans_cannot_double_credit_same_layer(self):
  p=self.steps()
  for x in p[:5]:run(self.db,x['command'],x['input'])
  for plan_id in ('a','b'):
   self.assertEqual(run(self.db,'eviction',{'event_id':'plan-'+plan_id,'op':'eviction-batch-plan','id':plan_id,'node':'node-a','needed':50,'at':5})['decision'],'batch_planned')
  self.assertEqual(run(self.db,'eviction',{'event_id':'delete-a','op':'delete-reconcile','id':'delete-a','plan':'a','layer':'layer-1','at':6,'success':True})['decision'],'space_reclaimed')
  self.assertEqual(run(self.db,'eviction',{'event_id':'delete-b','op':'delete-reconcile','id':'delete-b','plan':'b','layer':'layer-1','at':6,'success':True})['decision'],'cache_already_absent')
  self.assertEqual(one(self.db,'SELECT status FROM batch_plans WHERE id=?',('b',))['status'],'planned')
 def test_fixtures_and_import(self):
  for c in ('manifest','retention','eviction'):
   for suffix in ('normal','failure'):
    for x in json.loads((ROOT/'fixtures'/f'{c}_{suffix}.json').read_text(encoding='utf-8')):
     run(self.db,c,{**x,'event_id':f'{c}:{suffix}:{x["event_id"]}'})
  self.assertTrue(replay(str(self.path))['verified']);self.assertEqual(Path(layercourt.__file__).resolve().parents[1],ROOT)
print('module_file',layercourt.__file__)
