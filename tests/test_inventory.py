import tempfile,unittest
from pathlib import Path
from layercourt.store import connect,run,replay,one,rows
def manifest(db,eid,mid,layers):
 run(db,'manifest',{'event_id':eid+'-m','op':'manifest-register','id':mid,'arch':'amd64','digest':'sha:'+mid})
 for lid,size in layers:
  run(db,'manifest',{'event_id':eid+'-l-'+lid,'op':'layer-deduplicate','id':lid,'manifest':mid,'arch':'amd64','digest':'sha:'+lid,'size':size})
def frag(fid,generation,d,cut,layers):return {'id':fid,'generation':generation,'dir':d,'cut':cut,'layers':layers}
def move(mid,generation,layer,src,dst,src_cut,dst_cut,arrived):
 return {'id':mid,'generation':generation,'layer':layer,'src':src,'dst':dst,'src_cut':src_cut,'dst_cut':dst_cut,'arrived':arrived}
class Inventory(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'db.sqlite';self.db=connect(str(self.path))
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def gen(self,node,generation,eid,at=None):
  return run(self.db,'inventory',{'event_id':eid,'op':'storage-generation','node':node,'generation':generation,'at':at})
 def test_disk_swap_acceptance_sequence(self):
  db=self.db
  manifest(db,'acc','m-solo',[('L',100)])
  manifest(db,'acc2','m-pair',[('L',100),('L2',40)])
  self.assertEqual(self.gen('n1',1,'g-open',at=1)['decision'],'generation_opened')
  # Fixed image requested while maintenance freeze is unavailable: only an
  # empty A fragment exists, so the frozen contract must wait.
  wait=run(db,'inventory',{'event_id':'freeze-wait','op':'inventory-attest','id':'att-wait','node':'n1','generation':1,'mode':'frozen','scope':['L'],'fragments':[frag('a1',1,'A',10,[])]})
  self.assertEqual(wait['decision'],'frozen_image_unavailable');self.assertFalse(wait['complete']);self.assertEqual(wait['pending'],['L'])
  self.assertEqual(one(db,'SELECT status FROM layer_residency WHERE node=? AND layer=?',('n1','L'))['status'],'unresolved')
  # Online inventory: A scanned empty, full move receipt B->A, then B empty.
  online=run(db,'inventory',{'event_id':'online','op':'inventory-attest','id':'att-online','node':'n1','generation':1,'mode':'live','scope':['L'],
   'fragments':[frag('a2',1,'A',20,[]),frag('b2',1,'B',23,[])],
   'moves':[move('mv',1,'L','B','A',21,22,True)]})
  self.assertEqual(online['decision'],'online_attested');self.assertFalse(online['complete'])
  self.assertEqual(online['present'],['L']);self.assertEqual(online['findings'][0]['reason'],'move_boundary_closed')
  self.assertEqual(one(db,'SELECT status FROM layer_residency WHERE node=? AND layer=?',('n1','L'))['status'],'present')
  # The signed-off layer backs a new offline lease even though no fragment saw it.
  lease=run(db,'retention',{'event_id':'lease-ok','op':'offline-retain','id':'lease-ok','manifest':'m-solo','node':'n1','start':10,'expires':20,'approver':'owner-a'})
  self.assertEqual(lease['decision'],'lease_active')
  # Generation 2: departure-only receipt cannot prove L still resident.
  self.assertEqual(self.gen('n1',2,'g-roll',at=25)['decision'],'generation_advanced')
  half=run(db,'inventory',{'event_id':'half-move','op':'inventory-attest','id':'att-half','node':'n1','generation':2,'mode':'live','scope':['L'],
   'fragments':[frag('a3',2,'A',30,[]),frag('b3',2,'B',33,[])],
   'moves':[move('mv2',2,'L','B','A',31,None,False)]})
  self.assertEqual(half['decision'],'online_attested');self.assertEqual(half['present'],[]);self.assertEqual(half['pending'],['L'])
  denied=run(db,'retention',{'event_id':'lease-denied','op':'offline-retain','id':'lease-denied','manifest':'m-solo','node':'n1','start':30,'expires':40,'approver':'owner-a'})
  self.assertEqual(denied['decision'],'offline_closure_missing')
  # An old-generation complete surface report is rejected outright.
  stale=run(db,'inventory',{'event_id':'old-full','op':'inventory-attest','id':'att-old','node':'n1','generation':1,'mode':'frozen','scope':['L'],
   'fragments':[frag('oa',1,'A',5,[]),frag('ob',1,'B',5,[])]})
  self.assertEqual(stale['decision'],'stale_generation');self.assertEqual(stale['current'],2)
  self.assertEqual(one(db,'SELECT generation,status FROM layer_residency WHERE node=? AND layer=?',('n1','L'))['generation'],2)
  # Complete same-cut-point scan of the current generation proves the absence.
  full=run(db,'inventory',{'event_id':'full-gen2','op':'inventory-attest','id':'att-full','node':'n1','generation':2,'mode':'frozen','scope':['L'],
   'fragments':[frag('fa',2,'A',40,[]),frag('fb',2,'B',40,[])]})
  self.assertEqual(full['decision'],'surface_attested');self.assertTrue(full['complete']);self.assertEqual(full['absent'],['L'])
  again=run(db,'retention',{'event_id':'lease-denied2','op':'offline-retain','id':'lease-denied2','manifest':'m-solo','node':'n1','start':41,'expires':50,'approver':'owner-a'})
  self.assertEqual(again['decision'],'offline_closure_missing')
  # The earlier approved commitment is retained and reported as doubtful.
  doubts=run(db,'inventory',{'event_id':'doubts','op':'inventory-doubts','node':'n1'})
  self.assertEqual(doubts['decision'],'doubts_reported')
  self.assertEqual(doubts['doubtful'],[{'lease':'lease-ok','manifest':'m-solo','missing':['L']}])
  self.assertEqual(one(db,'SELECT status FROM retention_leases WHERE id=?',('lease-ok',))['status'],'active')
  self.assertTrue(replay(str(self.path))['verified'])
 def test_expired_scan_cannot_overwrite_later_cache_record(self):
  db=self.db
  manifest(db,'race','m-race',[('L1',100)])
  self.gen('n2',1,'g2',at=1)
  first=run(db,'inventory',{'event_id':'old-scan','op':'inventory-attest','id':'old-scan','node':'n2','generation':1,'mode':'frozen','scope':['L1'],
   'fragments':[frag('fa',1,'A',10,[]),frag('fb',1,'B',10,[])]})
  self.assertEqual(first['absent'],['L1'])
  # A later legitimate cache-record re-establishes residency.
  self.assertEqual(run(db,'eviction',{'event_id':'cache-late','op':'cache-record','node':'n2','layer':'L1','at':20})['decision'],'cache_present')
  self.assertEqual(run(db,'retention',{'event_id':'lease-race','op':'offline-retain','id':'lease-race','manifest':'m-race','node':'n2','start':21,'expires':30,'approver':'owner-a'})['decision'],'lease_active')
  # Re-submitting the stale cut point cannot negate the cache record.
  stale=run(db,'inventory',{'event_id':'old-scan-again','op':'inventory-attest','id':'old-scan-2','node':'n2','generation':1,'mode':'frozen','scope':['L1'],
   'fragments':[frag('fa2',1,'A',15,[]),frag('fb2',1,'B',15,[])]})
  self.assertEqual(stale['pending'],['L1']);self.assertEqual(stale['findings'][0]['reason'],'scan_superseded_by_cache_record')
  self.assertEqual(run(db,'retention',{'event_id':'lease-race2','op':'offline-retain','id':'lease-race2','manifest':'m-race','node':'n2','start':31,'expires':40,'approver':'owner-a'})['decision'],'lease_active')
  self.assertTrue(replay(str(self.path))['verified'])
 def test_live_inventory_never_claims_whole_surface_and_frozen_forbids_writes(self):
  db=self.db
  manifest(db,'live','m-live',[('L1',100),('L2',50)])
  self.gen('n3',1,'g3',at=1)
  violation=run(db,'inventory',{'event_id':'bad-freeze','op':'inventory-attest','id':'bad-freeze','node':'n3','generation':1,'mode':'frozen','scope':['L1'],
   'fragments':[frag('a',1,'A',2,['L1']),frag('b',1,'B',2,[])],
   'moves':[move('m',1,'L1','B','A',1,2,True)]})
  self.assertEqual(violation['decision'],'frozen_contract_violated')
  self.assertEqual(one(db,'SELECT COUNT(*) n FROM inventory_attestations')['n'],0)
  partial=run(db,'inventory',{'event_id':'partial','op':'inventory-attest','id':'partial','node':'n3','generation':1,'mode':'live','scope':['L1','L2'],
   'fragments':[frag('a2',1,'A',3,['L1'])]})
  self.assertEqual(partial['decision'],'online_attested');self.assertFalse(partial['complete'])
  self.assertEqual(partial['present'],['L1']);self.assertEqual(partial['pending'],['L2'])
  self.assertEqual(run(db,'retention',{'event_id':'no-closure','op':'offline-retain','id':'no-closure','manifest':'m-live','node':'n3','start':4,'expires':9,'approver':'owner-a'})['decision'],'offline_closure_missing')
 def test_broken_move_boundary_keeps_layer_pending(self):
  db=self.db
  manifest(db,'brk','m-brk',[('L',100)])
  self.gen('n4',1,'g4',at=1)
  # Source fragment sampled after the departure cut cannot witness the move origin.
  broken=run(db,'inventory',{'event_id':'broken','op':'inventory-attest','id':'broken','node':'n4','generation':1,'mode':'live','scope':['L'],
   'fragments':[frag('a',1,'A',25,[]),frag('b',1,'B',20,[])],
   'moves':[move('m',1,'L','B','A',21,22,True)]})
  self.assertEqual(broken['pending'],['L']);self.assertEqual(broken['findings'][0]['reason'],'move_boundary_broken');self.assertEqual(broken['findings'][0]['doubt'],1)
  # A fresh fragment on the destination side after the arrival cut also breaks the boundary.
  broken2=run(db,'inventory',{'event_id':'broken2','op':'inventory-attest','id':'broken2','node':'n4','generation':1,'mode':'live','scope':['L'],
   'fragments':[frag('a2',1,'A',30,[]),frag('b2',1,'B',20,[])],
   'moves':[move('m2',1,'L','B','A',21,22,True)]})
  self.assertEqual(broken2['pending'],['L']);self.assertEqual(broken2['findings'][0]['reason'],'move_boundary_broken')
 def test_contracts_generations_and_limits_are_validated(self):
  db=self.db
  manifest(db,'val','m-val',[('L1',10)])
  no_gen=run(db,'inventory',{'event_id':'no-gen','op':'inventory-attest','id':'no-gen','node':'n5','generation':1,'mode':'live','scope':['L1'],'fragments':[]})
  self.assertEqual(no_gen['decision'],'generation_undeclared')
  self.gen('n5',1,'g5',at=1)
  self.assertEqual(run(db,'inventory',{'event_id':'frag-mismatch','op':'inventory-attest','id':'frag-mismatch','node':'n5','generation':1,'mode':'live','scope':['L1'],
   'fragments':[frag('old',2,'A',1,[])]})['decision'],'fragment_generation_mismatch')
  self.assertEqual(run(db,'inventory',{'event_id':'too-big','op':'inventory-attest','id':'too-big','node':'n5','generation':1,'mode':'live','scope':['L%d'%i for i in range(7)],
   'fragments':[]})['decision'],'scope_exceeds_limit')
  self.assertEqual(run(db,'inventory',{'event_id':'two-moves','op':'inventory-attest','id':'two-moves','node':'n5','generation':1,'mode':'live','scope':['L1'],
   'fragments':[],'moves':[move('m1',1,'L1','B','A',1,2,True),move('m2',1,'L1','A','B',2,3,True)]})['decision'],'too_many_moves')
  self.assertEqual(run(db,'inventory',{'event_id':'bad-rcpt','op':'inventory-attest','id':'bad-rcpt','node':'n5','generation':1,'mode':'live','scope':['L1'],
   'fragments':[],'moves':[{'id':'x','generation':1,'layer':'L1','src':'B','dst':'B','src_cut':1}]})['decision'],'move_receipt_invalid')
  self.assertEqual(run(db,'inventory',{'event_id':'no-op','op':'inventory-nope','node':'n5'})['decision'],'unknown_operation')
  # Replaying an identical event id returns the stored decision.
  first=self.gen('n5',1,'dup-gen',at=2)
  second=run(db,'inventory',{'event_id':'dup-gen','op':'storage-generation','node':'n5','generation':1,'at':2})
  self.assertEqual(first,second)
 def test_generation_rollover_ignores_old_attestations_and_deletes(self):
  db=self.db
  manifest(db,'roll','m-roll',[('L1',100),('L2',50)])
  self.gen('n6',1,'g6a',at=1)
  run(db,'inventory',{'event_id':'g1-full','op':'inventory-attest','id':'g1-full','node':'n6','generation':1,'mode':'frozen','scope':['L1','L2'],
   'fragments':[frag('a',1,'A',2,['L1']),frag('b',1,'B',2,['L2'])]})
  self.gen('n6',2,'g6b',at=3)
  # Nothing from generation 1 is readable in generation 2.
  self.assertEqual(run(db,'retention',{'event_id':'roll-denied','op':'offline-retain','id':'roll-denied','manifest':'m-roll','node':'n6','start':4,'expires':8,'approver':'owner-a'})['decision'],'offline_closure_missing')
  # A fresh cache-record in the new generation proves exactly that layer again.
  run(db,'eviction',{'event_id':'cache-g2','op':'cache-record','node':'n6','layer':'L1','at':5})
  partial=run(db,'retention',{'event_id':'roll-partial','op':'offline-retain','id':'roll-partial','manifest':'m-roll','node':'n6','start':6,'expires':8,'approver':'owner-a'})
  self.assertEqual(partial['decision'],'offline_closure_missing');self.assertEqual(partial['present'],1)
  # A delete-reconcile removing L1 withdraws its current-generation proof too.
  run(db,'eviction',{'event_id':'plan','op':'eviction-plan','id':'plan','node':'n6','layer':'L1','needed':10,'at':7})
  run(db,'eviction',{'event_id':'del','op':'delete-reconcile','id':'del','plan':'plan','layer':'L1','at':7,'success':True})
  self.assertEqual(run(db,'retention',{'event_id':'roll-none','op':'offline-retain','id':'roll-none','manifest':'m-roll','node':'n6','start':8,'expires':9,'approver':'owner-a'})['decision'],'offline_closure_missing')
  self.assertTrue(replay(str(self.path))['verified'])
if __name__=='__main__':unittest.main()
