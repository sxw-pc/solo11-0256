import tempfile,unittest
from pathlib import Path
from layercourt.store import connect,run,replay,one,rows
class RecoveryBatch(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'db.sqlite';self.db=connect(str(self.path))
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def manifest(self,m):
  run(self.db,'manifest',{'event_id':'M:'+m,'op':'manifest-register','id':m,'arch':'amd64','digest':'sha:'+m})
 def layer(self,layer,manifest,size):
  return run(self.db,'manifest',{'event_id':'L:'+layer+':'+manifest,'op':'layer-deduplicate','id':layer,'manifest':manifest,'arch':'amd64','digest':'sha:'+layer,'size':size})
 def cache(self,layer,node='node-1'):
  run(self.db,'eviction',{'event_id':'C:'+layer,'op':'cache-record','node':node,'layer':layer})
 def board(self):
  # 题目盘面：五层 A/B/C/D/E 为 8/6/5/4/3 字节，发布甲引用 A/C/D/E，发布乙引用 B。
  self.manifest('rel-jia');self.manifest('rel-yi')
  for l,s in (('A',8),('C',5),('D',4),('E',3)):self.layer(l,'rel-jia',s)
  self.layer('B','rel-yi',6)
  for l in 'ABCDE':self.cache(l)
 def plan(self,pid,needed,prewarm,channels,at=1,node='node-1'):
  return run(self.db,'eviction',{'event_id':'P:'+pid,'op':'eviction-batch-plan','id':pid,'node':node,'needed':needed,'at':at,'prewarm':prewarm,'channels':channels})
 def assert_receipt(self,out):
  # 回执可按层与通道核验：每层恰好一条通道，通道未透支，used 与分层明细一致。
  layers=[b['layer'] for b in out['backfill']]
  self.assertEqual(len(layers),len(set(layers)))
  for ch in out['channels']:
   detail=[b for b in out['backfill'] if b['channel']==ch['channel']]
   self.assertLessEqual(ch['used'],ch['cap'])
   self.assertEqual(ch['used'],sum(b['size'] for b in detail))
   self.assertEqual(sorted(ch['layers']),sorted(b['layer'] for b in detail))
  self.assertEqual(sum(ch['used'] for ch in out['channels']),sum(b['size'] for b in out['backfill']))
 def test_recovery_prefers_fewer_affected_releases_over_excess(self):
  self.board()
  out=self.plan('p1',10,['rel-jia','rel-yi'],[7,7])
  # 裁决口径：B+D 超额 0 但影响两份发布；C+D+E 超额 2 只影响甲，先守恢复承诺选后者。
  self.assertEqual(out['decision'],'batch_planned')
  self.assertEqual(out['layers'],['C','D','E'])
  self.assertEqual(out['bytes'],12);self.assertEqual(out['excess'],2)
  self.assertEqual(out['affected'],['rel-jia'])
  self.assertEqual([b['layer'] for b in out['backfill']],['C','D','E'])
  self.assertEqual([(c['used'],c['layers']) for c in out['channels']],[(5,['C']),(7,['D','E'])])
  self.assert_receipt(out)
  self.assertEqual([r['layer'] for r in rows(self.db,'SELECT layer FROM batch_backfill WHERE plan=? ORDER BY layer',('p1',))],['C','D','E'])
  self.assertEqual([r['manifest'] for r in rows(self.db,'SELECT manifest FROM batch_affected WHERE plan=?',('p1',))],['rel-jia'])
  again=self.plan('p2',10,['rel-jia','rel-yi'],[7,7])
  self.assertEqual(again['layers'],out['layers']);self.assertEqual(again['backfill'],out['backfill'])
  self.assertTrue(replay(str(self.path))['verified'])
 def test_shared_layer_backfilled_once_via_merged_edges(self):
  self.manifest('rel-1');self.manifest('rel-2')
  self.assertEqual(self.layer('S','rel-1',4)['decision'],'layer_recorded')
  dup=run(self.db,'manifest',{'event_id':'L:dup','op':'layer-deduplicate','id':'S-dup','manifest':'rel-2','arch':'amd64','digest':'sha:S','size':4})
  self.assertEqual(dup,{'decision':'layer_shared','layer':'S'})
  self.layer('X','rel-1',3);self.layer('Y','rel-2',3)
  for l in ('S','X','Y'):self.cache(l)
  out=self.plan('p1',4,['rel-1','rel-2'],[5])
  self.assertEqual(out['decision'],'batch_planned')
  self.assertEqual(out['layers'],['S']);self.assertEqual(out['excess'],0)
  self.assertEqual(out['affected'],['rel-1','rel-2'])
  # 两份清单共用同一层，窗口内只回填一次：4 字节进唯一通道，而非 8。
  self.assertEqual(out['backfill'],[{'layer':'S','size':4,'channel':0}])
  self.assertEqual(out['channels'],[{'channel':0,'cap':5,'used':4,'layers':['S']}])
  self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM batch_backfill WHERE plan=?',('p1',))['n'],1)
  self.assertTrue(replay(str(self.path))['verified'])
 def test_lease_removes_layer_and_forces_readjudication(self):
  self.board()
  before=self.plan('p0',10,['rel-jia','rel-yi'],[7,7])
  self.assertEqual(before['layers'],['C','D','E'])
  self.manifest('rel-d')
  dup=run(self.db,'manifest',{'event_id':'L:d2','op':'layer-deduplicate','id':'D-dup','manifest':'rel-d','arch':'amd64','digest':'sha:D','size':4})
  self.assertEqual(dup['decision'],'layer_shared')
  lease=run(self.db,'retention',{'event_id':'lease-d','op':'offline-retain','id':'lease-d','manifest':'rel-d','node':'node-1','start':0,'expires':100,'approver':'duty'})
  self.assertEqual(lease['decision'],'lease_active')
  # D 取得覆盖申请时点的租约后从候选消失，同一请求重新裁定为 B+C。
  out=self.plan('p1',10,['rel-jia','rel-yi'],[7,7])
  self.assertEqual(out['decision'],'batch_planned')
  self.assertEqual(out['layers'],['B','C'])
  self.assertEqual(out['bytes'],11);self.assertEqual(out['excess'],1)
  self.assertEqual(out['affected'],['rel-jia','rel-yi'])
  self.assertNotIn('D',out['layers'])
  self.assertEqual([(c['used'],c['layers']) for c in out['channels']],[(6,['B']),(5,['C'])])
  self.assert_receipt(out)
  self.assertTrue(replay(str(self.path))['verified'])
 def test_unpackable_layer_yields_no_redeemable_plan(self):
  self.manifest('rel-jia');self.manifest('rel-yi')
  for l,s in (('A',8),('C',5),('D',4),('E',3)):self.layer(l,'rel-jia',s)
  self.layer('B','rel-yi',6)
  self.cache('A');self.cache('B')
  # 仅 A/B 可驱逐：A 超过单通道上限无法回填，B  alone 不足 10 字节。
  out=self.plan('p1',10,['rel-jia','rel-yi'],[7,7])
  self.assertEqual(out,{'decision':'recovery_unachievable','available':14})
  self.assertIsNone(one(self.db,'SELECT * FROM batch_plans WHERE id=?',('p1',)))
  self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM batch_items')['n'],0)
  self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM batch_backfill')['n'],0)
  self.assertTrue(replay(str(self.path))['verified'])
 def test_exact_total_capacity_still_unpackable(self):
  self.manifest('rel-x')
  for l,s in (('P',5),('Q',5),('R',4)):self.layer(l,'rel-x',s)
  for l in 'PQR':self.cache(l)
  # 总容量 14 刚好等于申请值，且每层单独都放得下，但 5/5/4 无法同时装入两条 7 字节通道。
  out=self.plan('p1',14,['rel-x'],[7,7])
  self.assertEqual(out,{'decision':'recovery_unachievable','available':14})
  self.assertIsNone(one(self.db,'SELECT * FROM batch_plans WHERE id=?',('p1',)))
  self.assertTrue(replay(str(self.path))['verified'])
 def test_backtracking_finds_feasible_channel_assignment(self):
  self.manifest('rel-x')
  for i,s in enumerate((6,5,3,2,2,2)):self.layer('L%d'%i,'rel-x',s)
  for i in range(6):self.cache('L%d'%i)
  # 首试装法会失败（6+3 与 5+2+2 后余下一个 2 无处可放），回溯搜索仍须给出可行安排。
  out=self.plan('p1',20,['rel-x'],[10,10])
  self.assertEqual(out['decision'],'batch_planned')
  self.assertEqual(out['bytes'],20);self.assertEqual(out['excess'],0)
  self.assertEqual(out['layers'],['L0','L1','L2','L3','L4','L5'])
  self.assertEqual(out['affected'],['rel-x'])
  self.assertEqual([(c['used'],c['layers']) for c in out['channels']],[(10,['L0','L3','L4']),(10,['L1','L2','L5'])])
  self.assert_receipt(out)
  self.assertTrue(replay(str(self.path))['verified'])
 def test_legacy_batch_plan_without_prewarm_unchanged(self):
  self.board()
  out=run(self.db,'eviction',{'event_id':'P:legacy','op':'eviction-batch-plan','id':'legacy','node':'node-1','needed':10,'at':1})
  self.assertEqual(out,{'decision':'batch_planned','layers':['A','B'],'bytes':14})
  self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM batch_backfill')['n'],0)
  self.assertTrue(replay(str(self.path))['verified'])
 def test_recovery_mode_reports_capacity_shortfall(self):
  self.board()
  out=self.plan('p1',100,['rel-jia','rel-yi'],[7,7])
  self.assertEqual(out,{'decision':'capacity_unachievable','available':26})
  self.assertIsNone(one(self.db,'SELECT * FROM batch_plans WHERE id=?',('p1',)))
 def test_recovery_spec_validation(self):
  self.board()
  missing=run(self.db,'eviction',{'event_id':'P:bad1','op':'eviction-batch-plan','id':'bad1','node':'node-1','needed':10,'at':1,'prewarm':['rel-jia']})
  self.assertEqual(missing['decision'],'recovery_spec_invalid')
  many=self.plan('bad2',10,['rel-jia'],[7,7,7,7,7])
  self.assertEqual(many['decision'],'recovery_spec_invalid')
  zero=self.plan('bad3',10,['rel-jia'],[7,0])
  self.assertEqual(zero['decision'],'recovery_spec_invalid')
  self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM batch_plans')['n'],0)
  self.assertTrue(replay(str(self.path))['verified'])
 def test_recovery_plan_delete_reconcile_unchanged(self):
  self.board()
  self.plan('p1',10,['rel-jia','rel-yi'],[7,7])
  r1=run(self.db,'eviction',{'event_id':'D:1','op':'delete-reconcile','id':'d1','plan':'p1','layer':'C','at':2,'success':True})
  self.assertEqual(r1,{'decision':'partial_progress','bytes':5})
  r2=run(self.db,'eviction',{'event_id':'D:2','op':'delete-reconcile','id':'d2','plan':'p1','layer':'D','at':2,'success':True})
  self.assertEqual(r2,{'decision':'partial_progress','bytes':9})
  r3=run(self.db,'eviction',{'event_id':'D:3','op':'delete-reconcile','id':'d3','plan':'p1','layer':'E','at':2,'success':True})
  self.assertEqual(r3,{'decision':'space_reclaimed','bytes':12})
  self.assertEqual(one(self.db,'SELECT status FROM batch_plans WHERE id=?',('p1',))['status'],'complete')
  self.assertTrue(replay(str(self.path))['verified'])
print('module_file',__file__)
