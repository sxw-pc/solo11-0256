"""Disk-swap inventory attestation.

A node stores image layers across exactly two directories (A, B). During a
disk swap a layer can be moved from one directory to the other between two
local scans, so neither fragment shows it even though the logical layer is
still resident. This module signs off per-layer residency for the *current*
node storage generation from:

* synthesized directory scan fragments bound to one storage generation;
* cross-directory move receipts preserving source, target, layer identity and
  sampling cut points;
* the maintenance contract under which the evidence was collected:
  - ``frozen``: one fixed directory image; absences need a same-cut-point
    complete image of both directories, otherwise the inventory must wait;
  - ``live``: business keeps writing; fragments are spliced with paired move
    receipts and a layer is only confirmed when every cross-fragment move
    boundary is restored. Live inventories never confirm absences and never
    claim the whole surface was verified.
"""
import json
from .store import one,rows
SCHEMA='''
CREATE TABLE IF NOT EXISTS node_storage(node TEXT PRIMARY KEY,generation INTEGER,at INTEGER);
CREATE TABLE IF NOT EXISTS inventory_attestations(id TEXT PRIMARY KEY,node TEXT,generation INTEGER,mode TEXT,complete INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS inventory_fragments(attestation TEXT,fragment TEXT,node TEXT,generation INTEGER,dir TEXT,cut INTEGER,layers TEXT,PRIMARY KEY(attestation,fragment));
CREATE TABLE IF NOT EXISTS inventory_move_receipts(attestation TEXT,receipt TEXT,node TEXT,generation INTEGER,layer TEXT,src TEXT,dst TEXT,src_cut INTEGER,dst_cut INTEGER,arrived INTEGER,PRIMARY KEY(attestation,receipt));
CREATE TABLE IF NOT EXISTS inventory_findings(attestation TEXT,node TEXT,layer TEXT,generation INTEGER,verdict TEXT,location TEXT,reason TEXT,doubt INTEGER,PRIMARY KEY(attestation,layer));
CREATE TABLE IF NOT EXISTS layer_residency(node TEXT,layer TEXT,generation INTEGER,status TEXT,attestation TEXT,cut INTEGER,doubt INTEGER,PRIMARY KEY(node,layer));
'''
DIRS=('A','B')
MAX_LAYERS=6
MAX_MOVES=1
def _generation(db,node):
 return one(db,'SELECT * FROM node_storage WHERE node=?',(node,))
def _rollover_seq(db,node,generation):
 best=0
 for e in rows(db,"SELECT seq,input FROM events WHERE command='inventory' ORDER BY seq"):
  x=json.loads(e['input'])
  if x.get('op')=='storage-generation' and x.get('node')==node and x.get('generation')==generation:best=e['seq']
 return best
def _latest_cache_at(db,node,layer,after_seq=0):
 best=None
 for e in rows(db,'SELECT seq,input FROM events WHERE command=? AND seq>? ORDER BY seq',('eviction',after_seq)):
  x=json.loads(e['input'])
  if x.get('op')=='cache-record' and x.get('node')==node and x.get('layer')==layer:best=x.get('at')
 return best
def resident_layers(db,node):
 """Layers proven readable on the node in its current generation.

 Returns None when the node has never declared a storage generation, in
 which case callers fall back to plain node_caches residency. Old-generation
 attestations never count; a cache-record observed after the generation
 rollover is itself a current-generation proof, so an expired scan conclusion
 cannot overwrite a later legitimate cache-record.
 """
 g=_generation(db,node)
 if not g:return None
 gen=g['generation'];roll=_rollover_seq(db,node,gen);proven=set()
 for e in rows(db,'SELECT input FROM events WHERE command=? AND seq>?',('eviction',roll)):
  x=json.loads(e['input'])
  if x.get('op')=='cache-record' and x.get('node')==node:proven.add(x['layer'])
 for r in rows(db,'SELECT layer FROM layer_residency WHERE node=? AND generation=? AND status=?',(node,gen,'present')):
  proven.add(r['layer'])
 deleted={r['layer'] for r in rows(db,"SELECT layer FROM node_caches WHERE node=? AND status='deleted'",(node,))}
 return proven-deleted
def _finding(layer,verdict,reason,location=None,cut=None,doubt=False):
 return {'layer':layer,'verdict':verdict,'location':location,'reason':reason,'cut':cut,'doubt':1 if doubt else 0}
def _union_scope(frags,moves):
 scope=[]
 for f in frags:
  for lid in f.get('layers',[]):
   if lid not in scope:scope.append(lid)
 for m in moves:
  if m.get('layer') and m['layer'] not in scope:scope.append(m['layer'])
 return scope
def _validate(db,x,gen):
 mode=x.get('mode')
 if mode not in ('frozen','live'):return None,{'decision':'contract_unknown'}
 if one(db,'SELECT 1 FROM inventory_attestations WHERE id=?',(x['id'],)):return None,{'decision':'attestation_identity_conflict'}
 frags=x.get('fragments',[]);moves=x.get('moves',[])
 if len(moves)>MAX_MOVES:return None,{'decision':'too_many_moves'}
 seen=set()
 for f in frags:
  if f.get('generation')!=gen:return None,{'decision':'fragment_generation_mismatch'}
  if f.get('dir') not in DIRS:return None,{'decision':'unknown_directory'}
  if f['dir'] in seen:return None,{'decision':'duplicate_fragment'}
  if not isinstance(f.get('cut'),int):return None,{'decision':'fragment_cut_invalid'}
  seen.add(f['dir'])
 for m in moves:
  if m.get('generation')!=gen:return None,{'decision':'receipt_generation_mismatch'}
  if m.get('src') not in DIRS or m.get('dst') not in DIRS or m['src']==m['dst']:return None,{'decision':'move_receipt_invalid'}
  if not m.get('layer') or not isinstance(m.get('src_cut'),int):return None,{'decision':'move_receipt_invalid'}
  if m.get('arrived') and not isinstance(m.get('dst_cut'),int):return None,{'decision':'move_receipt_invalid'}
 referenced=set(_union_scope(frags,moves))
 if x.get('scope'):
  if not referenced.issubset(set(x['scope'])):return None,{'decision':'scope_mismatch'}
 scope=list(dict.fromkeys(x['scope'] if x.get('scope') else _union_scope(frags,moves)))
 if len(scope)>MAX_LAYERS:return None,{'decision':'scope_exceeds_limit'}
 for lid in scope:
  if not one(db,'SELECT 1 FROM layers WHERE id=?',(lid,)):return None,{'decision':'unknown_layer','layer':lid}
 return (mode,frags,moves,scope),None
def _upsert_residency(db,node,gen,att,f):
 old=one(db,'SELECT * FROM layer_residency WHERE node=? AND layer=?',(node,f['layer']))
 if f['verdict']=='unresolved':
  # Pending cases never negate residency: keep any same-generation verdict,
  # only replace rows that belong to an older generation.
  if old and old['generation']==gen:return
  if old:
   db.execute('UPDATE layer_residency SET generation=?,status=?,attestation=?,cut=?,doubt=? WHERE node=? AND layer=?',(gen,'unresolved',att,f.get('cut'),f['doubt'],node,f['layer']))
  else:
   db.execute('INSERT INTO layer_residency VALUES(?,?,?,?,?,?,?)',(node,f['layer'],gen,'unresolved',att,f.get('cut'),f['doubt']))
  return
 db.execute('INSERT INTO layer_residency VALUES(?,?,?,?,?,?,?) ON CONFLICT(node,layer) DO UPDATE SET generation=excluded.generation,status=excluded.status,attestation=excluded.attestation,cut=excluded.cut,doubt=excluded.doubt',(node,f['layer'],gen,f['verdict'],att,f.get('cut'),f.get('doubt',0)))
def _persist(db,x,gen,complete,status,frags,moves,findings):
 db.execute('INSERT INTO inventory_attestations VALUES(?,?,?,?,?,?)',(x['id'],x['node'],gen,x['mode'],1 if complete else 0,status))
 for f in frags:
  db.execute('INSERT INTO inventory_fragments VALUES(?,?,?,?,?,?,?)',(x['id'],f.get('id','frag'),x['node'],f['generation'],f['dir'],f['cut'],json.dumps(f.get('layers',[]),sort_keys=True)))
 for m in moves:
  db.execute('INSERT INTO inventory_move_receipts VALUES(?,?,?,?,?,?,?,?,?,?)',(x['id'],m.get('id','move'),x['node'],m['generation'],m['layer'],m['src'],m['dst'],m['src_cut'],m.get('dst_cut'),1 if m.get('arrived') else 0))
 for f in findings:
  db.execute('INSERT INTO inventory_findings VALUES(?,?,?,?,?,?,?,?)',(x['id'],x['node'],f['layer'],gen,f['verdict'],f.get('location'),f.get('reason'),f.get('doubt',0)))
  _upsert_residency(db,x['node'],gen,x['id'],f)
def _result(decision,complete,findings):
 return {'decision':decision,'complete':complete,
  'present':[f['layer'] for f in findings if f['verdict']=='present'],
  'absent':[f['layer'] for f in findings if f['verdict']=='absent'],
  'pending':[f['layer'] for f in findings if f['verdict']=='unresolved'],
  'findings':findings}
def _frozen(db,x,gen,frags,scope):
 if x.get('moves'):return {'decision':'frozen_contract_violated'}
 bydir={f['dir']:f for f in frags}
 if set(bydir)!=set(DIRS) or len({f['cut'] for f in frags})!=1:
  findings=[_finding(lid,'unresolved','await_frozen_image') for lid in scope]
  _persist(db,x,gen,False,'waiting',frags,[],findings)
  return _result('frozen_image_unavailable',False,findings)
 cut=frags[0]['cut'];findings=[]
 roll=_rollover_seq(db,x['node'],gen)
 for lid in scope:
  locs=[d for d in DIRS if lid in bydir[d].get('layers',[])]
  if locs:
   findings.append(_finding(lid,'present','same_cut_image',location=','.join(locs),cut=cut))
   continue
  later=_latest_cache_at(db,x['node'],lid,roll)
  if isinstance(later,int) and later>cut:
   findings.append(_finding(lid,'unresolved','scan_superseded_by_cache_record',cut=cut,doubt=True))
  else:
   findings.append(_finding(lid,'absent','missing_in_complete_image',cut=cut))
 _persist(db,x,gen,True,'complete',frags,[],findings)
 return _result('surface_attested',True,findings)
def _live(db,x,gen,frags,moves,scope):
 bydir={f['dir']:f for f in frags};move=moves[0] if moves else None;findings=[]
 for lid in scope:
  observed=[d for d in DIRS if d in bydir and lid in bydir[d].get('layers',[])]
  if observed:
   cut=max(bydir[d]['cut'] for d in observed)
   findings.append(_finding(lid,'present','observed_in_fragment',location=','.join(observed),cut=cut))
   continue
  if not move or move.get('layer')!=lid:
   findings.append(_finding(lid,'unresolved','not_observed_live_surface'))
   continue
  if not move.get('arrived') or move.get('dst_cut') is None:
   findings.append(_finding(lid,'unresolved','move_arrival_missing',cut=move.get('src_cut')))
   continue
  ok=move['dst_cut']>=move['src_cut']
  srcf=bydir.get(move['src']);dstf=bydir.get(move['dst'])
  if ok and srcf is not None and srcf['cut']<move['src_cut']:ok=False
  if ok and dstf is not None and dstf['cut']>move['dst_cut']:ok=False
  if ok:
   findings.append(_finding(lid,'present','move_boundary_closed',location=move['dst'],cut=move['dst_cut']))
  else:
   findings.append(_finding(lid,'unresolved','move_boundary_broken',cut=move['dst_cut'],doubt=True))
 _persist(db,x,gen,False,'partial',frags,moves,findings)
 return _result('online_attested',False,findings)
def _attest(db,x):
 g=_generation(db,x['node'])
 if not g:return {'decision':'generation_undeclared'}
 if g['generation']!=x['generation']:return {'decision':'stale_generation','current':g['generation']}
 parsed,err=_validate(db,x,x['generation'])
 if err:return err
 mode,frags,moves,scope=parsed
 if mode=='frozen':return _frozen(db,x,x['generation'],frags,scope)
 return _live(db,x,x['generation'],frags,moves,scope)
def _generation_op(db,x):
 if not isinstance(x.get('generation'),int) or x['generation']<0:return {'decision':'generation_invalid'}
 g=_generation(db,x['node'])
 if not g:
  db.execute('INSERT INTO node_storage VALUES(?,?,?)',(x['node'],x['generation'],x.get('at')))
  return {'decision':'generation_opened','generation':x['generation']}
 if g['generation']==x['generation']:return {'decision':'generation_active','generation':x['generation']}
 if g['generation']>x['generation']:return {'decision':'stale_generation','current':g['generation']}
 db.execute('UPDATE node_storage SET generation=?,at=? WHERE node=?',(x['generation'],x.get('at'),x['node']))
 return {'decision':'generation_advanced','generation':x['generation'],'previous':g['generation']}
def _doubts(db,x):
 present=resident_layers(db,x['node']);doubtful=[]
 if present is not None:
  for lease in rows(db,"SELECT * FROM retention_leases WHERE node=? AND status IN ('active','frozen')",(x['node'],)):
   missing=[r['layer'] for r in rows(db,'SELECT layer FROM manifest_edges WHERE manifest=?',(lease['manifest'],)) if r['layer'] not in present]
   if missing:doubtful.append({'lease':lease['id'],'manifest':lease['manifest'],'missing':missing})
 return {'decision':'doubts_reported','doubtful':doubtful}
def handle(db,x):
 op=x['op']
 if op=='storage-generation':return _generation_op(db,x)
 if op=='inventory-attest':return _attest(db,x)
 if op=='inventory-doubts':return _doubts(db,x)
 return {'decision':'unknown_operation'}
