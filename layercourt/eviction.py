from .store import one,rows
SCHEMA='''
CREATE TABLE IF NOT EXISTS node_caches(node TEXT,layer TEXT,status TEXT,PRIMARY KEY(node,layer));
CREATE TABLE IF NOT EXISTS eviction_plans(id TEXT PRIMARY KEY,node TEXT,layer TEXT,needed INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS delete_receipts(id TEXT PRIMARY KEY,plan TEXT,layer TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS batch_plans(id TEXT PRIMARY KEY,node TEXT,needed INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS batch_items(plan TEXT,layer TEXT,size INTEGER,status TEXT,PRIMARY KEY(plan,layer));
'''
def protected(db,node,layer,at):
 refs=one(db,"SELECT 1 FROM workload_refs r JOIN manifest_edges e ON e.manifest=r.manifest WHERE r.node=? AND e.layer=? AND r.status='active' LIMIT 1",(node,layer))
 leases=one(db,"SELECT 1 FROM retention_leases l JOIN manifest_edges e ON e.manifest=l.manifest WHERE l.node=? AND e.layer=? AND l.status IN ('active','frozen') AND l.start<=? AND l.expires>? LIMIT 1",(node,layer,at,at))
 return bool(refs or leases)
def handle(db,x):
 op=x['op']
 if op=='cache-record':
  layer=one(db,'SELECT * FROM layers WHERE id=?',(x['layer'],))
  layer_id=x['layer']
  if not layer:
   alias=one(db,'SELECT canonical FROM layer_aliases WHERE alias=?',(x['layer'],)) if 'layer_aliases' in {r['name'] for r in rows(db,"SELECT name FROM sqlite_master WHERE type='table'")} else None
   if alias:
    layer_id=alias['canonical'];layer=one(db,'SELECT * FROM layers WHERE id=?',(layer_id,))
  if not layer:return {'decision':'unknown_layer'}
  db.execute("INSERT INTO node_caches VALUES(?,?,'present') ON CONFLICT(node,layer) DO UPDATE SET status='present'",(x['node'],layer_id))
  return {'decision':'cache_present'}
 if op=='eviction-plan':
  cache=one(db,'SELECT * FROM node_caches WHERE node=? AND layer=?',(x['node'],x['layer']))
  layer=one(db,'SELECT * FROM layers WHERE id=?',(x['layer'],))
  if not cache or cache['status']!='present' or not layer or layer['size']<x['needed'] or protected(db,x['node'],x['layer'],x['at']):return {'decision':'layer_protected_or_missing'}
  db.execute('INSERT INTO eviction_plans VALUES(?,?,?,?,?)',(x['id'],x['node'],x['layer'],x['needed'],'planned'))
  return {'decision':'planned'}
 if op=='eviction-batch-plan':
  candidates=[]
  for cache in rows(db,"SELECT c.layer,l.size FROM node_caches c JOIN layers l ON l.id=c.layer WHERE c.node=? AND c.status='present' ORDER BY l.size DESC,c.layer",(x['node'],)):
   if not protected(db,x['node'],cache['layer'],x['at']):candidates.append(cache)
  selected=[];total=0
  for candidate in candidates:
   if total>=x['needed']:break
   selected.append(candidate);total+=candidate['size']
  if x['needed']<=0 or total<x['needed']:return {'decision':'capacity_unachievable','available':total}
  db.execute('INSERT INTO batch_plans VALUES(?,?,?,?)',(x['id'],x['node'],x['needed'],'planned'))
  db.executemany('INSERT INTO batch_items VALUES(?,?,?,?)',[(x['id'],s['layer'],s['size'],'planned') for s in selected])
  return {'decision':'batch_planned','layers':[s['layer'] for s in selected],'bytes':total}
 if op=='delete-reconcile':
  p=one(db,'SELECT * FROM eviction_plans WHERE id=?',(x['plan'],))
  batch=one(db,'SELECT * FROM batch_plans WHERE id=?',(x['plan'],))
  if batch:
   item=one(db,'SELECT * FROM batch_items WHERE plan=? AND layer=?',(x['plan'],x['layer']))
   if batch['status']!='planned' or not item or item['status']=='deleted':return {'decision':'plan_unavailable'}
   cache=one(db,'SELECT * FROM node_caches WHERE node=? AND layer=?',(batch['node'],x['layer']))
   if not cache or cache['status']!='present':return {'decision':'cache_already_absent'}
   if protected(db,batch['node'],x['layer'],x['at']):return {'decision':'new_reference_blocks_delete'}
   status='deleted' if x['success'] else 'failed'
   db.execute('INSERT INTO delete_receipts VALUES(?,?,?,?)',(x['id'],x['plan'],x['layer'],status))
   db.execute('UPDATE batch_items SET status=? WHERE plan=? AND layer=?',(status,x['plan'],x['layer']))
   if x['success']:db.execute("UPDATE node_caches SET status='deleted' WHERE node=? AND layer=?",(batch['node'],x['layer']))
   credited=one(db,"SELECT COALESCE(SUM(size),0) n FROM batch_items WHERE plan=? AND status='deleted'",(x['plan'],))['n']
   if credited>=batch['needed']:db.execute("UPDATE batch_plans SET status='complete' WHERE id=?",(x['plan'],))
   return {'decision':'space_reclaimed' if credited>=batch['needed'] else 'partial_progress','bytes':credited}
  if not p or p['status']!='planned' or p['layer']!=x['layer']:return {'decision':'plan_unavailable'}
  cache=one(db,'SELECT * FROM node_caches WHERE node=? AND layer=?',(p['node'],p['layer']))
  if not cache or cache['status']!='present':return {'decision':'cache_already_absent'}
  if protected(db,p['node'],p['layer'],x['at']):return {'decision':'new_reference_blocks_delete'}
  status='deleted' if x['success'] else 'failed'
  db.execute('INSERT INTO delete_receipts VALUES(?,?,?,?)',(x['id'],p['id'],p['layer'],status))
  if x['success']:
   db.execute("UPDATE node_caches SET status='deleted' WHERE node=? AND layer=?",(p['node'],p['layer']))
   db.execute("UPDATE eviction_plans SET status='complete' WHERE id=?",(p['id'],))
  return {'decision':'space_reclaimed' if x['success'] else 'delete_failed'}
 return {'decision':'unknown_operation'}
