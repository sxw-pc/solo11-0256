from .store import one,rows
SCHEMA='''
CREATE TABLE IF NOT EXISTS node_caches(node TEXT,layer TEXT,status TEXT,PRIMARY KEY(node,layer));
CREATE TABLE IF NOT EXISTS eviction_plans(id TEXT PRIMARY KEY,node TEXT,layer TEXT,needed INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS delete_receipts(id TEXT PRIMARY KEY,plan TEXT,layer TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS batch_plans(id TEXT PRIMARY KEY,node TEXT,needed INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS batch_items(plan TEXT,layer TEXT,size INTEGER,status TEXT,PRIMARY KEY(plan,layer));
CREATE TABLE IF NOT EXISTS batch_backfill(plan TEXT,layer TEXT,channel INTEGER,size INTEGER,PRIMARY KEY(plan,layer));
CREATE TABLE IF NOT EXISTS batch_affected(plan TEXT,manifest TEXT,PRIMARY KEY(plan,manifest));
'''
def protected(db,node,layer,at):
 refs=one(db,"SELECT 1 FROM workload_refs r JOIN manifest_edges e ON e.manifest=r.manifest WHERE r.node=? AND e.layer=? AND r.status='active' LIMIT 1",(node,layer))
 leases=one(db,"SELECT 1 FROM retention_leases l JOIN manifest_edges e ON e.manifest=l.manifest WHERE l.node=? AND e.layer=? AND l.status IN ('active','frozen') AND l.start<=? AND l.expires>? LIMIT 1",(node,layer,at,at))
 return bool(refs or leases)
# 恢复承诺裁决口径：在同时满足申请字节与回填通道约束的可行计划中，
# 先取受影响预热发布数最少者，再取超额驱逐字节最少者，最后按层名元组定序。
# 该口径优先守住值班方对发布方的恢复承诺：每多一份受影响发布，同一窗口内就多一条必须兑现的恢复义务；
# 超额字节因通道分配已保证同窗口可回填而居次。层名定序保证相同输入给出可重放的裁决。
def _pack(items,caps):
 # 精确回填裁定：每层完整走一条通道，同通道可装多层，单通道容量不可透支。
 # 尺寸降序回溯 + 相同(负载,上限)通道对称剪枝；同输入结果确定，存在可行安排时不会误判。
 order=sorted(items,key=lambda kv:(-kv[1],kv[0]))
 loads=[0]*len(caps);assign={}
 def bt(i):
  if i==len(order):return True
  layer,size=order[i];seen=set()
  for c in range(len(caps)):
   if loads[c]+size>caps[c]:continue
   sig=(loads[c],caps[c])
   if sig in seen:continue
   seen.add(sig);loads[c]+=size;assign[layer]=c
   if bt(i+1):return True
   loads[c]-=size;del assign[layer]
  return False
 return assign if bt(0) else None
def _recovery_select(candidates,needed,closures,caps):
 # candidates 按(-size,layer)排序；closures 为预热清单经 manifest_edges 归并后的引用层集合。
 # 受影响发布数与超额字节随扩层单调不减，故字节达到申请值即停：其超集必被支配，无需展开。
 sizes={c['layer']:c['size'] for c in candidates}
 shared=set().union(*closures.values()) if closures else set()
 n=len(candidates);suffix=[0]*(n+1)
 for i in range(n-1,-1,-1):suffix[i]=suffix[i+1]+candidates[i]['size']
 leaves=[]
 def dfs(i,total,picked,aff):
  if total>=needed:leaves.append((len(aff),total-needed,tuple(sorted(picked))));return
  if i==n or total+suffix[i]<needed:return
  dfs(i+1,total,picked,aff)
  layer=candidates[i]['layer']
  dfs(i+1,total+sizes[layer],picked+[layer],aff|{m for m,clo in closures.items() if layer in clo})
 dfs(0,0,[],set())
 memo={}
 for count,excess,layers in sorted(leaves):
  backfill=tuple(sorted(set(layers)&shared))
  key=(tuple((l,sizes[l]) for l in backfill),tuple(caps))
  if key not in memo:memo[key]=_pack(list(key[0]),list(caps))
  if memo[key] is not None:return list(layers),[m for m in sorted(closures) if closures[m]&set(layers)],memo[key]
 return None
def handle(db,x):
 op=x['op']
 if op=='cache-record':
  layer=one(db,'SELECT * FROM layers WHERE id=?',(x['layer'],))
  if not layer:return {'decision':'unknown_layer'}
  db.execute("INSERT INTO node_caches VALUES(?,?,'present') ON CONFLICT(node,layer) DO UPDATE SET status='present'",(x['node'],x['layer']))
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
  prewarm=x.get('prewarm');channels=x.get('channels')
  if prewarm is None and channels is None:
   selected=[];total=0
   for candidate in candidates:
    if total>=x['needed']:break
    selected.append(candidate);total+=candidate['size']
   if x['needed']<=0 or total<x['needed']:return {'decision':'capacity_unachievable','available':total}
   db.execute('INSERT INTO batch_plans VALUES(?,?,?,?)',(x['id'],x['node'],x['needed'],'planned'))
   db.executemany('INSERT INTO batch_items VALUES(?,?,?,?)',[(x['id'],s['layer'],s['size'],'planned') for s in selected])
   return {'decision':'batch_planned','layers':[s['layer'] for s in selected],'bytes':total}
  # 带预热恢复承诺的容量裁定：入选层仍须当前驻留且未受保护，另须同窗口可回填。
  if x['needed']<=0:return {'decision':'capacity_unachievable','available':0}
  if not isinstance(prewarm,list) or not all(isinstance(m,str) for m in prewarm) or not isinstance(channels,list) or not channels or len(channels)>4 or any(not isinstance(c,int) or isinstance(c,bool) or c<=0 for c in channels) or len(candidates)>16:return {'decision':'recovery_spec_invalid'}
  closures={}
  for m in sorted(set(prewarm)):closures[m]=set(r['layer'] for r in rows(db,'SELECT layer FROM manifest_edges WHERE manifest=? ORDER BY layer',(m,)))
  available=sum(c['size'] for c in candidates)
  if available<x['needed']:return {'decision':'capacity_unachievable','available':available}
  picked=_recovery_select(candidates,x['needed'],closures,list(channels))
  if picked is None:return {'decision':'recovery_unachievable','available':available}
  layers,affected,assign=picked
  sizes={c['layer']:c['size'] for c in candidates}
  total=sum(sizes[l] for l in layers)
  backfill=sorted(assign)
  used=[0]*len(channels)
  for l in backfill:used[assign[l]]+=sizes[l]
  receipt=[{'layer':l,'size':sizes[l],'channel':assign[l]} for l in backfill]
  chans=[{'channel':i,'cap':channels[i],'used':used[i],'layers':[l for l in backfill if assign[l]==i]} for i in range(len(channels))]
  db.execute('INSERT INTO batch_plans VALUES(?,?,?,?)',(x['id'],x['node'],x['needed'],'planned'))
  db.executemany('INSERT INTO batch_items VALUES(?,?,?,?)',[(x['id'],l,sizes[l],'planned') for l in layers])
  db.executemany('INSERT INTO batch_backfill VALUES(?,?,?,?)',[(x['id'],l,assign[l],sizes[l]) for l in backfill])
  db.executemany('INSERT INTO batch_affected VALUES(?,?)',[(x['id'],m) for m in affected])
  return {'decision':'batch_planned','layers':layers,'bytes':total,'excess':total-x['needed'],'affected':affected,'backfill':receipt,'channels':chans}
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
