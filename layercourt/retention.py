from .store import one
SCHEMA='''
CREATE TABLE IF NOT EXISTS workload_refs(id TEXT PRIMARY KEY,pod TEXT,uid TEXT,manifest TEXT,node TEXT,generation INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS retention_leases(id TEXT PRIMARY KEY,manifest TEXT,node TEXT,start INTEGER,expires INTEGER,approver TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS lease_claims(id TEXT PRIMARY KEY,ref TEXT,uid TEXT,kind TEXT,termination_receipt TEXT,delete_receipt TEXT,status TEXT);
'''
def handle(db,x):
 op=x['op']
 if op=='workload-reference':
  m=one(db,'SELECT * FROM manifests WHERE id=?',(x['manifest'],))
  if not m or m['status']!='active':return {'decision':'manifest_unavailable'}
  old=one(db,"SELECT * FROM workload_refs WHERE pod=? AND status='active' ORDER BY generation DESC LIMIT 1",(x['pod'],))
  if old and x['generation']<=old['generation']:return {'decision':'stale_pod_generation'}
  # A new Pod generation does not prove that the old process has exited.
  db.execute('INSERT INTO workload_refs VALUES(?,?,?,?,?,?,?)',(x['id'],x['pod'],x['uid'],x['manifest'],x['node'],x['generation'],'active'))
  return {'decision':'reference_active'}
 if op=='offline-retain':
  m=one(db,'SELECT * FROM manifests WHERE id=?',(x['manifest'],))
  if not m or m['status']!='active' or x['expires']<=x['start'] or not x.get('approver'):return {'decision':'lease_denied'}
  closure=one(db,'SELECT COUNT(*) n FROM manifest_edges WHERE manifest=?',(x['manifest'],))['n']
  cached=one(db,"SELECT COUNT(*) n FROM manifest_edges e JOIN node_caches c ON c.layer=e.layer AND c.node=? AND c.status='present' WHERE e.manifest=?",(x['node'],x['manifest']))['n']
  if closure==0 or cached!=closure:return {'decision':'offline_closure_missing','required':closure,'present':cached}
  db.execute('INSERT INTO retention_leases VALUES(?,?,?,?,?,?,?)',(x['id'],x['manifest'],x['node'],x['start'],x['expires'],x['approver'],'active'))
  return {'decision':'lease_active'}
 if op=='reference-release':
  ref=one(db,'SELECT * FROM workload_refs WHERE id=?',(x['ref'],))
  if not ref or ref['status']!='active' or ref['uid']!=x['uid'] or not x.get('termination_receipt') or not x.get('delete_receipt'):
   return {'decision':'release_unproven'}
  db.execute('INSERT INTO lease_claims VALUES(?,?,?,?,?,?,?)',(x['id'],ref['id'],ref['uid'],'release',x['termination_receipt'],x['delete_receipt'],'accepted'))
  db.execute("UPDATE workload_refs SET status='released' WHERE id=?",(ref['id'],))
  return {'decision':'reference_released'}
 # Retirement evidence registration / settlement lives in its own module; the
 # retention entry forwards those operations so callers may keep one command.
 from . import retirement
 if op in retirement.OPERATIONS:
  return retirement.handle(db,x)
 return {'decision':'unknown_operation'}
