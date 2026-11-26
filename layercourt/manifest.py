from .store import one,rows
SCHEMA='''
CREATE TABLE IF NOT EXISTS manifests(id TEXT PRIMARY KEY,arch TEXT,digest TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS layers(id TEXT PRIMARY KEY,digest TEXT,size INTEGER,arch TEXT);
CREATE TABLE IF NOT EXISTS manifest_edges(manifest TEXT,layer TEXT,PRIMARY KEY(manifest,layer));
'''
def handle(db,x):
 op=x['op']
 if op=='manifest-register':
  old=one(db,'SELECT * FROM manifests WHERE id=?',(x['id'],))
  if old:return {'decision':'reused' if old['digest']==x['digest'] and old['arch']==x['arch'] else 'manifest_identity_conflict'}
  db.execute('INSERT INTO manifests VALUES(?,?,?,?)',(x['id'],x['arch'],x['digest'],'active'))
  return {'decision':'manifest_active'}
 if op=='layer-deduplicate':
  m=one(db,'SELECT * FROM manifests WHERE id=?',(x['manifest'],))
  if not m or m['status']!='active' or m['arch']!=x['arch'] or x['size']<=0:return {'decision':'layer_rejected'}
  if one(db,"SELECT 1 FROM workload_refs WHERE manifest=? AND status='active'",(x['manifest'],)) or one(db,"SELECT 1 FROM retention_leases WHERE manifest=? AND status IN ('active','frozen')",(x['manifest'],)):
   return {'decision':'manifest_closure_committed'}
  layer=one(db,'SELECT * FROM layers WHERE digest=? AND arch=?',(x['digest'],x['arch']))
  if layer and layer['size']!=x['size']:return {'decision':'digest_size_conflict'}
  lid=layer['id'] if layer else x['id']
  if not layer:db.execute('INSERT INTO layers VALUES(?,?,?,?)',(lid,x['digest'],x['size'],x['arch']))
  db.execute('INSERT OR IGNORE INTO manifest_edges VALUES(?,?)',(x['manifest'],lid))
  return {'decision':'layer_shared' if layer else 'layer_recorded','layer':lid}
 if op=='manifest-revoke':
  m=one(db,'SELECT * FROM manifests WHERE id=?',(x['id'],))
  if not m or m['status']!='active':return {'decision':'revoke_unavailable'}
  db.execute("UPDATE manifests SET status='revoked' WHERE id=?",(x['id'],))
  db.execute("UPDATE retention_leases SET status='frozen' WHERE manifest=? AND status='active'",(x['id'],))
  return {'decision':'manifest_revoked'}
 return {'decision':'unknown_operation'}
