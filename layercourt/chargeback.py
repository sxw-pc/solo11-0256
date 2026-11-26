"""Close a node cache statement against the tenants of current workload refs."""
from collections import defaultdict

from .store import one, rows

SCHEMA = '''
CREATE TABLE IF NOT EXISTS charge_tenants (
    ref TEXT PRIMARY KEY, tenant TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cache_statements (
    id TEXT PRIMARY KEY, node TEXT NOT NULL, charged_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS cache_charges (
    statement TEXT NOT NULL, layer TEXT NOT NULL, tenant TEXT NOT NULL,
    bytes INTEGER NOT NULL, PRIMARY KEY (statement, layer, tenant)
);
'''


def claimants(db, node):
    result = defaultdict(set)
    for row in rows(db, '''
        SELECT e.layer, t.tenant FROM workload_refs AS r
        JOIN charge_tenants AS t ON t.ref = r.id
        JOIN manifest_edges AS e ON e.manifest = r.manifest
        JOIN node_caches AS c ON c.node = r.node AND c.layer = e.layer
        WHERE r.node = ? AND r.status = 'active' AND c.status = 'present'
        ORDER BY e.layer, t.tenant
    ''', (node,)):
        result[row['layer']].add(row['tenant'])
    return result


def handle(db, event):
    op = event['op']
    if op == 'tenant-link':
        ref = one(db, 'SELECT status FROM workload_refs WHERE id = ?', (event['ref'],))
        if not ref or ref['status'] != 'active' or not event['tenant']:
            return {'decision': 'tenant_link_unavailable'}
        known = one(db, 'SELECT tenant FROM charge_tenants WHERE ref = ?', (event['ref'],))
        if known:
            return {'decision': 'tenant_linked' if known['tenant'] == event['tenant'] else 'tenant_link_conflict'}
        db.execute('INSERT INTO charge_tenants VALUES (?, ?)', (event['ref'], event['tenant']))
        return {'decision': 'tenant_linked'}
    if op != 'statement-close':
        return {'decision': 'unknown_operation'}
    if one(db, 'SELECT 1 FROM cache_statements WHERE id = ?', (event['id'],)):
        return {'decision': 'statement_id_taken'}
    if one(db, '''SELECT 1 FROM workload_refs AS r
        LEFT JOIN charge_tenants AS t ON t.ref = r.id
        WHERE r.node = ? AND r.status = 'active' AND t.tenant IS NULL''', (event['node'],)):
        return {'decision': 'attribution_incomplete'}
    allocations = []
    totals = defaultdict(int)
    for layer, tenants in sorted(claimants(db, event['node']).items()):
        size = one(db, 'SELECT size FROM layers WHERE id = ?', (layer,))['size']
        share, extra = divmod(size, len(tenants))
        for index, tenant in enumerate(sorted(tenants)):
            amount = share + int(index < extra)
            allocations.append((event['id'], layer, tenant, amount))
            totals[tenant] += amount
    charged = sum(totals.values())
    db.execute('INSERT INTO cache_statements VALUES (?, ?, ?)', (event['id'], event['node'], charged))
    db.executemany('INSERT INTO cache_charges VALUES (?, ?, ?, ?)', allocations)
    return {'decision': 'statement_closed', 'bytes': charged, 'tenant_bytes': dict(totals)}
