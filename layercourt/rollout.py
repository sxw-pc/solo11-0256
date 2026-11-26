"""Approve an offline publication wave when all targeted nodes can start it."""
from .store import one, rows

SCHEMA = '''
CREATE TABLE IF NOT EXISTS rollout_nodes (
    node TEXT PRIMARY KEY, arch TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rollout_waves (
    id TEXT PRIMARY KEY, manifest TEXT NOT NULL, status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rollout_seats (
    wave TEXT NOT NULL, node TEXT NOT NULL, PRIMARY KEY (wave, node)
);
'''


def missing_layers(db, node, manifest):
    return [row['layer'] for row in rows(db, '''
        SELECT e.layer FROM manifest_edges AS e
        WHERE e.manifest = ? AND NOT EXISTS (
            SELECT 1 FROM node_caches AS c
            WHERE c.node = ? AND c.layer = e.layer AND c.status = 'present'
        ) ORDER BY e.layer
    ''', (manifest, node))]


def handle(db, event):
    op = event['op']
    if op == 'node-enroll':
        if not event['node'] or not event['arch']:
            return {'decision': 'node_invalid'}
        known = one(db, 'SELECT arch FROM rollout_nodes WHERE node = ?', (event['node'],))
        if known:
            return {'decision': 'node_enrolled' if known['arch'] == event['arch'] else 'node_arch_conflict'}
        db.execute('INSERT INTO rollout_nodes VALUES (?, ?)', (event['node'], event['arch']))
        return {'decision': 'node_enrolled'}
    if op != 'wave-approve':
        return {'decision': 'unknown_operation'}
    if one(db, 'SELECT 1 FROM rollout_waves WHERE id = ?', (event['id'],)):
        return {'decision': 'wave_id_taken'}
    manifest = one(db, 'SELECT arch, status FROM manifests WHERE id = ?', (event['manifest'],))
    if not manifest or manifest['status'] != 'active':
        return {'decision': 'manifest_unavailable'}
    if not one(db, 'SELECT 1 FROM manifest_edges WHERE manifest = ?', (event['manifest'],)):
        return {'decision': 'empty_release'}
    if not event['nodes'] or len(set(event['nodes'])) != len(event['nodes']):
        return {'decision': 'invalid_wave_nodes'}
    blocked = {}
    for node in sorted(event['nodes']):
        platform = one(db, 'SELECT arch FROM rollout_nodes WHERE node = ?', (node,))
        if not platform or platform['arch'] != manifest['arch']:
            blocked[node] = {'reason': 'platform_mismatch'}
            continue
        missing = missing_layers(db, node, event['manifest'])
        if missing:
            blocked[node] = {'reason': 'incomplete_cache', 'layers': missing}
    if blocked:
        return {'decision': 'wave_waiting', 'blocked': blocked}
    db.execute('INSERT INTO rollout_waves VALUES (?, ?, ?)', (event['id'], event['manifest'], 'approved'))
    db.executemany('INSERT INTO rollout_seats VALUES (?, ?)', ((event['id'], node) for node in sorted(event['nodes'])))
    return {'decision': 'wave_approved', 'nodes': sorted(event['nodes'])}
