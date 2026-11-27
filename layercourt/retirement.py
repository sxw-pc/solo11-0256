"""Offline-node retirement evidence registration and settlement.

A reconnecting node may present two independent kinds of evidence for old
workload references:

``pod``
    One termination (exit) receipt plus one delete/purge receipt.  Both must
    bind to the same node, workload UID, reference generation and exit.

``outage``
    One frozen reference snapshot belonging to a single outage application,
    plus exit and purge phase evidence for exactly that application.  The
    whole batch is proven only when both phases are present for the same
    outage scope; two halves taken from different outage applications never
    combine, even when they mention the same UID.

Settlement writes proven releases back to ``workload_refs``; therefore
``eviction.protected`` (which only trusts active references) immediately
treats an exclusively released layer as evictable.  Every settlement receipt
records, per released reference, which evidence scope was actually used.  A
single settlement call uses exactly one granted scope and never mixes the two.

Limits per call: one node, at most two outage applications and ten references.
"""
import json

from .store import one, rows

MAX_REFS = 10
MAX_OUTAGES = 2

SCHEMA = '''
CREATE TABLE IF NOT EXISTS retirement_credentials(id TEXT PRIMARY KEY,node TEXT,uid TEXT,pod TEXT,ref TEXT,generation INTEGER,exit TEXT,termination_receipt TEXT,delete_receipt TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS retirement_outages(id TEXT PRIMARY KEY,node TEXT,witness TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS retirement_outage_refs(outage TEXT,ref TEXT,node TEXT,uid TEXT,pod TEXT,generation INTEGER,PRIMARY KEY(outage,ref));
CREATE TABLE IF NOT EXISTS retirement_outage_evidence(outage TEXT,phase TEXT,witness TEXT,receipt TEXT,source TEXT,PRIMARY KEY(outage,phase));
CREATE TABLE IF NOT EXISTS retirement_settlements(id TEXT PRIMARY KEY,node TEXT,authority TEXT,scope TEXT,request TEXT,result TEXT);
CREATE TABLE IF NOT EXISTS retirement_releases(settlement TEXT,ref TEXT,node TEXT,uid TEXT,pod TEXT,generation INTEGER,evidence TEXT,source TEXT,PRIMARY KEY(settlement,ref));
'''

_SNAPSHOT_OPS = {
    'retirement-outage', 'retirement-outage-open', 'retirement-outage-register',
    'retirement-outage-snapshot', 'retirement-outage-freeze',
    'retirement-outage-apply', 'retirement-application',
    'outage', 'outage-open', 'outage-register', 'outage-freeze',
    'retirement-freeze', 'retirement-snapshot', 'freeze',
}
_EVIDENCE_OPS = {
    'retirement-outage-evidence', 'retirement-outage-phase', 'retirement-phase',
    'retirement-phase-evidence', 'outage-evidence', 'outage-phase',
    'outage-credential', 'retirement-outage-credential',
}
_CREDENTIAL_OPS = {
    'retirement-credential', 'retirement-credential-register',
    'retirement-credential-record', 'retirement-register',
    'retirement-registration', 'retirement-record', 'retirement-evidence',
    'retirement-pod', 'retirement-pod-credential', 'retirement-pod-evidence',
    'pod-credential', 'pod-evidence', 'pod-receipt', 'credential',
    'credential-register', 'credential-record',
    'retire-credential', 'retire-register',
}
_SETTLE_OPS = {
    'retirement-settle', 'retirement-settlement', 'retirement-settlement-close',
    'retirement-close', 'retirement-finalize', 'retirement-settle-pod',
    'retirement-settle-outage', 'retirement-settle-batch',
    'retire-settle', 'settle', 'settlement',
}

_POD_AUTHORITIES = {'pod', 'per-pod', 'perpod', 'pod-duty', 'duty-pod', 'duty',
                    'pod-receipt', 'pod-credential', 'per-pod-duty'}
_OUTAGE_AUTHORITIES = {'outage', 'node', 'node-outage', 'outage-node', 'batch', 'site',
                       'shutdown', 'outage-snapshot', 'node-outage-settle',
                       'outage-settlement', 'node-settlement'}


def _field(x, *names, default=None):
    for name in names:
        value = x.get(name)
        if value not in (None, ''):
            return value
    return default


def _oid(x):
    return _field(x, 'outage', 'outage_id', 'application', 'outage_application')


def _snapshot_id(x):
    return _field(x, 'outage', 'outage_id', 'application', 'outage_application', 'id')


def _witness(x, fallback=''):
    return _field(x, 'witness', 'witness_id', 'attestation', default=fallback) or ''


def _termination(x):
    return _field(x, 'termination_receipt', 'termination_receipt_id', 'exit_receipt',
                  'exit_receipt_id', 'termination', 'termination_id',
                  'exit_evidence', 'termination_credential', 'exit_credential')


def _deletion(x):
    return _field(x, 'delete_receipt', 'delete_receipt_id', 'purge_receipt',
                  'purge_receipt_id', 'deletion_receipt', 'deletion', 'purge',
                  'purge_evidence', 'delete_evidence', 'purge_credential',
                  'clearance_receipt', 'eviction_receipt', 'removal_receipt')


def _exit_id(x):
    return _field(x, 'exit', 'exit_id', 'exit_event', 'exit_event_id',
                  'termination_exit', 'termination_event', 'termination_event_id',
                  'termination_at')


def _phase(x):
    raw = _field(x, 'phase', 'stage', 'kind')
    if raw is None:
        return None
    raw = str(raw).lower()
    if raw in ('exit', 'termination', 'terminated', 'pod-exit'):
        return 'exit'
    if raw in ('purge', 'delete', 'deleted', 'clear', 'cleared', 'removal', 'retire'):
        return 'purge'
    return None


def _authority(value, x):
    if _field(x, 'op') == 'retirement-settle-pod':
        return 'pod'
    if _field(x, 'op') == 'retirement-settle-outage':
        return 'outage'
    if value is None:
        return None
    value = str(value).lower().replace('_', '-').replace(' ', '-')
    if value in _POD_AUTHORITIES:
        return 'pod'
    if value in _OUTAGE_AUTHORITIES:
        return 'outage'
    return 'invalid'


def _active_ref(db, ref_id):
    ref = one(db, 'SELECT * FROM workload_refs WHERE id=?', (ref_id,))
    return ref if ref and ref['status'] == 'active' else None


def _resolve_item(db, node, item):
    """Resolve one snapshot entry (id or identifier mapping) to an active ref."""
    if isinstance(item, dict):
        ref_id = _field(item, 'ref', 'reference', 'ref_id', 'id')
        if ref_id is not None:
            ref = _active_ref(db, ref_id)
        else:
            uid = _field(item, 'uid', 'workload_uid', 'workload_id')
            generation = _field(item, 'generation', 'gen', 'ref_generation')
            pod = _field(item, 'pod', 'pod_name')
            if uid is None or generation is None:
                return None
            ref = one(db, '''SELECT * FROM workload_refs
                WHERE node=? AND uid=? AND generation=? AND status='active' ORDER BY id LIMIT 1''',
                      (node, uid, int(generation)))
            if ref and pod is not None and ref['pod'] != pod:
                return None
    else:
        ref = _active_ref(db, item)
    if not ref or ref['node'] != node:
        return None
    return ref


def _freeze(db, oid, node, requested, witness):
    refs = []
    if requested is None:
        refs = rows(db, "SELECT * FROM workload_refs WHERE node=? AND status='active' ORDER BY generation,id", (node,))
    else:
        if not isinstance(requested, list):
            return {'decision': 'outage_invalid', 'field': 'refs'}
        seen = set()
        for item in requested:
            ref = _resolve_item(db, node, item)
            if not ref:
                return {'decision': 'outage_reference_unavailable', 'item': item}
            if ref['id'] in seen:
                return {'decision': 'outage_reference_conflict', 'ref': ref['id']}
            seen.add(ref['id'])
            refs.append(ref)
    if not refs:
        return {'decision': 'outage_empty', 'outage': oid}
    if len(refs) > MAX_REFS:
        return {'decision': 'retirement_limit_exceeded', 'limit': MAX_REFS, 'field': 'refs'}
    db.execute('INSERT INTO retirement_outages VALUES(?,?,?,?)', (oid, node, witness, 'frozen'))
    for ref in refs:
        db.execute('INSERT INTO retirement_outage_refs VALUES(?,?,?,?,?,?)',
                   (oid, ref['id'], ref['node'], ref['uid'], ref['pod'], ref['generation']))
    return {'decision': 'outage_frozen', 'outage': oid, 'node': node,
            'refs': [ref['id'] for ref in refs], 'witness': witness}


def _snapshot_phase_fields(x):
    """Optional phase evidence carried by the outage application itself."""
    exit_witness = _field(x, 'exit_witness', 'termination_witness', 'exit_attestation')
    purge_witness = _field(x, 'purge_witness', 'delete_witness', 'purge_attestation')
    termination = _termination(x)
    deletion = _deletion(x)
    return exit_witness, purge_witness, termination, deletion


def _apply_snapshot_phases(db, outage, x):
    """Record the phase evidence an outage application may carry inline.

    Returns None when the inline evidence contradicts what is already stored,
    otherwise a (possibly empty) list of recorded/reused phases.
    """
    exit_witness, purge_witness, termination, deletion = _snapshot_phase_fields(x)
    if not termination and not deletion:
        return []
    scope = outage['witness'] or outage['id']
    source = _oid(x) or _field(x, 'id')
    phases = []
    if termination:
        state = _record_phase(db, outage, 'exit', termination,
                              exit_witness or scope, source)
        if not state:
            return None
        phases.append(('exit', state))
    if deletion:
        state = _record_phase(db, outage, 'purge', deletion,
                              purge_witness or scope, source)
        if not state:
            return None
        phases.append(('purge', state))
    return phases


def _requested_ids(db, stored_rows, requested):
    """Map a retried snapshot request back onto the stored reference ids."""
    if requested is None:
        return [row['ref'] for row in stored_rows]
    if not isinstance(requested, list):
        return None
    by_uid = {(row['uid'], row['generation']): row['ref'] for row in stored_rows}
    by_pod = {(row['pod'], row['generation']): row['ref'] for row in stored_rows}
    ids = []
    for item in requested:
        if isinstance(item, str):
            ids.append(item)
            continue
        if not isinstance(item, dict):
            return None
        ident = _field(item, 'ref', 'reference', 'ref_id', 'id')
        if ident is not None:
            ids.append(ident)
            continue
        uid = _field(item, 'uid', 'workload_uid', 'workload_id')
        pod = _field(item, 'pod', 'pod_name')
        generation = _field(item, 'generation', 'gen', 'ref_generation')
        if generation is not None:
            generation = int(generation)
            if uid is not None and (uid, generation) in by_uid:
                ids.append(by_uid[(uid, generation)])
                continue
            if pod is not None and (pod, generation) in by_pod:
                ids.append(by_pod[(pod, generation)])
                continue
        return None
    return ids


def _outage(db, x):
    oid = _snapshot_id(x)
    node = _field(x, 'node', 'node_id')
    if not oid or not node:
        return {'decision': 'outage_invalid'}
    witness = _witness(x)
    old = one(db, 'SELECT * FROM retirement_outages WHERE id=?', (oid,))
    if old:
        frozen = rows(db, 'SELECT * FROM retirement_outage_refs WHERE outage=? ORDER BY ref', (oid,))
        stored = [row['ref'] for row in frozen]
        ids = _requested_ids(db, frozen, x.get('refs'))
        if ids is None:
            return {'decision': 'outage_invalid', 'field': 'refs'}
        witness_conflict = bool(witness and old['witness'] and witness != old['witness'])
        if not witness_conflict and old['node'] == node and sorted(ids) == sorted(stored):
            phases = _apply_snapshot_phases(db, old, x)
            if phases is None:
                return {'decision': 'outage_identity_conflict', 'outage': oid}
            return {'decision': 'outage_reused', 'outage': oid, 'node': node,
                    'refs': stored, 'witness': old['witness']}
        return {'decision': 'outage_identity_conflict', 'outage': oid}
    result = _freeze(db, oid, node, x.get('refs'), witness)
    if result['decision'] != 'outage_frozen':
        return result
    outage = one(db, 'SELECT * FROM retirement_outages WHERE id=?', (oid,))
    phases = _apply_snapshot_phases(db, outage, x)
    if phases is None:
        return {'decision': 'outage_evidence_conflict', 'outage': oid}
    result['phases'] = sorted(phase for phase, _ in phases) if phases else []
    return result


def _credential_fields(x):
    return (_field(x, 'node', 'node_id'),
            _field(x, 'uid', 'workload_uid', 'workload_id'),
            _field(x, 'generation', 'ref_generation', 'gen'),
            _field(x, 'pod', 'pod_name'))


def _pod_credential(db, x):
    cid = _field(x, 'id', 'credential', 'credential_id')
    if not cid:
        return {'decision': 'credential_invalid'}
    ref_id = _field(x, 'ref', 'reference', 'ref_id')
    node, uid, generation, pod = _credential_fields(x)
    termination = _termination(x)
    deletion = _deletion(x)
    raw_exit = _exit_id(x)
    exit_id = raw_exit or termination

    # A replay of an already confirmed credential stays idempotent even after the
    # reference was settled; it never reaches a newer generation of the same name.
    old = one(db, 'SELECT * FROM retirement_credentials WHERE id=?', (cid,))
    if old:
        incoming = {'node': node, 'uid': uid, 'pod': pod, 'ref': ref_id,
                    'generation': generation, 'termination_receipt': termination,
                    'delete_receipt': deletion, 'exit': raw_exit}
        for key, value in incoming.items():
            if value is None:
                continue
            if key == 'generation':
                value = int(value)
            if old[key] != value:
                return {'decision': 'credential_identity_conflict', 'credential': cid,
                        'field': key}
        return {'decision': 'credential_reused', 'credential': cid, 'ref': old['ref']}

    if ref_id:
        ref = one(db, 'SELECT * FROM workload_refs WHERE id=?', (ref_id,))
        if not ref or ref['status'] != 'active':
            return {'decision': 'reference_unavailable', 'ref': ref_id}
    else:
        if not node or not uid or generation is None:
            return {'decision': 'credential_invalid'}
        ref = one(db, '''SELECT * FROM workload_refs
            WHERE node=? AND uid=? AND generation=? AND status='active' ORDER BY id LIMIT 1''',
                  (node, uid, int(generation)))
        if not ref:
            return {'decision': 'reference_unavailable'}
    if node is not None and ref['node'] != node:
        return {'decision': 'credential_mismatch', 'field': 'node'}
    if uid is not None and ref['uid'] != uid:
        return {'decision': 'credential_mismatch', 'field': 'uid'}
    if generation is not None and ref['generation'] != int(generation):
        return {'decision': 'credential_mismatch', 'field': 'generation'}
    if pod is not None and ref['pod'] != pod:
        return {'decision': 'credential_mismatch', 'field': 'pod'}
    if not termination or not deletion or not exit_id:
        return {'decision': 'credential_unproven'}
    if termination == deletion:
        return {'decision': 'credential_unproven', 'reason': 'receipts_not_independent'}
    # One exit belongs to exactly one reference generation.  Reusing a
    # confirmed exit for a different (later, same-name) reference is refused so
    # a retransmission can never sweep in a new generation.
    bound = one(db, 'SELECT * FROM retirement_credentials WHERE exit=? AND ref<>? LIMIT 1',
                (exit_id, ref['id']))
    if bound:
        return {'decision': 'credential_identity_conflict', 'field': 'exit',
                'credential': cid}
    duplicate = one(db, '''SELECT * FROM retirement_credentials
        WHERE ref=? AND exit=? AND termination_receipt=? AND delete_receipt=?''',
                    (ref['id'], exit_id, termination, deletion))
    if duplicate:
        return {'decision': 'credential_reused', 'credential': duplicate['id'], 'ref': ref['id']}
    db.execute('INSERT INTO retirement_credentials VALUES(?,?,?,?,?,?,?,?,?,?)',
               (cid, ref['node'], ref['uid'], ref['pod'], ref['id'], ref['generation'],
                exit_id, termination, deletion, 'confirmed'))
    return {'decision': 'credential_recorded', 'credential': cid, 'ref': ref['id'],
            'uid': ref['uid'], 'generation': ref['generation'], 'exit': exit_id}


def _record_phase(db, outage, phase, receipt, witness, source):
    """Record one outage phase.  Returns 'recorded', 'reused' or None on conflict."""
    old = one(db, 'SELECT * FROM retirement_outage_evidence WHERE outage=? AND phase=?',
              (outage['id'], phase))
    if old:
        if old['receipt'] == receipt and old['witness'] == witness:
            return 'reused'
        return None
    db.execute('INSERT INTO retirement_outage_evidence VALUES(?,?,?,?,?)',
               (outage['id'], phase, witness, receipt, source or ''))
    return 'recorded'


def _phase_evidence(db, x):
    oid = _snapshot_id(x)
    if not oid:
        return {'decision': 'evidence_invalid'}
    node = _field(x, 'node', 'node_id')
    outage = one(db, 'SELECT * FROM retirement_outages WHERE id=?', (oid,))
    if not outage:
        requested = x.get('refs')
        if requested is None or not node:
            return {'decision': 'outage_unavailable', 'outage': oid}
        frozen = _freeze(db, oid, node, requested, _witness(x))
        if frozen['decision'] != 'outage_frozen':
            return frozen
        outage = one(db, 'SELECT * FROM retirement_outages WHERE id=?', (oid,))
    if node is not None and outage['node'] != node:
        return {'decision': 'outage_node_mismatch', 'outage': oid}
    phase = _phase(x)
    source = _field(x, 'id', 'evidence_id', 'credential')
    witness = _witness(x, outage['witness'] or outage['id'])
    if phase is None:
        termination, deletion = _termination(x), _deletion(x)
        if not termination and not deletion:
            return {'decision': 'evidence_invalid', 'outage': oid}
        if termination and deletion and termination == deletion:
            return {'decision': 'evidence_unproven', 'outage': oid,
                    'reason': 'receipts_not_independent'}
        states = []
        if termination:
            state = _record_phase(db, outage, 'exit', termination,
                                  _field(x, 'exit_witness', 'termination_witness') or witness,
                                  source)
            if not state:
                return {'decision': 'evidence_identity_conflict', 'outage': oid, 'phase': 'exit'}
            states.append(state)
        if deletion:
            state = _record_phase(db, outage, 'purge', deletion,
                                  _field(x, 'purge_witness', 'delete_witness') or witness,
                                  source)
            if not state:
                return {'decision': 'evidence_identity_conflict', 'outage': oid, 'phase': 'purge'}
            states.append(state)
        return {'decision': 'evidence_recorded' if 'recorded' in states else 'evidence_reused',
                'outage': oid,
                'phases': [name for name, present in (('exit', termination), ('purge', deletion)) if present],
                'witness': witness}
    receipt = _termination(x) if phase == 'exit' else _deletion(x)
    if not receipt:
        receipt = _field(x, 'receipt', 'evidence', 'receipt_id')
    if not receipt:
        return {'decision': 'evidence_unproven', 'outage': oid, 'phase': phase}
    state = _record_phase(db, outage, phase, receipt, witness, source)
    if not state:
        return {'decision': 'evidence_identity_conflict', 'outage': oid, 'phase': phase}
    return {'decision': 'evidence_recorded' if state == 'recorded' else 'evidence_reused',
            'outage': oid, 'phase': phase, 'witness': witness}


def _credential(db, x):
    if _oid(x) or _phase(x):
        return _phase_evidence(db, x)
    return _pod_credential(db, x)


def _phase_evidence_row(db, oid, phase):
    return one(db, 'SELECT * FROM retirement_outage_evidence WHERE outage=? AND phase=?',
               (oid, phase))


def _proven_outage(db, outage):
    exit_evidence = _phase_evidence_row(db, outage['id'], 'exit')
    purge_evidence = _phase_evidence_row(db, outage['id'], 'purge')
    if not exit_evidence or not purge_evidence:
        return False
    # Both phases must name the same witness for this same outage application.
    # When a phase does not carry its own witness, the outage itself is the
    # shared witness, so phases recorded for the one freeze still agree.
    scope = outage['witness'] or outage['id']
    exit_witness = exit_evidence['witness'] or scope
    purge_witness = purge_evidence['witness'] or scope
    if exit_witness != purge_witness:
        return False
    if outage['witness'] and exit_witness != outage['witness']:
        return False
    return True


def _release_detail(ref, scope, source, termination, deletion, **extra):
    detail = {'ref': ref['id'], 'reference': ref['id'], 'uid': ref['uid'],
              'pod': ref['pod'], 'generation': ref['generation'],
              'scope': scope, 'evidence': scope, 'source': source,
              'termination_receipt': termination, 'delete_receipt': deletion}
    detail.update(extra)
    return detail


def _sorted_refs(db, node):
    return rows(db, "SELECT * FROM workload_refs WHERE node=? AND status='active' ORDER BY id", (node,))


def _candidate_authority(db, node):
    """Pick the scope the node is granted and holds complete evidence for.

    Per-pod duty evidence is preferred when it can release something, because
    that is the finer, directly proven scope.  The outage scope is only used
    when it alone carries a proven snapshot.  The two are never merged.
    """
    for credential in rows(db, 'SELECT * FROM retirement_credentials WHERE node=? ORDER BY id', (node,)):
        ref = one(db, 'SELECT * FROM workload_refs WHERE id=?', (credential['ref'],))
        if ref and ref['status'] == 'active' and (
                ref['node'], ref['uid'], ref['pod'], ref['generation']) == (
                credential['node'], credential['uid'], credential['pod'], credential['generation']):
            return 'pod'
    for outage in rows(db, 'SELECT * FROM retirement_outages WHERE node=? ORDER BY id', (node,)):
        if _proven_outage(db, outage):
            return 'outage'
    if one(db, 'SELECT 1 FROM retirement_outages WHERE node=? LIMIT 1', (node,)):
        return 'outage'
    return 'pod'


def _settle_pod(db, node, ref_filter):
    releases = {}
    for credential in rows(db, 'SELECT * FROM retirement_credentials WHERE node=? ORDER BY id', (node,)):
        ref = one(db, 'SELECT * FROM workload_refs WHERE id=?', (credential['ref'],))
        if not ref or ref['status'] != 'active':
            continue
        if (ref['node'], ref['uid'], ref['pod'], ref['generation']) != (
                credential['node'], credential['uid'], credential['pod'], credential['generation']):
            continue
        if ref_filter is not None and ref['id'] not in ref_filter:
            continue
        releases[ref['id']] = _release_detail(
            ref, 'pod', credential['id'], credential['termination_receipt'],
            credential['delete_receipt'], credential=credential['id'],
            exit=credential['exit'])
    return releases, []


def _settle_outage(db, node, outage_ids, ref_filter):
    releases = {}
    used = []
    for oid in outage_ids:
        outage = one(db, 'SELECT * FROM retirement_outages WHERE id=?', (oid,))
        if not outage or outage['node'] != node:
            continue
        if not _proven_outage(db, outage):
            continue
        used.append(oid)
        exit_evidence = _phase_evidence_row(db, oid, 'exit')
        purge_evidence = _phase_evidence_row(db, oid, 'purge')
        for snapshot in rows(db, 'SELECT * FROM retirement_outage_refs WHERE outage=? ORDER BY ref', (oid,)):
            ref = one(db, 'SELECT * FROM workload_refs WHERE id=?', (snapshot['ref'],))
            if not ref or ref['status'] != 'active':
                continue
            # The frozen row must still describe this very reference generation;
            # a later same-name generation is never admitted to the snapshot.
            if (ref['node'], ref['uid'], ref['pod'], ref['generation']) != (
                    snapshot['node'], snapshot['uid'], snapshot['pod'], snapshot['generation']):
                continue
            if ref_filter is not None and ref['id'] not in ref_filter:
                continue
            releases.setdefault(ref['id'], _release_detail(
                ref, 'outage', oid, exit_evidence['receipt'], purge_evidence['receipt'],
                outage=oid, witness=outage['witness']))
    return releases, used


def _settle(db, x):
    sid = _field(x, 'id', 'settlement', 'settlement_id')
    node = _field(x, 'node', 'node_id')
    if not sid or not node:
        return {'decision': 'settlement_invalid'}
    authority = _authority(_field(x, 'authority', 'grant', 'scope', 'method',
                                  'authorization'), x)
    if authority == 'invalid':
        return {'decision': 'settlement_invalid', 'field': 'authority'}

    requested_outages = x.get('outages')
    if requested_outages is not None:
        if isinstance(requested_outages, str):
            requested_outages = [requested_outages]
        if not isinstance(requested_outages, list):
            return {'decision': 'settlement_invalid', 'field': 'outages'}
        requested_outages = list(requested_outages)
    elif authority != 'pod':
        single = _oid(x)
        requested_outages = [single] if single else None
    if authority == 'pod' and requested_outages is not None:
        # A pod-authorised settlement may not borrow the outage scope.
        return {'decision': 'settlement_invalid', 'field': 'outages', 'scope': 'pod'}
    if requested_outages is not None:
        if len(requested_outages) > MAX_OUTAGES:
            return {'decision': 'retirement_limit_exceeded', 'limit': MAX_OUTAGES, 'field': 'outages'}
        if len(set(requested_outages)) != len(requested_outages):
            return {'decision': 'settlement_invalid', 'field': 'outages'}
        for oid in requested_outages:
            outage = one(db, 'SELECT * FROM retirement_outages WHERE id=?', (oid,))
            if outage and outage['node'] != node:
                return {'decision': 'settlement_invalid', 'field': 'outages'}

    requested_refs = x.get('refs')
    if requested_refs is None:
        requested_refs = x.get('references')
    if requested_refs is None:
        single_ref = _field(x, 'ref', 'reference')
        requested_refs = [single_ref] if single_ref is not None else None
    elif isinstance(requested_refs, str):
        requested_refs = [requested_refs]
    if requested_refs is not None:
        if not isinstance(requested_refs, list):
            return {'decision': 'settlement_invalid', 'field': 'refs'}
        resolved = []
        for row in requested_refs:
            if isinstance(row, str):
                resolved.append(row)
            elif isinstance(row, dict):
                resolved.append(_field(row, 'ref', 'reference', 'ref_id', 'id'))
            else:
                resolved.append(None)
        if any(item is None for item in resolved):
            return {'decision': 'settlement_invalid', 'field': 'refs'}
        if len(resolved) > MAX_REFS:
            return {'decision': 'retirement_limit_exceeded', 'limit': MAX_REFS, 'field': 'refs'}
        requested_refs = list(dict.fromkeys(resolved))
    ref_filter = set(requested_refs) if requested_refs is not None else None

    if authority is None:
        if requested_outages is not None:
            authority = 'outage'
        else:
            authority = _candidate_authority(db, node)

    if authority == 'outage' and requested_outages is None:
        available = [row['id'] for row in rows(
            db, 'SELECT id FROM retirement_outages WHERE node=? ORDER BY id', (node,))]
        if len(available) > MAX_OUTAGES:
            return {'decision': 'retirement_limit_exceeded', 'limit': MAX_OUTAGES,
                    'field': 'outages', 'available': available}
        requested_outages = available

    request = json.dumps({'node': node, 'authority': authority,
                          'outages': requested_outages, 'refs': requested_refs},
                         sort_keys=True, ensure_ascii=False)
    old = one(db, 'SELECT * FROM retirement_settlements WHERE id=?', (sid,))
    if old:
        if old['authority'] != authority:
            return {'decision': 'settlement_identity_conflict', 'settlement': sid}
        if old['request'] == request:
            result = json.loads(old['result'])
            result['decision'] = 'settlement_reused'
            return result
        return {'decision': 'settlement_identity_conflict', 'settlement': sid}

    if authority == 'pod':
        releases, used_outages = _settle_pod(db, node, ref_filter)
    else:
        releases, used_outages = _settle_outage(db, node, requested_outages or [], ref_filter)

    if len(releases) > MAX_REFS:
        return {'decision': 'retirement_limit_exceeded', 'limit': MAX_REFS,
                'field': 'refs', 'eligible': len(releases)}

    ordered = [releases[key] for key in sorted(releases)]
    retained = [row['id'] for row in _sorted_refs(db, node) if row['id'] not in releases]
    base = {'settlement': sid, 'node': node, 'scope': authority,
            'authority': authority, 'released_refs': [detail['ref'] for detail in ordered],
            'releases': ordered, 'retained': retained[:MAX_REFS], 'outages': used_outages,
            'evidence_scope': {detail['ref']: detail['evidence'] for detail in ordered}}
    if not ordered:
        base['decision'] = 'settlement_denied'
        base['reason'] = 'evidence_incomplete_or_absent'
        base['released'] = []
        return base

    for detail in ordered:
        db.execute("UPDATE workload_refs SET status='released' WHERE id=? AND status='active'",
                   (detail['ref'],))
        db.execute('INSERT INTO retirement_releases VALUES(?,?,?,?,?,?,?,?)',
                   (sid, detail['ref'], node, detail['uid'], detail['pod'],
                    detail['generation'], detail['evidence'], detail['source']))
        db.execute('INSERT INTO lease_claims VALUES(?,?,?,?,?,?,?)',
                   ('retirement:' + sid + ':' + detail['ref'], detail['ref'],
                    detail['uid'], 'release', detail['termination_receipt'],
                    detail['delete_receipt'], 'accepted'))
    result = dict(base, decision='settled', released=ordered, count=len(ordered))
    db.execute('INSERT INTO retirement_settlements VALUES(?,?,?,?,?,?)',
               (sid, node, authority, authority, request,
                json.dumps(result, sort_keys=True, ensure_ascii=False)))
    return result


def _classify(op):
    """Classify an operation name, tolerating unknown spellings by keywords."""
    if op in _SNAPSHOT_OPS:
        return 'snapshot'
    if op in _EVIDENCE_OPS:
        return 'evidence'
    if op in _CREDENTIAL_OPS:
        return 'credential'
    if op in _SETTLE_OPS:
        return 'settle'
    token = str(op).lower().replace('_', '-')
    compact = token.replace('-', '')
    if 'settle' in compact or 'settlement' in compact or 'finalize' in compact:
        return 'settle'
    if 'outage' in compact or 'freeze' in compact or 'snapshot' in compact:
        if any(word in compact for word in ('open', 'freeze', 'snapshot', 'register',
                                             'application', 'apply', 'start')):
            return 'snapshot'
        if any(word in compact for word in ('evidence', 'phase', 'credential', 'receipt')):
            return 'evidence'
        return 'snapshot'
    if any(word in compact for word in ('credential', 'evidence', 'receipt')):
        return 'credential'
    return None


def handle(db, x):
    kind = _classify(x['op'])
    if kind == 'snapshot':
        return _outage(db, x)
    if kind == 'evidence':
        return _phase_evidence(db, x)
    if kind == 'credential':
        return _credential(db, x)
    if kind == 'settle':
        return _settle(db, x)
    return {'decision': 'unknown_operation'}


OPERATIONS = _SNAPSHOT_OPS | _EVIDENCE_OPS | _CREDENTIAL_OPS | _SETTLE_OPS
