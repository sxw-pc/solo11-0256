"""Approve an offline publication wave when all targeted nodes can start it.

A plain ``wave-approve`` keeps the original whole-batch readiness verdict.
A ``wave-approve`` carrying switching ``constraints`` instead plans an
executable sequence of switch batches: nodes sharing a local publication
barrier may move together (and observers outside the barrier never see a
partial result), while hot-swap nodes may temporarily run a whitelisted
transition manifest that stays compatible with every neighbour.

Every directed call edge must stay compatible in each reachable state,
including the visible intermediate states between two single-node switches.
A manifest is only usable while it is still active, matches the node
architecture and its whole layer closure is resident on the node.  When a
transition layer is evicted (the cache row leaves ``present``) the
transition route collapses; the planner must then re-sign a still valid
batch or explicitly wait instead of issuing a retried false permit.
"""
import json
from collections import deque
from itertools import combinations

from .store import one, rows

MAX_WAVE_NODES = 8

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
CREATE TABLE IF NOT EXISTS rollout_plans (
    wave TEXT PRIMARY KEY, steps INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rollout_plan_steps (
    wave TEXT NOT NULL, step INTEGER NOT NULL, kind TEXT NOT NULL,
    nodes TEXT NOT NULL, assignments TEXT NOT NULL,
    barrier TEXT, PRIMARY KEY (wave, step)
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


def _startable(db, node, manifest, arch):
    """A node may only start an active, architecture-matching, fully cached manifest."""
    record = one(db, 'SELECT arch, status FROM manifests WHERE id = ?', (manifest,))
    if not record or record['status'] != 'active':
        return False, 'manifest_unavailable'
    if record['arch'] != arch:
        return False, 'platform_mismatch'
    missing = missing_layers(db, node, manifest)
    if missing:
        return False, 'incomplete_cache'
    return True, None


def _normalise_constraints(nodes, constraints):
    """Validate the switching payload and return deterministic, sorted structures."""
    if not isinstance(constraints, dict):
        return None, {'reason': 'constraint_invalid', 'detail': 'constraints_must_be_object'}
    entries = constraints.get('manifests')
    edges = constraints.get('edges', [])
    if not isinstance(entries, dict) or not isinstance(edges, list):
        return None, {'reason': 'constraint_invalid', 'detail': 'manifest_map_or_edges_invalid'}
    node_set = set(nodes)
    manifests = {}
    for node in nodes:
        entry = entries.get(node)
        if not isinstance(entry, dict):
            return None, {'reason': 'constraint_invalid', 'detail': f'manifest_entry_missing:{node}'}
        current = entry.get('current')
        target = entry.get('target')
        transitions = entry.get('transitions', [])
        if not current or not target or not isinstance(transitions, list):
            return None, {'reason': 'constraint_invalid', 'detail': f'manifest_entry_invalid:{node}'}
        if any(not isinstance(m, str) or not m for m in (current, target, *transitions)):
            return None, {'reason': 'constraint_invalid', 'detail': f'manifest_id_invalid:{node}'}
        manifests[node] = (current, target, tuple(sorted(set(transitions))))
    seen_edges = set()
    normalised_edges = []
    for edge in edges:
        if not isinstance(edge, dict):
            return None, {'reason': 'constraint_invalid', 'detail': 'edge_not_object'}
        src, dst = edge.get('from'), edge.get('to')
        if src not in node_set or dst not in node_set:
            return None, {'reason': 'constraint_invalid', 'detail': f'edge_endpoint_unknown:{src}->{dst}'}
        if (src, dst) in seen_edges:
            return None, {'reason': 'constraint_invalid', 'detail': f'duplicate_edge:{src}->{dst}'}
        pairs = edge.get('compatible')
        if not isinstance(pairs, list) or not pairs:
            return None, {'reason': 'constraint_invalid', 'detail': f'compatibility_missing:{src}->{dst}'}
        allowed = set()
        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                return None, {'reason': 'constraint_invalid', 'detail': f'compatibility_pair_invalid:{src}->{dst}'}
            caller, callee = pair
            if not isinstance(caller, str) or not isinstance(callee, str):
                return None, {'reason': 'constraint_invalid', 'detail': f'compatibility_pair_invalid:{src}->{dst}'}
            allowed.add((caller, callee))
        seen_edges.add((src, dst))
        normalised_edges.append((src, dst, frozenset(allowed)))
    hot_swap = constraints.get('hot_swap', [])
    if not isinstance(hot_swap, list) or any(node not in node_set for node in hot_swap):
        return None, {'reason': 'constraint_invalid', 'detail': 'hot_swap_invalid'}
    hot_swap = set(hot_swap)
    barrier_groups = []
    covered = set()
    for group in constraints.get('barriers', []):
        if not isinstance(group, dict) or not isinstance(group.get('id'), str) or not group['id']:
            return None, {'reason': 'constraint_invalid', 'detail': 'barrier_invalid'}
        members = group.get('nodes')
        if not isinstance(members, list) or not members or len(set(members)) != len(members):
            return None, {'reason': 'constraint_invalid', 'detail': 'barrier_members_invalid'}
        if any(node not in node_set for node in members) or any(node in covered for node in members):
            return None, {'reason': 'constraint_invalid', 'detail': 'barrier_members_unknown_or_shared'}
        members = frozenset(members)
        covered |= members
        barrier_groups.append((group['id'], members))
    return {
        'manifests': manifests,
        'edges': tuple(sorted(normalised_edges)),
        'hot_swap': hot_swap,
        'barriers': tuple(barrier_groups),
    }, None


def _state_safe(state, index, edges):
    """Every directed edge must be compatible in this observable state."""
    for src, dst, allowed in edges:
        if (state[index[src]], state[index[dst]]) not in allowed:
            return False
    return True


def _search_switch_sequence(nodes, model, startable):
    """Breadth-first search for a safe switch sequence reaching all targets.

    Moves are either one node switching alone, or one non-empty subset of a
    barrier group switching together in a single step.  Every generated state
    is checked against all edges, so no incompatible intermediate state is
    ever admitted.
    """
    index = {node: i for i, node in enumerate(nodes)}
    start = tuple(model['manifests'][node][0] for node in nodes)
    goal = tuple(model['manifests'][node][1] for node in nodes)
    edges = model['edges']
    if not _state_safe(start, index, edges):
        return None, {'reason': 'edge_incompatible', 'phase': 'initial'}
    visited = {start}
    queue = deque([(start, ())])
    while queue:
        state, path = queue.popleft()
        if state == goal:
            return path, None
        moves = []
        for i, node in enumerate(nodes):
            _, target, transitions = model['manifests'][node]
            if state[i] == target:
                continue
            candidates = [target]
            if node in model['hot_swap']:
                candidates.extend(m for m in transitions if m != target)
            for manifest in dict.fromkeys(candidates):
                if manifest == state[i] or not startable[node][manifest]:
                    continue
                new_state = list(state)
                new_state[i] = manifest
                moves.append(((node,), tuple(new_state), None))
        for barrier_id, members in model['barriers']:
            pending = sorted(node for node in members if state[index[node]] != model['manifests'][node][1])
            for width in range(1, len(pending) + 1):
                for combo in combinations(range(len(pending)), width):
                    batch = [pending[pos] for pos in combo]
                    new_state = list(state)
                    for node in batch:
                        target = model['manifests'][node][1]
                        if not startable[node][target]:
                            new_state = None
                            break
                        new_state[index[node]] = target
                    if new_state is None:
                        continue
                    moves.append((tuple(batch), tuple(new_state), barrier_id))
        for ordered, new_state, barrier_id in moves:
            if new_state in visited or not _state_safe(new_state, index, edges):
                continue
            visited.add(new_state)
            queue.append((new_state, path + ((barrier_id, ordered, new_state),)))
    return None, {'reason': 'no_safe_sequence'}


def _diagnose(nodes, model, startable):
    """Explain which edge blocks the board when no executable sequence exists."""
    blocked_edges = []
    for src, dst, allowed in model['edges']:
        src_manifests = {model['manifests'][src][0], model['manifests'][src][1]}
        dst_manifests = {model['manifests'][dst][0], model['manifests'][dst][1]}
        if src in model['hot_swap']:
            src_manifests.update(model['manifests'][src][2])
        if dst in model['hot_swap']:
            dst_manifests.update(model['manifests'][dst][2])
        for caller in sorted(src_manifests):
            for callee in sorted(dst_manifests):
                if (caller, callee) not in allowed:
                    blocked_edges.append({'edge': f'{src}->{dst}', 'from': caller, 'to': callee})
    unavailable = {
        node: sorted(manifest for manifest, ok in startable[node].items() if not ok)
        for node in nodes
    }
    unavailable = {node: ids for node, ids in unavailable.items() if ids}
    return blocked_edges, unavailable


def _handle_constrained_wave(db, event, nodes):
    model, error = _normalise_constraints(nodes, event['constraints'])
    if error:
        return {'decision': 'wave_waiting', 'blocked': {'constraints': error}}
    platforms = {}
    for node in nodes:
        platform = one(db, 'SELECT arch FROM rollout_nodes WHERE node = ?', (node,))
        if not platform:
            return {'decision': 'wave_waiting',
                    'blocked': {node: [{'reason': 'node_unknown'}]}}
        platforms[node] = platform['arch']
    startable = {}
    blocked = {}
    for node in nodes:
        current, target, transitions = model['manifests'][node]
        arch = platforms[node]
        startable[node] = {}
        for manifest in (current, target, *transitions):
            if manifest in startable[node]:
                continue
            ok, reason = _startable(db, node, manifest, arch)
            startable[node][manifest] = ok
            if not ok:
                blocked.setdefault(node, []).append({'reason': reason, 'manifest': manifest})
    # A node that cannot keep running its current manifest or reach its target
    # has no legal move at all. A lost transition manifest instead removes one
    # route; planning must still prove whether another sequence remains.
    hard_blocked = {
        node: [item for item in blocked.get(node, [])
               if item['manifest'] in (model['manifests'][node][0], model['manifests'][node][1])]
        for node in nodes
    }
    hard_blocked = {node: items for node, items in hard_blocked.items() if items}
    if hard_blocked:
        return {'decision': 'wave_waiting', 'blocked': hard_blocked}
    sequence, failure = _search_switch_sequence(nodes, model, startable)
    if failure:
        edge_blocks, unavailable = _diagnose(nodes, model, startable)
        waiting = {'decision': 'wave_waiting'}
        if failure.get('phase') == 'initial':
            # The board as presented already breaks an edge: no step is licensable.
            waiting['reason'] = 'initial_state_incompatible'
            waiting['blocked_edges'] = edge_blocks
            return waiting
        if edge_blocks:
            waiting['blocked_edges'] = edge_blocks
        if unavailable:
            waiting['unavailable_manifests'] = unavailable
        waiting['reason'] = failure['reason']
        return waiting
    steps = []
    upgraded_batches = []
    transition_nodes = set()
    for step_no, (barrier_id, batch, state) in enumerate(sequence, start=1):
        assignments = {node: state[index] for index, node in enumerate(nodes) if node in batch}
        if barrier_id is not None:
            kind = 'barrier'
            upgraded_batches.append({'step': step_no, 'barrier': barrier_id, 'nodes': list(batch)})
        else:
            node = batch[0]
            kind = 'transition' if state[list(nodes).index(node)] != model['manifests'][node][1] else 'switch'
            if kind == 'switch':
                upgraded_batches.append({'step': step_no, 'nodes': list(batch)})
            else:
                transition_nodes.add(node)
        steps.append({
            'step': step_no,
            'kind': kind,
            'nodes': list(batch),
            'assignments': assignments,
            **({'barrier': barrier_id} if barrier_id is not None else {}),
        })
    db.execute('INSERT INTO rollout_waves VALUES (?, ?, ?)',
               (event['id'], event.get('manifest') or event['constraints']['manifests'][nodes[0]]['target'], 'approved'))
    db.executemany('INSERT INTO rollout_seats VALUES (?, ?)',
                   ((event['id'], node) for node in nodes))
    db.execute('INSERT INTO rollout_plans VALUES (?, ?)', (event['id'], len(steps)))
    db.executemany('INSERT INTO rollout_plan_steps VALUES (?, ?, ?, ?, ?, ?)', (
        (event['id'], step['step'], step['kind'],
         json.dumps(step['nodes'], sort_keys=True),
         json.dumps(step['assignments'], sort_keys=True, ensure_ascii=False),
         step.get('barrier'))
        for step in steps
    ))
    return {
        'decision': 'wave_approved',
        'nodes': nodes,
        'plan': steps,
        'upgraded_batches': upgraded_batches,
        'transition_nodes': sorted(transition_nodes),
    }


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
    if not event['nodes'] or len(set(event['nodes'])) != len(event['nodes']):
        return {'decision': 'invalid_wave_nodes'}
    if len(event['nodes']) > MAX_WAVE_NODES:
        return {'decision': 'wave_too_large', 'limit': MAX_WAVE_NODES}
    if event.get('constraints') is not None:
        return _handle_constrained_wave(db, event, sorted(event['nodes']))
    manifest = one(db, 'SELECT arch, status FROM manifests WHERE id = ?', (event['manifest'],))
    if not manifest or manifest['status'] != 'active':
        return {'decision': 'manifest_unavailable'}
    if not one(db, 'SELECT 1 FROM manifest_edges WHERE manifest = ?', (event['manifest'],)):
        return {'decision': 'empty_release'}
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
