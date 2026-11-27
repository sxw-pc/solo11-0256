"""Close a node cache statement against the tenants of current workload refs.

Two settlement contracts coexist:

* legacy settlement (a ``statement-close`` without an ``agreement``) keeps the
  original rule: every resident layer referenced by active workloads is split
  evenly across its distinct referencing tenants;
* responsibility-agreement settlement charges each referenced physical layer
  exactly once, in full, to a single payer that is entitled to bear that whole
  layer (one of its current beneficiary tenants, or the platform under a
  dedicated, per-layer backstop commitment), while no payer may exceed the
  commitment it made for the statement.

Backstop money is ring-fenced per layer: a platform commitment for one layer
can never be lent to another layer, and the platform only becomes an eligible
payer when every tenant currently benefiting from the layer has authorized it.
Beneficiaries are recomputed from *active* references at close time, so a
``reference-release`` accepted by ``retention`` reshapes eligibility and can
both remove a billable layer and invalidate a previously valid backstop.
"""
from collections import defaultdict

from .store import one, rows

SCHEMA = '''
CREATE TABLE IF NOT EXISTS charge_tenants (
    ref TEXT PRIMARY KEY, tenant TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cache_statements (
    id TEXT PRIMARY KEY, node TEXT NOT NULL, charged_bytes INTEGER NOT NULL,
    mode TEXT NOT NULL DEFAULT 'legacy'
);
CREATE TABLE IF NOT EXISTS cache_charges (
    statement TEXT NOT NULL, layer TEXT NOT NULL, tenant TEXT,
    bytes INTEGER NOT NULL, route TEXT NOT NULL DEFAULT 'direct',
    PRIMARY KEY (statement, layer, tenant)
);
CREATE TABLE IF NOT EXISTS backstop_commits (
    layer TEXT PRIMARY KEY, bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS backstop_authorizations (
    layer TEXT NOT NULL, tenant TEXT NOT NULL, status TEXT NOT NULL,
    PRIMARY KEY (layer, tenant)
);
'''

# Internal payer token for the platform. It is a sentinel (never a tenant name)
# so platform capacity can never be merged with a tenant's commitment.
PLATFORM = object()
PLATFORM_NAME = 'platform'
ROUTE_DIRECT = 'direct'
ROUTE_BACKSTOP = 'platform-backstop'
MODE_AGREEMENT = 'agreement'


def claimants(db, node):
    """Current beneficiary tenants of each resident layer on the node.

    Only active references count: a released reference drops the tenant from
    the layer's beneficiary set immediately, and a layer without any remaining
    active reference stops being a billable physical layer.
    """
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


def _active_authorizations(db, layer):
    return {row['tenant'] for row in rows(
        db,
        'SELECT tenant FROM backstop_authorizations WHERE layer = ? AND status = ?',
        (layer, 'active'))}


def caps_cover_none(layer, size_of, eligible, caps):
    """True when no eligible payer has room for the whole layer."""
    return all(caps.get(payer, 0) < size_of[layer] for payer in eligible[layer])


def _agreement_invalid(event):
    agreement = event.get('agreement')
    if not isinstance(agreement, dict):
        return True
    commitments = agreement.get('commitments', {})
    if not isinstance(commitments, dict):
        return True
    for tenant, cap in commitments.items():
        if not tenant or not isinstance(cap, int) or isinstance(cap, bool) or cap < 0:
            return True
    return False


def _solve(layer_ids, size_of, eligible, caps):
    """Assign whole layers to payers under cumulative commitment caps.

    ``eligible[layer]`` is the ordered list of payers allowed to bear the whole
    layer; ``caps[payer]`` bounds that payer's cumulative bytes across the whole
    statement. Multiple references from one tenant do not appear here -- a
    tenant is one payer with one cap.

    The search is exhaustive (MRV layer ordering, deterministic payer ordering,
    remaining-capacity pruning and memoized failure states), so a ``None``
    result is a proven infeasibility rather than a greedy shortfall. With at
    most twelve layers and six payers the tiny search space closes instantly.
    """
    remaining = dict(caps)
    chosen = {}
    seen_failures = set()

    def options(layer):
        size = size_of[layer]
        return [payer for payer in eligible[layer] if remaining.get(payer, 0) >= size]

    def dfs(pending):
        if not pending:
            return dict(chosen)
        state = (frozenset(pending), tuple(sorted(
            remaining.items(),
            key=lambda item: (item[0] is PLATFORM, _payer_name(item[0])))))
        if state in seen_failures:
            return None
        # Most constrained layer first: fewest payers that still fit, then the
        # biggest layer, then a stable name tie-break.
        layer = min(pending, key=lambda name: (len(options(name)), -size_of[name], name))
        for payer in options(layer):
            chosen[layer] = payer
            remaining[payer] -= size_of[layer]
            result = dfs(pending - {layer})
            if result is not None:
                return result
            remaining[payer] += size_of[layer]
            del chosen[layer]
        seen_failures.add(state)
        return None

    return dfs(frozenset(layer_ids))


def _payer_name(payer):
    return PLATFORM_NAME if payer is PLATFORM else payer


def _minimal_infeasible_subset(candidates, size_of, eligible, caps):
    """Return an inclusion-minimal infeasible subset, or None when feasible."""
    if not candidates or _solve(sorted(candidates), size_of, eligible, caps) is not None:
        return None
    core = set(candidates)
    # Drop layers one at a time while the remainder stays infeasible; whatever
    # remains is inclusion-minimal, i.e. every member is part of the contention.
    changed = True
    while changed:
        changed = False
        for layer in sorted(core):
            trial = core - {layer}
            if trial and _solve(sorted(trial), size_of, eligible, caps) is None:
                core = trial
                changed = True
    return frozenset(core)


def _infeasible_cores(layer_ids, size_of, eligible, caps):
    """Extract inclusion-minimal layer subsets that cannot be fully closed.

    Each returned core is a group of layers that contend for the same pool of
    commitments: removing any one layer makes the remainder assignable, so the
    evidence names exactly the layers and payers an on-call engineer must
    renegotiate. Layers without any payer that can afford the whole layer are
    reported first as standalone ``no_eligible_payer`` cores.
    """
    unpayable = sorted(
        layer for layer in layer_ids
        if caps_cover_none(layer, size_of, eligible, caps))
    cores = [(layer,) for layer in unpayable]
    remaining = set(layer_ids) - set(unpayable)
    while True:
        core = _minimal_infeasible_subset(remaining, size_of, eligible, caps)
        if core is None:
            break
        cores.append(tuple(sorted(core)))
        # Feasible layers that only looked unsolvable beside this core stay in
        # the pool so further independent contention groups can be discovered.
        remaining -= set(core)
    evidence = []
    for core in cores:
        involved_payers = sorted({payer for layer in core for payer in eligible[layer]},
                                 key=lambda p: (p is PLATFORM, _payer_name(p)))
        involved_tenants = [p for p in involved_payers if p is not PLATFORM]
        evidence.append({
            'reason': 'no_eligible_payer' if len(core) == 1 and caps_cover_none(
                core[0], size_of, eligible, caps)
            else 'commitment_contention',
            'layers': list(core),
            'layer_bytes': {layer: size_of[layer] for layer in core},
            'eligible_payers': {
                layer: [_payer_name(p) for p in eligible[layer]] for layer in core},
            'payer_layers': {
                _payer_name(payer): sorted(layer for layer in core if payer in eligible[layer])
                for payer in involved_payers},
            'commitments': {tenant: caps.get(tenant, 0) for tenant in involved_tenants},
            'platform_commit': caps.get(PLATFORM, 0) if any(
                PLATFORM in eligible[layer] for layer in core) else 0,
            'chargeable_bytes': sum(size_of[layer] for layer in core),
            'committed_bytes': sum(caps.get(tenant, 0) for tenant in involved_tenants),
        })
    return sorted(evidence, key=lambda item: item['layers']), unpayable


def _close_under_agreement(db, event):
    if _agreement_invalid(event):
        return {'decision': 'agreement_invalid'}
    commitments = event['agreement'].get('commitments', {})
    beneficiaries = claimants(db, event['node'])
    layer_ids = sorted(beneficiaries)
    size_of = {}
    for layer in layer_ids:
        size_of[layer] = one(db, 'SELECT size FROM layers WHERE id = ?', (layer,))['size']

    tenant_caps = defaultdict(int, commitments)
    full_eligible = {}
    direct_eligible = {}
    platform_layers = set()
    for layer in layer_ids:
        tenants = tuple(sorted(beneficiaries[layer]))
        direct_eligible[layer] = tenants
        payers = list(tenants)
        commit = one(db, 'SELECT bytes FROM backstop_commits WHERE layer = ?', (layer,))
        grants = _active_authorizations(db, layer)
        # The platform may only stand behind this exact layer, with a commitment
        # large enough for the whole layer and unanimous authorization from the
        # tenants that currently benefit from it.
        if (commit and commit['bytes'] >= size_of[layer]
                and beneficiaries[layer] and beneficiaries[layer] <= grants):
            payers.append(PLATFORM)
            platform_layers.add(layer)
        full_eligible[layer] = tuple(payers)

    # Phase 1: spend tenant commitments first so the finite platform backstop is
    # preserved unless the statement genuinely cannot close without it.
    assignment = _solve(layer_ids, size_of, direct_eligible, dict(tenant_caps))
    caps = dict(tenant_caps)
    if assignment is None and platform_layers:
        # Platform capacity is the sum of its ring-fenced per-layer commitments;
        # money committed to one layer can never cover another.
        caps[PLATFORM] = sum(
            one(db, 'SELECT bytes FROM backstop_commits WHERE layer = ?', (layer,))['bytes']
            for layer in platform_layers)
        assignment = _solve(layer_ids, size_of, full_eligible, caps)

    if assignment is None:
        contention, unpayable = _infeasible_cores(
            layer_ids, size_of, full_eligible, caps)
        # No partial bill may be left behind: the caller only gets evidence.
        return {
            'decision': 'statement_uncloseable',
            'unpayable_layers': unpayable,
            'contention': contention,
        }

    allocations = []
    tenant_bytes = defaultdict(int)
    platform_bytes = 0
    assignments = []
    for layer in layer_ids:
        payer = assignment[layer]
        amount = size_of[layer]
        if payer is PLATFORM:
            route = ROUTE_BACKSTOP
            tenant = None
            platform_bytes += amount
        else:
            route = ROUTE_DIRECT
            tenant = payer
            tenant_bytes[tenant] += amount
        allocations.append((event['id'], layer, tenant, amount, route))
        assignments.append({
            'layer': layer, 'bytes': amount, 'payer': _payer_name(payer), 'route': route})

    charged = sum(size_of.values())
    db.execute(
        'INSERT INTO cache_statements VALUES (?, ?, ?, ?)',
        (event['id'], event['node'], charged, MODE_AGREEMENT))
    db.executemany('INSERT INTO cache_charges VALUES (?, ?, ?, ?, ?)', allocations)
    return {
        'decision': 'statement_closed',
        'mode': MODE_AGREEMENT,
        'bytes': charged,
        'assignments': assignments,
        'tenant_bytes': dict(sorted(tenant_bytes.items())),
        'platform_bytes': platform_bytes,
    }


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
    if op == 'backstop-commit':
        layer = one(db, 'SELECT id FROM layers WHERE id = ?', (event['layer'],))
        amount = event.get('bytes')
        if not layer or not isinstance(amount, int) or isinstance(amount, bool) or amount < 0:
            return {'decision': 'backstop_rejected'}
        db.execute(
            'INSERT INTO backstop_commits VALUES (?, ?) '
            'ON CONFLICT(layer) DO UPDATE SET bytes = excluded.bytes',
            (event['layer'], amount))
        return {'decision': 'backstop_committed', 'layer': event['layer'], 'bytes': amount}
    if op == 'backstop-authorize':
        layer = one(db, 'SELECT id FROM layers WHERE id = ?', (event['layer'],))
        if not layer or not event.get('tenant'):
            return {'decision': 'backstop_rejected'}
        db.execute(
            'INSERT INTO backstop_authorizations VALUES (?, ?, ?) '
            'ON CONFLICT(layer, tenant) DO UPDATE SET status = excluded.status',
            (event['layer'], event['tenant'], 'active'))
        return {'decision': 'backstop_authorized', 'layer': event['layer'],
                'tenant': event['tenant']}
    if op == 'backstop-revoke':
        existing = one(
            db,
            'SELECT status FROM backstop_authorizations WHERE layer = ? AND tenant = ?',
            (event['layer'], event.get('tenant')))
        if not existing or existing['status'] != 'active':
            return {'decision': 'backstop_authorization_absent'}
        db.execute(
            "UPDATE backstop_authorizations SET status = 'revoked' "
            'WHERE layer = ? AND tenant = ?',
            (event['layer'], event['tenant']))
        return {'decision': 'backstop_revoked', 'layer': event['layer'],
                'tenant': event['tenant']}
    if op != 'statement-close':
        return {'decision': 'unknown_operation'}
    if one(db, 'SELECT 1 FROM cache_statements WHERE id = ?', (event['id'],)):
        return {'decision': 'statement_id_taken'}
    if one(db, '''SELECT 1 FROM workload_refs AS r
        LEFT JOIN charge_tenants AS t ON t.ref = r.id
        WHERE r.node = ? AND r.status = 'active' AND t.tenant IS NULL''', (event['node'],)):
        return {'decision': 'attribution_incomplete'}
    if 'agreement' in event:
        return _close_under_agreement(db, event)
    # Legacy settlement without a responsibility agreement: unchanged contract,
    # including the response shape, so archived statements keep replaying.
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
    db.execute('INSERT INTO cache_statements VALUES (?, ?, ?, ?)',
               (event['id'], event['node'], charged, 'legacy'))
    db.executemany(
        "INSERT INTO cache_charges (statement, layer, tenant, bytes) VALUES (?, ?, ?, ?)",
        allocations)
    return {'decision': 'statement_closed', 'bytes': charged, 'tenant_bytes': dict(totals)}
