import tempfile
import unittest
from itertools import count
from pathlib import Path

from layercourt.rollout import MAX_WAVE_NODES
from layercourt.store import connect, one, replay, rows, run


# Compatibility matrix from the release team's interface contract for A->B:
# old-old, new-new, transition-A -> old-B, transition-A -> new-B are callable;
# every other pairing is incompatible.
AB_COMPAT = [
    ['a-old', 'b-old'],
    ['a-new', 'b-new'],
    ['a-trans', 'b-old'],
    ['a-trans', 'b-new'],
]


class SwitchWave(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / 'case.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        self.events = count(1)

    def send(self, command, op, **data):
        data.setdefault('event_id', f'test-{next(self.events)}')
        return run(self.db, command, dict(op=op, **data))

    def manifest(self, release, layers, arch='arm64'):
        self.assertEqual('manifest_active',
                         self.send('manifest', 'manifest-register', id=release, digest=release, arch=arch)['decision'])
        for layer, size in layers.items():
            outcome = self.send('manifest', 'layer-deduplicate', id=layer, manifest=release,
                                digest=layer, arch=arch, size=size)
            self.assertEqual(layer, outcome['layer'])

    def enroll(self, node, arch='arm64'):
        self.assertEqual('node_enrolled', self.send('rollout', 'node-enroll', node=node, arch=arch)['decision'])

    def cache(self, node, *layers):
        for layer in layers:
            self.assertEqual('cache_present',
                             self.send('eviction', 'cache-record', node=node, layer=layer)['decision'])

    def delete_layer(self, node, layer):
        # Drive the resident layer through the eviction ledger: present -> deleted.
        self.assertEqual('planned', self.send('eviction', 'eviction-plan', id=f'plan-{node}-{layer}',
                                              node=node, layer=layer, needed=1, at=10)['decision'])
        self.assertEqual('space_reclaimed', self.send('eviction', 'delete-reconcile', id=f'del-{node}-{layer}',
                                                       plan=f'plan-{node}-{layer}', layer=layer,
                                                       at=11, success=True)['decision'])
        self.assertEqual('deleted', one(self.db, 'SELECT status FROM node_caches WHERE node=? AND layer=?',
                                        (node, layer))['status'])

    def ab_board(self):
        # Three releases per node family: current (old), target (new), transition.
        self.manifest('a-old', {'a-base': 1, 'a-old-app': 2})
        self.manifest('a-new', {'a-base': 1, 'a-new-app': 2})
        self.manifest('a-trans', {'a-base': 1, 'a-trans-app': 2})
        self.manifest('b-old', {'b-base': 1, 'b-old-app': 2})
        self.manifest('b-new', {'b-base': 1, 'b-new-app': 2})
        self.enroll('A', 'arm64')
        self.enroll('B', 'arm64')
        # Both target closures are resident; A additionally cached the transition.
        self.cache('A', 'a-base', 'a-old-app', 'a-new-app', 'a-trans-app')
        self.cache('B', 'b-base', 'b-old-app', 'b-new-app')
        return {
            'manifests': {
                'A': {'current': 'a-old', 'target': 'a-new', 'transitions': ['a-trans']},
                'B': {'current': 'b-old', 'target': 'b-new', 'transitions': []},
            },
            'edges': [{'from': 'A', 'to': 'B', 'compatible': AB_COMPAT}],
        }

    def wave(self, constraints, wave_id='wave', manifest='a-new', nodes=('A', 'B'), **extra):
        payload = dict(event_id=f'wave-{wave_id}', op='wave-approve', id=wave_id,
                       manifest=manifest, nodes=list(nodes), constraints=constraints)
        payload.update(extra)
        return self.send('rollout', 'wave-approve', **{k: v for k, v in payload.items() if k != 'op'})

    def assert_edge_compatible(self, assignment, edges):
        for edge in edges:
            pair = [assignment[edge['from']], assignment[edge['to']]]
            self.assertIn(pair, edge['compatible'])

    def assert_plan_safe(self, result, constraints):
        """Replay the authorised sequence and prove every visible state is compatible."""
        current = {node: spec['current'] for node, spec in constraints['manifests'].items()}
        self.assert_edge_compatible(current, constraints['edges'])
        for step in result['plan']:
            current.update(step['assignments'])
            self.assert_edge_compatible(current, constraints['edges'])
            if step['kind'] == 'barrier':
                # No intermediate state exists inside a barrier batch: all members move at once.
                self.assertEqual(len(step['nodes']), len(step['assignments']))
        for node, spec in constraints['manifests'].items():
            self.assertEqual(spec['target'], current[node])

    def test_barrier_switches_a_and_b_together_in_one_executable_batch(self):
        constraints = self.ab_board()
        constraints['barriers'] = [{'id': 'ab-barrier', 'nodes': ['A', 'B']}]
        result = self.wave(constraints)
        self.assertEqual('wave_approved', result['decision'])
        self.assertEqual(1, len(result['plan']))
        step = result['plan'][0]
        self.assertEqual('barrier', step['kind'])
        self.assertEqual(['A', 'B'], step['nodes'])
        self.assertEqual('ab-barrier', step['barrier'])
        self.assertEqual({'A': 'a-new', 'B': 'b-new'}, step['assignments'])
        self.assertEqual([{'step': 1, 'barrier': 'ab-barrier', 'nodes': ['A', 'B']}],
                         result['upgraded_batches'])
        self.assertEqual([], result['transition_nodes'])
        self.assert_plan_safe(result, constraints)
        self.assertEqual(1, one(self.db, 'SELECT steps n FROM rollout_plans WHERE wave=?', ('wave',))['n'])
        self.assertTrue(replay(self.path)['verified'])

    def test_hot_swap_route_parks_a_on_transition_while_b_reaches_target_first(self):
        constraints = self.ab_board()
        constraints['hot_swap'] = ['A']
        result = self.wave(constraints, wave_id='hot')
        self.assertEqual('wave_approved', result['decision'])
        kinds = [(step['kind'], step['nodes']) for step in result['plan']]
        self.assertEqual([('transition', ['A']), ('switch', ['B']), ('switch', ['A'])], kinds)
        self.assertEqual({'A': 'a-trans'}, result['plan'][0]['assignments'])
        self.assertEqual({'B': 'b-new'}, result['plan'][1]['assignments'])
        self.assertEqual({'A': 'a-new'}, result['plan'][2]['assignments'])
        self.assertNotIn('barrier', result['plan'][0])
        self.assertEqual(['A'], result['transition_nodes'])
        # Steps 1 and 2 leave A parked on the transition manifest while B upgrades.
        self.assertEqual('a-trans', result['plan'][1]['assignments'].get('A', 'a-trans'))
        self.assert_plan_safe(result, constraints)
        persisted = rows(self.db, 'SELECT step, kind, barrier FROM rollout_plan_steps ORDER BY step')
        self.assertEqual([
            {'step': 1, 'kind': 'transition', 'barrier': None},
            {'step': 2, 'kind': 'switch', 'barrier': None},
            {'step': 3, 'kind': 'switch', 'barrier': None},
        ], persisted)
        self.assertTrue(replay(self.path)['verified'])

    def test_with_both_authorisations_barrier_is_preferred_but_transition_still_available(self):
        constraints = self.ab_board()
        constraints['barriers'] = [{'id': 'ab-barrier', 'nodes': ['A', 'B']}]
        constraints['hot_swap'] = ['A']
        result = self.wave(constraints, wave_id='both')
        # Shortest safe sequence is the single barrier step; no transition manifest is consumed.
        self.assertEqual(1, len(result['plan']))
        self.assertEqual('barrier', result['plan'][0]['kind'])
        self.assertEqual([], result['transition_nodes'])
        self.assert_plan_safe(result, constraints)
        # Dropping the barrier still yields a genuine hot-swap plan rather than a failure.
        del constraints['barriers']
        hot = self.wave(constraints, wave_id='hot-only')
        self.assertEqual('wave_approved', hot['decision'])
        self.assertEqual(3, len(hot['plan']))
        self.assert_plan_safe(hot, constraints)
        self.assertTrue(replay(self.path)['verified'])

    def test_deleted_transition_layer_without_barrier_forces_wait_without_false_permit(self):
        constraints = self.ab_board()
        constraints['hot_swap'] = ['A']
        # Eviction turns the resident transition layer into a deleted cache row.
        self.delete_layer('A', 'a-trans-app')
        first = self.wave(constraints, wave_id='blocked')
        self.assertEqual('wave_waiting', first['decision'])
        self.assertIn('A', first['unavailable_manifests'])
        self.assertIn('a-trans', first['unavailable_manifests']['A'])
        # The static target closure is still complete, proving the old readiness check would pass.
        self.assertEqual([], self._missing('A', 'a-new'))
        blocked_pairs = {(b['edge'], b['from'], b['to']) for b in first['blocked_edges']}
        self.assertIn(('A->B', 'a-old', 'b-new'), blocked_pairs)
        self.assertIn(('A->B', 'a-new', 'b-old'), blocked_pairs)
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_waves'))
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_plans'))
        # A retried identical request must keep waiting: no back-and-forth executable permit.
        retry = self.wave(constraints, wave_id='blocked-retry')
        self.assertEqual('wave_waiting', retry['decision'])
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_waves'))
        self.assertTrue(replay(self.path)['verified'])

    def test_three_node_board_mixes_barrier_and_hot_swap_instead_of_whole_order_failure(self):
        constraints = self.ab_board()
        # C only talks old/old, transition/old, transition/new and new/new with B.
        self.manifest('c-old', {'c-base': 1, 'c-old-app': 2})
        self.manifest('c-new', {'c-base': 1, 'c-new-app': 2})
        self.manifest('c-trans', {'c-base': 1, 'c-trans-app': 2})
        self.enroll('C', 'arm64')
        self.cache('C', 'c-base', 'c-old-app', 'c-new-app', 'c-trans-app')
        constraints['manifests']['C'] = {'current': 'c-old', 'target': 'c-new', 'transitions': ['c-trans']}
        constraints['edges'].append({'from': 'C', 'to': 'B', 'compatible': [
            ['c-old', 'b-old'], ['c-trans', 'b-old'], ['c-trans', 'b-new'], ['c-new', 'b-new'],
        ]})
        constraints['barriers'] = [{'id': 'ab-barrier', 'nodes': ['A', 'B']}]
        constraints['hot_swap'] = ['C']
        result = self.wave(constraints, wave_id='three', nodes=('A', 'B', 'C'), manifest='a-new')
        self.assertEqual('wave_approved', result['decision'])
        kinds = [(step['kind'], step['nodes'], step['assignments']) for step in result['plan']]
        self.assertEqual([
            ('transition', ['C'], {'C': 'c-trans'}),
            ('barrier', ['A', 'B'], {'A': 'a-new', 'B': 'b-new'}),
            ('switch', ['C'], {'C': 'c-new'}),
        ], kinds)
        self.assertEqual('ab-barrier', result['plan'][1]['barrier'])
        self.assertEqual(['C'], result['transition_nodes'])
        self.assert_plan_safe(result, constraints)
        self.assertTrue(replay(self.path)['verified'])

    def test_legacy_request_without_constraints_keeps_original_verdict(self):
        self.manifest('release', {'base': 4, 'app': 5})
        self.enroll('n')
        self.enroll('m')
        self.cache('n', 'base', 'app')
        self.cache('m', 'base')
        waiting = self.send('rollout', 'wave-approve', id='legacy-wave', manifest='release', nodes=['n', 'm'])
        self.assertEqual({'decision': 'wave_waiting',
                          'blocked': {'m': {'reason': 'incomplete_cache', 'layers': ['app']}}}, waiting)
        self.cache('m', 'app')
        approved = self.send('rollout', 'wave-approve', id='legacy-wave', manifest='release', nodes=['n', 'm'])
        self.assertEqual({'decision': 'wave_approved', 'nodes': ['m', 'n']}, approved)
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_plans'))
        self.assertTrue(replay(self.path)['verified'])

    def test_malformed_constraints_and_unknown_nodes_are_rejected_without_writes(self):
        constraints = self.ab_board()
        bad = dict(constraints, manifests={'A': {'current': 'a-old'}})
        result = self.wave(bad, wave_id='bad')
        self.assertEqual('wave_waiting', result['decision'])
        self.assertEqual('constraint_invalid', result['blocked']['constraints']['reason'])
        edge_bad = dict(constraints, edges=[{'from': 'A', 'to': 'X', 'compatible': AB_COMPAT}])
        self.assertEqual('constraint_invalid', self.wave(edge_bad, wave_id='bad-edge')['blocked']['constraints']['reason'])
        missing_pair = dict(constraints, edges=[{'from': 'A', 'to': 'B', 'compatible': [['a-old']]}])
        self.assertEqual('constraint_invalid',
                         self.wave(missing_pair, wave_id='bad-pair')['blocked']['constraints']['reason'])
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_waves'))

    def test_revoked_transition_manifest_removes_the_hot_swap_route(self):
        constraints = self.ab_board()
        constraints['hot_swap'] = ['A']
        self.send('manifest', 'manifest-revoke', id='a-trans')
        result = self.wave(constraints, wave_id='revoked-trans')
        self.assertEqual('wave_waiting', result['decision'])
        self.assertEqual('no_safe_sequence', result['reason'])
        self.assertIn('a-trans', result['unavailable_manifests']['A'])
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_waves'))

    def test_platform_mismatch_blocks_constrained_wave(self):
        constraints = self.ab_board()
        constraints['barriers'] = [{'id': 'ab-barrier', 'nodes': ['A', 'B']}]
        self.db.execute("UPDATE rollout_nodes SET arch='amd64' WHERE node='B'")
        result = self.wave(constraints, wave_id='arch')
        self.assertEqual('wave_waiting', result['decision'])
        reasons = {item['reason'] for item in result['blocked']['B']}
        self.assertIn('platform_mismatch', reasons)

    def test_wave_is_limited_to_eight_nodes(self):
        constraints = self.ab_board()
        result = self.send('rollout', 'wave-approve', id='too-big', manifest='a-new',
                           nodes=[f'N{i}' for i in range(MAX_WAVE_NODES + 1)], constraints=constraints)
        self.assertEqual('wave_too_large', result['decision'])
        self.assertEqual(MAX_WAVE_NODES, result['limit'])

    def test_board_that_starts_with_a_broken_edge_gets_no_permit(self):
        constraints = self.ab_board()
        constraints['barriers'] = [{'id': 'ab-barrier', 'nodes': ['A', 'B']}]
        # Remove old-old compatibility: the current deployment already violates the edge.
        constraints['edges'][0]['compatible'] = [pair for pair in AB_COMPAT if pair != ['a-old', 'b-old']]
        result = self.wave(constraints, wave_id='broken-start')
        self.assertEqual('wave_waiting', result['decision'])
        self.assertEqual('initial_state_incompatible', result['reason'])
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_waves'))
        self.assertTrue(replay(self.path)['verified'])

    def _missing(self, node, manifest):
        return [row['layer'] for row in rows(self.db, '''
            SELECT e.layer FROM manifest_edges AS e
            WHERE e.manifest = ? AND NOT EXISTS (
                SELECT 1 FROM node_caches AS c
                WHERE c.node = ? AND c.layer = e.layer AND c.status = 'present'
            ) ORDER BY e.layer
        ''', (manifest, node))]


if __name__ == '__main__':
    unittest.main()
