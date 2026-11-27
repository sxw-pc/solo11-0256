"""Responsibility-agreement settlement tests for statement-close.

Covers whole-layer single-payer assignment, per-payer commitment caps,
ring-fenced platform backstop with unanimous beneficiary authorization,
re-determined eligibility after reference-release, infeasibility evidence
without partial bills, and preservation of the legacy split settlement.
"""
import tempfile
import unittest
from itertools import count
from pathlib import Path

from layercourt.store import connect, replay, run, rows, one


class Settlement(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / 'case.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        self.events = count(1)

    def send(self, command, op, **data):
        return run(self.db, command, dict(event_id=f'test-{next(self.events)}', op=op, **data))

    def manifest(self, name, layers, arch='arm64'):
        self.assertEqual('manifest_active',
                         self.send('manifest', 'manifest-register', id=name, digest=name, arch=arch)['decision'])
        for layer, size in layers.items():
            outcome = self.send('manifest', 'layer-deduplicate', id=layer, manifest=name,
                                digest=layer, arch=arch, size=size)
            self.assertEqual(layer, outcome['layer'])

    def cache(self, node, *layers):
        for layer in layers:
            self.assertEqual('cache_present',
                             self.send('eviction', 'cache-record', node=node, layer=layer)['decision'])

    def link(self, ref, manifest, tenant, node='n'):
        self.assertEqual('reference_active', self.send(
            'retention', 'workload-reference', id=ref, pod=ref, uid=ref,
            node=node, manifest=manifest, generation=1)['decision'])
        self.assertEqual('tenant_linked',
                         self.send('chargeback', 'tenant-link', ref=ref, tenant=tenant)['decision'])

    def release(self, ref):
        return self.send('retention', 'reference-release', id=f'rel-{ref}', ref=ref, uid=ref,
                         termination_receipt='term', delete_receipt='drop')

    def close(self, statement, commitments, node='n'):
        return self.send('chargeback', 'statement-close', id=statement, node=node,
                         agreement={'commitments': commitments})

    def charges(self, statement):
        return rows(self.db,
                    'SELECT layer, tenant, bytes, route FROM cache_charges WHERE statement = ? '
                    'ORDER BY layer', (statement,))

    # --- narrative盘面: X/Y/Z = 6/4/4, beneficiaries X:{A,B}, Y:{A}, Z:{B,C} ---
    def seed_xyz(self):
        self.manifest('mXA', {'X': 6})
        self.manifest('mXB', {'X': 6})
        self.manifest('mYA', {'Y': 4})
        self.manifest('mZB', {'Z': 4})
        self.manifest('mZC', {'Z': 4})
        self.cache('n', 'X', 'Y', 'Z')
        self.link('rXA', 'mXA', 'A')
        self.link('rXB', 'mXB', 'B')
        self.link('rYA', 'mYA', 'A')
        self.link('rZB', 'mZB', 'B')
        self.link('rZC', 'mZC', 'C')

    def test_direct_attribution_closes_with_layer_level_responsibility(self):
        self.seed_xyz()
        out = self.close('s1', {'A': 4, 'B': 6, 'C': 4})
        self.assertEqual('statement_closed', out['decision'])
        self.assertEqual(14, out['bytes'])
        self.assertEqual(0, out['platform_bytes'])
        self.assertEqual({'A': 4, 'B': 6, 'C': 4}, out['tenant_bytes'])
        by_layer = {item['layer']: item for item in out['assignments']}
        self.assertEqual(('B', 'direct'), (by_layer['X']['payer'], by_layer['X']['route']))
        self.assertEqual(('A', 'direct'), (by_layer['Y']['payer'], by_layer['Y']['route']))
        self.assertEqual(('C', 'direct'), (by_layer['Z']['payer'], by_layer['Z']['route']))
        # Every referenced physical layer is charged exactly once, in full.
        self.assertEqual([
            {'layer': 'X', 'tenant': 'B', 'bytes': 6, 'route': 'direct'},
            {'layer': 'Y', 'tenant': 'A', 'bytes': 4, 'route': 'direct'},
            {'layer': 'Z', 'tenant': 'C', 'bytes': 4, 'route': 'direct'},
        ], self.charges('s1'))
        self.assertTrue(replay(self.path)['verified'])

    def test_platform_backstop_bears_whole_layer_and_keeps_tenant_balance_intact(self):
        self.seed_xyz()
        # Tenants alone cannot cover the 6-byte layer X (caps A=B=4), but the
        # platform commits up to 6 for X and both current beneficiaries grant it.
        self.assertEqual('backstop_committed',
                         self.send('chargeback', 'backstop-commit', layer='X', bytes=6)['decision'])
        self.assertEqual('backstop_authorized',
                         self.send('chargeback', 'backstop-authorize', layer='X', tenant='A')['decision'])
        self.assertEqual('backstop_authorized',
                         self.send('chargeback', 'backstop-authorize', layer='X', tenant='B')['decision'])
        out = self.close('s1', {'A': 4, 'B': 4, 'C': 4})
        self.assertEqual('statement_closed', out['decision'])
        self.assertEqual(6, out['platform_bytes'])
        # B's commitment is never consumed by X; it remains fully available for
        # the shared layers it actually benefits from (here Z), and nothing runs over.
        self.assertEqual({'A': 4, 'B': 4}, out['tenant_bytes'])
        by_layer = {item['layer']: item for item in out['assignments']}
        self.assertEqual(('platform', 'platform-backstop'),
                         (by_layer['X']['payer'], by_layer['X']['route']))
        self.assertEqual('A', by_layer['Y']['payer'])
        self.assertEqual('B', by_layer['Z']['payer'])
        self.assertEqual([
            {'layer': 'X', 'tenant': None, 'bytes': 6, 'route': 'platform-backstop'},
            {'layer': 'Y', 'tenant': 'A', 'bytes': 4, 'route': 'direct'},
            {'layer': 'Z', 'tenant': 'B', 'bytes': 4, 'route': 'direct'},
        ], self.charges('s1'))
        self.assertTrue(replay(self.path)['verified'])

    def test_revoked_backstop_authorization_still_allows_full_closure(self):
        # A greedy order (Z->B before X) would manufacture a false gap; the
        # exhaustive settlement must still close the complete bill.
        self.seed_xyz()
        self.send('chargeback', 'backstop-commit', layer='X', bytes=6)
        self.send('chargeback', 'backstop-authorize', layer='X', tenant='A')
        self.send('chargeback', 'backstop-authorize', layer='X', tenant='B')
        self.assertEqual('backstop_revoked',
                         self.send('chargeback', 'backstop-revoke', layer='X', tenant='B')['decision'])
        out = self.close('s1', {'A': 4, 'B': 6, 'C': 4})
        self.assertEqual('statement_closed', out['decision'])
        self.assertEqual(0, out['platform_bytes'])
        self.assertEqual({'A': 4, 'B': 6, 'C': 4}, out['tenant_bytes'])
        by_layer = {item['layer']: item for item in out['assignments']}
        self.assertEqual('B', by_layer['X']['payer'])
        self.assertEqual('A', by_layer['Y']['payer'])
        self.assertEqual('C', by_layer['Z']['payer'])
        self.assertTrue(replay(self.path)['verified'])

    def test_three_layers_contending_for_two_commitments_cannot_close(self):
        # Three 4-byte layers, each only payable by A or B, caps 6/6, no
        # platform commitment: total commitment suffices and every layer has an
        # eligible payer, yet no compliant whole-layer bill exists.
        for name in ('p', 'q', 'r'):
            self.manifest(f'mA-{name}', {name: 4})
            self.manifest(f'mB-{name}', {name: 4})
        self.cache('n', 'p', 'q', 'r')
        for layer in ('p', 'q', 'r'):
            self.link(f'rA-{layer}', f'mA-{layer}', 'A')
            self.link(f'rB-{layer}', f'mB-{layer}', 'B')
        out = self.close('s1', {'A': 6, 'B': 6})
        self.assertEqual('statement_uncloseable', out['decision'])
        self.assertEqual([], out['unpayable_layers'])
        self.assertEqual(1, len(out['contention']))
        core = out['contention'][0]
        self.assertEqual('commitment_contention', core['reason'])
        self.assertEqual(['p', 'q', 'r'], core['layers'])
        self.assertEqual({'A': 6, 'B': 6}, core['commitments'])
        self.assertEqual(12, core['chargeable_bytes'])
        self.assertEqual(12, core['committed_bytes'])
        self.assertEqual({'A': ['p', 'q', 'r'], 'B': ['p', 'q', 'r']}, core['payer_layers'])
        # No partial bill is left behind.
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_statements'))
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_charges'))
        # Re-negotiated caps close the same盘面 afterwards.
        fixed = self.close('s2', {'A': 8, 'B': 8})
        self.assertEqual('statement_closed', fixed['decision'])
        self.assertEqual(12, fixed['bytes'])
        self.assertEqual({'A': 8, 'B': 4}, fixed['tenant_bytes'])
        self.assertTrue(replay(self.path)['verified'])

    def test_reference_release_reshapes_billable_layers_and_backstop_eligibility(self):
        self.manifest('mW', {'W': 3})          # exclusive layer, only A
        self.manifest('mSA', {'S': 5})
        self.manifest('mSB', {'S': 5})
        self.cache('n', 'W', 'S')
        self.link('rW', 'mW', 'A')
        self.link('rSA', 'mSA', 'A')
        self.link('rSB', 'mSB', 'B')
        # Platform ring-fences money on both layers, with unanimous grants.
        self.send('chargeback', 'backstop-commit', layer='W', bytes=3)
        self.send('chargeback', 'backstop-authorize', layer='W', tenant='A')
        self.send('chargeback', 'backstop-commit', layer='S', bytes=5)
        self.send('chargeback', 'backstop-authorize', layer='S', tenant='A')
        self.send('chargeback', 'backstop-authorize', layer='S', tenant='B')
        first = self.close('s1', {'A': 8, 'B': 8})
        self.assertEqual('statement_closed', first['decision'])
        self.assertEqual([row['layer'] for row in self.charges('s1')], ['S', 'W'])

        # Releasing the sole active reference removes W from the billable set.
        self.assertEqual('reference_released', self.release('rW')['decision'])
        second = self.close('s2', {'A': 8, 'B': 8})
        self.assertEqual('statement_closed', second['decision'])
        self.assertEqual(['S'], [row['layer'] for row in self.charges('s2')])
        self.assertEqual(5, second['bytes'])
        # The archived statement is never rewritten.
        self.assertEqual([row['layer'] for row in self.charges('s1')], ['S', 'W'])
        self.assertEqual('agreement',
                         one(self.db, 'SELECT mode FROM cache_statements WHERE id=?', ('s1',))['mode'])
        self.assertEqual('agreement',
                         one(self.db, 'SELECT mode FROM cache_statements WHERE id=?', ('s2',))['mode'])

        # After B's release too, S's beneficiaries shrink to A; the platform
        # stays eligible because the remaining beneficiary A still grants it.
        self.assertEqual('reference_released', self.release('rSB')['decision'])
        still_eligible = self.close('s3', {'A': 5, 'B': 8})
        self.assertEqual('statement_closed', still_eligible['decision'])
        self.assertEqual(0, still_eligible['platform_bytes'])  # tenants cover it first
        # Withdrawing the last beneficiary's grant makes the platform ineligible;
        # an unaffordable tenant cap then proves the layer cannot be closed.
        self.assertEqual('backstop_revoked',
                         self.send('chargeback', 'backstop-revoke', layer='S', tenant='A')['decision'])
        blocked = self.close('s4', {'A': 0, 'B': 8})
        self.assertEqual('statement_uncloseable', blocked['decision'])
        self.assertEqual(['S'], blocked['unpayable_layers'])
        self.assertEqual(['A'], blocked['contention'][0]['eligible_payers']['S'])
        self.assertFalse(rows(self.db, "SELECT * FROM cache_statements WHERE id='s4'"))
        self.assertTrue(replay(self.path)['verified'])

    def test_backstop_money_is_ring_fenced_per_layer(self):
        self.seed_xyz()
        # Commitment exists only for X, and Y has unanimous grants but no
        # commitment: platform money may not be lent from X to Y.
        self.send('chargeback', 'backstop-commit', layer='X', bytes=6)
        self.send('chargeback', 'backstop-authorize', layer='X', tenant='A')
        self.send('chargeback', 'backstop-authorize', layer='X', tenant='B')
        self.send('chargeback', 'backstop-authorize', layer='Y', tenant='A')
        # Caps make X unaffordable by tenants, and Y unaffordable by A; the
        # platform may take X but must not touch Y.
        out = self.close('s1', {'A': 0, 'B': 4, 'C': 4})
        self.assertEqual('statement_uncloseable', out['decision'])
        y_core = next(c for c in out['contention'] if 'Y' in c['layers'])
        self.assertEqual(['A'], y_core['eligible_payers']['Y'])  # platform absent
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_statements'))

    def test_multiple_references_from_one_tenant_do_not_add_capacity(self):
        self.manifest('mL1', {'L': 5})
        self.manifest('mL2', {'L': 5})
        self.cache('n', 'L')
        self.link('r1', 'mL1', 'A')
        self.link('r2', 'mL2', 'A')
        blocked = self.close('s1', {'A': 4})
        self.assertEqual('statement_uncloseable', blocked['decision'])
        self.assertEqual(['L'], blocked['unpayable_layers'])
        self.assertEqual(['A'], blocked['contention'][0]['eligible_payers']['L'])
        ok = self.close('s2', {'A': 5})
        self.assertEqual('statement_closed', ok['decision'])
        # One physical layer, one full charge row regardless of the two refs.
        self.assertEqual(1, len(self.charges('s2')))
        self.assertEqual(5, self.charges('s2')[0]['bytes'])
        self.assertTrue(replay(self.path)['verified'])

    def test_incomplete_authorization_keeps_platform_ineligible(self):
        self.seed_xyz()
        self.send('chargeback', 'backstop-commit', layer='X', bytes=6)
        self.send('chargeback', 'backstop-authorize', layer='X', tenant='A')
        # B (still a current beneficiary) never authorizes.
        out = self.close('s1', {'A': 4, 'B': 4, 'C': 4})
        self.assertEqual('statement_uncloseable', out['decision'])
        x_core = next(c for c in out['contention'] if 'X' in c['layers'])
        self.assertEqual(['A', 'B'], x_core['eligible_payers']['X'])
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_statements'))

    def test_legacy_settlement_without_agreement_is_unchanged(self):
        self.seed_xyz()
        out = self.send('chargeback', 'statement-close', id='legacy', node='n')
        # Exact legacy response shape, split across distinct beneficiary tenants.
        self.assertEqual({
            'decision': 'statement_closed', 'bytes': 14,
            'tenant_bytes': {'A': 7, 'B': 5, 'C': 2},
        }, out)
        self.assertEqual('legacy',
                         one(self.db, 'SELECT mode FROM cache_statements WHERE id=?', ('legacy',))['mode'])
        self.assertTrue(all(row['route'] == 'direct' for row in self.charges('legacy')))
        # Statement ids remain unique across both settlement modes.
        self.assertEqual('statement_id_taken',
                         self.send('chargeback', 'statement-close', id='legacy', node='n')['decision'])
        self.assertTrue(replay(self.path)['verified'])

    def test_invalid_agreement_and_backstop_requests_are_rejected_without_writes(self):
        self.seed_xyz()
        # An empty commitments map is valid input but cannot settle anything;
        # malformed agreements are rejected before any search or write happens.
        self.assertEqual('statement_uncloseable',
                         self.close('empty', {})['decision'])
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_statements'))
        for bad in ({'commitments': []}, {'commitments': {'A': -1}},
                    {'commitments': {'A': True}}, {'commitments': {'': 4}}):
            self.assertEqual('agreement_invalid', self.send(
                'chargeback', 'statement-close', id=f'bad-{id(bad)}', node='n', agreement=bad)['decision'])
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_statements'))
        self.assertEqual('backstop_rejected',
                         self.send('chargeback', 'backstop-commit', layer='ghost', bytes=5)['decision'])
        self.assertEqual('backstop_rejected',
                         self.send('chargeback', 'backstop-commit', layer='X', bytes=True)['decision'])
        self.assertEqual('backstop_rejected',
                         self.send('chargeback', 'backstop-authorize', layer='X', tenant='')['decision'])
        self.assertEqual('backstop_authorization_absent',
                         self.send('chargeback', 'backstop-revoke', layer='X', tenant='A')['decision'])
        # Idempotent re-authorization and commitment updates are ledger events.
        self.send('chargeback', 'backstop-authorize', layer='X', tenant='A')
        self.assertEqual('backstop_authorized',
                         self.send('chargeback', 'backstop-authorize', layer='X', tenant='A')['decision'])
        self.send('chargeback', 'backstop-commit', layer='X', bytes=6)
        self.assertEqual(6, one(self.db, 'SELECT bytes FROM backstop_commits WHERE layer=?', ('X',))['bytes'])
        self.send('chargeback', 'backstop-commit', layer='X', bytes=7)
        self.assertEqual(7, one(self.db, 'SELECT bytes FROM backstop_commits WHERE layer=?', ('X',))['bytes'])
        self.assertTrue(replay(self.path)['verified'])

    def test_missing_tenant_link_still_blocks_agreement_close(self):
        self.manifest('m', {'L': 4})
        self.cache('n', 'L')
        self.assertEqual('reference_active', self.send(
            'retention', 'workload-reference', id='r', pod='r', uid='r',
            node='n', manifest='m', generation=1)['decision'])
        self.assertEqual('attribution_incomplete', self.close('s1', {'A': 4})['decision'])
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_statements'))
        self.assertTrue(replay(self.path)['verified'])


if __name__ == '__main__':
    unittest.main()
