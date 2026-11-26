import tempfile
import unittest
from itertools import count
from pathlib import Path

from layercourt.store import connect, replay, run, rows


class RolloutChargeback(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / 'case.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        self.events = count(1)

    def send(self, command, op, **data):
        return run(self.db, command, dict(event_id=f'test-{next(self.events)}', op=op, **data))

    def manifest(self, release, layers, arch='arm64'):
        self.assertEqual('manifest_active', self.send('manifest', 'manifest-register', id=release, digest=release, arch=arch)['decision'])
        for layer, size in layers.items():
            outcome = self.send('manifest', 'layer-deduplicate', id=layer, manifest=release, digest=layer, arch=arch, size=size)
            self.assertEqual(layer, outcome['layer'])

    def cache(self, node, *layers):
        for layer in layers:
            self.assertEqual('cache_present', self.send('eviction', 'cache-record', node=node, layer=layer)['decision'])

    def link(self, ref, manifest, tenant=None):
        self.assertEqual('reference_active', self.send('retention', 'workload-reference', id=ref, pod=ref, uid=ref, node='n', manifest=manifest, generation=1)['decision'])
        if tenant:
            self.assertEqual('tenant_linked', self.send('chargeback', 'tenant-link', ref=ref, tenant=tenant)['decision'])

    def test_wave_waits_for_every_node_and_approves_after_cache_receipt(self):
        self.manifest('release', {'base': 4, 'app': 5})
        self.send('rollout', 'node-enroll', node='n', arch='arm64')
        self.send('rollout', 'node-enroll', node='m', arch='arm64')
        self.cache('n', 'base', 'app')
        self.cache('m', 'base')
        waiting = self.send('rollout', 'wave-approve', id='wave', manifest='release', nodes=['n', 'm'])
        self.assertEqual({'decision': 'wave_waiting', 'blocked': {'m': {'reason': 'incomplete_cache', 'layers': ['app']}}}, waiting)
        self.assertFalse(rows(self.db, 'SELECT * FROM rollout_waves'))
        self.cache('m', 'app')
        self.assertEqual({'decision': 'wave_approved', 'nodes': ['m', 'n']}, self.send('rollout', 'wave-approve', id='wave', manifest='release', nodes=['n', 'm']))
        self.assertEqual(2, len(rows(self.db, 'SELECT * FROM rollout_seats')))
        self.assertTrue(replay(self.path)['verified'])

    def test_wave_rejects_platform_mismatch_and_revoked_release(self):
        self.manifest('release', {'base': 4})
        self.send('rollout', 'node-enroll', node='m', arch='amd64')
        self.cache('m', 'base')
        blocked = self.send('rollout', 'wave-approve', id='wave', manifest='release', nodes=['m'])
        self.assertEqual('platform_mismatch', blocked['blocked']['m']['reason'])
        self.send('manifest', 'manifest-revoke', id='release')
        self.assertEqual('manifest_unavailable', self.send('rollout', 'wave-approve', id='wave', manifest='release', nodes=['m'])['decision'])
        self.assertTrue(replay(self.path)['verified'])

    def test_charge_shared_layer_once_and_close_an_immutable_statement(self):
        self.manifest('alpha', {'shared': 9, 'a': 3})
        self.manifest('beta', {'shared': 9})
        self.cache('n', 'shared', 'a')
        self.link('ra', 'alpha', 'alpha')
        self.link('ra2', 'alpha', 'alpha')
        self.link('rb', 'beta', 'beta')
        old = self.send('chargeback', 'statement-close', id='old', node='n')
        self.assertEqual({'decision': 'statement_closed', 'bytes': 12, 'tenant_bytes': {'alpha': 8, 'beta': 4}}, old)
        self.send('retention', 'reference-release', id='release-b', ref='rb', uid='rb', termination_receipt='term', delete_receipt='drop')
        new = self.send('chargeback', 'statement-close', id='new', node='n')
        self.assertEqual({'decision': 'statement_closed', 'bytes': 12, 'tenant_bytes': {'alpha': 12}}, new)
        old_b = rows(self.db, "SELECT bytes FROM cache_charges WHERE statement='old' AND tenant='beta'")
        self.assertEqual([{'bytes': 4}], old_b)
        self.assertTrue(replay(self.path)['verified'])

    def test_missing_tenant_link_blocks_statement_without_writes(self):
        self.manifest('alpha', {'base': 4})
        self.cache('n', 'base')
        self.link('ra', 'alpha')
        self.assertEqual('attribution_incomplete', self.send('chargeback', 'statement-close', id='s', node='n')['decision'])
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_statements'))
        self.assertFalse(rows(self.db, 'SELECT * FROM cache_charges'))
        self.assertTrue(replay(self.path)['verified'])
