import json
import tempfile
import unittest
from pathlib import Path

from layercourt import retirement
from layercourt.store import connect, one, replay, rows, run


class RetirementCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / 'db.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        self.seq = 0

    def act(self, op, **fields):
        self.seq += 1
        return run(self.db, 'retirement', dict(event_id=f'e{self.seq}', op=op, **fields))

    def manifest(self, release, layers):
        run(self.db, 'manifest', dict(event_id=f'm:{release}', op='manifest-register',
                                      id=release, arch='amd64', digest='sha:' + release))
        for layer, size in layers.items():
            run(self.db, 'manifest', dict(event_id=f'l:{layer}', op='layer-deduplicate',
                                          id=layer, manifest=release, arch='amd64',
                                          digest='sha:' + layer, size=size))

    def cache(self, node, *layers):
        for layer in layers:
            run(self.db, 'eviction', dict(event_id=f'c:{node}:{layer}', op='cache-record',
                                          node=node, layer=layer))

    def reference(self, ref, node, generation, manifest, pod='pod-a', uid=None):
        return run(self.db, 'retention', dict(event_id=f'w:{ref}', op='workload-reference',
                                              id=ref, pod=pod, uid=uid or ('uid-' + ref),
                                              manifest=manifest, node=node,
                                              generation=generation))

    def pod_credential(self, credential, ref, termination, deletion, exit_id=None, **extra):
        row = one(self.db, 'SELECT * FROM workload_refs WHERE id=?', (ref,))
        fields = dict(node=row['node'], uid=row['uid'], pod=row['pod'],
                      generation=row['generation'], ref=ref,
                      termination_receipt=termination, delete_receipt=deletion,
                      exit=exit_id or ('exit-' + ref))
        fields.update(extra)
        return self.act('retirement-credential', id=credential, **fields)

    def plan(self, node, layer, needed, at=1):
        ident = f'p:{node}:{layer}:{needed}:{at}'
        return run(self.db, 'eviction', dict(event_id=ident, op='eviction-plan', id=ident,
                                             node=node, layer=layer, needed=needed, at=at))

    def status(self, ref):
        return one(self.db, 'SELECT status FROM workload_refs WHERE id=?', (ref,))['status']

    # -- acceptance board -------------------------------------------------
    def board(self, after_outage_generation=True):
        self.manifest('rel2', {'L2': 10})
        self.manifest('rel3', {'L3': 10})
        self.manifest('rel4', {'L4': 10})
        self.cache('node-a', 'L2', 'L3', 'L4')
        # One Pod, generations 2/3/4; each generation exclusively owns its layer.
        self.assertEqual(self.reference('ref-g2', 'node-a', 2, 'rel2')['decision'], 'reference_active')
        self.assertEqual(self.reference('ref-g3', 'node-a', 3, 'rel3')['decision'], 'reference_active')
        # Complete pod evidence only for generation 3.
        self.assertEqual(self.pod_credential('cred-g3', 'ref-g3', 'term-3', 'drop-3')['decision'],
                         'credential_recorded')
        # Outage H freezes only generations 2 and 3, both phases same witness.
        frozen = self.act('retirement-outage', id='H', node='node-a',
                          refs=['ref-g2', 'ref-g3'], witness='witness-h')
        self.assertEqual(frozen['decision'], 'outage_frozen')
        self.assertEqual(self.act('retirement-credential', id='H-exit', outage='H', node='node-a',
                                  phase='exit', termination_receipt='h-term',
                                  witness='witness-h')['decision'], 'evidence_recorded')
        self.assertEqual(self.act('retirement-credential', id='H-purge', outage='H', node='node-a',
                                  phase='purge', delete_receipt='h-drop',
                                  witness='witness-h')['decision'], 'evidence_recorded')
        # Generation 4 is created after the outage snapshot and must stay retained.
        self.assertEqual(self.reference('ref-g4', 'node-a', 4, 'rel4')['decision'], 'reference_active')

    def test_pod_settlement_releases_only_generation_three(self):
        self.board()
        self.assertEqual(self.plan('node-a', 'L2', 5)['decision'], 'layer_protected_or_missing')
        out = self.act('retirement-settle', id='s-pod', node='node-a', authority='pod')
        self.assertEqual(out['decision'], 'settled')
        self.assertEqual(out['scope'], 'pod')
        self.assertEqual(out['released_refs'], ['ref-g3'])
        by_ref = {detail['ref']: detail for detail in out['released']}
        self.assertEqual(by_ref['ref-g3']['evidence'], 'pod')
        self.assertEqual(by_ref['ref-g3']['termination_receipt'], 'term-3')
        self.assertEqual(by_ref['ref-g3']['delete_receipt'], 'drop-3')
        # L2 and L4 stay protected; L3 is now evictable.
        self.assertEqual(self.plan('node-a', 'L2', 5, at=2)['decision'], 'layer_protected_or_missing')
        self.assertEqual(self.plan('node-a', 'L4', 5, at=2)['decision'], 'layer_protected_or_missing')
        self.assertEqual(self.plan('node-a', 'L3', 5, at=2)['decision'], 'planned')
        self.assertEqual(self.status('ref-g2'), 'active')
        self.assertEqual(self.status('ref-g4'), 'active')
        self.assertTrue(replay(self.path)['verified'])

    def test_outage_settlement_releases_frozen_batch_and_keeps_later_generation(self):
        self.board()
        out = self.act('retirement-settle', id='s-out', node='node-a', authority='outage', outages=['H'])
        self.assertEqual(out['decision'], 'settled')
        self.assertEqual(out['scope'], 'outage')
        self.assertEqual(sorted(out['released_refs']), ['ref-g2', 'ref-g3'])
        self.assertEqual({d['ref']: d['evidence'] for d in out['released']},
                         {'ref-g2': 'outage', 'ref-g3': 'outage'})
        # Only L4 remains protected after node settlement.
        self.assertEqual(self.plan('node-a', 'L2', 5, at=2)['decision'], 'planned')
        self.assertEqual(self.plan('node-a', 'L3', 5, at=2)['decision'], 'planned')
        self.assertEqual(self.plan('node-a', 'L4', 5, at=2)['decision'], 'layer_protected_or_missing')
        self.assertEqual(self.status('ref-g4'), 'active')
        self.assertTrue(replay(self.path)['verified'])

    def test_two_outages_each_holding_one_phase_never_combine(self):
        self.manifest('rel2', {'L2': 10})
        self.manifest('rel3', {'L3': 10})
        self.cache('node-a', 'L2', 'L3')
        self.reference('ref-g2', 'node-a', 2, 'rel2')
        self.reference('ref-g3', 'node-a', 3, 'rel3')
        self.pod_credential('cred-g3', 'ref-g3', 'term-3', 'drop-3')
        # H only sees the exit; K only sees the purge, and both name generation 2.
        self.assertEqual(self.act('retirement-outage', id='H', node='node-a',
                                  refs=['ref-g2'], witness='w-h')['decision'], 'outage_frozen')
        self.assertEqual(self.act('retirement-outage', id='K', node='node-a',
                                  refs=['ref-g2'], witness='w-k')['decision'], 'outage_frozen')
        self.act('retirement-credential', id='H-exit', outage='H', node='node-a', phase='exit',
                 termination_receipt='h-term', witness='w-h')
        self.act('retirement-credential', id='K-purge', outage='K', node='node-a', phase='purge',
                 delete_receipt='k-drop', witness='w-k')
        out = self.act('retirement-settle', id='s-out', node='node-a', authority='outage',
                       outages=['H', 'K'])
        self.assertEqual(out['decision'], 'settlement_denied')
        self.assertEqual(self.status('ref-g2'), 'active')
        self.assertEqual(self.plan('node-a', 'L2', 5)['decision'], 'layer_protected_or_missing')
        # The generation-3 pod evidence is still independently usable.
        pod = self.act('retirement-settle', id='s-pod', node='node-a', authority='pod')
        self.assertEqual(pod['decision'], 'settled')
        self.assertEqual(pod['released_refs'], ['ref-g3'])
        self.assertEqual(self.status('ref-g2'), 'active')
        self.assertEqual(self.plan('node-a', 'L3', 5, at=2)['decision'], 'planned')
        self.assertEqual(self.plan('node-a', 'L2', 5, at=2)['decision'], 'layer_protected_or_missing')
        self.assertTrue(replay(self.path)['verified'])

    def test_outage_witnesses_must_agree(self):
        self.manifest('rel2', {'L2': 10})
        self.cache('node-a', 'L2')
        self.reference('ref-g2', 'node-a', 2, 'rel2')
        self.act('retirement-outage', id='H', node='node-a', refs=['ref-g2'], witness='w-h')
        self.act('retirement-credential', id='H-exit', outage='H', node='node-a', phase='exit',
                 termination_receipt='h-term', witness='w-h')
        self.act('retirement-credential', id='H-purge', outage='H', node='node-a', phase='purge',
                 delete_receipt='h-drop', witness='forged')
        out = self.act('retirement-settle', id='s', node='node-a', authority='outage', outages=['H'])
        self.assertEqual(out['decision'], 'settlement_denied')
        self.assertEqual(self.status('ref-g2'), 'active')
        self.assertTrue(replay(self.path)['verified'])

    def test_outage_deposit_of_different_application_does_not_settle(self):
        self.manifest('rel2', {'L2': 10})
        self.cache('node-a', 'L2')
        self.reference('ref-g2', 'node-a', 2, 'rel2')
        self.act('retirement-outage', id='H', node='node-a', refs=['ref-g2'], witness='w-h')
        self.act('retirement-outage', id='K', node='node-a', refs=['ref-g2'], witness='w-k')
        self.act('retirement-credential', id='H-exit', outage='H', node='node-a', phase='exit',
                 termination_receipt='h-term', witness='w-h')
        self.act('retirement-credential', id='K-purge', outage='K', node='node-a', phase='purge',
                 delete_receipt='k-drop', witness='w-k')
        out = self.act('retirement-settle', id='s', node='node-a', authority='outage', outages=['H'])
        self.assertEqual(out['decision'], 'settlement_denied')
        self.assertEqual(self.status('ref-g2'), 'active')

    def test_pod_receipts_bind_generation_and_do_not_touch_later_generation(self):
        self.manifest('rel3', {'L3': 10})
        self.manifest('rel4', {'L4': 10})
        self.cache('node-a', 'L3', 'L4')
        self.reference('ref-g3', 'node-a', 3, 'rel3', pod='pod-a', uid='uid-3')
        self.reference('ref-g4', 'node-a', 4, 'rel4', pod='pod-a', uid='uid-4')
        stale = self.act('retirement-credential', id='bad', node='node-a', uid='uid-3',
                         pod='pod-a', generation=4, ref='ref-g3',
                         termination_receipt='t', delete_receipt='d', exit='e')
        self.assertEqual(stale['decision'], 'credential_mismatch')
        self.assertEqual(stale['field'], 'generation')
        # Missing a receipt cannot be settled.
        missing = self.act('retirement-credential', id='half', node='node-a', uid='uid-3',
                           pod='pod-a', generation=3, ref='ref-g3',
                           termination_receipt='t', exit='e')
        self.assertEqual(missing['decision'], 'credential_unproven')
        self.assertEqual(self.pod_credential('ok', 'ref-g3', 'term', 'drop')['decision'],
                         'credential_recorded')
        out = self.act('retirement-settle', id='s', node='node-a', authority='pod')
        self.assertEqual(out['released_refs'], ['ref-g3'])
        self.assertEqual(self.status('ref-g4'), 'active')
        self.assertEqual(self.plan('node-a', 'L3', 5, at=2)['decision'], 'planned')
        self.assertEqual(self.plan('node-a', 'L4', 5, at=2)['decision'], 'layer_protected_or_missing')

    def test_replay_and_retransmission_are_idempotent(self):
        self.board()
        first = self.act('retirement-settle', id='s-pod', node='node-a', authority='pod')
        again = run(self.db, 'retirement', dict(event_id='e-new', op='retirement-settle',
                                                id='s-pod', node='node-a', authority='pod'))
        self.assertEqual(again, dict(first, decision='settlement_reused'))
        conflict = run(self.db, 'retirement', dict(event_id='e-conf', op='retirement-settle',
                                                   id='s-pod', node='node-a', authority='outage'))
        self.assertEqual(conflict['decision'], 'settlement_identity_conflict')
        # Re-sending the very same pod credential does not reach a newer generation.
        self.assertEqual(self.act('retirement-credential', id='cred-g3', ref='ref-g3',
                                  node='node-a', uid='uid-ref-g3', pod='pod-a', generation=3,
                                  termination_receipt='term-3', delete_receipt='drop-3',
                                  exit='exit-ref-g3')['decision'], 'credential_reused')
        mismatch = self.act('retirement-credential', id='cred-g3', ref='ref-g4',
                            node='node-a', uid='uid-ref-g4', pod='pod-a', generation=4,
                            termination_receipt='term-4', delete_receipt='drop-4',
                            exit='exit-ref-g4')
        self.assertEqual(mismatch['decision'], 'credential_identity_conflict')
        self.assertEqual(self.status('ref-g4'), 'active')
        self.assertTrue(replay(self.path)['verified'])

    def test_scope_is_never_mixed(self):
        self.board()
        pod = self.act('retirement-settle', id='s1', node='node-a', authority='pod')
        self.assertEqual(pod['released_refs'], ['ref-g3'])
        self.assertEqual({d['evidence'] for d in pod['released']}, {'pod'})
        outage = self.act('retirement-settle', id='s2', node='node-a', authority='outage')
        self.assertEqual(sorted(outage['released_refs']), ['ref-g2'])
        self.assertEqual({d['evidence'] for d in outage['released']}, {'outage'})
        # A pod-authorised request may not borrow the outage scope.
        refused = self.act('retirement-settle', id='s3', node='node-a', authority='pod', outages=['H'])
        self.assertEqual(refused['decision'], 'settlement_invalid')

    def test_limits(self):
        self.manifest('rel', {f'L{i}': 5 for i in range(12)})
        refs = []
        for i in range(12):
            ref = f'r{i}'
            self.reference(ref, 'node-a', i + 1, 'rel', pod=f'pod-{i}')
            refs.append(ref)
        frozen = self.act('retirement-outage', id='big', node='node-a', refs=refs)
        self.assertEqual(frozen['decision'], 'retirement_limit_exceeded')
        self.assertEqual(self.act('retirement-settle', id='s', node='node-a', authority='outage',
                                  outages=['a', 'b', 'c'])['decision'], 'retirement_limit_exceeded')

    def test_existing_release_still_works(self):
        self.board()
        run(self.db, 'retention', dict(event_id='rel', op='reference-release', id='rel-old',
                                       ref='ref-g2', uid='uid-ref-g2',
                                       termination_receipt='t', delete_receipt='d'))
        self.assertEqual(self.status('ref-g2'), 'released')
        self.assertTrue(replay(self.path)['verified'])

    def test_default_authority_prefers_complete_pod_scope(self):
        self.board()
        out = self.act('retirement-settle', id='s-default', node='node-a')
        self.assertEqual(out['decision'], 'settled')
        self.assertEqual(out['scope'], 'pod')
        self.assertEqual(out['released_refs'], ['ref-g3'])
        self.assertEqual({d['evidence'] for d in out['released']}, {'pod'})

    def test_default_authority_falls_back_to_proven_outage(self):
        self.manifest('rel2', {'L2': 10})
        self.cache('node-a', 'L2')
        self.reference('ref-g2', 'node-a', 2, 'rel2')
        self.act('retirement-outage', id='H', node='node-a', refs=['ref-g2'], witness='W')
        self.act('retirement-credential', id='H', outage='H', node='node-a',
                 termination_receipt='t', delete_receipt='d', witness='W')
        out = self.act('retirement-settle', id='s-default', node='node-a')
        self.assertEqual(out['scope'], 'outage')
        self.assertEqual(out['released_refs'], ['ref-g2'])

    def test_one_exit_cannot_be_reused_for_another_reference(self):
        self.manifest('rel2', {'L2': 10})
        self.manifest('rel3', {'L3': 10})
        self.cache('node-a', 'L2', 'L3')
        self.reference('ref-a', 'node-a', 2, 'rel2', pod='pod-a', uid='uid-a')
        self.reference('ref-b', 'node-a', 3, 'rel3', pod='pod-b', uid='uid-b')
        self.pod_credential('cred-a', 'ref-a', 'term', 'drop', exit_id='shared-exit')
        reused = self.act('retirement-credential', id='cred-b', ref='ref-b',
                          node='node-a', uid='uid-b', pod='pod-b', generation=3,
                          termination_receipt='term', delete_receipt='drop',
                          exit='shared-exit')
        self.assertEqual(reused['decision'], 'credential_identity_conflict')
        self.assertEqual(reused['field'], 'exit')
        self.assertEqual(self.status('ref-b'), 'active')

    def test_outage_snapshot_does_not_include_later_same_name_reference(self):
        self.manifest('rel2', {'L2': 10})
        self.cache('node-a', 'L2')
        self.reference('ref-old', 'node-a', 2, 'rel2', pod='pod-a')
        self.act('retirement-outage', id='H', node='node-a', refs=['ref-old'], witness='W')
        self.act('retirement-credential', id='H', outage='H', node='node-a',
                 termination_receipt='t', delete_receipt='d', witness='W')
        # A later generation with the same Pod name is out of the frozen scope.
        run(self.db, 'manifest', dict(event_id='m:rel4', op='manifest-register',
                                      id='rel4', arch='amd64', digest='sha:rel4'))
        run(self.db, 'manifest', dict(event_id='l:L4', op='layer-deduplicate', id='L4',
                                      manifest='rel4', arch='amd64', digest='sha:L4', size=10))
        self.cache('node-a', 'L4')
        self.assertEqual(self.reference('ref-new', 'node-a', 4, 'rel4', pod='pod-a')['decision'],
                         'reference_active')
        out = self.act('retirement-settle', id='s', node='node-a', authority='outage', outages=['H'])
        self.assertEqual(out['released_refs'], ['ref-old'])
        self.assertEqual(self.status('ref-new'), 'active')

    def test_authority_cannot_be_smuggled_between_scopes(self):
        self.board()
        self.assertEqual(self.act('retirement-settle', id='s1', node='node-a',
                                  authority='pod', outages=['H'])['decision'],
                         'settlement_invalid')
        self.assertEqual(self.act('retirement-settle', id='s2', node='node-a',
                                  authority='nonsense')['decision'], 'settlement_invalid')

    def test_unwitnessed_phases_from_same_freeze_agree(self):
        self.manifest('rel2', {'L2': 10})
        self.cache('node-a', 'L2')
        self.reference('ref-g2', 'node-a', 2, 'rel2')
        self.act('retirement-outage', id='H', node='node-a', refs=['ref-g2'])
        self.act('retirement-credential', id='H-exit', outage='H', phase='exit',
                 termination_receipt='t')
        self.act('retirement-credential', id='H-purge', outage='H', phase='purge',
                 delete_receipt='d')
        out = self.act('retirement-settle', id='s', node='node-a', authority='outage', outages=['H'])
        self.assertEqual(out['released_refs'], ['ref-g2'])


if __name__ == '__main__':
    unittest.main()
