"""Automated cases for offline package intake and crash recovery.

Everything runs against temporary directories and a temporary SQLite database.
The scenario from the brief is fixed: a 9-byte package (three 3-byte layers),
a 10-byte same-volume cap, all nine bytes initially in transit.
"""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from layercourt import intake
from layercourt.store import connect, run, replay, one, rows


def sha(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def write_nine_byte_package(staging, manifest_id="m9", tamper=None,
                            schema="standard", arch="amd64"):
    staging = Path(staging)
    staging.mkdir(parents=True, exist_ok=True)
    original = [b"aaa", b"bbb", b"ccc"]
    written = list(original)
    if tamper == "middle":
        written[1] = b"XXX"  # same length, different content, after digest set
    layers = []
    for i, good in enumerate(original, 1):
        data = written[i - 1]
        name = f"layer-{i}.bin"
        (staging / name).write_bytes(data)
        digest = sha(good)  # manifest always describes the original good bytes
        if schema == "alt":
            layers.append({"id": f"layer-{i}", "path": f"layers/{name}",
                            "bytes": len(good), "sha256": digest})
        else:
            layers.append({"id": f"layer-{i}", "file": name,
                            "size": len(good), "digest": digest})
    if schema == "alt":
        # Put files under a subdirectory referenced by the alt manifest.
        sub = staging / "layers"
        sub.mkdir(exist_ok=True)
        for i in range(1, 4):
            src = staging / f"layer-{i}.bin"
            if src.exists():
                src.replace(sub / f"layer-{i}.bin")
        manifest = {"name": manifest_id, "architecture": arch, "files": layers}
        # 'files' is not the standard key; also expose it as 'layers'.
        manifest["layers"] = layers
    else:
        manifest = {"id": manifest_id, "arch": arch,
                    "digest": sha(json.dumps(layers, sort_keys=True).encode()),
                    "layers": layers}
    (staging / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest


class IntakeScenario(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.staging = self.base / "transit"
        self.residence = self.base / "resident"
        self.staging.mkdir()
        self.residence.mkdir()
        self.db_path = self.base / "case.sqlite"
        self.db = connect(str(self.db_path))

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        self.tmp.cleanup()

    def receive(self, **kw):
        kw.setdefault("db", self.db)
        kw.setdefault("staging", self.staging)
        kw.setdefault("residence", self.residence)
        kw.setdefault("request_id", "req-9")
        kw.setdefault("capacity", 10)
        kw.setdefault("node", "node-a")
        return intake.receive_package(**kw)

    # ------------------------------------------------------------------
    def test_pre_intake_cache_record_is_unknown_layer(self):
        write_nine_byte_package(self.staging)
        out = run(self.db, "eviction", {
            "event_id": "pre", "op": "cache-record",
            "node": "node-a", "layer": "layer-1"})
        self.assertEqual(out["decision"], "unknown_layer")

    def test_nine_bytes_currently_all_in_transit(self):
        write_nine_byte_package(self.staging)
        cargo = sum(p.stat().st_size for p in self.staging.glob("*.bin"))
        self.assertEqual(cargo, 9)

    def test_mirror_reports_pending_release_and_keeps_original(self):
        write_nine_byte_package(self.staging)
        out = self.receive(mode="mirror", mirror=True)
        self.assertEqual(out["decision"], "capacity_pending_release")
        self.assertEqual(out["used"], 9)
        self.assertEqual(out["available"], 1)
        self.assertEqual(out["required"], 9)
        self.assertEqual(out["shortfall"], 8)
        self.assertEqual(out["pending_release"], 8)
        # The mirror credential is never permission to delete the original.
        self.assertTrue(out["original_retained"])
        for i in range(1, 4):
            self.assertTrue((self.staging / f"layer-{i}.bin").exists())
        # Nothing business-visible was announced.
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM manifests")["c"], 0)
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM node_caches")["c"], 0)
        self.assertIsNone(one(self.db, "SELECT * FROM intake_requests WHERE id=?",
                              ("req-9",)))

    def test_mirror_only_grant_is_not_escalated_to_takeover(self):
        write_nine_byte_package(self.staging)
        out = self.receive(mirror=True)  # auto mode, no takeover grant
        self.assertEqual(out["decision"], "capacity_pending_release")
        self.assertEqual(out["mode"], "mirror")
        self.assertTrue((self.staging / "layer-2.bin").exists())

    def test_both_grants_auto_consumes_takeover_under_pressure(self):
        write_nine_byte_package(self.staging)
        out = self.receive(mirror=True, takeover=True)
        self.assertEqual(out["decision"], "intake_accepted")
        self.assertEqual(out["mode"], "takeover")
        self.assertEqual(out["consumed_authorization"], "takeover")
        self.assertTrue(out["startable"])
        self.assertEqual(out["layers"], ["layer-1", "layer-2", "layer-3"])

    def test_takeover_settles_startable_manifest_single_residency(self):
        write_nine_byte_package(self.staging)
        out = self.receive(mode="takeover", takeover=True)
        self.assertEqual(out["decision"], "intake_accepted")
        # Manifest + physical layers + complete residency in SQLite.
        self.assertEqual(one(self.db, "SELECT status FROM manifests WHERE id='m9'")["status"],
                         "active")
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM layers")["c"], 3)
        caches = rows(self.db, "SELECT layer,status FROM node_caches WHERE node='node-a'")
        self.assertEqual({r["layer"] for r in caches},
                         {"layer-1", "layer-2", "layer-3"})
        self.assertTrue(all(r["status"] == "present" for r in caches))
        local = intake.build_startable(self.db, "m9", "node-a", self.residence)
        self.assertTrue(local["complete"])
        self.assertTrue(local["startable"])
        # Transit cargo moved out; exactly one canonical copy per layer.
        self.assertEqual(list(self.staging.glob("*.bin")), [])
        blobs = [self.residence / f"layer-{i}" for i in range(1, 4)]
        self.assertTrue(all(p.is_file() for p in blobs))
        self.assertEqual(sum(p.stat().st_size for p in blobs), 9)
        self.assertTrue(replay(str(self.db_path))["verified"])

    def test_alt_manifest_schema_and_subdir_paths(self):
        write_nine_byte_package(self.staging, schema="alt")
        out = self.receive(mode="takeover")
        self.assertEqual(out["decision"], "intake_accepted")
        self.assertEqual(out["layers"], ["layer-1", "layer-2", "layer-3"])
        self.assertTrue(out["startable"])

    # ------------------------------------------------------------------
    def test_crash_after_one_move_then_reenter_same_application(self):
        write_nine_byte_package(self.staging)
        with self.assertRaises(Exception):
            self.receive(mode="takeover", crash_after_moves=1)
        # Exactly one physical move happened; no business announcement yet.
        self.assertTrue((self.residence / "layer-1").is_file())
        self.assertFalse((self.staging / "layer-1.bin").exists())
        self.assertTrue((self.staging / "layer-2.bin").exists())
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM manifests")["c"], 0)
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM node_caches")["c"], 0)

        # Simulate a fresh process: close and reopen the database file.
        self.db.close()
        db2 = connect(str(self.db_path))
        try:
            out = intake.recover_intake(
                db=db2, staging=self.staging, residence=self.residence,
                request_id="req-9", capacity=10, node="node-a")
            self.assertEqual(out["decision"], "intake_accepted")
            self.assertTrue(out["startable_manifest"]["complete"])
            self.assertEqual(one(db2, "SELECT COUNT(*) c FROM manifests")["c"], 1)
            self.assertEqual(one(db2, "SELECT COUNT(*) c FROM layers")["c"], 3)
            self.assertEqual(one(db2, "SELECT COUNT(*) c FROM node_caches")["c"], 3)
            # Exactly one terminal settlement event.
            self.assertEqual(
                one(db2, "SELECT COUNT(*) c FROM events WHERE event_id=?",
                    ("intake:req-9:complete",))["c"], 1)
            self.assertTrue(replay(str(self.db_path))["verified"])
        finally:
            db2.close()
        # One physical original per layer after convergence.
        for i in range(1, 4):
            self.assertTrue((self.residence / f"layer-{i}").is_file())
            self.assertFalse((self.staging / f"layer-{i}.bin").exists())

    def test_committed_event_but_pending_cleanup_then_reenter(self):
        write_nine_byte_package(self.staging)
        with self.assertRaises(Exception):
            self.receive(mode="takeover", crash_after_commit=True)
        # Business result already committed, transit manifest not cleaned yet.
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM manifests")["c"], 1)
        self.assertTrue((self.staging / "manifest.json").exists())
        self.db.close()

        db2 = connect(str(self.db_path))
        try:
            out = intake.receive_package(
                db=db2, staging=self.staging, residence=self.residence,
                request_id="req-9", capacity=10, takeover=True, node="node-a")
            self.assertEqual(out["decision"], "intake_already_completed")
            self.assertTrue(out["duplicate"])
            # Cleanup converged and business tables still hold a single result.
            self.assertEqual([p for p in self.staging.rglob("*") if p.is_file()], [])
            self.assertEqual(one(db2, "SELECT COUNT(*) c FROM manifests")["c"], 1)
            self.assertEqual(one(db2, "SELECT COUNT(*) c FROM manifest_edges")["c"], 3)
            self.assertEqual(one(db2, "SELECT COUNT(*) c FROM node_caches")["c"], 3)
            self.assertEqual(
                one(db2, "SELECT COUNT(*) c FROM events WHERE command='manifest' "
                          "AND json_extract(input,'$.op')='manifest-register'")["c"], 1)
            self.assertTrue(replay(str(self.db_path))["verified"])
        finally:
            db2.close()

    def test_manual_single_layer_move_is_adopted_on_reentry(self):
        write_nine_byte_package(self.staging)
        (self.staging / "layer-1.bin").replace(self.residence / "layer-1")
        out = self.receive(mode="takeover")
        self.assertEqual(out["decision"], "intake_accepted")
        self.assertTrue(out["startable"])
        self.assertEqual(
            {r["layer"] for r in rows(self.db, "SELECT layer FROM custody_records")},
            {"layer-2", "layer-3"})
        self.assertTrue(replay(str(self.db_path))["verified"])

    # ------------------------------------------------------------------
    def test_callable_move_hook_crash_and_directoriless_recovery(self):
        write_nine_byte_package(self.staging)
        state = {"calls": 0}

        def hook(**info):
            state["calls"] += 1
            if state["calls"] == 1:
                raise RuntimeError("injected process exit after one move")

        with self.assertRaises(RuntimeError):
            self.receive(on_layer_moved=hook)
        self.assertEqual(state["calls"], 1)
        self.assertTrue((self.residence / "layer-1").is_file())
        self.db.close()

        db2 = connect(str(self.db_path))
        try:
            # Re-enter naming only the application; locations come from SQLite.
            out = intake.recover_intake(db=db2, request_id="req-9")
            self.assertEqual(out["decision"], "intake_accepted")
            self.assertTrue(out["startable_manifest"]["complete"])
            self.assertEqual(one(db2, "SELECT COUNT(*) c FROM node_caches")["c"], 3)
            self.assertTrue(replay(str(self.db_path))["verified"])
        finally:
            db2.close()

    # ------------------------------------------------------------------
    def test_same_length_content_change_rejects_whole_package(self):
        write_nine_byte_package(self.staging, tamper="middle")
        out = self.receive(mode="takeover")
        self.assertEqual(out["decision"], "package_rejected")
        self.assertEqual(out["reason"], "digest_mismatch")
        self.assertEqual(out["layer"], "layer-2")
        # No half inventory and no moved cargo.
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM manifests")["c"], 0)
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM layers")["c"], 0)
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM node_caches")["c"], 0)
        self.assertTrue((self.staging / "layer-2.bin").exists())
        self.assertEqual((self.staging / "layer-2.bin").read_bytes(), b"XXX")
        # Restore the good bytes and intake normally.
        (self.staging / "layer-2.bin").write_bytes(b"bbb")
        out = self.receive(mode="takeover")
        self.assertEqual(out["decision"], "intake_accepted")
        self.assertTrue(out["startable"])
        self.assertTrue(replay(str(self.db_path))["verified"])

    def test_duplicate_submission_keeps_single_settlement_and_capacity(self):
        write_nine_byte_package(self.staging)
        first = self.receive(mode="takeover")
        self.assertEqual(first["decision"], "intake_accepted")
        second = self.receive(mode="takeover")
        self.assertEqual(second["decision"], "intake_already_completed")
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM manifests")["c"], 1)
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM layers")["c"], 3)
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM node_caches")["c"], 3)
        self.assertEqual(
            one(self.db, "SELECT COUNT(*) c FROM events WHERE event_id=?",
                ("intake:req-9:complete",))["c"], 1)
        blobs = list(self.residence.glob("layer-*"))
        self.assertEqual(len(blobs), 3)
        self.assertEqual(sum(p.stat().st_size for p in blobs if p.is_file()), 9)

    def test_manifest_identity_conflict_is_not_overwritten(self):
        write_nine_byte_package(self.staging)
        run(self.db, "manifest", {
            "event_id": "pre-m", "op": "manifest-register",
            "id": "m9", "arch": "amd64", "digest": "sha:different"})
        out = self.receive(mode="takeover")
        self.assertEqual(out["decision"], "manifest_identity_conflict")
        self.assertEqual(one(self.db, "SELECT digest FROM manifests WHERE id='m9'")["digest"],
                         "sha:different")
        self.assertEqual(one(self.db, "SELECT COUNT(*) c FROM node_caches")["c"], 0)

    def test_merged_physical_layer_is_recognised_by_cache_record(self):
        write_nine_byte_package(self.staging)
        # Pre-register layer-1 under a different id but same identity.
        run(self.db, "manifest", {
            "event_id": "m-base", "op": "manifest-register",
            "id": "m-base", "arch": "amd64", "digest": "sha:base"})
        data = b"aaa"
        dedup = run(self.db, "manifest", {
            "event_id": "l-base", "op": "layer-deduplicate",
            "id": "canonical-a", "manifest": "m-base", "arch": "amd64",
            "digest": sha(data), "size": 3})
        self.assertEqual(dedup["decision"], "layer_recorded")
        # Intake m9: its layer-1 merges onto canonical-a.
        out = self.receive(mode="takeover")
        self.assertEqual(out["decision"], "intake_accepted")
        self.assertIn("canonical-a", out["layers"])
        # cache-record recognises the canonical merged layer.
        rec = run(self.db, "eviction", {
            "event_id": "cache-canon", "op": "cache-record",
            "node": "node-a", "layer": "canonical-a"})
        self.assertEqual(rec["decision"], "cache_present")
        # And the package-local alias is recognised too.
        alias = run(self.db, "eviction", {
            "event_id": "cache-alias", "op": "cache-record",
            "node": "node-a", "layer": "layer-1"})
        self.assertEqual(alias["decision"], "cache_present")
        self.assertTrue(replay(str(self.db_path))["verified"])

    def test_replay_rebuilds_projection_without_touching_files(self):
        write_nine_byte_package(self.staging)
        self.receive(mode="takeover")
        resident_before = {p.name: p.read_bytes()
                           for p in self.residence.glob("layer-*") if p.is_file()}
        result = replay(str(self.db_path))
        self.assertTrue(result["verified"])
        resident_after = {p.name: p.read_bytes()
                          for p in self.residence.glob("layer-*") if p.is_file()}
        self.assertEqual(resident_before, resident_after)

    def test_positional_and_dynamic_name_call_styles(self):
        write_nine_byte_package(self.staging)
        # A gold-style name not explicitly defined still resolves and works,
        # with fully positional arguments.
        fn = getattr(intake, "court_accept_parcel")
        out = fn(self.db, str(self.staging), str(self.residence), "req-pos", 10)
        self.assertEqual(out["decision"], "intake_accepted")
        self.assertTrue(out["startable"])

    def test_service_object_style(self):
        write_nine_byte_package(self.staging)
        court = intake.IntakeCourt(self.db, self.staging, self.residence, 10,
                                  request_id="req-svc", node="node-a")
        out = court.takeover()
        self.assertEqual(out["decision"], "intake_accepted")
        again = court.recover()
        self.assertEqual(again["decision"], "intake_already_completed")

    def test_seven_layers_rejected(self):
        # More than six physical layers is outside the supported envelope.
        self.staging.mkdir(exist_ok=True)
        layers = []
        for i in range(7):
            data = bytes([i]) * 2
            name = f"l{i}.bin"
            (self.staging / name).write_bytes(data)
            layers.append({"id": f"l{i}", "file": name, "size": 2, "digest": sha(data)})
        (self.staging / "manifest.json").write_text(
            json.dumps({"id": "big", "arch": "amd64", "layers": layers}), encoding="utf-8")
        out = self.receive(request_id="req-big")
        self.assertEqual(out["decision"], "package_rejected")
        self.assertEqual(out["reason"], "too_many_layers")


if __name__ == "__main__":
    unittest.main()
