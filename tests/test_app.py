import tempfile
import unittest
from pathlib import Path

from app import Database, seed_demo


class OceanSyncFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.voyage = seed_demo(self.db)["voyage"]

    def tearDown(self):
        self.tmp.cleanup()

    def record(self, kind, uuid, revision, data, device="tablet-A"):
        return {"type": kind, "local_uuid": uuid, "revision": revision, "data": data}

    def test_full_offline_sync_conflict_confirm_and_file_dedupe(self):
        station = {"voyage_id": self.voyage, "station_code": "S-01", "latitude": 30.1, "longitude": 122.0, "sampled_at": "2026-09-05T08:30:00+08:00", "owner": "member-a"}
        batch = {"device_id": "tablet-A", "records": [self.record("station", "st-001", 1, station)]}
        first = self.db.sync("member-a", "member", batch)
        self.assertEqual(first["created"], 1)
        duplicate = self.db.sync("member-a", "member", batch)
        self.assertEqual(duplicate["duplicates"], 1)
        station_id = self.db.list_stations()[0]["id"]

        sample = {"station_id": station_id, "sample_code": "W-001", "sample_type": "water", "depth_m": 5, "storage_condition": "4C", "owner": "member-a"}
        self.db.sync("member-a", "member", {"device_id": "tablet-A", "records": [self.record("sample", "sample-001", 1, sample)]})
        updated = dict(sample, storage_condition="negative-20C")
        result = self.db.sync("member-a", "member", {"device_id": "tablet-A", "records": [self.record("sample", "sample-001", 2, updated)]})
        self.assertEqual(result["updated"], 1)
        stale = self.db.sync("member-a", "member", {"device_id": "tablet-A", "records": [self.record("sample", "sample-001", 1, sample)]})
        self.assertEqual(len(stale["conflicts"]), 1)
        sample_id = self.db.list_samples()[0]["id"]
        self.db.confirm("sample", sample_id, "lead-01", "lead")
        locked = self.db.sync("member-a", "member", {"device_id": "tablet-A", "records": [self.record("sample", "sample-001", 3, dict(updated, depth_m=6))]})
        self.assertIn("不能覆盖", locked["conflicts"][0]["reason"])

        custody = {"sample_id": sample_id, "event_type": "handover", "from_party": "member-a", "to_party": "shore-lab", "occurred_at": "2026-09-26T09:00:00+08:00"}
        self.db.sync("member-a", "member", {"device_id": "tablet-A", "records": [self.record("custody", "custody-001", 1, custody)]})
        self.assertEqual(len(self.db.list_custody()), 1)

        digest = "a" * 64
        file_record = {"voyage_id": self.voyage, "station_id": station_id, "file_name": "ctd.csv", "sha256": digest, "size_bytes": 120, "captured_at": "2026-09-05T08:20:00+08:00"}
        self.db.sync("member-a", "member", {"device_id": "tablet-A", "records": [self.record("instrument_file", "file-001", 1, file_record)]})
        duplicate_file = self.db.sync("member-b", "member", {"device_id": "tablet-B", "records": [self.record("instrument_file", "file-002", 1, file_record, "tablet-B")]})
        self.assertEqual(duplicate_file["duplicates"], 1)
        self.assertEqual(len(self.db.list_files()), 1)

    def test_duplicate_code_and_member_update_are_isolated(self):
        station = {"voyage_id": self.voyage, "station_code": "S-01", "latitude": 30.1, "longitude": 122.0, "sampled_at": "2026-09-05T08:30:00+08:00", "owner": "member-a"}
        self.db.sync("member-a", "member", {"device_id": "A", "records": [self.record("station", "st-a", 1, station, "A")]})
        station_id = self.db.list_stations()[0]["id"]
        sample = {"station_id": station_id, "sample_code": "W-001", "sample_type": "water", "depth_m": 5, "storage_condition": "4C", "owner": "member-a"}
        self.db.sync("member-a", "member", {"device_id": "A", "records": [self.record("sample", "sample-a", 1, sample, "A")]})
        conflict = self.db.sync("member-b", "member", {"device_id": "B", "records": [self.record("sample", "sample-b", 1, sample, "B")]})
        self.assertEqual(conflict["created"], 1)
        self.assertIn("已分配", conflict["conflicts"][0]["reason"])
        samples = self.db.list_samples()
        sample_b = next(item for item in samples if item["id"] != samples[0]["id"])
        unauthorized = self.db.sync("member-c", "member", {"device_id": "B", "records": [self.record("sample", "sample-b", 2, dict(sample, sample_code=sample_b["sample_code"], depth_m=9), "B")]})
        self.assertIn("只有记录人", unauthorized["conflicts"][0]["reason"])

    # ----- 冲突处置台 -----

    def _station_sample(self, code="S-01", sample_code="W-001"):
        station = {"voyage_id": self.voyage, "station_code": code, "latitude": 30.1, "longitude": 122.0,
                   "sampled_at": "2026-09-05T08:30:00+08:00", "owner": "member-a"}
        self.db.sync("member-a", "member", {"device_id": "A", "records": [self.record("station", "st-a", 1, station, "A")]})
        station_id = self.db.list_stations()[0]["id"]
        sample = {"station_id": station_id, "sample_code": sample_code, "sample_type": "water",
                  "depth_m": 5, "storage_condition": "4C", "owner": "member-a"}
        self.db.sync("member-a", "member", {"device_id": "A", "records": [self.record("sample", "sa", 1, sample, "A")]})
        return station_id, self.db.list_samples()[0]["id"]

    def _dup_conflict(self, overrides=None, device="B", uuid="sb"):
        incoming = {"sample_code": "W-001", "sample_type": "water", "depth_m": 8,
                    "storage_condition": "negative-20C", "owner": "member-b",
                    "station_id": self.db.list_stations()[0]["id"]}
        if overrides:
            incoming.update(overrides)
        result = self.db.sync("member-b", "member", {"device_id": device, "records": [self.record("sample", uuid, 1, incoming, device)]})
        return result["conflicts"][0]["id"], incoming

    def test_conflict_list_filters_by_voyage_and_status(self):
        self._station_sample()
        cid, _ = self._dup_conflict()
        self.assertEqual(len(self.db.list_conflicts(status="pending")), 1)
        self.assertEqual(len(self.db.list_conflicts(status="resolved")), 0)
        self.assertEqual(len(self.db.list_conflicts(voyage_id=self.voyage)), 1)
        self.assertEqual(len(self.db.list_conflicts(voyage_id=999)), 0)
        detail = self.db.get_conflict_detail(cid)
        self.assertEqual(detail["incoming"]["storage_condition"], "negative-20C")
        self.assertEqual(detail["current_revision"], 1)
        self.assertEqual(detail["current"]["depth_m"], 5)

    def test_merge_only_changes_selected_fields_and_bumps_revision(self):
        self._station_sample()
        cid, incoming = self._dup_conflict()
        res = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"decision": "merge", "base_revision": 1,
                                        "field_values": {"storage_condition": "negative-20C"}})
        self.assertEqual(res["status"], "resolved")
        self.assertEqual(res["new_revision"], 2)
        sample = self.db.list_samples()[0]
        self.assertEqual(sample["storage_condition"], "negative-20C")  # 采用离线
        self.assertEqual(sample["depth_m"], 5)                        # 未勾选，保留岸端
        self.assertEqual(self.db.get_conflict_detail(cid)["status"], "resolved")
        records = self.db.list_resolution_records("sample", sample["id"])
        self.assertEqual(records[0]["decision"], "merge")
        self.assertEqual((records[0]["old_revision"], records[0]["new_revision"]), (1, 2))

    def test_take_offline_and_keep_shore(self):
        self._station_sample()
        cid, _ = self._dup_conflict()
        self.db.resolve_conflict(cid, "lead-01", "lead", {"decision": "take_offline", "base_revision": 1})
        self.assertEqual(self.db.list_samples()[0]["depth_m"], 8)
        self.assertEqual(self.db.list_samples()[0]["storage_condition"], "negative-20C")

        cid2, _ = self._dup_conflict(device="C", uuid="sc")
        # 岸端已在上一步变为离线值，保留岸端后值不变但仍生成新修订
        self.db.resolve_conflict(cid2, "lead-01", "lead", {"decision": "keep_shore", "base_revision": 2})
        sample = self.db.list_samples()[0]
        self.assertEqual(sample["depth_m"], 8)
        self.assertEqual(sample["revision"], 3)

    def test_stale_base_revision_returns_to_pending_with_contention(self):
        self._station_sample()
        cid, _ = self._dup_conflict()
        # 当前修订为 1，处理人却带着 base_revision=99 提交
        out = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"decision": "take_offline", "base_revision": 99})
        self.assertEqual(out["status"], "pending")
        self.assertEqual(out["contention"][0]["type"], "revision_changed")
        detail = self.db.get_conflict_detail(cid)
        self.assertEqual(detail["status"], "pending")
        self.assertEqual(detail["attempts"], 1)
        self.assertEqual(detail["contention"][0]["actual"], 1)
        # 记录未被修改
        self.assertEqual(self.db.list_samples()[0]["revision"], 1)
        # 带上正确修订重提即可成功
        ok = self.db.resolve_conflict(cid, "lead-01", "lead",
                                      {"decision": "take_offline", "base_revision": 1})
        self.assertEqual(ok["status"], "resolved")

    def test_confirmed_record_blocks_resolution(self):
        _, sample_id = self._station_sample()
        self.db.confirm("sample", sample_id, "lead-01", "lead")
        cid, _ = self._dup_conflict()
        out = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"decision": "take_offline", "base_revision": 1})
        self.assertEqual([c["type"] for c in out["contention"]], ["confirmed"])
        self.assertEqual(self.db.list_samples()[0]["storage_condition"], "4C")

    def test_taken_code_is_a_contention_item(self):
        _, wid1 = self._station_sample(sample_code="W-001")
        other = {"station_id": self.db.list_stations()[0]["id"], "sample_code": "W-002",
                 "sample_type": "water", "depth_m": 1, "storage_condition": "4C", "owner": "member-a"}
        self.db.sync("member-a", "member", {"device_id": "A", "records": [self.record("sample", "sa2", 1, other, "A")]})
        cid, _ = self._dup_conflict()
        out = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"decision": "merge", "base_revision": 1,
                                        "field_values": {"sample_code": "W-002"}})
        self.assertEqual(out["contention"][0]["type"], "code_taken")
        self.assertEqual(self.db.list_samples()[0]["sample_code"], "W-001")
        # 改用空闲编号后成功
        ok = self.db.resolve_conflict(cid, "lead-01", "lead",
                                      {"decision": "merge", "base_revision": 1,
                                       "field_values": {"sample_code": "W-009"}})
        self.assertEqual(ok["status"], "resolved")
        self.assertEqual(self.db.list_samples()[0]["sample_code"], "W-009")

    def test_resolution_requires_lead(self):
        self._station_sample()
        cid, _ = self._dup_conflict()
        with self.assertRaisesRegex(Exception, "只有航次负责人"):
            self.db.resolve_conflict(cid, "member-a", "member",
                                     {"decision": "take_offline", "base_revision": 1})

    def test_revision_history_keeps_old_values(self):
        _, sample_id = self._station_sample()
        cid, _ = self._dup_conflict()
        self.db.resolve_conflict(cid, "lead-01", "lead",
                                 {"decision": "take_offline", "base_revision": 1})
        history = self.db.entity_revision_history("sample", sample_id)
        revisions = {h["revision"]: h for h in history}
        self.assertEqual(revisions[1]["snapshot"]["storage_condition"], "4C")
        self.assertEqual(revisions[2]["snapshot"]["storage_condition"], "negative-20C")
        self.assertTrue(revisions[2]["source"].startswith("resolution:"))
        self.assertEqual(revisions[2]["conflict_id"], cid)


if __name__ == "__main__":
    unittest.main()
