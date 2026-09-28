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


if __name__ == "__main__":
    unittest.main()
