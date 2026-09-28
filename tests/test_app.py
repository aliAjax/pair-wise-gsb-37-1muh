import tempfile
import unittest
from pathlib import Path

from app import Database, ContentionError, seed_demo


class OceanSyncFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.voyage = seed_demo(self.db)["voyage"]

    def tearDown(self):
        self.tmp.cleanup()

    def record(self, kind, uuid, revision, data, device="tablet-A"):
        return {"type": kind, "local_uuid": uuid, "revision": revision, "data": data}

    def _station(self, **over):
        data = {"voyage_id": self.voyage, "station_code": "S-01", "latitude": 30.1,
                "longitude": 122.0, "sampled_at": "2026-09-05T08:30:00+08:00", "owner": "member-a"}
        data.update(over)
        return data

    def _sample(self, station_id, **over):
        data = {"station_id": station_id, "sample_code": "W-001", "sample_type": "water",
                "depth_m": 5, "storage_condition": "4C", "owner": "member-a"}
        data.update(over)
        return data

    def _sync(self, device, records, actor="member-a"):
        return self.db.sync(actor, "member", {"device_id": device,
                                              "records": [self.record(k, u, r, d, device) for k, u, r, d in records]})

    def _one_conflict(self):
        return self.db.list_conflicts(status="pending")[-1]["id"]

    def _station_id(self):
        return self.db.list_stations()[0]["id"]

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


class ConflictWorkbenchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "wb.db")
        self.voyage = seed_demo(self.db)["voyage"]

    def tearDown(self):
        self.tmp.cleanup()

    def record(self, kind, uuid, revision, data, device="tablet-A"):
        return {"type": kind, "local_uuid": uuid, "revision": revision, "data": data}

    def _sync(self, device, records, actor="member-a"):
        return self.db.sync(actor, "member", {"device_id": device,
                                              "records": [self.record(k, u, r, d, device) for k, u, r, d in records]})

    def _station(self, **over):
        data = {"voyage_id": self.voyage, "station_code": "S-01", "latitude": 30.1,
                "longitude": 122.0, "sampled_at": "2026-09-05T08:30:00+08:00", "owner": "member-a",
                "notes": "岸端备注"}
        data.update(over)
        return data

    def _sample(self, station_id, **over):
        data = {"station_id": station_id, "sample_code": "W-001", "sample_type": "water",
                "depth_m": 5, "storage_condition": "4C", "owner": "member-a"}
        data.update(over)
        return data

    def _seed_station(self):
        self._sync("tablet-A", [("station", "st-001", 1, self._station())])
        return self.db.list_stations()[0]["id"]

    def _one_conflict(self):
        pending = self.db.list_conflicts(status="pending")
        return pending[-1]["id"]

    def test_take_offline_creates_revision_and_resolution(self):
        station_id = self._seed_station()
        offline = self._station(latitude=31.5, longitude=123.2, notes="船上修订")
        result = self._sync("tablet-B", [("station", "st-001", 1, offline)])
        self.assertTrue(result["conflicts"])
        cid = self._one_conflict()
        detail = self.db.get_conflict(cid)
        self.assertEqual(detail["target_id"], station_id)
        self.assertEqual(detail["current_revision"], 1)
        self.assertIn("latitude", detail["editable_fields"])

        out = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"action": "take_offline", "expected_revision": 1})
        self.assertEqual(out["conflict"]["status"], "resolved")
        station = out["shore"]
        self.assertEqual(station["latitude"], 31.5)
        self.assertEqual(station["longitude"], 123.2)
        self.assertEqual(station["notes"], "船上修订")
        self.assertEqual(station["revision"], 2)
        # 旧值仍可在修订历史查到
        revs = {h["revision"]: h["snapshot_json"] for h in out["history"]}
        self.assertEqual(revs[1]["latitude"], 30.1)
        self.assertEqual(revs[2]["latitude"], 31.5)
        res = out["resolutions"][0]
        self.assertEqual(res["base_revision"], 1)
        self.assertEqual(res["new_revision"], 2)
        self.assertEqual(res["action"], "take_offline")

    def test_merge_picks_field_by_field(self):
        station_id = self._seed_station()
        # 岸端先产生 r2：只改了备注
        self._sync("tablet-A", [("station", "st-001", 2, self._station(notes="岸端新备注"))])
        # 离线来件改了坐标
        offline = self._station(latitude=33.0, longitude=124.0)
        self._sync("tablet-C", [("station", "st-001", 2, offline)])
        cid = self._one_conflict()
        out = self.db.resolve_conflict(
            cid, "lead-01", "lead",
            {"action": "merge", "expected_revision": 2,
             "merged": {"latitude": 33.0, "longitude": 124.0, "notes": "岸端新备注",
                        "station_code": "S-01", "sampled_at": offline["sampled_at"]}})
        s = out["shore"]
        self.assertEqual((s["latitude"], s["longitude"], s["notes"]), (33.0, 124.0, "岸端新备注"))
        self.assertEqual(s["revision"], 3)

    def test_stale_base_revision_reparks_and_logs_contention(self):
        self._seed_station()
        self._sync("tablet-A", [("station", "st-001", 2, self._station(notes="r2"))])
        offline = self._station(latitude=40.0)
        self._sync("tablet-B", [("station", "st-001", 2, offline)])
        cid = self._one_conflict()
        # 处理人拿着旧的 r1 提交
        with self.assertRaises(ContentionError) as ctx:
            self.db.resolve_conflict(cid, "lead-01", "lead",
                                     {"action": "take_offline", "expected_revision": 1})
        self.assertEqual(ctx.exception.status, 409)
        kinds = [c["kind"] for c in ctx.exception.payload["contentions"]]
        self.assertIn("revision_changed", kinds)
        self.assertEqual(ctx.exception.payload["status"], "pending")
        detail = self.db.get_conflict(cid)
        self.assertEqual(detail["conflict"]["status"], "pending")
        self.assertTrue(any(c["kind"] == "revision_changed" for c in detail["contentions"]))
        # 按最新修订重新提交即可处置成功
        out = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"action": "keep_shore", "expected_revision": 2})
        self.assertEqual(out["conflict"]["status"], "resolved")
        self.assertEqual(out["shore"]["revision"], 3)

    def test_confirmed_record_reparks_on_submit(self):
        self._seed_station()
        station_id = self.db.list_stations()[0]["id"]
        self.db.confirm("station", station_id, "lead-01", "lead")
        self._sync("tablet-B", [("station", "st-001", 2, self._station(notes="晚到"))])
        cid = self._one_conflict()
        with self.assertRaises(ContentionError) as ctx:
            self.db.resolve_conflict(cid, "lead-01", "lead",
                                     {"action": "take_offline", "expected_revision": 1})
        kinds = [c["kind"] for c in ctx.exception.payload["contentions"]]
        self.assertIn("confirmed", kinds)
        self.assertEqual(self.db.get_conflict(cid)["conflict"]["status"], "pending")

    def test_code_taken_is_contention_for_offline_and_merge(self):
        self._seed_station()  # S-01
        self._sync("tablet-A", [("station", "st-002", 1, self._station(station_code="S-02"))])
        # 另一设备使用 S-02 -> 生成 DUP 副本，正本仍是 #2
        result = self._sync("tablet-D", [("station", "st-009", 1,
                                          self._station(station_code="S-02", latitude=41.0))])
        cid = result["conflicts"][0]["id"]
        detail = self.db.get_conflict(cid)
        self.assertTrue(detail["dup_record"])
        self.assertEqual(detail["dup_record"]["station_code"], "S-02-DUP-st-009")
        target_code = detail["shore"]["station_code"]
        # 合并时把正本编号改成已被 S-01 占用
        with self.assertRaises(ContentionError) as ctx:
            self.db.resolve_conflict(cid, "lead-01", "lead",
                                     {"action": "merge", "expected_revision": 1,
                                      "merged": {"station_code": "S-01"}})
        self.assertIn("code_taken", [c["kind"] for c in ctx.exception.payload["contentions"]])
        # 保留岸端可成功处置：正本产生新修订，副本保留
        out = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"action": "keep_shore", "expected_revision": 1,
                                        "merged": {"station_code": target_code}})
        self.assertEqual(out["conflict"]["status"], "resolved")
        self.assertEqual(out["shore"]["station_code"], "S-02")
        self.assertEqual(out["dup_record"]["station_code"], "S-02-DUP-st-009")

    def test_resolution_feedback_makes_repush_idempotent(self):
        self._seed_station()
        offline = self._station(latitude=31.5)
        self._sync("tablet-B", [("station", "st-001", 1, offline)])
        cid = self._one_conflict()
        self.db.resolve_conflict(cid, "lead-01", "lead",
                                 {"action": "take_offline", "expected_revision": 1})
        # 同一设备把相同的晚到内容再补传一次：不再进隔离，直接按重复确认
        again = self._sync("tablet-B", [("station", "st-001", 1, offline)])
        self.assertEqual(again["duplicates"], 1)
        self.assertEqual(again["conflicts"], [])

    def test_only_lead_resolves(self):
        self._seed_station()
        self._sync("tablet-B", [("station", "st-001", 1, self._station(latitude=31.5))])
        cid = self._one_conflict()
        with self.assertRaisesRegex(Exception, "负责人"):
            self.db.resolve_conflict(cid, "member-a", "member",
                                     {"action": "keep_shore", "expected_revision": 1})

    def test_filters_by_voyage_and_status(self):
        self._seed_station()
        self._sync("tablet-B", [("station", "st-001", 1, self._station(latitude=31.5))])
        cid = self._one_conflict()
        self.assertEqual(len(self.db.list_conflicts(voyage_id=self.voyage, status="pending")), 1)
        self.assertEqual(len(self.db.list_conflicts(voyage_id=self.voyage, status="resolved")), 0)
        other_voyage = self.db.create_voyage("lead-01", {"code": "2026-SCS-02", "name": "南海航次",
                                                          "starts_on": "2026-10-01", "ends_on": "2026-10-20"}, "lead")["id"]
        self.assertEqual(len(self.db.list_conflicts(voyage_id=other_voyage)), 0)
        # 船端可按设备拉回本设备的冲突与处置结果
        own = self.db.list_conflicts(device_id="tablet-B")
        self.assertEqual([c["device_id"] for c in own], ["tablet-B"])
        self.assertIsNone(own[0]["last_resolution"])
        self.db.resolve_conflict(cid, "lead-01", "lead",
                                 {"action": "keep_shore", "expected_revision": 1})
        self.assertEqual(len(self.db.list_conflicts(voyage_id=self.voyage, status="resolved")), 1)
        self.assertEqual(self.db.list_conflicts(device_id="tablet-B")[0]["last_resolution"]["action"],
                         "keep_shore")

    def test_concurrent_resolution_only_one_wins(self):
        import threading
        from app import DomainError
        self._seed_station()
        self._sync("tablet-B", [("station", "st-001", 1, self._station(latitude=31.5))])
        cid = self._one_conflict()
        outcomes = []

        def resolve(action):
            try:
                self.db.resolve_conflict(cid, "lead-01", "lead",
                                         {"action": action, "expected_revision": 1})
                outcomes.append(("ok", action))
            except ContentionError as exc:
                outcomes.append(("contention", [c["kind"] for c in exc.payload["contentions"]]))
            except DomainError as exc:
                # BEGIN IMMEDIATE 下输家拿到锁时赢家已提交：冲突已处置。
                outcomes.append(("rejected", str(exc)))

        t1 = threading.Thread(target=resolve, args=("keep_shore",))
        t2 = threading.Thread(target=resolve, args=("take_offline",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(outcomes), 2)
        oks = [o for o in outcomes if o[0] == "ok"]
        lost = [o for o in outcomes if o[0] != "ok"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(lost), 1)
        self.assertTrue(lost[0][0] in {"contention", "rejected"})
        # 记录修订只前进一次
        station = self.db.list_stations()[0]
        self.assertEqual(station["revision"], 2)

    def test_resolved_conflict_cannot_be_resubmitted(self):
        self._seed_station()
        self._sync("tablet-B", [("station", "st-001", 1, self._station(latitude=31.5))])
        cid = self._one_conflict()
        self.db.resolve_conflict(cid, "lead-01", "lead",
                                 {"action": "keep_shore", "expected_revision": 1})
        with self.assertRaisesRegex(Exception, "已处置"):
            self.db.resolve_conflict(cid, "lead-01", "lead",
                                     {"action": "take_offline", "expected_revision": 2})

    def test_custody_conflict_only_keep_shore(self):
        station_id = self._seed_station()
        sample = self._sample(station_id)
        self._sync("tablet-A", [("sample", "smp-1", 1, sample),
                                ("custody", "cu-1", 1, {"sample_id": 1, "event_type": "handover",
                                                         "from_party": "a", "to_party": "b",
                                                         "occurred_at": "2026-09-26T09:00:00+08:00"})])
        self._sync("tablet-A", [("custody", "cu-1", 2, {"sample_id": 1, "event_type": "handover",
                                                         "from_party": "a", "to_party": "b",
                                                         "occurred_at": "2026-09-26T09:00:00+08:00",
                                                         "notes": "改写"})])
        cid = self._one_conflict()
        with self.assertRaisesRegex(Exception, "只能保留岸端"):
            self.db.resolve_conflict(cid, "lead-01", "lead",
                                     {"action": "take_offline", "expected_revision": 0})
        out = self.db.resolve_conflict(cid, "lead-01", "lead",
                                       {"action": "keep_shore", "expected_revision": 0})
        self.assertEqual(out["conflict"]["status"], "resolved")


if __name__ == "__main__":
    unittest.main()
