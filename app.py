"""Offline-first ocean sampling records and shore synchronization service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "ocean_samples.db"
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class Database:
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB):
        self.path = str(path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS voyages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    starts_on TEXT NOT NULL,
                    ends_on TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS stations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    voyage_id INTEGER NOT NULL REFERENCES voyages(id),
                    station_code TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    sampled_at TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    notes TEXT NOT NULL DEFAULT '',
                    confirmed INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL,
                    UNIQUE(voyage_id, station_code)
                );
                CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    voyage_id INTEGER NOT NULL REFERENCES voyages(id),
                    station_id INTEGER NOT NULL REFERENCES stations(id),
                    parent_sample_id INTEGER REFERENCES samples(id),
                    sample_code TEXT NOT NULL UNIQUE,
                    sample_type TEXT NOT NULL,
                    depth_m REAL NOT NULL DEFAULT 0,
                    storage_condition TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    confirmed INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS custody_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_id INTEGER NOT NULL REFERENCES samples(id),
                    event_type TEXT NOT NULL,
                    from_party TEXT NOT NULL,
                    to_party TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    notes TEXT NOT NULL DEFAULT '',
                    recorded_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS instrument_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    voyage_id INTEGER NOT NULL REFERENCES voyages(id),
                    station_id INTEGER REFERENCES stations(id),
                    file_name TEXT NOT NULL,
                    sha256 TEXT NOT NULL UNIQUE,
                    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
                    captured_at TEXT NOT NULL,
                    source_device TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_records (
                    local_uuid TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    server_id INTEGER NOT NULL,
                    revision INTEGER NOT NULL,
                    payload_hash TEXT NOT NULL,
                    synced_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    voyage_id INTEGER REFERENCES voyages(id),
                    device_id TEXT NOT NULL,
                    local_uuid TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    resolved_server_id INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    resolution TEXT,
                    contention TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    resolved_by TEXT
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS entity_revisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    revision INTEGER NOT NULL,
                    snapshot TEXT NOT NULL,
                    source TEXT NOT NULL,
                    conflict_id INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(entity_type, entity_id, revision)
                );
                CREATE TABLE IF NOT EXISTS resolution_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conflict_id INTEGER NOT NULL REFERENCES conflicts(id),
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    old_revision INTEGER NOT NULL,
                    new_revision INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    field_values TEXT NOT NULL,
                    resolved_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._migrate_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        existing = {r["name"] for r in conn.execute("PRAGMA table_info(conflicts)").fetchall()}
        migrations = {
            "voyage_id": "ALTER TABLE conflicts ADD COLUMN voyage_id INTEGER REFERENCES voyages(id)",
            "status": "ALTER TABLE conflicts ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'",
            "resolution": "ALTER TABLE conflicts ADD COLUMN resolution TEXT",
            "contention": "ALTER TABLE conflicts ADD COLUMN contention TEXT",
            "attempts": "ALTER TABLE conflicts ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
            "resolved_at": "ALTER TABLE conflicts ADD COLUMN resolved_at TEXT",
            "resolved_by": "ALTER TABLE conflicts ADD COLUMN resolved_by TEXT",
        }
        for column, statement in migrations.items():
            if column not in existing:
                conn.execute(statement)
        self._backfill_revisions(conn)

    def _backfill_revisions(self, conn: sqlite3.Connection) -> None:
        # 旧库没有修订快照；为已存在的站位/样本补一条初始修订，保证处置后旧值仍可查。幂等。
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='entity_revisions'").fetchone():
            return
        for entity_type, table, payload in (
            ("station", "stations", self._station_payload),
            ("sample", "samples", self._sample_payload),
        ):
            rows = conn.execute(f"SELECT id,revision FROM {table}").fetchall()
            for row in rows:
                conn.execute(
                    """INSERT OR IGNORE INTO entity_revisions(entity_type,entity_id,revision,snapshot,source,conflict_id,created_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (entity_type, row["id"], int(row["revision"]),
                     canonical(payload(conn, row["id"])), "legacy", None, "system", utcnow()),
                )

    def _snapshot_revision(self, conn: sqlite3.Connection, entity_type: str, entity_id: int,
                           revision: int, snapshot: dict[str, Any], source: str,
                           created_by: str, conflict_id: int | None = None) -> None:
        conn.execute(
            """INSERT OR IGNORE INTO entity_revisions(entity_type,entity_id,revision,snapshot,source,conflict_id,created_by,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (entity_type, entity_id, revision, canonical(snapshot), source, conflict_id, created_by, utcnow()),
        )

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id, json.dumps(details, ensure_ascii=False), utcnow()),
        )

    def _conflict(self, conn: sqlite3.Connection, entity_type: str, device_id: str, local_uuid: str,
                  reason: str, payload: dict[str, Any], server_id: int | None = None,
                  voyage_id: int | None = None) -> dict[str, Any]:
        cur = conn.execute(
            "INSERT INTO conflicts(entity_type,voyage_id,device_id,local_uuid,reason,payload,resolved_server_id,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (entity_type, voyage_id, device_id, local_uuid, reason, canonical(payload), server_id, utcnow()),
        )
        return {"id": int(cur.lastrowid), "entity_type": entity_type, "local_uuid": local_uuid, "reason": reason, "server_id": server_id}

    def create_voyage(self, actor: str, payload: dict[str, Any], role: str = "lead") -> dict[str, Any]:
        if role not in {"lead", "editor"}:
            raise DomainError("只有航次负责人可以建立航次", 403)
        code, name = str(payload.get("code", "")).strip(), str(payload.get("name", "")).strip()
        starts_on, ends_on = str(payload.get("starts_on", "")), str(payload.get("ends_on", ""))
        if not code or not name or not starts_on or not ends_on or starts_on > ends_on:
            raise DomainError("航次编号、名称和有效日期不完整")
        with self.connect() as conn:
            try:
                cur = conn.execute("INSERT INTO voyages(code,name,starts_on,ends_on,created_at) VALUES(?,?,?,?,?)", (code, name, starts_on, ends_on, utcnow()))
            except sqlite3.IntegrityError as exc:
                raise DomainError("航次编号已存在", 409) from exc
            self._audit(conn, actor, "voyage.created", "voyage", cur.lastrowid, {"code": code})
            return dict(conn.execute("SELECT * FROM voyages WHERE id=?", (cur.lastrowid,)).fetchone())

    def _mapping(self, conn: sqlite3.Connection, local_uuid: str) -> sqlite3.Row | None:
        return conn.execute("SELECT * FROM sync_records WHERE local_uuid=?", (local_uuid,)).fetchone()

    def _entity_voyage(self, conn: sqlite3.Connection, entity_type: str, entity_id: int | None) -> int | None:
        if entity_id is None:
            return None
        if entity_type in {"station", "sample"}:
            table = "stations" if entity_type == "station" else "samples"
            row = conn.execute(f"SELECT voyage_id FROM {table} WHERE id=?", (entity_id,)).fetchone()
            return int(row["voyage_id"]) if row else None
        return None

    def _station_payload(self, conn: sqlite3.Connection, station_id: int) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()
        return {
            "voyage_id": row["voyage_id"], "station_code": row["station_code"],
            "latitude": row["latitude"], "longitude": row["longitude"],
            "sampled_at": row["sampled_at"], "owner": row["owner"], "notes": row["notes"],
        }

    def _sample_payload(self, conn: sqlite3.Connection, sample_id: int) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM samples WHERE id=?", (sample_id,)).fetchone()
        return {
            "voyage_id": row["voyage_id"], "station_id": row["station_id"],
            "parent_sample_id": row["parent_sample_id"], "sample_code": row["sample_code"],
            "sample_type": row["sample_type"], "depth_m": row["depth_m"],
            "storage_condition": row["storage_condition"], "owner": row["owner"],
        }

    STATION_FIELDS = ("station_code", "latitude", "longitude", "sampled_at", "owner", "notes")
    SAMPLE_FIELDS = ("sample_code", "sample_type", "depth_m", "storage_condition", "owner", "station_id", "parent_sample_id")

    def _build_values(self, entity_type: str, base: dict[str, Any], incoming: dict[str, Any],
                      decision: str, field_values: dict[str, Any] | None) -> dict[str, Any]:
        """Construct the effective content for a resolution decision.

        keep_shore:   保留岸端当前值；
        take_offline: 凡离线内容携带的字段都采用离线值；
        merge:        从岸端值出发，仅采用处理人逐字段勾选/填写的 field_values。
        """
        editable = self.STATION_FIELDS if entity_type == "station" else self.SAMPLE_FIELDS
        merged = {k: base.get(k) for k in editable}
        if decision == "take_offline":
            for key in editable:
                if key in incoming:
                    merged[key] = incoming[key]
        elif decision == "merge":
            for key, value in (field_values or {}).items():
                if key in editable:
                    merged[key] = value
        return merged

    def _validate_merged(self, entity_type: str, merged: dict[str, Any]) -> None:
        if entity_type == "station":
            self._validate_station(merged)
            if not str(merged.get("station_code", "")).strip():
                raise DomainError("站位编号不能为空")
        else:
            code, sample_type, _depth, storage = self._validate_sample(merged)
            merged["sample_code"] = code
            merged["sample_type"] = sample_type
            merged["storage_condition"] = storage
            if merged.get("station_id") is None:
                raise DomainError("样本必须关联站位")

    def _sync_station(self, conn: sqlite3.Connection, actor: str, role: str, device_id: str,
                      local_uuid: str, revision: int, record: dict[str, Any], result: dict[str, Any]) -> None:
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["entity_type"] != "station" or mapping["device_id"] != device_id:
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "同一本地 UUID 被其他设备或实体使用", record, int(mapping["server_id"]), self._entity_voyage(conn, "station", mapping["server_id"])))
                return
            station = conn.execute("SELECT * FROM stations WHERE id=?", (mapping["server_id"],)).fetchone()
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
                return
            if revision <= int(mapping["revision"]):
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "修订号过旧或相同但内容不同", record, int(mapping["server_id"]), station["voyage_id"] if station else None))
                return
            if not station:
                raise DomainError("同步索引指向的站位不存在", 409)
            if station["confirmed"]:
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "站位已确认，不能覆盖", record, station["id"], station["voyage_id"]))
                return
            if role != "lead" and actor != station["owner"]:
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "只有记录人或负责人可以修改站位", record, station["id"], station["voyage_id"]))
                return
            latitude, longitude = self._validate_station(record)
            conn.execute(
                "UPDATE stations SET latitude=?,longitude=?,sampled_at=?,notes=?,revision=?,updated_at=? WHERE id=?",
                (latitude, longitude, str(record.get("sampled_at", station["sampled_at"])), str(record.get("notes", station["notes"])), revision, utcnow(), station["id"]),
            )
            conn.execute("UPDATE sync_records SET revision=?,payload_hash=?,synced_at=? WHERE local_uuid=?", (revision, payload_hash, utcnow(), local_uuid))
            self._snapshot_revision(conn, "station", station["id"], revision, self._station_payload(conn, station["id"]), f"sync:{device_id}", actor)
            result["updated"] += 1
            return
        voyage = self._resolve_voyage(conn, record)
        latitude, longitude = self._validate_station(record)
        code = str(record.get("station_code", "")).strip()
        if not code:
            raise DomainError("站位编号不能为空")
        existing = conn.execute("SELECT * FROM stations WHERE voyage_id=? AND station_code=?", (voyage["id"], code)).fetchone()
        if existing:
            code = f"{code}-DUP-{local_uuid[:8]}"
            result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, f"站位编号 {record.get('station_code')} 已存在，已分配 {code}", record, int(existing["id"]), voyage["id"]))
        cur = conn.execute(
            """INSERT INTO stations(voyage_id,station_code,latitude,longitude,sampled_at,owner,notes,revision,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (voyage["id"], code, latitude, longitude, str(record.get("sampled_at", utcnow())), str(record.get("owner", actor)), str(record.get("notes", "")), revision, utcnow()),
        )
        conn.execute("INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at) VALUES(?,?,?,?,?,?,?)",
                     (local_uuid, device_id, "station", cur.lastrowid, revision, payload_hash, utcnow()))
        self._snapshot_revision(conn, "station", cur.lastrowid, revision, self._station_payload(conn, cur.lastrowid), f"sync:{device_id}", actor)
        result["created"] += 1

    def _validate_station(self, record: dict[str, Any]) -> tuple[float, float]:
        try:
            latitude, longitude = float(record.get("latitude")), float(record.get("longitude"))
        except (TypeError, ValueError) as exc:
            raise DomainError("站位经纬度必须是数值") from exc
        if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
            raise DomainError("站位经纬度超出范围")
        return latitude, longitude

    def _resolve_voyage(self, conn: sqlite3.Connection, record: dict[str, Any]) -> sqlite3.Row:
        voyage_id = record.get("voyage_id") or record.get("voyage_server_id")
        if voyage_id is not None:
            row = conn.execute("SELECT * FROM voyages WHERE id=?", (int(voyage_id),)).fetchone()
        else:
            row = conn.execute("SELECT * FROM voyages WHERE code=?", (str(record.get("voyage_code", "")),)).fetchone()
        if not row:
            raise DomainError("找不到对应航次", 404)
        return row

    def _station(self, conn: sqlite3.Connection, record: dict[str, Any]) -> sqlite3.Row:
        station_id = record.get("station_id") or record.get("station_server_id")
        if station_id is None:
            raise DomainError("样本必须关联站位")
        row = conn.execute("SELECT * FROM stations WHERE id=?", (int(station_id),)).fetchone()
        if not row:
            raise DomainError("站位不存在", 404)
        return row

    def _parent_sample(self, conn: sqlite3.Connection, record: dict[str, Any]) -> sqlite3.Row | None:
        parent = record.get("parent_sample_id")
        if parent is None:
            return None
        row = conn.execute("SELECT * FROM samples WHERE id=?", (int(parent),)).fetchone()
        if not row:
            raise DomainError("母样不存在", 404)
        return row

    def _validate_sample(self, record: dict[str, Any]) -> tuple[str, str, float, str]:
        code = str(record.get("sample_code", "")).strip()
        sample_type = str(record.get("sample_type", "")).strip()
        storage = str(record.get("storage_condition", "")).strip()
        try:
            depth = float(record.get("depth_m", 0))
        except (TypeError, ValueError) as exc:
            raise DomainError("采样深度必须是数值") from exc
        if not code or not sample_type or not storage or depth < 0:
            raise DomainError("样本编号、类型、保存条件不能为空，深度不能为负")
        return code, sample_type, depth, storage

    def _sync_sample(self, conn: sqlite3.Connection, actor: str, role: str, device_id: str,
                     local_uuid: str, revision: int, record: dict[str, Any], result: dict[str, Any]) -> None:
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["entity_type"] != "sample" or mapping["device_id"] != device_id:
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "同一本地 UUID 被其他设备或实体使用", record, int(mapping["server_id"]), self._entity_voyage(conn, "sample", mapping["server_id"])))
                return
            sample = conn.execute("SELECT * FROM samples WHERE id=?", (mapping["server_id"],)).fetchone()
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
                return
            if revision <= int(mapping["revision"]):
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "修订号过旧或相同但内容不同", record, int(mapping["server_id"]), sample["voyage_id"] if sample else None))
                return
            if not sample:
                raise DomainError("同步索引指向的样本不存在", 409)
            if sample["confirmed"]:
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "样本已确认，不能覆盖", record, sample["id"], sample["voyage_id"]))
                return
            if role != "lead" and actor != sample["owner"]:
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "只有记录人或负责人可以修改样本", record, sample["id"], sample["voyage_id"]))
                return
            code, sample_type, depth, storage = self._validate_sample(record)
            if code != sample["sample_code"]:
                # A changed client code is accepted only when it remains unique.
                other = conn.execute("SELECT 1 FROM samples WHERE sample_code=? AND id<>?", (code, sample["id"])).fetchone()
                if other:
                    result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "修改后的样本编号已被占用", record, sample["id"], sample["voyage_id"]))
                    return
            conn.execute(
                "UPDATE samples SET sample_code=?,sample_type=?,depth_m=?,storage_condition=?,revision=?,updated_at=? WHERE id=?",
                (code, sample_type, depth, storage, revision, utcnow(), sample["id"]),
            )
            conn.execute("UPDATE sync_records SET revision=?,payload_hash=?,synced_at=? WHERE local_uuid=?", (revision, payload_hash, utcnow(), local_uuid))
            self._snapshot_revision(conn, "sample", sample["id"], revision, self._sample_payload(conn, sample["id"]), f"sync:{device_id}", actor)
            result["updated"] += 1
            return
        station = self._station(conn, record)
        parent = self._parent_sample(conn, record)
        code, sample_type, depth, storage = self._validate_sample(record)
        existing = conn.execute("SELECT * FROM samples WHERE sample_code=?", (code,)).fetchone()
        if existing:
            code = f"{code}-DUP-{local_uuid[:8]}"
            result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, f"样本编号 {record.get('sample_code')} 已存在，已分配 {code}", record, int(existing["id"]), station["voyage_id"]))
        cur = conn.execute(
            """INSERT INTO samples(voyage_id,station_id,parent_sample_id,sample_code,sample_type,depth_m,storage_condition,owner,revision,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (station["voyage_id"], station["id"], parent["id"] if parent else None, code, sample_type, depth, storage, str(record.get("owner", actor)), revision, utcnow()),
        )
        conn.execute("INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at) VALUES(?,?,?,?,?,?,?)",
                     (local_uuid, device_id, "sample", cur.lastrowid, revision, payload_hash, utcnow()))
        self._snapshot_revision(conn, "sample", cur.lastrowid, revision, self._sample_payload(conn, cur.lastrowid), f"sync:{device_id}", actor)
        result["created"] += 1

    def _sync_custody(self, conn: sqlite3.Connection, actor: str, device_id: str, local_uuid: str,
                      revision: int, record: dict[str, Any], result: dict[str, Any]) -> None:
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
            else:
                result["conflicts"].append(self._conflict(conn, "custody", device_id, local_uuid, "保管事件为追加记录，不能改写", record, int(mapping["server_id"])))
            return
        sample_id = record.get("sample_id") or record.get("sample_server_id")
        sample = conn.execute("SELECT * FROM samples WHERE id=?", (int(sample_id),)).fetchone() if sample_id is not None else None
        if not sample:
            raise DomainError("保管事件必须关联已同步样本", 404)
        event_type, from_party, to_party = str(record.get("event_type", "")).strip(), str(record.get("from_party", "")).strip(), str(record.get("to_party", "")).strip()
        occurred_at = str(record.get("occurred_at", "")).strip()
        if not event_type or not from_party or not to_party or not occurred_at:
            raise DomainError("保管事件类型、双方和发生时间不能为空")
        cur = conn.execute(
            "INSERT INTO custody_events(sample_id,event_type,from_party,to_party,occurred_at,notes,recorded_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (sample["id"], event_type, from_party, to_party, occurred_at, str(record.get("notes", "")), actor, utcnow()),
        )
        conn.execute("INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at) VALUES(?,?,?,?,?,?,?)",
                     (local_uuid, device_id, "custody", cur.lastrowid, revision, payload_hash, utcnow()))
        result["created"] += 1

    def _sync_file(self, conn: sqlite3.Connection, actor: str, device_id: str, local_uuid: str,
                   revision: int, record: dict[str, Any], result: dict[str, Any]) -> None:
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
            else:
                result["conflicts"].append(self._conflict(conn, "instrument_file", device_id, local_uuid, "仪器文件元数据不能覆盖", record, int(mapping["server_id"])))
            return
        voyage = self._resolve_voyage(conn, record)
        sha = str(record.get("sha256", "")).lower()
        if not SHA256_RE.fullmatch(sha):
            raise DomainError("仪器文件 sha256 格式错误")
        try:
            size = int(record.get("size_bytes", 0))
        except (TypeError, ValueError) as exc:
            raise DomainError("文件大小必须是整数") from exc
        if size < 0:
            raise DomainError("文件大小不能为负")
        existing = conn.execute("SELECT * FROM instrument_files WHERE sha256=?", (sha,)).fetchone()
        if existing:
            conn.execute("INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at) VALUES(?,?,?,?,?,?,?)",
                         (local_uuid, device_id, "instrument_file", existing["id"], revision, payload_hash, utcnow()))
            result["duplicates"] += 1
            return
        station_id = record.get("station_id") or record.get("station_server_id")
        if station_id is not None and not conn.execute("SELECT 1 FROM stations WHERE id=?", (int(station_id),)).fetchone():
            raise DomainError("仪器文件关联的站位不存在", 404)
        cur = conn.execute(
            "INSERT INTO instrument_files(voyage_id,station_id,file_name,sha256,size_bytes,captured_at,source_device,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (voyage["id"], station_id, str(record.get("file_name", "")), sha, size, str(record.get("captured_at", utcnow())), device_id, utcnow()),
        )
        conn.execute("INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at) VALUES(?,?,?,?,?,?,?)",
                     (local_uuid, device_id, "instrument_file", cur.lastrowid, revision, payload_hash, utcnow()))
        result["created"] += 1

    def sync(self, actor: str, role: str, payload: dict[str, Any]) -> dict[str, Any]:
        device_id = str(payload.get("device_id", "")).strip()
        records = payload.get("records")
        if not device_id or not isinstance(records, list) or not records:
            raise DomainError("同步请求需要 device_id 和非空 records")
        result: dict[str, Any] = {"device_id": device_id, "created": 0, "updated": 0, "duplicates": 0, "conflicts": []}
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for item in records:
                if not isinstance(item, dict):
                    raise DomainError("每条同步记录必须是对象")
                record_type = str(item.get("type", "")).strip()
                local_uuid = str(item.get("local_uuid", "")).strip()
                try:
                    revision = int(item.get("revision", 1))
                except (TypeError, ValueError) as exc:
                    raise DomainError("revision 必须是整数") from exc
                data = item.get("data")
                if not local_uuid or revision < 1 or not isinstance(data, dict):
                    raise DomainError("同步记录缺少 local_uuid、revision 或 data")
                if record_type == "station":
                    self._sync_station(conn, actor, role, device_id, local_uuid, revision, data, result)
                elif record_type == "sample":
                    self._sync_sample(conn, actor, role, device_id, local_uuid, revision, data, result)
                elif record_type == "custody":
                    self._sync_custody(conn, actor, device_id, local_uuid, revision, data, result)
                elif record_type == "instrument_file":
                    self._sync_file(conn, actor, device_id, local_uuid, revision, data, result)
                else:
                    raise DomainError(f"不支持的同步类型: {record_type}")
            self._audit(conn, actor, "sync.batch", "device", None, {k: result[k] for k in ("created", "updated", "duplicates")})
        return result

    def confirm(self, entity_type: str, entity_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        if role != "lead":
            raise DomainError("只有航次负责人可以确认记录", 403)
        if entity_type not in {"station", "sample"}:
            raise DomainError("只有站位或样本支持确认")
        with self.connect() as conn:
            table = "stations" if entity_type == "station" else "samples"
            row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (entity_id,)).fetchone()
            if not row:
                raise DomainError("记录不存在", 404)
            if row["confirmed"]:
                return dict(row)
            conn.execute(f"UPDATE {table} SET confirmed=1,updated_at=? WHERE id=?", (utcnow(), entity_id))
            self._audit(conn, actor, f"{entity_type}.confirmed", entity_type, entity_id, {})
            return dict(conn.execute(f"SELECT * FROM {table} WHERE id=?", (entity_id,)).fetchone())

    # ----- 冲突处置台 -----

    def _conflict_row(self, conn: sqlite3.Connection, conflict_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
        if not row:
            raise DomainError("冲突记录不存在", 404)
        return row

    def get_conflict_detail(self, conflict_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            row = self._conflict_row(conn, conflict_id)
            return self._serialize_conflict(conn, row, with_body=True)

    def _serialize_conflict(self, conn: sqlite3.Connection, row: sqlite3.Row,
                            with_body: bool = False) -> dict[str, Any]:
        item = {
            "id": row["id"],
            "entity_type": row["entity_type"],
            "voyage_id": row["voyage_id"],
            "device_id": row["device_id"],
            "local_uuid": row["local_uuid"],
            "reason": row["reason"],
            "resolved_server_id": row["resolved_server_id"],
            "status": row["status"],
            "resolution": json.loads(row["resolution"]) if row["resolution"] else None,
            "contention": json.loads(row["contention"]) if row["contention"] else None,
            "attempts": row["attempts"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
            "resolved_by": row["resolved_by"],
        }
        if with_body:
            item["incoming"] = json.loads(row["payload"])
            item["current"] = self._current_entity(conn, row["entity_type"], row["resolved_server_id"])
            item["current_revision"] = item["current"]["revision"] if item["current"] else None
            item["revisions"] = self.list_revisions(conn, row["entity_type"], row["resolved_server_id"]) if row["resolved_server_id"] else []
        else:
            item["payload"] = json.loads(row["payload"])
        return item

    def _current_entity(self, conn: sqlite3.Connection, entity_type: str,
                        entity_id: int | None) -> dict[str, Any] | None:
        if entity_id is None or entity_type not in {"station", "sample"}:
            return None
        table = "stations" if entity_type == "station" else "samples"
        row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (entity_id,)).fetchone()
        if not row:
            return None
        data = self._station_payload(conn, entity_id) if entity_type == "station" else self._sample_payload(conn, entity_id)
        data["id"] = entity_id
        data["confirmed"] = bool(row["confirmed"])
        data["revision"] = int(row["revision"])
        data["updated_at"] = row["updated_at"]
        return data

    def list_revisions(self, conn: sqlite3.Connection, entity_type: str, entity_id: int) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT id,revision,snapshot,source,conflict_id,created_by,created_at FROM entity_revisions WHERE entity_type=? AND entity_id=? ORDER BY revision DESC",
            (entity_type, entity_id),
        ).fetchall()
        return [{"id": r["id"], "revision": r["revision"], "snapshot": json.loads(r["snapshot"]),
                 "source": r["source"], "conflict_id": r["conflict_id"],
                 "created_by": r["created_by"], "created_at": r["created_at"]} for r in rows]

    def list_conflicts(self, voyage_id: int | None = None, status: str | None = None) -> list[dict[str, Any]]:
        clauses, params = [], []
        if voyage_id is not None:
            clauses.append("voyage_id=?")
            params.append(voyage_id)
        if status:
            if status not in {"pending", "resolved"}:
                raise DomainError("状态只能是 pending 或 resolved")
            clauses.append("status=?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM conflicts{where} ORDER BY id DESC", params).fetchall()
            return [self._serialize_conflict(conn, r) for r in rows]

    def resolve_conflict(self, conflict_id: int, actor: str, role: str, payload: dict[str, Any]) -> dict[str, Any]:
        if role != "lead":
            raise DomainError("只有航次负责人可以处置冲突", 403)
        decision = str(payload.get("decision", "")).strip()
        if decision not in {"keep_shore", "take_offline", "merge"}:
            raise DomainError("处置方式必须是 keep_shore、take_offline 或 merge")
        field_values = payload.get("field_values") if decision == "merge" else None
        if decision == "merge" and not isinstance(field_values, dict):
            raise DomainError("逐字段合并需要提供 field_values")
        base_revision = payload.get("base_revision")
        try:
            base_revision = int(base_revision) if base_revision is not None else None
        except (TypeError, ValueError) as exc:
            raise DomainError("base_revision 必须是整数") from exc

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._conflict_row(conn, conflict_id)
            if row["entity_type"] not in {"station", "sample"}:
                raise DomainError("只有站位或样本冲突支持处置台合并")
            entity_id = row["resolved_server_id"]
            if entity_id is None:
                # 编号冲突时记录可能尚未关联到已有实体，无法直接合并
                raise DomainError("该冲突没有可处置的已入库记录", 409)
            table = "stations" if row["entity_type"] == "station" else "samples"
            current = conn.execute(f"SELECT * FROM {table} WHERE id=?", (entity_id,)).fetchone()
            incoming = json.loads(row["payload"])
            attempts = int(row["attempts"]) + 1
            contention: list[dict[str, Any]] = []

            if not current:
                contention.append({"type": "missing", "message": "关联的已入库记录已不存在"})
            if current and current["confirmed"]:
                contention.append({"type": "confirmed", "message": f"{row['entity_type']} 已确认锁定，不能再改写"})
            if base_revision is not None and current and int(current["revision"]) != base_revision:
                contention.append({
                    "type": "revision_changed",
                    "message": f"当前修订已变为 {current['revision']}，你核对的是 {base_revision}",
                    "expected": base_revision, "actual": int(current["revision"]),
                })

            # 先按处理人选择构造最终内容，供编号占用争用检查与最终写入共用
            current_data = self._current_entity(conn, row["entity_type"], entity_id) if current else None
            new_values = self._build_values(row["entity_type"], current_data or {}, incoming, decision, field_values)

            # 编号占用争用：只有在未确认且修订未变化、且确实要改编号时才检查
            code_column = "station_code" if row["entity_type"] == "station" else "sample_code"
            chosen_code = str(new_values.get(code_column, "")).strip()
            if current and not current["confirmed"] and not any(c["type"] in {"revision_changed", "confirmed", "missing"} for c in contention):
                if chosen_code and chosen_code != current[code_column]:
                    other = conn.execute(
                        f"SELECT id FROM {table} WHERE {code_column}=? AND id<>?",
                        (chosen_code, entity_id),
                    ).fetchone()
                    if other:
                        contention.append({
                            "type": "code_taken",
                            "message": f"编号 {chosen_code} 已被其他记录占用",
                            "code": chosen_code, "holder_id": int(other["id"]),
                        })

            if contention:
                conn.execute(
                    "UPDATE conflicts SET status='pending',contention=?,attempts=? WHERE id=?",
                    (canonical(contention), attempts, conflict_id),
                )
                self._audit(conn, actor, "conflict.contended", row["entity_type"], entity_id,
                            {"conflict_id": conflict_id, "contention": contention})
                return {"status": "pending", "contention": contention,
                        "conflict": self._serialize_conflict(conn, self._conflict_row(conn, conflict_id), with_body=True)}

            # 无争用：应用处置
            self._validate_merged(row["entity_type"], new_values)
            old_revision = int(current["revision"])
            new_revision = old_revision + 1
            self._apply_entity_update(conn, row["entity_type"], entity_id, new_values, new_revision, actor)

            # 让设备的本地修订索引指向最新状态，便于后续同步对照
            self._realign_mapping(conn, row, entity_id, new_revision, incoming)

            resolution = {"decision": decision, "field_values": new_values,
                          "old_revision": old_revision, "new_revision": new_revision}
            conn.execute(
                "UPDATE conflicts SET status='resolved',resolution=?,contention=NULL,attempts=?,resolved_at=?,resolved_by=? WHERE id=?",
                (canonical(resolution), attempts, utcnow(), actor, conflict_id),
            )
            conn.execute(
                """INSERT INTO resolution_records(conflict_id,entity_type,entity_id,old_revision,new_revision,decision,field_values,resolved_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (conflict_id, row["entity_type"], entity_id, old_revision, new_revision, decision,
                 canonical(new_values), actor, utcnow()),
            )
            self._snapshot_revision(conn, row["entity_type"], entity_id, new_revision,
                                    self._station_payload(conn, entity_id) if row["entity_type"] == "station" else self._sample_payload(conn, entity_id),
                                    f"resolution:{decision}", actor, conflict_id)
            self._audit(conn, actor, "conflict.resolved", row["entity_type"], entity_id,
                        {"conflict_id": conflict_id, "decision": decision,
                         "old_revision": old_revision, "new_revision": new_revision})
            return {"status": "resolved", "new_revision": new_revision,
                    "conflict": self._serialize_conflict(conn, self._conflict_row(conn, conflict_id), with_body=True)}

    def _apply_entity_update(self, conn: sqlite3.Connection, entity_type: str, entity_id: int,
                             values: dict[str, Any], revision: int, actor: str) -> None:
        now = utcnow()
        if entity_type == "station":
            conn.execute(
                """UPDATE stations SET station_code=?,latitude=?,longitude=?,sampled_at=?,owner=?,notes=?,revision=?,updated_at=? WHERE id=?""",
                (str(values["station_code"]), float(values["latitude"]), float(values["longitude"]),
                 str(values["sampled_at"]), str(values["owner"]), str(values.get("notes", "")), revision, now, entity_id),
            )
        else:
            conn.execute(
                """UPDATE samples SET sample_code=?,sample_type=?,depth_m=?,storage_condition=?,owner=?,revision=?,updated_at=? WHERE id=?""",
                (str(values["sample_code"]), str(values["sample_type"]), float(values["depth_m"]),
                 str(values["storage_condition"]), str(values["owner"]), revision, now, entity_id),
            )

    def _realign_mapping(self, conn: sqlite3.Connection, row: sqlite3.Row, entity_id: int,
                         revision: int, incoming: dict[str, Any]) -> None:
        # 设备本地 UUID 的同步索引指向处置后的实体与修订，下一批同 UUID 数据可正常做幂等/比对。
        # 若该 UUID 属于另一台设备（跨设备 UUID 争用），不覆盖其索引，只记录处置结果。
        existing = self._mapping(conn, row["local_uuid"])
        if existing and existing["device_id"] != row["device_id"]:
            return
        conn.execute(
            """INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(local_uuid) DO UPDATE SET entity_type=excluded.entity_type,server_id=excluded.server_id,
                   revision=excluded.revision,payload_hash=excluded.payload_hash,synced_at=excluded.synced_at""",
            (row["local_uuid"], row["device_id"], row["entity_type"], entity_id, revision,
             hashlib.sha256(canonical(incoming).encode()).hexdigest(), utcnow()),
        )

    def list_resolution_records(self, entity_type: str | None = None, entity_id: int | None = None) -> list[dict[str, Any]]:
        clauses, params = [], []
        if entity_type:
            clauses.append("entity_type=?")
            params.append(entity_type)
        if entity_id is not None:
            clauses.append("entity_id=?")
            params.append(entity_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM resolution_records{where} ORDER BY id DESC", params).fetchall()
            return [{"id": r["id"], "conflict_id": r["conflict_id"], "entity_type": r["entity_type"],
                     "entity_id": r["entity_id"], "old_revision": r["old_revision"],
                     "new_revision": r["new_revision"], "decision": r["decision"],
                     "field_values": json.loads(r["field_values"]), "resolved_by": r["resolved_by"],
                     "created_at": r["created_at"]} for r in rows]

    def entity_revision_history(self, entity_type: str, entity_id: int) -> list[dict[str, Any]]:
        if entity_type not in {"station", "sample"}:
            raise DomainError("只有站位或样本有修订历史")
        with self.connect() as conn:
            if not conn.execute(f"SELECT 1 FROM {'stations' if entity_type == 'station' else 'samples'} WHERE id=?", (entity_id,)).fetchone():
                raise DomainError("记录不存在", 404)
            return self.list_revisions(conn, entity_type, entity_id)


    def list_voyages(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM voyages ORDER BY id").fetchall()]

    def list_stations(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id").fetchall()]

    def list_samples(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM samples ORDER BY id").fetchall()]

    def list_custody(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM custody_events ORDER BY id DESC").fetchall()]

    def list_files(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM instrument_files ORDER BY id DESC").fetchall()]

    def audit(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM audit_log ORDER BY id DESC").fetchall()]


def seed_demo(db: Database) -> dict[str, int]:
    voyages = db.list_voyages()
    if voyages:
        return {"voyage": int(voyages[0]["id"])}
    voyage = db.create_voyage("lead-01", {"code": "2026-ECS-01", "name": "东海秋季综合调查", "starts_on": "2026-09-01", "ends_on": "2026-09-25"}, "lead")
    return {"voyage": int(voyage["id"])}


class Handler(BaseHTTPRequestHandler):
    db: Database
    server_version = "OceanSamples/1.0"

    def _send(self, payload: Any, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _html(self) -> None:
        data = (ROOT / "static" / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            raise DomainError("请求体不是合法 JSON") from exc

    def _auth(self) -> tuple[str, str]:
        return self.headers.get("X-User", "anonymous"), self.headers.get("X-Role", "viewer")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"/", "/index.html"}:
                return self._html()
            if parsed.path == "/api/health":
                return self._send({"ok": True})
            query = parse_qs(parsed.query)
            if parsed.path == "/api/conflicts":
                voyage_id = int(query["voyage_id"][0]) if query.get("voyage_id") else None
                status = query["status"][0] if query.get("status") else None
                return self._send({"items": self.db.list_conflicts(voyage_id, status)})
            if parsed.path.startswith("/api/conflicts/"):
                parts = [p for p in parsed.path.split("/") if p]
                if len(parts) == 3:
                    return self._send(self.db.get_conflict_detail(int(parts[2])))
            if parsed.path.startswith("/api/revisions/"):
                parts = [p for p in parsed.path.split("/") if p]
                if len(parts) == 4 and parts[1] == "revisions":
                    return self._send({"items": self.db.entity_revision_history(parts[2], int(parts[3]))})
            if parsed.path == "/api/resolutions":
                entity_type = query["entity_type"][0] if query.get("entity_type") else None
                entity_id = int(query["entity_id"][0]) if query.get("entity_id") else None
                return self._send({"items": self.db.list_resolution_records(entity_type, entity_id)})
            endpoints = {
                "/api/voyages": self.db.list_voyages,
                "/api/stations": self.db.list_stations,
                "/api/samples": self.db.list_samples,
                "/api/custody": self.db.list_custody,
                "/api/instrument-files": self.db.list_files,
                "/api/audit": self.db.audit,
            }
            if parsed.path in endpoints:
                return self._send({"items": endpoints[parsed.path]()})
            raise DomainError("接口不存在", 404)
        except (ValueError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            actor, role = self._auth()
            body = self._body()
            if parsed.path == "/api/voyages":
                return self._send(self.db.create_voyage(actor, body, role), 201)
            if parsed.path == "/api/sync":
                return self._send(self.db.sync(actor, role, body))
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) == 4 and parts[:2] == ["api", "confirm"]:
                return self._send(self.db.confirm(parts[2], int(parts[3]), actor, role))
            if len(parts) == 4 and parts[:2] == ["api", "conflicts"] and parts[3] == "resolve":
                return self._send(self.db.resolve_conflict(int(parts[2]), actor, role, body))
            raise DomainError("接口不存在", 404)
        except (ValueError, TypeError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[ocean] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="海洋科考采样记录与岸端同步")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8008")))
    parser.add_argument("--db", default=os.getenv("OCEAN_DB", str(DEFAULT_DB)))
    parser.add_argument("--init", action="store_true", help="创建数据库与示例航次")
    args = parser.parse_args()
    db = Database(args.db)
    if args.init:
        result = seed_demo(db)
        print(f"initialized database at {args.db}; voyage={result['voyage']}")
        return
    Handler.db = db
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"ocean-samples listening on http://127.0.0.1:{args.port} (db={args.db})")
    server.serve_forever()


if __name__ == "__main__":
    main()
