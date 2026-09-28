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
from urllib.parse import urlparse, parse_qs

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


class ContentionError(DomainError):
    """Raised when a resolution must re-park the conflict at pending and log contenders."""

    def __init__(self, payload: dict[str, Any]):
        super().__init__("提交时记录已被其他修订或确认抢先处理，请重新核对", 409)
        self.payload = payload


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
                    voyage_id INTEGER,
                    device_id TEXT NOT NULL,
                    local_uuid TEXT NOT NULL,
                    incoming_revision INTEGER NOT NULL DEFAULT 1,
                    reason TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    resolved_server_id INTEGER,
                    dup_server_id INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    resolved_at TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conflict_contentions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conflict_id INTEGER NOT NULL REFERENCES conflicts(id),
                    kind TEXT NOT NULL,
                    expected_revision INTEGER,
                    actual_revision INTEGER,
                    detail TEXT NOT NULL DEFAULT '',
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conflict_resolutions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conflict_id INTEGER NOT NULL REFERENCES conflicts(id),
                    action TEXT NOT NULL,
                    base_revision INTEGER NOT NULL,
                    new_revision INTEGER NOT NULL,
                    final_payload TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS entity_revisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    server_id INTEGER NOT NULL,
                    revision INTEGER NOT NULL,
                    snapshot TEXT NOT NULL,
                    source TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL
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
                """
            )
            self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """Bring older databases up to the conflict-workbench schema."""
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(conflicts)")}
        add_column = [
            ("voyage_id", "INTEGER"),
            ("incoming_revision", "INTEGER NOT NULL DEFAULT 1"),
            ("dup_server_id", "INTEGER"),
            ("status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("resolved_at", "TEXT"),
        ]
        for name, ddl in add_column:
            if name not in cols:
                conn.execute(f"ALTER TABLE conflicts ADD COLUMN {name} {ddl}")
        conn.execute(
            "UPDATE conflicts SET voyage_id=(SELECT voyage_id FROM stations WHERE id=resolved_server_id) "
            "WHERE entity_type='station' AND voyage_id IS NULL AND resolved_server_id IS NOT NULL"
        )
        conn.execute(
            "UPDATE conflicts SET voyage_id=(SELECT voyage_id FROM samples WHERE id=resolved_server_id) "
            "WHERE entity_type='sample' AND voyage_id IS NULL AND resolved_server_id IS NOT NULL"
        )

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id, json.dumps(details, ensure_ascii=False), utcnow()),
        )

    def _conflict(self, conn: sqlite3.Connection, entity_type: str, device_id: str, local_uuid: str,
                  reason: str, payload: dict[str, Any], server_id: int | None = None,
                  incoming_revision: int = 1, voyage_id: int | None = None,
                  dup_server_id: int | None = None) -> dict[str, Any]:
        if voyage_id is None and server_id is not None and entity_type in {"station", "sample"}:
            table = "stations" if entity_type == "station" else "samples"
            row = conn.execute(f"SELECT voyage_id FROM {table} WHERE id=?", (server_id,)).fetchone()
            if row:
                voyage_id = int(row["voyage_id"])
        elif voyage_id is None and server_id is not None and entity_type == "custody":
            row = conn.execute(
                "SELECT s.voyage_id FROM custody_events ce JOIN samples s ON s.id=ce.sample_id WHERE ce.id=?",
                (server_id,),
            ).fetchone()
            if row:
                voyage_id = int(row["voyage_id"])
        elif voyage_id is None and server_id is not None and entity_type == "instrument_file":
            row = conn.execute("SELECT voyage_id FROM instrument_files WHERE id=?", (server_id,)).fetchone()
            if row:
                voyage_id = int(row["voyage_id"])
        cur = conn.execute(
            "INSERT INTO conflicts(entity_type,voyage_id,device_id,local_uuid,incoming_revision,reason,payload,"
            "resolved_server_id,dup_server_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (entity_type, voyage_id, device_id, local_uuid, incoming_revision, reason, canonical(payload),
             server_id, dup_server_id, utcnow()),
        )
        return {"id": int(cur.lastrowid), "entity_type": entity_type, "local_uuid": local_uuid, "reason": reason, "server_id": server_id}

    def _already_handled(self, conn: sqlite3.Connection, entity_type: str, device_id: str,
                         local_uuid: str, record: dict[str, Any]) -> bool:
        """同设备、同本地编号、相同内容的来件已被处置过，则重传直接幂等确认。"""
        row = conn.execute(
            "SELECT 1 FROM conflicts WHERE entity_type=? AND device_id=? AND local_uuid=? AND payload=? AND status='resolved' LIMIT 1",
            (entity_type, device_id, local_uuid, canonical(record)),
        ).fetchone()
        return row is not None

    def _snapshot(self, conn: sqlite3.Connection, entity_type: str, server_id: int) -> dict[str, Any]:
        table = "stations" if entity_type == "station" else "samples"
        row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (server_id,)).fetchone()
        return dict(row) if row else {}

    def _save_revision(self, conn: sqlite3.Connection, entity_type: str, server_id: int,
                       revision: int, source: str, actor: str) -> None:
        snapshot = self._snapshot(conn, entity_type, server_id)
        conn.execute(
            "INSERT INTO entity_revisions(entity_type,server_id,revision,snapshot,source,actor,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (entity_type, server_id, revision, canonical(snapshot), source, actor, utcnow()),
        )

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

    def _sync_station(self, conn: sqlite3.Connection, actor: str, role: str, device_id: str,
                      local_uuid: str, revision: int, record: dict[str, Any], result: dict[str, Any]) -> None:
        if self._already_handled(conn, "station", device_id, local_uuid, record):
            result["duplicates"] += 1
            return
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["entity_type"] != "station" or mapping["device_id"] != device_id:
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "同一本地 UUID 被其他设备或实体使用", record, int(mapping["server_id"]), revision))
                return
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
                return
            if revision <= int(mapping["revision"]):
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "修订号过旧或相同但内容不同", record, int(mapping["server_id"]), revision))
                return
            station = conn.execute("SELECT * FROM stations WHERE id=?", (mapping["server_id"],)).fetchone()
            if not station:
                raise DomainError("同步索引指向的站位不存在", 409)
            if station["confirmed"]:
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "站位已确认，不能覆盖", record, station["id"], revision))
                return
            if role != "lead" and actor != station["owner"]:
                result["conflicts"].append(self._conflict(conn, "station", device_id, local_uuid, "只有记录人或负责人可以修改站位", record, station["id"], revision))
                return
            latitude, longitude = self._validate_station(record)
            conn.execute(
                "UPDATE stations SET latitude=?,longitude=?,sampled_at=?,notes=?,revision=?,updated_at=? WHERE id=?",
                (latitude, longitude, str(record.get("sampled_at", station["sampled_at"])), str(record.get("notes", station["notes"])), revision, utcnow(), station["id"]),
            )
            conn.execute("UPDATE sync_records SET revision=?,payload_hash=?,synced_at=? WHERE local_uuid=?", (revision, payload_hash, utcnow(), local_uuid))
            self._save_revision(conn, "station", station["id"], revision, "sync", actor)
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
            dup_conflict = self._conflict(conn, "station", device_id, local_uuid,
                                          f"站位编号 {record.get('station_code')} 已存在，已分配 {code}",
                                          record, int(existing["id"]), revision, int(voyage["id"]))
        cur = conn.execute(
            """INSERT INTO stations(voyage_id,station_code,latitude,longitude,sampled_at,owner,notes,revision,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (voyage["id"], code, latitude, longitude, str(record.get("sampled_at", utcnow())), str(record.get("owner", actor)), str(record.get("notes", "")), revision, utcnow()),
        )
        conn.execute("INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at) VALUES(?,?,?,?,?,?,?)",
                     (local_uuid, device_id, "station", cur.lastrowid, revision, payload_hash, utcnow()))
        self._save_revision(conn, "station", int(cur.lastrowid), revision, "sync", actor)
        if existing:
            conn.execute("UPDATE conflicts SET dup_server_id=? WHERE id=?", (cur.lastrowid, dup_conflict["id"]))
            dup_conflict["dup_server_id"] = int(cur.lastrowid)
            result["conflicts"].append(dup_conflict)
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
        if self._already_handled(conn, "sample", device_id, local_uuid, record):
            result["duplicates"] += 1
            return
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["entity_type"] != "sample" or mapping["device_id"] != device_id:
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "同一本地 UUID 被其他设备或实体使用", record, int(mapping["server_id"]), revision))
                return
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
                return
            if revision <= int(mapping["revision"]):
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "修订号过旧或相同但内容不同", record, int(mapping["server_id"]), revision))
                return
            sample = conn.execute("SELECT * FROM samples WHERE id=?", (mapping["server_id"],)).fetchone()
            if not sample:
                raise DomainError("同步索引指向的样本不存在", 409)
            if sample["confirmed"]:
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "样本已确认，不能覆盖", record, sample["id"], revision))
                return
            if role != "lead" and actor != sample["owner"]:
                result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "只有记录人或负责人可以修改样本", record, sample["id"], revision))
                return
            code, sample_type, depth, storage = self._validate_sample(record)
            if code != sample["sample_code"]:
                # A changed client code is accepted only when it remains unique.
                other = conn.execute("SELECT 1 FROM samples WHERE sample_code=? AND id<>?", (code, sample["id"])).fetchone()
                if other:
                    result["conflicts"].append(self._conflict(conn, "sample", device_id, local_uuid, "修改后的样本编号已被占用", record, sample["id"], revision))
                    return
            conn.execute(
                "UPDATE samples SET sample_code=?,sample_type=?,depth_m=?,storage_condition=?,revision=?,updated_at=? WHERE id=?",
                (code, sample_type, depth, storage, revision, utcnow(), sample["id"]),
            )
            conn.execute("UPDATE sync_records SET revision=?,payload_hash=?,synced_at=? WHERE local_uuid=?", (revision, payload_hash, utcnow(), local_uuid))
            self._save_revision(conn, "sample", sample["id"], revision, "sync", actor)
            result["updated"] += 1
            return
        station = self._station(conn, record)
        parent = self._parent_sample(conn, record)
        code, sample_type, depth, storage = self._validate_sample(record)
        existing = conn.execute("SELECT * FROM samples WHERE sample_code=?", (code,)).fetchone()
        if existing:
            code = f"{code}-DUP-{local_uuid[:8]}"
            dup_conflict = self._conflict(conn, "sample", device_id, local_uuid,
                                          f"样本编号 {record.get('sample_code')} 已存在，已分配 {code}",
                                          record, int(existing["id"]), revision, int(station["voyage_id"]))
        cur = conn.execute(
            """INSERT INTO samples(voyage_id,station_id,parent_sample_id,sample_code,sample_type,depth_m,storage_condition,owner,revision,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (station["voyage_id"], station["id"], parent["id"] if parent else None, code, sample_type, depth, storage, str(record.get("owner", actor)), revision, utcnow()),
        )
        conn.execute("INSERT INTO sync_records(local_uuid,device_id,entity_type,server_id,revision,payload_hash,synced_at) VALUES(?,?,?,?,?,?,?)",
                     (local_uuid, device_id, "sample", cur.lastrowid, revision, payload_hash, utcnow()))
        self._save_revision(conn, "sample", int(cur.lastrowid), revision, "sync", actor)
        if existing:
            conn.execute("UPDATE conflicts SET dup_server_id=? WHERE id=?", (cur.lastrowid, dup_conflict["id"]))
            dup_conflict["dup_server_id"] = int(cur.lastrowid)
            result["conflicts"].append(dup_conflict)
        result["created"] += 1

    def _sync_custody(self, conn: sqlite3.Connection, actor: str, device_id: str, local_uuid: str,
                      revision: int, record: dict[str, Any], result: dict[str, Any]) -> None:
        if self._already_handled(conn, "custody", device_id, local_uuid, record):
            result["duplicates"] += 1
            return
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
            else:
                result["conflicts"].append(self._conflict(conn, "custody", device_id, local_uuid, "保管事件为追加记录，不能改写", record, int(mapping["server_id"]), revision))
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
        if self._already_handled(conn, "instrument_file", device_id, local_uuid, record):
            result["duplicates"] += 1
            return
        mapping = self._mapping(conn, local_uuid)
        payload_hash = hashlib.sha256(canonical(record).encode()).hexdigest()
        if mapping:
            if mapping["payload_hash"] == payload_hash:
                result["duplicates"] += 1
            else:
                result["conflicts"].append(self._conflict(conn, "instrument_file", device_id, local_uuid, "仪器文件元数据不能覆盖", record, int(mapping["server_id"]), revision))
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

    # ---- 冲突处置台 ----------------------------------------------------

    EDITABLE_FIELDS: dict[str, dict[str, type]] = {
        "station": {"station_code": str, "latitude": float, "longitude": float, "sampled_at": str, "notes": str},
        "sample": {"sample_code": str, "sample_type": str, "depth_m": float, "storage_condition": str},
    }

    def list_conflicts(self, voyage_id: int | None = None, status: str | None = None,
                       device_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM conflicts"
        where, params = [], []
        if voyage_id is not None:
            where.append("voyage_id=?")
            params.append(voyage_id)
        if status and status != "all":
            if status not in {"pending", "resolved"}:
                raise DomainError("状态只能是 pending 或 resolved")
            where.append("status=?")
            params.append(status)
        if device_id:
            where.append("device_id=?")
            params.append(device_id)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, id DESC"
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            items = [dict(r) for r in rows]
            for item in items:
                item["payload_json"] = json.loads(item["payload"])
                last = conn.execute(
                    "SELECT action,base_revision,new_revision,actor,created_at FROM conflict_resolutions "
                    "WHERE conflict_id=? ORDER BY id DESC LIMIT 1", (item["id"],)).fetchone()
                item["last_resolution"] = dict(last) if last else None
        return items

    def _conflict_row(self, conn: sqlite3.Connection, conflict_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
        if not row:
            raise DomainError("冲突记录不存在", 404)
        return row

    def _resolve_target(self, conn: sqlite3.Connection, cf: sqlite3.Row) -> tuple[str, sqlite3.Row]:
        """Return ('dup'|'primary', target row) for a station/sample conflict.

        处置台始终以岸端正本（编号持有者）为处置目标；-DUP- 隔离副本只作参考展示。
        """
        primary_id = cf["resolved_server_id"]
        if primary_id is None:
            return "primary", None  # type: ignore[return-value]
        table = "stations" if cf["entity_type"] == "station" else "samples"
        return "primary", conn.execute(f"SELECT * FROM {table} WHERE id=?", (primary_id,)).fetchone()

    def _entity_history(self, conn: sqlite3.Connection, entity_type: str, server_id: int) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM entity_revisions WHERE entity_type=? AND server_id=? ORDER BY revision DESC",
            (entity_type, server_id),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            item["snapshot_json"] = json.loads(item["snapshot"])
            items.append(item)
        return items

    def get_conflict(self, conflict_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            return self._conflict_detail(conn, conflict_id)

    def _conflict_detail(self, conn: sqlite3.Connection, conflict_id: int) -> dict[str, Any]:
        cf = self._conflict_row(conn, conflict_id)
        data = {
            "conflict": {k: cf[k] for k in cf.keys()},
            "entity_type": cf["entity_type"],
            "offline": json.loads(cf["payload"]),
            "contentions": [dict(r) for r in conn.execute(
                "SELECT * FROM conflict_contentions WHERE conflict_id=? ORDER BY id DESC LIMIT 20",
                (conflict_id,)).fetchall()],
            "resolutions": [dict(r) for r in conn.execute(
                "SELECT * FROM conflict_resolutions WHERE conflict_id=? ORDER BY id DESC",
                (conflict_id,)).fetchall()],
        }
        data["conflict"]["payload_json"] = data["offline"]
        if cf["entity_type"] in {"station", "sample"}:
            kind, target = self._resolve_target(conn, cf)
            data["target_kind"] = kind
            if target:
                shore = dict(target)
                data["target_id"] = shore["id"]
                data["shore"] = shore
                data["current_revision"] = int(shore["revision"])
                data["confirmed"] = bool(shore["confirmed"])
                data["editable_fields"] = list(self.EDITABLE_FIELDS[cf["entity_type"]])
                data["history"] = self._entity_history(conn, cf["entity_type"], shore["id"])
            else:
                data["target_missing"] = True
            if cf["dup_server_id"] is not None:
                table = "stations" if cf["entity_type"] == "station" else "samples"
                dup_row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (cf["dup_server_id"],)).fetchone()
                if dup_row:
                    data["dup_record"] = dict(dup_row)
        return data

    def _code_holder(self, conn: sqlite3.Connection, entity_type: str, voyage_id: int,
                     code: str, target_id: int) -> sqlite3.Row | None:
        if entity_type == "station":
            return conn.execute(
                "SELECT id,station_code AS code FROM stations WHERE voyage_id=? AND station_code=? AND id<>?",
                (voyage_id, code, target_id)).fetchone()
        return conn.execute(
            "SELECT id,sample_code AS code FROM samples WHERE sample_code=? AND id<>?",
            (code, target_id)).fetchone()

    def _coerce_fields(self, entity_type: str, values: dict[str, Any]) -> dict[str, Any]:
        spec = self.EDITABLE_FIELDS[entity_type]
        out: dict[str, Any] = {}
        for key, caster in spec.items():
            if key not in values:
                continue
            raw = values[key]
            try:
                out[key] = caster(raw)
            except (TypeError, ValueError) as exc:
                raise DomainError(f"字段 {key} 的值 {raw!r} 不是 {caster.__name__}") from exc
        return out

    def _assemble_station(self, shore: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        record = dict(shore)
        record.update(overrides)
        self._validate_station(record)
        record["station_code"] = str(record.get("station_code", "")).strip()
        record["sampled_at"] = str(record.get("sampled_at", ""))
        record["notes"] = str(record.get("notes", ""))
        if not record["station_code"] or not record["sampled_at"]:
            raise DomainError("站位编号和采样时间不能为空")
        return record

    def _assemble_sample(self, shore: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        record = dict(shore)
        record.update(overrides)
        code, sample_type, depth, storage = self._validate_sample(record)
        record["sample_code"], record["sample_type"] = code, sample_type
        record["depth_m"], record["storage_condition"] = depth, storage
        return record

    def _record_contention(self, conn: sqlite3.Connection, conflict_id: int, kind: str,
                           actor: str, expected: int | None, actual: int | None, detail: str) -> None:
        conn.execute(
            "INSERT INTO conflict_contentions(conflict_id,kind,expected_revision,actual_revision,detail,actor,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (conflict_id, kind, expected, actual, detail, actor, utcnow()),
        )

    def resolve_conflict(self, conflict_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "lead":
            raise DomainError("只有航次负责人可以处置冲突", 403)
        action = str(body.get("action", "")).strip()
        if action not in {"keep_shore", "take_offline", "merge"}:
            raise DomainError("处置方式必须是 keep_shore、take_offline 或 merge")
        note = str(body.get("note", "")).strip()
        try:
            expected_revision = int(body.get("expected_revision"))
        except (TypeError, ValueError) as exc:
            raise DomainError("expected_revision 必须是整数") from exc

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cf = self._conflict_row(conn, conflict_id)
            entity_type = cf["entity_type"]
            if cf["status"] == "resolved":
                raise DomainError("该冲突已处置；若有新的晚到修订请处理新的隔离项", 409)

            if entity_type not in {"station", "sample"}:
                # 保管事件与仪器文件只允许保留岸端（它们本身不可覆盖）。
                if action != "keep_shore":
                    raise DomainError("保管事件和仪器文件冲突只能保留岸端")
                conn.execute("UPDATE conflicts SET status='resolved',resolved_at=? WHERE id=?", (utcnow(), conflict_id))
                conn.execute(
                    "INSERT INTO conflict_resolutions(conflict_id,action,base_revision,new_revision,final_payload,actor,note,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (conflict_id, action, 0, 0, cf["payload"], actor, note, utcnow()),
                )
                self._audit(conn, actor, "conflict.resolved", entity_type, conflict_id,
                            {"action": action, "local_uuid": cf["local_uuid"]})
                return self._conflict_detail(conn, conflict_id)

            _kind, target_row = self._resolve_target(conn, cf)
            contentions: list[dict[str, Any]] = []
            if target_row is None:
                self._record_contention(conn, conflict_id, "target_missing", actor,
                                        expected_revision, None, "处置目标记录不存在，可能已被清理")
                contentions.append({"kind": "target_missing"})
            else:
                target = dict(target_row)
                if int(target["revision"]) != expected_revision:
                    detail = f"当前修订为 r{target['revision']}，提交基于 r{expected_revision}"
                    self._record_contention(conn, conflict_id, "revision_changed", actor,
                                            expected_revision, int(target["revision"]), detail)
                    contentions.append({"kind": "revision_changed", "actual_revision": int(target["revision"])})
                if int(target["confirmed"]):
                    self._record_contention(conn, conflict_id, "confirmed", actor,
                                            expected_revision, int(target["revision"]), "记录已被负责人确认锁定")
                    contentions.append({"kind": "confirmed"})

            if not contentions:
                target = dict(target_row)  # type: ignore[possibly-undefined]
                overrides: dict[str, Any]
                offline = json.loads(cf["payload"])
                if action == "keep_shore":
                    overrides = {}
                elif action == "take_offline":
                    overrides = self._coerce_fields(entity_type, {
                        k: offline.get(k, target.get(k)) for k in self.EDITABLE_FIELDS[entity_type]
                    })
                else:
                    merged = body.get("merged")
                    if not isinstance(merged, dict):
                        raise DomainError("逐字段合并需要提供 merged 对象")
                    overrides = self._coerce_fields(entity_type, merged)

                if entity_type == "station":
                    final = self._assemble_station(target, overrides)
                    holder = self._code_holder(conn, "station", int(target["voyage_id"]),
                                               final["station_code"], int(target["id"]))
                else:
                    final = self._assemble_sample(target, overrides)
                    holder = self._code_holder(conn, "sample", int(target["voyage_id"]),
                                               final["sample_code"], int(target["id"]))
                if holder is not None:
                    detail = f"编号 {holder['code']} 已被记录 #{holder['id']} 占用"
                    self._record_contention(conn, conflict_id, "code_taken", actor,
                                            expected_revision, int(target["revision"]), detail)
                    contentions.append({"kind": "code_taken", "holder_id": int(holder["id"]), "code": holder["code"]})

            if contentions:
                # 重新停在待核，等待处理人按最新状态重开处置；先固化争用记录再通知前端。
                conn.execute("UPDATE conflicts SET status='pending',resolved_at=NULL WHERE id=?", (conflict_id,))
                self._audit(conn, actor, "conflict.contended", entity_type, conflict_id,
                            {"contentions": contentions, "local_uuid": cf["local_uuid"]})
                latest = self._conflict_detail(conn, conflict_id)
                conn.commit()
                raise ContentionError({"conflict_id": conflict_id, "status": "pending",
                                       "contentions": contentions, "latest": latest})

            base_revision = int(target["revision"])
            new_revision = base_revision + 1
            now = utcnow()
            if action != "keep_shore":
                if entity_type == "station":
                    conn.execute(
                        "UPDATE stations SET station_code=?,latitude=?,longitude=?,sampled_at=?,notes=?,"
                        "revision=?,updated_at=? WHERE id=?",
                        (final["station_code"], final["latitude"], final["longitude"], final["sampled_at"],
                         final["notes"], new_revision, now, target["id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE samples SET sample_code=?,sample_type=?,depth_m=?,storage_condition=?,"
                        "revision=?,updated_at=? WHERE id=?",
                        (final["sample_code"], final["sample_type"], final["depth_m"], final["storage_condition"],
                         new_revision, now, target["id"]),
                    )
            else:
                conn.execute("UPDATE stations SET revision=?,updated_at=? WHERE id=?"
                             if entity_type == "station"
                             else "UPDATE samples SET revision=?,updated_at=? WHERE id=?",
                             (new_revision, now, target["id"]))

            self._save_revision(conn, entity_type, int(target["id"]), new_revision, f"conflict:{action}", actor)
            snapshot = self._snapshot(conn, entity_type, int(target["id"]))
            conn.execute(
                "INSERT INTO conflict_resolutions(conflict_id,action,base_revision,new_revision,final_payload,actor,note,created_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (conflict_id, action, base_revision, new_revision, canonical(snapshot), actor, note, now),
            )
            # 把处置结果回写到船端同步索引：该设备再补传同修订时直接识别为重复，不再进隔离。
            mapping = conn.execute(
                "SELECT * FROM sync_records WHERE local_uuid=? AND device_id=?",
                (cf["local_uuid"], cf["device_id"]),
            ).fetchone()
            if mapping and mapping["entity_type"] == entity_type and int(mapping["server_id"]) == int(target["id"]):
                conn.execute(
                    "UPDATE sync_records SET revision=?,payload_hash=?,synced_at=? WHERE local_uuid=? AND device_id=?",
                    (new_revision, hashlib.sha256(canonical(snapshot).encode()).hexdigest(), now,
                     cf["local_uuid"], cf["device_id"]),
                )
            conn.execute(
                "UPDATE conflicts SET status='resolved',resolved_server_id=?,resolved_at=? WHERE id=?",
                (int(target["id"]), now, conflict_id),
            )
            self._audit(conn, actor, "conflict.resolved", entity_type, conflict_id,
                        {"action": action, "target": int(target["id"]), "base_revision": base_revision,
                         "new_revision": new_revision, "local_uuid": cf["local_uuid"]})
        return self.get_conflict(conflict_id)

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

    def _html(self, name: str = "index.html") -> None:
        data = (ROOT / "static" / name).read_bytes()
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
            if parsed.path in {"/conflicts", "/workbench", "/conflicts.html"}:
                return self._html("workbench.html")
            if parsed.path == "/api/health":
                return self._send({"ok": True})
            query = parse_qs(parsed.query)
            if parsed.path == "/api/conflicts":
                voyage = query.get("voyage_id", [None])[0]
                status = query.get("status", ["all"])[0]
                device = query.get("device_id", [None])[0]
                return self._send({"items": self.db.list_conflicts(
                    int(voyage) if voyage not in (None, "", "all") else None, status, device)})
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) == 4 and parts[:2] == ["api", "conflicts"] and parts[3] == "detail":
                return self._send(self.db.get_conflict(int(parts[2])))
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
            if isinstance(exc, ContentionError):
                self._send(exc.payload, exc.status)
            else:
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
            if isinstance(exc, ContentionError):
                self._send(exc.payload, exc.status)
            else:
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
