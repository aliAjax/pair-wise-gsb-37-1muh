# 海洋科考采样记录与岸端同步

一个仅使用 Python 标准库实现的离线优先采样记录服务。船端可以按批次上传站位、样本、母样/分样、保管交接和仪器文件元数据；岸端负责幂等接收、冲突隔离、编号分配和确认锁定。

## 运行

```bash
python app.py --init
python app.py --port 8008
```

打开 <http://127.0.0.1:8008>。`--init` 会创建示例航次 `2026-ECS-01`。数据库默认是 `ocean_samples.db`，可用 `--db` 或 `OCEAN_DB` 修改。

## 离线同步

`POST /api/sync` 的每个记录都包含 `type`、`local_uuid`、`revision` 和 `data`。同一条记录重复上传会识别为 `duplicates`，不会重复建库。

```json
{
  "device_id": "tablet-A",
  "records": [
    {"type": "station", "local_uuid": "st-001", "revision": 1, "data": {
      "voyage_id": 1, "station_code": "S-01", "latitude": 30.1,
      "longitude": 122.0, "sampled_at": "2026-09-05T08:30:00+08:00",
      "owner": "member-a"
    }},
    {"type": "sample", "local_uuid": "sample-001", "revision": 1, "data": {
      "station_id": 1, "sample_code": "W-001", "sample_type": "water",
      "depth_m": 5, "storage_condition": "4C", "owner": "member-a"
    }}
  ]
}
```

## 业务规则

- `local_uuid + device_id` 是幂等键；相同内容重复上传直接返回已有记录。
- `revision` 必须递增。旧修订或同修订不同内容会进入冲突表，不会覆盖新数据。
- 站位编号在航次内唯一，样本编号全局唯一；检测到另一设备使用相同编号时分配 `-DUP-xxxx` 后缀并把冲突写入隔离表。
- 记录人员只能修改自己创建的站位/样本，`lead` 可以处理全部记录。
- `lead` 确认样本或站位后，该记录变为只读；发现错误必须通过新记录处理，不能用同步覆盖历史。
- 保管交接是追加式事件；仪器文件以 SHA-256 去重，原始内容相同但来自不同设备时只保存一次元数据。
- 冲突不再只停在隔离清单：`lead` 在冲突处置台（<http://127.0.0.1:8008/conflicts>）按设备、本地编号、岸端当前修订和离线来件逐项核对，选择保留岸端、采用离线值或逐字段合并。提交携带所基于的修订号；提交时当前修订已变、记录已确认或业务编号已被占用，冲突会重新停在待核并写争用记录，处理人按最新状态重开后可继续处置。
- 处置成功后岸端记录生成下一条修订（站位/样本保留全部历史快照，旧值可查），并写处置记录；同一设备再补传相同晚到内容会幂等识别为重复，处理结果以同步响应和 `GET /api/conflicts?device_id=` 回传船端。
- 航次、站位、样本、交接、文件和冲突都保存在 SQLite 中，所有同步批次有审计记录。

## API

- `POST /api/voyages`：创建航次。
- `POST /api/sync`：批量同步离线记录。
- `POST /api/confirm/station/{id}` 或 `/api/confirm/sample/{id}`：负责人确认锁定。
- `GET /api/stations`、`/api/samples`、`/api/custody`、`/api/instrument-files`：查询记录。
- `GET /api/conflicts`：查看编号冲突、旧修订和权限冲突。支持 `voyage_id`、`status=pending|resolved|all`、`device_id` 筛选。
- `GET /api/conflicts/{id}/detail`：处置台核对视图（设备、本地编号、岸端当前修订、离线内容、隔离副本、争用记录、处置记录、修订历史）。
- `POST /api/conflicts/{id}/resolve`：提交处置。Body 为 `{"action":"keep_shore|take_offline|merge","expected_revision":<核对时的当前修订>,"merged":{...},"note":""}`；修订不匹配/已确认/编号被占用时返回 409 及 `contentions`，冲突保持 `pending`。仅 `lead` 可调用。
- `GET /api/audit`：查看操作审计。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖完整离线同步、幂等重复、编号冲突、成员越权、旧修订、确认锁定、追加式交接和文件哈希去重，以及冲突处置台的三种处置方式、乐观修订争用（修订变更/已确认/编号占用）、修订历史与船端幂等回传。
