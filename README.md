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
- 航次、站位、样本、交接、文件和冲突都保存在 SQLite 中，所有同步批次有审计记录。

## 冲突处置台

回港后多台设备补传同站位/样本时，迟到内容进入隔离清单后由 `lead` 在处置台核对。每条冲突展示**设备、本地编号（local_uuid）、离线补传内容、岸端当前值和当前修订**，处理人三选一：

- `keep_shore` 保留岸端：内容不变，仍生成处置记录与下一条修订；
- `take_offline` 采用离线值：离线携带的字段覆盖岸端；
- `merge` 逐字段合并：从岸端值出发，仅采用逐字段勾选/手填的字段。

提交采用乐观并发：请求需带上核对时的 `base_revision`。若**当前修订又变了、记录已确认锁定，或合并后的编号已被其他记录占用**，提交会被整体拒绝，冲突重新停在 `pending` 并在 `contention` 中列出全部争用项（`revision_changed` / `confirmed` / `code_taken` / `missing`），处理人核对最新内容后可再次提交。

处理成功时：站位/样本写入下一条修订（旧值保存在 `entity_revisions`，随时可查），生成一条 `resolution_records` 处置记录，写审计日志，并把该设备的本地 UUID 同步索引对齐到最新修订。页面可按航次和状态筛选，筛选条件、当前选中冲突、处置方式与逐字段草稿都存于浏览器 `localStorage`，重开页面可接着处置。

## API

- `POST /api/voyages`：创建航次。
- `POST /api/sync`：批量同步离线记录。
- `POST /api/confirm/station/{id}` 或 `/api/confirm/sample/{id}`：负责人确认锁定。
- `GET /api/stations`、`/api/samples`、`/api/custody`、`/api/instrument-files`：查询记录。
- `GET /api/conflicts?voyage_id=&status=pending|resolved`：按航次/状态筛选编号冲突、旧修订和权限冲突。
- `GET /api/conflicts/{id}`：冲突详情（离线内容、岸端当前值、当前修订、修订历史、争用项）。
- `POST /api/conflicts/{id}/resolve`：负责人处置冲突，body 为 `{"decision": "keep_shore|take_offline|merge", "base_revision": n, "field_values": {...}}`。
- `GET /api/revisions/station/{id}`、`/api/revisions/sample/{id}`：逐条修订与旧值快照。
- `GET /api/resolutions?entity_type=&entity_id=`：处置记录。
- `GET /api/audit`：查看操作审计。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖完整离线同步、幂等重复、编号冲突、成员越权、旧修订、确认锁定、追加式交接和文件哈希去重。
