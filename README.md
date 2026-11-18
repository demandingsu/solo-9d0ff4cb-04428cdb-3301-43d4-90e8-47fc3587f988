# 排污口清单同步 API

环保数据员接收监管系统的**排污口全量清单**（分片上传）与**连续变更流**，巡查端按区查询**现役排口**。
FastAPI + SQLite（WAL 模式）实现，发布切换采用单事务原子换版：任何缺片、断号或写入失败都不会影响已供出的旧版清单。

## 一、快速启动

### Docker Compose（推荐）

```bash
docker compose up -d --build
# 服务：http://localhost:8000
# 交互文档：http://localhost:8000/docs  （可直接在页面上试接口）
# ReDoc：http://localhost:8000/redoc
```

SQLite 数据文件位于命名卷 `discharge-data` 内（`/data/discharge.db`）。

### 本地直接运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 数据库路径可用环境变量覆盖：DB_PATH=/somewhere/discharge.db
```

### 运行测试

```bash
pip install -r requirements.txt
pytest                      # 31 个单元/并发用例（含截止序号基线与废弃重开专项）
python3 tests/smoke_http.py # 需先启动服务，44 项 HTTP 端到端检查
```

## 二、核心业务规则

### 1. 全量清单（批次 + 分片）

- `POST /batches` **开批**：声明分片总数 `total_shards` 与来源截止序号 `source_cutoff_seq`（快照基线）。
  同一时刻只允许一个 `open` 批次，重复开批返回 `409 OPEN_BATCH_EXISTS`。
- `POST /batches/{id}/shards` **分片提交**：每片含排口号、区号、启停状态。
  - 同 `(批次, 片号)` 重试且**内容相同** → 幂等，只生效一次（响应 `idempotent=true`）。
  - 同片号**内容不同** → `409 SHARD_CONTENT_CONFLICT`。
  - 排口号在批次内重复（片内或跨片）→ `409 DUPLICATE_OUTLET(_IN_SHARD)`。
- 暂存区与已发布清单物理隔离：发布前巡查端查不到任何新批数据。

### 2. 变更流（连续序号）

`POST /changes` 提交 `add`（新增，需 district+active）/ `move`（迁区，需 district）/ `deactivate`（停用）。

- **有效连续性基线** = `max(已供出水位 last_served_seq, 在传批次来源截止序号 source_cutoff_seq)`。
  全量快照已覆盖的历史序号（1..截止序号）内嵌在快照里、不属于增量变更流，**不算缺口、不可补写**：
  首批全量截止序号为 100 时，上传期间到达的 101 号变更直接接收，不会再被误报为“缺少 1～100”。
- 序号 `seq` 必须在基线之后**连续**：越过真实缺号提交返回 `422 SEQ_GAP`，`details.missing_seqs`
  只含基线之后真正未到的序号。
- 基线之内的新序号提交返回 `409 SEQ_COVERED_BY_SNAPSHOT`（快照已覆盖）；
  已供出水位之内且变更流中不存在的旧序号返回 `409 SEQ_REGRESSION`（已发布历史不可改）。
- 同序号内容相同重试 → 幂等；内容不同 → `409 CHANGE_CONTENT_CONFLICT`。
- **全量上传期间变更照常接收**：
  - 已有现役清单时，合法变更立即套用到现役（旧版）清单，巡查端实时可见；
  - 首批全量尚未发布（无现役版本）时，变更进入**暂挂**状态（`applied=false, blocked=false, held=true`），
    发布时由窗口重放纳入现役清单——暂挂是正常等待，不是链上断点。
- 变更引用的排口在现役清单不存在时，该序号置 `blocked`，**其后的变更不得越过断点**；
  待新快照补齐排口并发布时，窗口重放自动解除阻断。
- 响应与查询视图新增 `effective_baseline`（有效基线）、`held`/`held_change_count`（暂挂标记）；
  `next_expected_seq` 与缺号列表均以有效基线为起点，快照已覆盖的历史不会被算作待补序号。

### 3. 发布切换（原子换版）

`POST /batches/{id}/publish`，指定追平序号 `catch_up_seq`。全部条件满足才发布：

| 门禁 | 失败码 |
| --- | --- |
| 追平序号不得低于已供出水位（不可倒退） | `409 CATCHUP_REGRESSION` |
| 追平序号不得小于批次来源截止序号 | `422 CATCHUP_BELOW_CUTOFF` |
| 分片必须齐全 | `422 MISSING_SHARDS`（返回待补片号） |
| 截止序号之后至追平序号之间变更无缺号 | `422 CHANGE_SEQ_GAP`（返回待补序号） |
| 窗口变更在新快照上可套用 | `422 PUBLISH_CHANGE_FAILED` |
| 已生效的窗口外高序号变更，快照必须已含其效果 | `422 SNAPSHOT_MISSING_LIVE_CHANGE` |

发布在**单个 IMMEDIATE 事务**内完成：写入新版本快照 → 按序重放窗口 `(cutoff, catch_up_seq]` 内变更
（含上传期间暂挂、以及在旧版被阻断的变更）→ 续跑窗口外未套用变更 → 删除旧版 → 推进版本号与水位 → 关闭批次。
任一步失败整体回滚，**旧版照常供数，批次保持 open**（可用 `POST /batches/{id}/abort` 废弃后等下一周期重开）。

**废弃与重开**：`abort` 只清理暂存分片、把批次置为 `aborted`，**不推进现役版本与水位、不删除已接收变更**。
用相同来源截止序号重开批次后，此前接收（含暂挂/阻断）的变更仍在变更流中，可直接用于追平；
有效基线不变，下一应收序号与待补序号同样不把快照已覆盖的历史算作缺口。

### 4. 查询（始终取同一已提交状态）

- `GET /outlets?district=&active_only=&limit=&offset=`：按区查询现役排口。
- `GET /stats`：按区计数（总数/启用/停用），与 `/outlets` 读同一版本。
- `GET /batches/current`：当前批次的已收片号、**待补片号**、待补变更序号、断点原因、水位。
- `GET /changes`：变更流、全量缺号列表、阻断信息。
- `GET /health`：当前版本号与已供出序号水位。

所有读请求走 SQLite 只读连接（WAL 模式），不阻塞写。

## 三、接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches` | 开批（片数 + 来源截止序号） |
| GET | `/batches/current` | 当前批次进度与待补片号/序号 |
| POST | `/batches/{batch_id}/shards` | 提交分片（幂等/冲突） |
| POST | `/batches/{batch_id}/publish` | 发布切换（指定追平序号） |
| POST | `/batches/{batch_id}/abort` | 废弃进行中的失败批次 |
| POST | `/changes` | 提交连续序号变更 |
| GET | `/changes` | 变更流与缺号/断点查询 |
| GET | `/outlets` | 按区查询现役排口 |
| GET | `/stats` | 按区计数 |
| GET | `/health` | 健康检查与版本水位 |

错误响应统一为：`{"code": "错误码", "message": "中文说明", "details": {...}}`。

## 四、目录结构

```
app/
  database.py   # SQLite 连接、WAL、建表、IMMEDIATE 事务
  models.py     # Pydantic 请求/响应模型
  service.py    # 全部业务规则与事务边界
  main.py       # FastAPI 路由与异常处理
tests/
  test_sync_api.py       # 18 个功能用例（含切换交错、失败回滚、阻断续链）
  test_cutoff_baseline.py# 9 个截止序号基线专项（暂挂追平、真实断号、废弃重开、发布失败交错）
  test_concurrency.py    # 4 个多线程并发用例（串行化、幂等、序号约束）
  smoke_http.py          # 针对运行中服务的 44 项 HTTP 冒烟检查
docker-compose.yml / Dockerfile
```

## 五、关键设计说明

- **有效基线**：变更连续性以 `max(已供出水位, 在传批次截止序号)` 为基线判定，快照内嵌的历史序号
  不进变更流、不算缺口；`next_expected_seq`/缺号列表/发布门禁共用同一基线口径。
- **暂挂与阻断分离**：首批发布前到达的变更为暂挂（`held`，可发布重放）；只有真实套用失败才置
  `blocked` 断点，断点之后不得越号，发布重放成功即解除。
- **幂等判定**：请求体规范化 JSON（排序键、无空格）后取 SHA-256 存库，重试只比对哈希。
- **并发控制**：所有写请求 `BEGIN IMMEDIATE` 串行化，配合 `busy_timeout=30s`；读请求只读连接，读写不互斥。
- **版本隔离**：`outlet` 表带 `version` 字段，发布成功前旧版行不动，切换时同事务删旧插新。
- **废弃安全**：废弃批次不触碰 `version_state` 与 `change_log`，现役水位不提前推进，重开后历史接收可复用。
