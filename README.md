# Reliable Webhook Delivery Service

可靠 Webhook 事件投递服务：接收事件，并通过 HTTP POST 把事件可靠地投递给订阅了该事件类型的 Webhook Endpoint。仅依赖 FastAPI + SQLAlchemy + SQLite，无需任何外部中间件；事件持久化、任务调度、多 Worker 并发控制与失败恢复全部由应用自身与 SQLite 完成。

## 功能

- 订阅管理：创建 / 查询 / 启用 / 禁用 Webhook Subscription
- 事件创建：按 event type 扇出，为每个匹配的启用订阅生成独立 Delivery
- 幂等创建：客户端可携带 idempotency key，重复提交（包括并发提交）不会产生重复 Event / Delivery
- 后台 Worker：从 SQLite 领取待执行 Delivery 并发起 HTTP POST
- 失败重试：指数退避，超过最大尝试次数进入永久失败
- 多 Worker 并发：数据库级 claim/lease，崩溃自动回收
- 重启恢复：进程重启后状态完整恢复，不重复投递、不丢任务

## 架构

```
app/
├── config.py    # Settings（可用 WEBHOOK_* 环境变量覆盖）
├── clock.py     # 可注入时钟（SystemClock / FakeClock），测试无需真实 sleep
├── db.py        # engine / session 工厂（WAL、busy_timeout、BEGIN IMMEDIATE）
├── models.py    # SQLAlchemy 模型：Subscription / Event / Delivery
├── schemas.py   # Pydantic 请求/响应模型
├── services.py  # 业务服务层：全部状态流转集中在此
├── worker.py    # 投递 Worker（可独立进程运行，可多个并行）
└── main.py      # FastAPI 应用工厂与路由
tests/           # pytest 测试（API、并发、lease、重试、恢复、统计）
```

分层：API 层（main.py）只做参数校验与响应组装；业务与状态机集中在 services.py，API 与 Worker 共用同一套状态流转；worker.py 只负责"领取 → 投递 → 上报结果"。

## Delivery 状态流转

```
                 claim                    投递成功
pending  ──────────────────►  running  ──────────────►  succeeded
  ▲                            │  │
  │        失败且未达上限        │  │ 失败且达到 max_attempts
  └────────────────────────────┘  └────────────────────►  failed（永久失败）
            （attempt+1，按指数退避设置 next_attempt_at）

running 且 lease 过期 ──可被其他 Worker 重新 claim──► running（新 lease_token）
```

- `pending` + `next_attempt_at <= now`：可被领取
- `running` + `lease_expires_at <= now`：lease 过期，可被重新领取
- `succeeded` / `failed`：终态，永远不会再被领取

## Lease 与并发控制

- 每条 Delivery 有 `lease_owner`、`lease_expires_at`、`lease_token` 三个字段。
- Worker 领取时使用**单条条件 UPDATE**：

  ```sql
  UPDATE deliveries SET status='running', lease_owner=?, lease_expires_at=?, lease_token=?
  WHERE id=? AND ((status='pending' AND next_attempt_at<=?) OR (status='running' AND lease_expires_at<=?))
  ```

  该语句是原子的，影响的行数（rowcount）为 1 才算领取成功；两个 Worker 竞争同一条 Delivery 时只有一个能成功。不依赖任何进程内锁。
- SQLite 以 WAL 模式运行，所有事务使用 `BEGIN IMMEDIATE` 并设置 `busy_timeout`，并发写者串行化而非死锁。

## Stale Worker 防护（fencing token）

每次 claim 都会生成新的随机 `lease_token`。Worker 上报结果时同样使用条件 UPDATE：

```sql
UPDATE deliveries SET ... WHERE id=? AND lease_token=? AND status='running'
```

若 Worker A 执行超时、lease 过期、Delivery 被 Worker B 重新领取（token 已更换），A 之后的更新匹配 0 行，被安全丢弃——旧 Worker 无法覆盖新 Worker 的执行状态。

## 幂等实现

`events.idempotency_key` 上有数据库**唯一约束**。创建流程为"先查（快速路径）→ 同事务插入 Event + Deliveries"；并发下两个相同 key 的请求会有一个在 INSERT 时触发 `IntegrityError`，回滚后重新查询并返回先到的那个 Event。正确性由唯一约束保证，不存在"先 SELECT 再 INSERT"的竞争窗口。不带 key 的事件不去重（NULL 不参与唯一约束）。

## 失败重试

- 非 2xx、连接失败、超时等一律视为失败。
- 退避：`delay = base_delay_seconds * 2^(attempt-1)`，到达 `next_attempt_at` 前不会被领取。
- `attempt_count >= max_attempts` 后进入 `failed` 终态。
- 单个 endpoint 异常只影响自己的 Delivery，Worker 不会崩溃，其余 Delivery 照常处理。

## 重启恢复

所有状态都在 SQLite 中，应用启动时**不做任何状态重置**：

- `succeeded` / `failed` 终态不会再被领取；
- 等待重试的 Delivery 保留原 `next_attempt_at`；
- lease 未过期的 `running` Delivery 不会被提前重复执行；
- lease 已过期的 `running` Delivery 会被其他（或重启后的）Worker 自动重新领取。

## 安装

需要 Python 3.10+：

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows；Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

## 启动

```bash
# 终端 1：API 服务（默认 sqlite 文件 ./webhook.db）
uvicorn app.main:app --port 8000

# 终端 2（可开多个）：投递 Worker
python -m app.worker
```

配置项（环境变量）：`WEBHOOK_DATABASE_URL`、`WEBHOOK_MAX_ATTEMPTS`（默认 5）、`WEBHOOK_BASE_DELAY_SECONDS`（默认 1.0）、`WEBHOOK_LEASE_SECONDS`（默认 30）、`WEBHOOK_HTTP_TIMEOUT_SECONDS`、`WEBHOOK_WORKER_BATCH_SIZE`、`WEBHOOK_WORKER_POLL_INTERVAL_SECONDS`。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/subscriptions` | 创建订阅 `{url, event_type}` |
| GET | `/subscriptions` | 订阅列表 |
| GET | `/subscriptions/{id}` | 订阅详情 |
| PATCH | `/subscriptions/{id}` | 启用/禁用 `{enabled: bool}` |
| POST | `/events` | 创建事件 `{event_type, payload, idempotency_key?}`；重复 key 返回 200 与原事件 |
| GET | `/events/{id}/status` | 事件投递统计：total/pending/running/succeeded/failed + 每条 Delivery 状态与 attempt 数 |

投递请求体：

```json
{"event_id": "...", "event_type": "...", "payload": {...}, "delivery_id": "..."}
```

## 测试

```bash
pytest
```

测试使用 `FakeClock`（时间可注入，无真实 sleep）与 `httpx.MockTransport` / fake sender（不依赖公网）。覆盖：订阅管理与禁用、扇出、幂等（含并发）、claim 竞争、lease 阻止/过期回收、stale worker 防护、2xx/非 2xx/网络异常、attempt 递增、指数退避、永久失败、重启恢复、事件状态统计。
