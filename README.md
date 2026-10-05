# Reliable Webhook Delivery Service

一个从 0 到 1 实现的**可靠 Webhook 事件投递服务**：接收业务事件，按订阅关系将事件
通过 HTTP POST 投递给每个订阅方，并保证：

- 事件与投递记录持久化在 SQLite 中，进程退出/崩溃不会丢失；
- 多个 Worker（同进程多线程或**多个独立进程**）可以并发运行，基于数据库级
  lease/fencing 机制保证同一条 Delivery 同一时刻只被一个 Worker 执行；
- Worker 崩溃后 lease 到期可被其他 Worker 自动接管，不会永久卡在 `running`；
- 过期（stale）Worker 的迟到结果无法覆盖当前 Owner 的状态；
- 失败按指数退避重试，超过最大次数进入永久失败；
- 创建 Event 支持幂等键，重复/并发提交不会产生重复 Event 和 Delivery。

整个系统**只依赖应用本身和 SQLite**，不使用 Redis/RabbitMQ/Kafka/Celery 等任何
外部中间件，也不依赖内存锁或全局变量实现互斥。

---

## 1. 技术栈

- Python 3.10+（开发验证使用 3.11）
- FastAPI + Uvicorn（HTTP API / 进程内 Worker 线程）
- SQLAlchemy 2.x（ORM / Core 条件更新）
- SQLite（WAL 模式，唯一持久化与调度介质）
- httpx（投递 HTTP 请求）
- pytest（自动化测试）

无 Docker。

---

## 2. 目录结构

```
app/
├── config.py              # 配置（全部可通过 WEBHOOK_* 环境变量覆盖）
├── clock.py               # 可注入时钟（SystemClock / 测试用 FakeClock）
├── db.py                  # 引擎、Session 工厂、SQLite PRAGMA、建表
├── models.py              # Subscription / Event / DeliveryRow ORM 模型与状态常量
├── container.py           # 应用装配（settings + engine + session_factory + clock）
├── main.py                # FastAPI 工厂、健康检查、可选的进程内 Worker 线程
├── worker_main.py         # 独立 Worker 进程入口 (python -m app.worker_main)
├── api/
│   ├── schemas.py         # Pydantic 请求/响应模型
│   ├── deps.py            # FastAPI 依赖：Session、Clock、各 Service
│   └── routes.py          # 订阅 / 事件 / 状态查询路由
├── services/
│   ├── subscriptions.py   # 订阅业务
│   ├── events.py          # 事件创建 + fan-out + 幂等（DB 唯一约束）
│   ├── deliveries.py      # ★ Delivery 状态机：原子 claim/fenced 完成/失败/统计
│   └── retry.py           # 纯函数：指数退避计算
└── worker/
    ├── sender.py          # HttpSender（真实投递）与 Sender 协议
    └── worker.py          # Worker 循环：claim → POST → 成功/失败落库

tests/                     # 61 个 pytest 用例（含真实 HTTP、多 OS 进程竞争、端到端）
examples/
└── fake_receiver.py       # 本地手工验证用的接收端
```

状态流转的 SQL 全部集中在 `services/deliveries.py`，API 层和 Worker 共用同一套
状态机，不存在两套实现。

---

## 3. HTTP API

前缀 `/api/v1`。

### 订阅

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/subscriptions` | 创建订阅，body：`url`, `event_type`, `enabled`(默认 true) |
| GET | `/subscriptions?event_type=...` | 查询订阅列表（可按类型过滤） |
| GET | `/subscriptions/{id}` | 查询单个订阅 |
| PATCH | `/subscriptions/{id}` | 启用/禁用，body：`{"enabled": false}` |

禁用后不再为**新事件**生成 Delivery，但历史 Delivery 原样保留。

### 事件

`POST /events`，body：

```json
{
  "event_type": "order.created",
  "payload": {"order_id": 123},
  "idempotency_key": "optional-client-key"
}
```

创建后会为每个 `event_type` 匹配且 `enabled=true` 的订阅生成一条独立 Delivery。
重复使用同一 `idempotency_key` 返回第一次创建的 Event（HTTP 200），不产生重复数据。

### 状态查询

`GET /events/{event_id}/status` 返回：

```json
{
  "event": { "...": "事件基本信息" },
  "total": 3,
  "pending": 1,
  "running": 0,
  "succeeded": 1,
  "permanently_failed": 1,
  "deliveries": [
    {
      "id": "...", "subscription_id": "...", "status": "pending",
      "attempts": 2, "max_attempts": 5,
      "next_attempt_at": "...", "last_error": "HTTP 500: ...",
      "lease_owner": null, "lease_expires_at": null
    }
  ]
}
```

统计在查询时从数据库实时聚合，不维护任何可能失准的内存计数。

另：`GET /health`。

---

## 4. Delivery 状态流转

```
                         (创建 Event 时 fan-out)
                                  │
                                  ▼
                              ┌────────┐
                ┌────────────▶│ PENDING│◀──────────────┐
                │ 失败(未超次) └────────┘ 失败但未达上限   │
                │              │  ▲                     │
                │   next_attempt_at 到期 / lease 过期     │
                │              ▼  │                     │
                │           ┌────────┐                  │
                │           │RUNNING │  成功 (2xx)        │
                │           └────────┘──────────▶┌───────────┐
                │               │ 失败且达到上限     │SUCCEEDED  │(终态)
                │               └──────────────▶└───────────┘
                │                                 ┌────────┐
                └─────────────────────────────────│ FAILED │(永久失败,终态)
                                                  └────────┘
```

- `pending` 同时表示“从未尝试”和“等待重试”，区别由 `attempts` 与
  `next_attempt_at` 表达；
- `running` 表示某 Worker 持有有效（或刚过期但尚未被接管）的 lease；
- `succeeded` / `failed` 为终态，永不再被 claim；
- `attempts` 在 claim 的同一条 UPDATE 中原子 +1。

---

## 5. Lease 与并发控制方案（多 Worker / 多进程安全）

### 5.1 原子 claim

可被领取的 Delivery 有两种：

1. `pending` 且 `next_attempt_at <= now`（新任务或重试到期）；
2. `running` 但 `lease_expires_at < now`（旧 Worker 崩溃/卡住留下的过期租约）。

claim 由**一条** `UPDATE ... WHERE id IN (SELECT ... LIMIT 1) ... RETURNING`
完成：

```sql
UPDATE deliveries
SET status='running',
    attempts = attempts + 1,
    lease_owner = :owner,
    lease_expires_at = :now + lease_duration,
    lease_generation = lease_generation + 1
WHERE id IN (
    SELECT id FROM deliveries
    WHERE (status='pending' AND next_attempt_at <= :now)
       OR (status='running' AND lease_expires_at <  :now)
    ORDER BY <pending 优先>, next_attempt_at, id
    LIMIT 1
)
RETURNING id, event_id, subscription_id, attempts, lease_generation, lease_expires_at;
```

SQLite 的写操作在单条语句级别串行化（配合 WAL + `busy_timeout`），两个 Worker
（即使是不同 OS 进程）执行同一条 UPDATE 时只有一个能匹配到该 id 并 RETURNING 数据，
另一个影响 0 行。这不是“先 SELECT 再 UPDATE”的应用层加锁，竞争窗口由数据库消除。
该行为在 `tests/test_multiprocess.py` 中用两个真实子进程验证。

### 5.2 租约字段

- `lease_owner`：Worker 标识（如 `worker-<uuid>`）；
- `lease_expires_at`：租约到期时间（UTC）；
- `lease_generation`：每次（重新）claim 单调递增，用作 **fencing token**。

### 5.3 崩溃恢复

Worker 崩溃时来不及清理自己的 `running` 行。由于 claim 候选包含“过期 running”，
lease 到期后其他 Worker 可直接重新领取并把 generation +1。系统**启动时不做任何
`running → pending` 批量重置**：未过期的 running 仍受保护，过期的 running 由常规
claim 流程接管。

---

## 6. 防止过期 Worker 覆盖状态（Fencing）

场景：A 领取 D → A 执行过久 lease 过期 → B 重新领取 D（generation 变为更大值）
→ A 才收到成功响应并尝试把 D 写成成功。

所有终态/回退写入都带条件：

```sql
UPDATE deliveries SET status='succeeded', ...
WHERE id = :id
  AND status='running'
  AND lease_owner = :owner
  AND lease_generation = :generation;
```

A 持有的是旧 generation，与当前行不匹配，`rowcount = 0`，更新被拒绝；只有当前
Owner B（持有新 generation）能写入。失败路径 `record_failure` 同样先校验 owner +
generation 再决定 `pending(重试)` 或 `failed(永久失败)`。Worker 对 `rowcount=0`
仅记录一条 stale-write 警告，不抛异常。

---

## 7. 失败重试与指数退避

- HTTP 2xx 视为成功；非 2xx、连接失败、DNS 失败、读写超时等 `httpx` 异常都视为失败；
- 失败后：`attempts` 已在 claim 时 +1；
  - 若 `attempts >= max_attempts`：进入 `failed`（永久失败）；
  - 否则回到 `pending`，释放 lease，设置
    `next_attempt_at = now + base_delay * 2^(attempt-1)`；
- 默认 `base_delay=5s`，`max_attempts=5`（均可配置）；
- 单个 endpoint 的异常只记录在对应 Delivery 上，Worker 循环不会崩溃，也不会
  影响其它 Delivery。

退避公式为纯函数（`app/services/retry.py`），独立可测，测试中通过注入时钟推进
时间，不使用真实 sleep 等待退避。

---

## 8. 幂等实现

`events.idempotency_key` 上建有 **UNIQUE 约束**。创建 Event 时：

1. 在一个 SAVEPOINT 中 INSERT Event 并 flush；
2. 若两个携带相同 key 的请求并发到达，数据库只允许一个 INSERT 成功；
3. 失败方捕获 `IntegrityError`、回滚到 SAVEPOINT（外层事务仍可用），按 key
   查出胜者 Event 并返回，不再 fan-out、不生成 Delivery。

安全性来自数据库约束，而不是“先 SELECT 再 INSERT”。并发测试使用两个线程+独立
Session 同时提交同一 key（`tests/test_idempotency.py`）。

---

## 9. 配置项

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `WEBHOOK_DATABASE_URL` | `sqlite:///./webhook.db` | 数据库连接串 |
| `WEBHOOK_LEASE_DURATION_SECONDS` | `30` | 单次领取的租约时长 |
| `WEBHOOK_BASE_DELAY_SECONDS` | `5` | 指数退避基数 |
| `WEBHOOK_MAX_ATTEMPTS` | `5` | 最大尝试次数 |
| `WEBHOOK_REQUEST_TIMEOUT_SECONDS` | `10` | 投递 HTTP 超时 |
| `WEBHOOK_WORKER_IDLE_SECONDS` | `0.5` | 无任务时 Worker 空转间隔 |
| `WEBHOOK_WORKER_BATCH_SIZE` | `10` | 单次 tick 最多领取数 |
| `WEBHOOK_RUN_WORKER_IN_API` | `true` | API 进程内是否同时跑 Worker 线程 |

SQLite 连接统一设置：`journal_mode=WAL`、`synchronous=NORMAL`、
`busy_timeout=10000`、`foreign_keys=ON`。

---

## 10. 安装

要求 Python 3.10+。

```bash
python -m venv .venv
# Windows bash
source .venv/Scripts/activate
# Linux/macOS
# source .venv/bin/activate

pip install -r requirements.txt
```

---

## 11. 启动方式

### 方式 A：单进程（API + 后台 Worker 线程）

适合本地运行与单机部署：

```bash
python -m app.main
# 或
uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000
```

打开 `http://127.0.0.1:8000/health`。可用 `WEBHOOK_RUN_WORKER_IN_API=false`
只起 API 不起 Worker。

### 方式 B：API 与 Worker 分进程（可水平扩 Worker）

```bash
# 进程 1..N：API（关闭内置 Worker）
WEBHOOK_RUN_WORKER_IN_API=false python -m app.main

# 进程 1..N：独立 Worker，可起任意多个，天然互斥
python -m app.worker_main
```

所有进程使用同一个 SQLite 文件即可（建议放共享磁盘路径）。

### 手工验证

```bash
# 终端 1：本地接收端（前两次返回 500，观察重试）
python examples/fake_receiver.py

# 终端 2：启动服务
python -m app.main

# 终端 3
curl -X POST localhost:8000/api/v1/subscriptions -H "Content-Type: application/json" \
  -d '{"url":"http://127.0.0.1:8787/hook","event_type":"demo"}'
curl -X POST localhost:8000/api/v1/events -H "Content-Type: application/json" \
  -d '{"event_type":"demo","payload":{"hello":"world"},"idempotency_key":"k1"}'
```

---

## 12. 测试

```bash
pytest
```

测试策略：

- **时间**：全部走可注入的 `FakeClock`，用 `clock.advance(...)` 确定性推进，
  仅真实后台线程的端到端用例使用轮询等待（无固定长 sleep）；
- **HTTP 端点**：用线程内 `http.server` 假接收端 / 脚本化 `FakeSender`，
  不访问公网；
- **并发**：同进程多线程 + **两个真实 OS 子进程**竞争同一行；
- **重启**：对同一文件数据库反复构建全新 engine/session/worker 模拟进程重启；
- **状态统计**：断言来自真实数据库的计数。

覆盖需求中的 18 类场景：订阅增查/禁用、fan-out 独立性、幂等重复与并发、
claim 竞争、有效 lease 互斥、lease 过期接管、stale worker 成功/失败写入被拒、
2xx 成功、非 2xx 重试、网络异常不崩溃、attempts 递增、退避公式、最大次数耗尽、
永久失败不再领取、重启五类状态语义、Event 实时统计。

---

## 13. 设计取舍说明

- 投递信封字段同时放在 JSON body（`delivery_id/event_id/event_type/payload`）
  和响应头（`X-Webhook-*`）中，方便接收方任意取用。
- `HttpSender` 默认不继承系统代理环境变量（`trust_env=False`），避免机器上的
  代理软件意外改变投递路径。
- Delivery 的唯一约束 `(event_id, subscription_id)` 作为 fan-out 的第二道防线。
