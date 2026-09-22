# 设备接入与命令下发网关 — 设计说明

本服务是一个可在本地完整运行的 IoT 网关：设备注册/鉴权、遥测摄入、命令下发与
离线补发全部在单进程内用一把全局锁串行化关键状态迁移，保证并发上报、设备离线
重连、协议版本差异下的结论一致且可解释。

## 1. 模块结构

| 文件 | 职责 |
| --- | --- |
| `gateway/models.py` | `Device` / `TelemetryRecord` / `Command` 数据模型与带锁 `Store` |
| `gateway/errors.py` | 稳定错误码 `ErrorCode` 与 `GatewayError` |
| `gateway/versions.py` | 协议版本白名单与冻结的 `VersionPolicy`（缺省语义、时间戳窗口） |
| `gateway/auth.py` | 设备密钥派生与恒定时间比较 |
| `gateway/devices.py` | 设备注册、鉴权、上下线状态机（含重连补发） |
| `gateway/telemetry.py` | 遥测摄入管线：鉴权→去重→时间戳窗口→落库 |
| `gateway/commands.py` | 命令状态机、离线 FIFO 队列、超时/取消/确认 |
| `gateway/app.py` | FastAPI HTTP 层，错误码到 HTTP 状态码的映射 |

## 2. 设备状态机

```
                 register
   (不存在) ───────────────▶ REGISTERED
                               │
                               │ connect(密钥正确, 版本匹配)
                               ▼
        disconnect ┌──────▶ ONLINE ◀──────┐
                   │        │             │ connect(重连, 幂等)
                   │        │ issue 命令  │ connect(已在线, 幂等)
                   ▼        ▼             │
                 OFFLINE ────┴────────────┘
```

- `REGISTERED`：已注册未连接。此时下发命令按**离线**处理（进入队列）。
- `ONLINE`：连接成功。连接时在**同一把锁内**排空离线队列（FIFO 补发）。
- `OFFLINE`：显式断开。重复 disconnect 幂等。
- 重复连接（已在线再次 connect）幂等：状态不变，不重复补发。
- 重复注册 → `DUPLICATE_DEVICE`；未知设备 → `DEVICE_NOT_FOUND`；
  密钥错误 → `INVALID_CREDENTIAL`；连接版本与注册版本不一致 →
  `UNSUPPORTED_VERSION`（不静默改写档案版本）。

## 3. 命令状态机

```
   设备在线 issue                 SENT ──ack──▶ ACKED      (终态)
       ┌───────────────────────▶   │
       │                           ├──timeout(超过 timeout_at)──▶ TIMEOUT (终态)
       │                           └──cancel──────────────────▶ CANCELLED(终态)
       │
   设备离线 issue -> QUEUED ──重连 FIFO 补发──▶ SENT
                       │
                       └──cancel────────────▶ CANCELLED (同时从队列移除)
```

- **离线补发**：离线期间命令按 issue 顺序进入每设备队列；重连时在同一事务
  （同一把锁，伴随设备状态翻 ONLINE）内 FIFO 全部转 `SENT`，每条恰好一次。
  再次 drain / 重复重连不会二次补发。
- **超时**：`SENT` 命令带 `timeout_at = 下发时刻 + ttl`。任何后续操作
  （ack/cancel/timeout/查询）都会先判定：`now >= timeout_at`
  则确定性地先转 `TIMEOUT`，随后的 ack/cancel 以
  `INVALID_COMMAND_TRANSITION` 拒绝。即 deadline 端点时刻即算超时；
  查询接口本身也会触发到期，因此超时后任何读取结论一致。
- **重复确认**：对已 `ACKED` 的命令再次 ack 幂等返回，`acked_at` 不变。
- **取消**：仅 `QUEUED`/`SENT` 可取消；取消 `QUEUED` 会同步从队列移除。
- 任何非法迁移（对终态再迁移、对 `QUEUED` 做 timeout 等）一律
  `INVALID_COMMAND_TRANSITION` 拒绝，状态不被覆盖。
- 并发的 ack/cancel/timeout 在全局锁内串行，**恰好一个**操作完成迁移，
  其余得到可区分的拒绝（见 `test_concurrent_lifecycle_operations_do_not_overwrite`）。

## 4. 遥测摄入规则与幂等

判定顺序（短路，每步失败错误码互不相同）：

1. 版本解析与字段归一化：`UNSUPPORTED_VERSION` / `MISSING_FIELD`
2. `message_id` 必填：`MISSING_FIELD`
3. 鉴权（锁内）：`DEVICE_NOT_FOUND` / `INVALID_CREDENTIAL`
4. 幂等去重：`(device_id, message_id)` 已存在 → `TELEMETRY_DUPLICATE`
   （重试报文即使内容或时间戳不同也按重复拒绝，且不覆盖原始数据）
5. 时间戳窗口：`|ts - 接收时刻| > 版本窗口` → `TIMESTAMP_OUT_OF_RANGE`
   （恰好在窗口端点上合法，即允许的偏差为闭区间）

通过后在锁内分配单调递增的 `ingested_seq` 落库：

- **并发不重复计数**：N 个线程并发不同消息 → 恰好 N 条、序号 1..N 连续唯一；
  并发同一 `message_id` → 恰好 1 条 accepted，其余全部 TELEMETRY_DUPLICATE。
- **乱序可解释**：存储保留真实到达顺序（`ingested_seq`），读取按
  `(ts, ingested_seq)` 升序，因此乱序时间戳不会污染读取顺序，又能追溯摄入先后。

## 5. 协议版本兼容规则（不静默降级）

版本策略**冻结**在代码中（`VersionPolicy` 为 frozen dataclass）：版本规则只能
新增、不能改动，既存设备数据均留存 `protocol_version`，判定永远可复现。

| 版本 | 缺失 `version` 头 | `ts` | `metrics` | 时间戳窗口 |
| --- | --- | --- | --- | --- |
| `v1`（默认） | 按 v1 处理（文档化缺省，非降级） | 可缺失→服务端以摄入时刻补戳 | 可缺失→`{}` | ±300s |
| `v2` | —— | 必填 | 必填 | ±60s |
| 其它（`v3`、`v9`…） | 一律 `UNSUPPORTED_VERSION` 拒绝，绝不回退到 v1 |

版本字符串大小写不敏感并去除空白（`" V2 "` → `v2`）。
设备注册时绑定协议版本；连接时携带不同版本会被明确拒绝。

## 6. 错误码（调用方据此分支，勿依赖文案）

| 错误码 | HTTP | 含义 |
| --- | --- | --- |
| `DEVICE_NOT_FOUND` | 404 | 设备未注册 |
| `DUPLICATE_DEVICE` | 409 | 设备重复注册 |
| `INVALID_CREDENTIAL` | 401 | 密钥无效（与未知设备严格区分） |
| `UNSUPPORTED_VERSION` | 400 | 未知/不支持版本或连接版本不匹配 |
| `MISSING_FIELD` | 422 | 该版本下必填字段缺失/类型错误 |
| `TIMESTAMP_OUT_OF_RANGE` | 422 | 时间戳超出该版本窗口 |
| `TELEMETRY_DUPLICATE` | 409 | 同一 `(device_id, message_id)` 重复上报 |
| `COMMAND_NOT_FOUND` | 404 | 命令不存在 |
| `INVALID_COMMAND_TRANSITION` | 409 | 命令非法状态迁移 |

## 7. HTTP 接口摘要

```
POST   /devices/register                 {device_id, protocol_version?} -> {…, secret}
POST   /devices/{id}/connect             {secret, version?} -> {…, redelivered:[…]}
POST   /devices/{id}/disconnect
GET    /devices/{id}
POST   /devices/{id}/telemetry           {secret, message_id, ts?, metrics?, version?}
GET    /devices/{id}/telemetry           按 (ts, 摄入序号) 升序
POST   /devices/{id}/commands            {payload, ttl_seconds?}
GET    /commands/{cid}
POST   /commands/{cid}/ack
POST   /commands/{cid}/cancel
POST   /commands/{cid}/timeout
GET    /health
```

## 8. 本地验证

```bash
uv sync
uv run pytest -q                     # 32 个单测（含并发/乱序/版本不匹配）
uv run python -m uvicorn main:app --port 8000
```

单测运行时网关日志会打印**关键输入与判定依据**（消息 id、时间戳偏差、版本、
状态迁移与拒绝原因），便于逐条解释结论。

手工冒烟示例：

```bash
curl -s -X POST localhost:8000/devices/register \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"demo","protocol_version":"v2"}'
```
