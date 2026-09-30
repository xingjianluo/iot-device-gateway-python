# IoT Device Gateway (Python)

本地可完整运行的设备接入与命令下发网关：内存存储 + 可选 JSON 本地持久化，
模拟设备、遥测与命令的完整状态流转，保证并发上报、离线重连与协议版本
差异下结论一致且可解释。

## 结构

```
gateway/
  models.py     # 类型定义：Device / Command / ExecutionResult / 队列状态 / 枚举
  protocol.py   # 协议版本兼容规则（含结果上报能力版本，纯函数可复现）
  store.py      # 线程安全内存存储 + JSON 持久化（全局锁 + 每设备锁，队尾重排）
  devices.py    # 注册、鉴权、上下线、每设备队列/重试策略配置
  telemetry.py  # 遥测摄入：幂等、时间戳窗口、乱序容忍
  commands.py   # 命令状态机、离线队列、按序补发、执行结果、失败重试、超时/取消
  service.py    # Gateway 门面（可注入时钟，便于确定性测试）
main.py         # FastAPI HTTP 前端（结果上报 / 队列查询 / 策略配置端点）
tests/          # pytest：结果交错、重试到顶、队列边界、新老协议混用、持久化
```

## 状态机

### 设备

```
OFFLINE --connect--> ONLINE --disconnect--> OFFLINE
```

`connect` 会触发离线队列的按序补发（FIFO）。

### 命令

**v1.0 客户端（不能上报执行结果）：**

```
QUEUED --deliver--> SENT --ack--> ACKED                  (终态)
  |                  |
  |cancel            +--expire(超过 ack_deadline)--> TIMED_OUT  (终态)
  |                  |
  v                  +--cancel--> CANCELLED                     (终态)
CANCELLED (终态)
```

**v2.0 客户端（可上报执行结果，失败自动重试）：**

```
QUEUED --deliver--> SENT --ack--> ACKED --result 成功--------> SUCCEEDED (终态)
  |                  |              |
  |cancel            |cancel        +--result 失败, 仍有重试额--> QUEUED(排到队尾重试)
  |                  |              |
  v                  v              +--result 失败, 重试次数到顶--> FAILED   (终态)
CANCELLED         CANCELLED
                   SENT --expire(超过 ack_deadline)--> TIMED_OUT (终态)
```

- 设备离线时 `dispatch` 进入 `QUEUED`；重连后 `flush` 按 FIFO 补发，
  每条命令只投递一次，**不会重复执行**。
- `ack` 幂等：对已 `ACKED` 的命令重复确认返回 `duplicate=True`，状态不变；
  对 `TIMED_OUT`/`CANCELLED`/未知命令的确认被拒绝并给出可区分原因。
- **执行结果上报**（v2.0，`report_result` / `.../result`）：
  - 只有在 `ACKED` 状态才接受结果并落终态（`SUCCEEDED` / 失败重试 / `FAILED`）；
  - 尚未确认（`QUEUED`/`SENT`）就报结果 → `result_not_acked`；
  - 已超时后报结果 → `result_after_timeout`；已取消后报结果 →
    `result_after_cancelled`。三种原因互不相同；
  - 同一次执行（相同 `attempt` + 成败 + `error_code`）重复上报只记一次：
    返回 `duplicate=True`，**绝不改变已定结论、绝不触发第二次重试**；
  - 与已定结论矛盾的结果（如已成功又报失败、已 `FAILED` 又报成功）→
    `invalid_state`，先到的结论生效；
  - 失败结果必须带非空 `error_code`，`duration` 为非负数；
    畸形请求 → `invalid_result`，不产生任何状态变更。
- 确认 / 结果 / 取消 / 超时在网络抖动下**到达顺序不确定**，但同一设备的
  所有生命周期操作都在每设备锁内串行，且结果只能从 `ACKED` 落定，因此
  **一条命令最终恰好落到一个终态**，不会因为谁先到得出矛盾结论
  （测试用全排列穷举了四种动作的所有到达顺序）。
- `cancel` 仅允许从 `QUEUED`/`SENT` 迁移；v2 已 `ACKED`（执行中）的命令
  不再允许取消，终态上的操作返回 `invalid_state`。
- 超时由注入时钟驱动的 `expire()` 显式推进（无后台线程），判定确定、可复现；
  超时只作用于 `SENT`，已 `ACKED` 的 v2 命令不会被超时抢占。

## 执行结果与自动重试

- v2.0 设备上报结果字段：`success`（必填布尔）、`duration`（执行耗时秒，
  非负）、`error_code`（失败时必填）、`attempt`（执行序号，缺省取 1）。
- 失败命令按**每台设备的策略**自动重排重试：
  - 注册时可用 `max_retries`（默认 3）、`retry_backoff`（默认 0 秒）配置，
    也可用 `PUT /devices/{id}/policy` 在线调整；
  - 重试沿用**同一个 `command_id`**，但 `attempt` 从 1 递增，可与初次执行
    区分；总投递次数 = `1 + max_retries`（`max_retries=0` 表示失败即终态）；
  - 重试命令被重新排到该设备 FIFO 队列的**队尾**：先让已在等待的命令补发，
    顺序可解释，也不会覆盖尚未处理完的命令；
  - `retry_backoff > 0` 时重试带 `not_before`，到期前的补发会跳过它但保留
    其队列位置；设备在线且退避已过时，失败结果处理完会立即重投；
  - 重试次数到顶后落到稳定可区分的终态 `FAILED`，保留最终 `error_code` 与
    完整 `failed_attempts` 列表；之后任何结果都不能推翻它。
- 结果上报与重试均幂等：设备重发同一 `attempt` 的同一结果只生效一次。

## 离线队列容量

- 每台设备的离线队列容量由 `max_queue_size` 配置（默认 100）。容量只约束
  **处于 `QUEUED` 的等待命令**：
  - 离线 `dispatch` 时若队列已满 → 明确拒绝 `queue_full`（与
    `unknown_device`、`invalid_state` 等原因可区分），**排队中的命令绝不丢弃**；
  - 在线设备的命令立即投递、不占用队列，因此容量为 0 的在线设备仍可受控；
  - 内部失败重试的重新排队**不受容量限制**（重试绝不丢），满队列只拒绝新下发。
- 占用查询：`GET /devices/{id}/queue` 返回
  `queued`（等待数）/ `capacity`（容量）/ `in_flight`（已投递未落定：
  SENT+ACKED）/ `full`；`GET /devices/{id}/state` 也包含该信息及每条命令的
  `attempt`、`failed_attempts`、最终结果。
- 缩小容量不会删除已排队命令，只会让新下发被拒，直到队列自然排空。
- 容量上限、失败重试重排、重连补发同时发生时仍保证**不丢、不重、
  顺序可解释**（见 `tests/test_retries.py`、`tests/test_queue.py`）。

## 协议版本兼容

- 支持版本：`1.0`、`2.0`（`SUPPORTED_VERSIONS`）。
- 未知版本（如 `0.9`/`3.0`）**明确拒绝** `unsupported_version`，绝不静默降级，
  也不会写入任何状态。
- **执行结果能力范围**：仅 `2.0`（`RESULT_CAPABLE_VERSIONS`）支持上报执行结果。
  - v2.0：`ACKED` 是“已确认、执行中”的中间态，等待结果；失败自动重试，
    终态为 `SUCCEEDED` / `FAILED` / `TIMED_OUT` / `CANCELLED`。
  - v1.0：客户端不会、也不能上报结果；其**确定的缺省语义**是 `ACKED` 即为
    终态结论（收到即视为完成），**不触发任何重试**。v1.0 设备若上报结果，
    一律以 `unsupported_version` 拒绝且命令保持 `ACKED`。
  - 判定依据是设备注册时记录的协议版本，因此新老设备混用时同一种情况不会
    出现两种判定。
- 缺省语义（遥测，确定且稳定）：
  - `1.0`：缺省 `seq=0`、`qos=0`；
  - `2.0`：必须提供 `seq`，缺省 `qos=0`。
- 归一化是 `(version, message)` 的纯函数，规则按**消息声明的版本**而非
  网关当前状态判定——规则升级后，对历史消息重新判定结果仍可复现。

## 幂等规则

- 遥测按 `(device_id, message_id)` 去重：重复上报返回
  `duplicate=True` + `duplicate_telemetry`，**绝不重复计数**；
  检查-插入在每设备锁内完成，64 线程并发同一 message_id 仅 1 条被接受。
- 乱序时间戳：落在窗口 `[now-24h, now+5min]` 内即接受并缓冲；
  超出窗口拒绝 `timestamp_out_of_range`。
- 命令确认按 `command_id` 幂等（见上）。
- 执行结果按 `(command_id, attempt, success, error_code)` 幂等：重复上报同一
  执行结论只记一次，失败重试也只按“不同的 attempt”计数（见上）。

## 可区分的失败原因

`unknown_device` / `invalid_credentials` / `timestamp_out_of_range` /
`duplicate_telemetry` / `unsupported_version` / `unknown_command` /
`invalid_state` / `result_not_acked` / `result_after_timeout` /
`result_after_cancelled` / `invalid_result` / `queue_full`

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q -s     # 运行测试；-s 可看到关键输入与判定依据日志
uv run ruff check .     # 代码风格检查
uv run ruff format --check .
uv run uvicorn main:app # 启动 HTTP 服务，文档见 http://127.0.0.1:8000/docs
```

测试覆盖的新场景（关键输入与判定在 `-s` 日志中直接可见）：

- `tests/test_results.py`：成功/失败结果落定、结果幂等与结论不可变、
  ack/结果/取消/超时**全排列交错**到达、多线程并发交错、畸形结果拒绝；
- `tests/test_retries.py`：重试次数到顶落 `FAILED`、重试与初次可区分、
  队尾重排 FIFO、退避等待、重试不受容量限制、v1 缺省语义与新老混用；
- `tests/test_queue.py`：容量边界拒绝、占用查询、在线/离线容量语义、
  在线调策略不丢命令、重试与结果状态跨重启持久化。

HTTP 示例：

```bash
# 注册一台 v2 设备，离线队列容量 2、失败最多重试 1 次
curl -X POST localhost:8000/devices -H 'content-type: application/json' \
  -d '{"device_id":"dev2","secret":"pw","protocol_version":"2.0",
       "max_queue_size":2,"max_retries":1}'
curl -X POST localhost:8000/devices/dev2/connect
curl -X POST localhost:8000/devices/dev2/commands \
  -H 'content-type: application/json' -d '{"name":"reboot"}'
#   -> {"dispatched":true,"command_id":"cmd-1","status":"sent"}
CID=cmd-1   # 用上一步返回的 command_id
curl -X POST localhost:8000/devices/dev2/commands/$CID/ack \
  -H 'content-type: application/json' -d '{"secret":"pw"}'
# 上报失败结果（触发 1 次重试，attempt 递增为 2）
curl -X POST localhost:8000/devices/dev2/commands/$CID/result \
  -H 'content-type: application/json' \
  -d '{"secret":"pw","success":false,"duration":0.5,
       "error_code":"EIO","attempt":1}'
# 再次确认后上报成功，命令落 succeeded
curl -X POST localhost:8000/devices/dev2/commands/$CID/result \
  -H 'content-type: application/json' \
  -d '{"secret":"pw","success":true,"duration":1.0,"attempt":2}'
curl localhost:8000/devices/dev2/queue     # queued/capacity/in_flight/full
curl -X PUT localhost:8000/devices/dev2/policy \
  -H 'content-type: application/json' -d '{"max_queue_size":10}'
curl localhost:8000/devices/dev2/state     # 含 attempt/failed_attempts/result
```

HTTP 服务默认将状态持久化到 `gateway_state.json`（重启后状态、去重集合、
离线队列均保留）；测试使用临时路径与注入时钟，互不干扰。
