# IoT Device Gateway (Python)

本地可完整运行的设备接入与命令下发网关：内存存储 + 可选 JSON 本地持久化，
模拟设备、遥测与命令的完整状态流转，保证并发上报、离线重连与协议版本
差异下结论一致且可解释。

## 结构

```
gateway/
  models.py     # 类型定义：Device / TelemetryRecord / Command / AttemptResult /
                #   结果与队列状态 / 拒绝原因枚举
  protocol.py   # 协议版本兼容规则（纯函数，可复现；结果上报能力版本集合）
  store.py      # 线程安全内存存储 + JSON 持久化（全局锁 + 每设备锁）、
                #   队列占用查询与失败重排（requeue 到队尾）
  devices.py    # 注册、鉴权、上下线、每设备重试/队列容量策略
  telemetry.py  # 遥测摄入：幂等、时间戳窗口、乱序容忍
  commands.py   # 命令状态机、执行结果、失败重排重试、离线队列容量、
                #   按序补发、超时/取消
  service.py    # Gateway 门面（可注入时钟，便于确定性测试）
main.py         # FastAPI HTTP 前端（含 /result、/queue、/policy、/flush）
tests/          # pytest：结果乱序/并发交错、重试到顶、队列容量边界、
                #   新老协议混用、并发、重复确认、持久化
```

## 状态机

### 设备

```
OFFLINE --connect--> ONLINE --disconnect--> OFFLINE
```

`connect` 会触发离线队列的按序补发（FIFO）。

### 命令

```
QUEUED --deliver--> SENT --ack--> ACKED
  |                  |              |
  |cancel            |cancel        +--v2.0 上报成功--> SUCCEEDED            (终态)
  v                  v              |
CANCELLED (终态)   CANCELLED (终态) +--v2.0 上报失败、仍有重试名额
                                   |      --> QUEUED(队尾) --> SENT ...（attempt+1）
                                   +--v2.0 上报失败、重试用尽
                                   |      --> FAILED / RETRIES_EXHAUSTED   (终态)
                                   +--cancel--> CANCELLED                  (终态)
                   SENT --expire(超过 ack_deadline)--> TIMED_OUT           (终态)
```

- 设备离线时 `dispatch` 进入 `QUEUED`；重连后 `flush` 按 FIFO 补发，
  每次 `QUEUED->SENT` 投递都会把 `attempt` 加一（首次=1，第 n 次重试=n），
  因此**每条命令每次投递最多执行一次，重试与首次可区分**。
- `ack` 幂等且**按协议版本区分语义**：重复确认返回 `duplicate=True`，状态不变；
  对 `QUEUED`/终态命令的确认返回 `INVALID_STATE`。
  - **v1.0 客户端**：`ACKED` 就是它的终态（“已送达，结果未知”），
    网关绝不替它推断成功/失败，也不会重试；竞争中的取消也无法再推翻它。
  - **v2.0 客户端**：`ACKED` 只是“已收到、执行中”的中间态。
- `report_result`（**仅 v2.0**）每次上报**必须携带投递时收到的轮次号
  `attempt`**，结果按“属于哪一轮投递”归属，而不是按命令当前所处的状态归属
  （详见下一节“重试轮次与结果归属”）。只有**当前正处于 `ACKED` 的那一轮**
  才能引入新结论。网络抖动导致 ack/结果/取消/超时乱序、旧结果延迟重传时，
  每种情况都有**可互相区分的判定**：
  - **已记录过的轮次再次上报（无论多晚、处于什么阶段到达）**：
    结论相同（success+error_code 相同）→ `accepted=true, duplicate=true`，
    只记一次、状态不移动（不重排、不重试、不改变既定结论）；
    结论不同 → `conflicting_result` 拒绝，已记录结论不被顶替。
  - 上报了一个**更老、已被取代且当时没有留下记录**的轮次 →
    `stale_result_attempt`（该轮已结束，迟到的结果不能影响当前轮）。
  - 上报了一个**尚未投递、根本不存在**的轮次（如轮次号偏大、或 0）→
    `unknown_result_attempt` / `invalid_state`，绝不误记成当前轮结果。
  - 当前轮在未确认前（`QUEUED`/`SENT`）报结果 → `result_before_ack`；
    超时后报结果 → `result_after_timeout`；取消后报结果 →
    `result_after_cancel`；v1.0 客户端报结果 →
    `result_unsupported_version`。
  - 同一设备的所有生命周期动作由每设备锁串行化，因此无论动作以什么顺序到达，
    一条命令最终只落到**一个**终态，不会出现互相矛盾的判定
    （见 `tests/test_results.py` 的交错/并发用例与
    `tests/test_result_ordering.py` 的旧结果重传/乱序用例）。
- `cancel` 允许从 `QUEUED`/`SENT` 迁移；v2.0 在 `ACKED`（执行中）阶段也可取消，
  v1.0 的 `ACKED` 终态上取消返回 `INVALID_STATE`。
- 超时由注入时钟驱动的 `expire()` 显式推进（无后台线程），判定确定、可复现。

## 执行结果与自动重试

- 结果包含：成功/失败、失败时的 `error_code`（失败必填）、本次执行耗时
  `duration_ms`。每次投递的结果以 `attempt` 编号追加保存（append-only）。
- 失败后是否自动重排，由**每台设备**的 `max_retries` 策略决定（默认 0=不重试）：
  - 仍有名额（`attempt <= max_retries`）：命令以**同一 command_id** 重排到
    FIFO 队尾，不新增队列条目、不覆盖任何其它在途/排队命令；下次 `flush`
    （重连或显式调用）按序补发，`attempt` 加 1。
  - 用尽名额：稳定落到 `RETRIES_EXHAUSTED`（配置过重试）或 `FAILED`
    （未配置重试），二者可区分且均为终态。
  - 成功：终态 `SUCCEEDED`。重试用尽前成功即提前结束。
- 重排豁免容量准入（命令已被接纳），因此“容量满 + 重试 + 重连补发”同时发生时
  仍然**不丢、不重、顺序可解释**（见
  `tests/test_retries.py::test_capacity_full_and_retry_interplay_keeps_order_no_loss`）。

## 重试轮次与结果归属（重复上报 / 乱序到达判定）

每条命令的每次投递（`QUEUED->SENT`）是一个**轮次**，`attempt` 从 1 起递增，
设备在结果上报中原样回传该轮次号。网关对结果的第一判定键是
`(command_id, attempt)`，其次才是命令当前的生命周期状态：

1. **该轮次已有记录**（重复上报）：它就是那一次执行的同一个结果，
   **无论晚多久、在 `QUEUED`/`SENT`/`ACKED`/任一终态哪个阶段到达**——
   - 结论相同 → 幂等重复（`duplicate=true`），不追加记录、不重排、不重试、
     不改写任何已定结论；
   - 结论不同 → `conflicting_result` 拒绝，旧结论与当前轮都不被顶替。
   这正是修复的核心：第 1 轮失败已记录并重试后，网络重传的那份**旧失败**
   永远属于第 1 轮，不会在第 2 轮 `ACKED` 时被当成第 2 轮的新失败，
   因而既不会多排一次重试/多执行一遍，也不会把命令提前判成
   `RETRIES_EXHAUSTED`；第 2 轮真正的成功结果仍照常收下并判定为
   `SUCCEEDED`（`max_retries=1` 时同样成立）。
2. **轮次号更老、但该轮没有任何记录**（`attempt < 当前轮`）：该轮已被
   取代且已结束，迟到结果判 `stale_result_attempt`，只观察、不改状态。
   正常失败重试流程中第 1 轮必然先留下失败记录（走第 1 条），此分支只用于
   内部恢复等防御路径。
3. **轮次号指向尚未投递的轮次**（`attempt > 当前轮`，或 `attempt<1`）：
   `unknown_result_attempt` / `invalid_state`，绝不误算成当前轮结果。
4. **轮次号等于当前轮**时再套用生命周期状态门：未确认 `result_before_ack`、
   超时后 `result_after_timeout`、取消后 `result_after_cancel`、
   v1.0 `result_unsupported_version`。

不变量：任何一条命令在任意时刻**只有一个明确结论**；同一次执行的同一个结果
**只算一次**；重复的旧结果**不吃掉重试名额、不多产生一次投递、不顶替当前轮
真正的结果**。每设备锁保证上述判定在并发/乱序下依然稳定。

## 离线队列容量

- 每台设备可配置 `queue_capacity`（`None`=不限，默认不限；可为 0）。
- 容量只统计 `QUEUED`（等待投递）的命令：
  - 离线设备队列满时，**新下发**命令被明确拒绝，原因 `queue_full`，
    与其它拒绝原因可区分；已排队命令**不会被悄悄丢弃**。
  - 在线设备的命令直接投递为 `SENT`，不占 `QUEUED` 名额；失败重排也不重新走准入。
  - 容量可随时调小到当前占用以下：已排队命令保留不丢，仅拒绝后续新下发。
- 占用查询：`GET /devices/{id}/queue` 返回 `queued / capacity / full`
  （门面：`gateway.queue_status(device_id)`）。
- 容量按设备独立，互不影响。

## 幂等规则

- 遥测按 `(device_id, message_id)` 去重：重复上报返回
  `duplicate=True` + `DUPLICATE_TELEMETRY`，**绝不重复计数**；
  检查-插入在每设备锁内完成，64 线程并发同一 message_id 仅 1 条被接受。
- 乱序时间戳：落在窗口 `[now-24h, now+5min]` 内即接受并缓冲；
  超出窗口拒绝 `TIMESTAMP_OUT_OF_RANGE`。
- 命令确认按 `command_id` 幂等（见上）；v2.0 的**执行结果**同样幂等：
  相同结论重复上报只记一次，矛盾结论被 `CONFLICTING_RESULT` 拒绝。

## 协议版本兼容

| 能力 | v1.0 | v2.0 |
| --- | --- | --- |
| 遥测默认字段 | `seq=0`、`qos=0` | 必须提供 `seq`，`qos` 默认 0 |
| 命令确认 `ack` | 终态 `ACKED`（已送达、结果未知，不重试） | 中间态 `ACKED`（执行中） |
| 上报执行结果 `report_result` | **不支持**，明确拒绝 `result_unsupported_version` | 支持：成功/失败/错误码/耗时 |
| 失败自动重试 | 不发生（无结果可判） | 按设备 `max_retries` 执行 |

- 支持版本：`1.0`、`2.0`（`SUPPORTED_VERSIONS`）。
- 未知版本（如 `0.9`/`3.0`）**明确拒绝** `UNSUPPORTED_VERSION`，绝不静默降级，
  也不会写入任何状态。
- 支持结果上报的版本集合是协议模块中的纯数据 `RESULT_REPORT_VERSIONS`
  （当前为 `2.0`）；`supports_result_report(version)` 是纯函数。
  新老设备混用同一套网关时，同一种情况只有一种判定：v1.0 的 `ACKED` 永远是
  终态，v2.0 才会进入成功/失败/重试流程。
- 归一化是 `(version, message)` 的纯函数，规则按**消息声明的版本**而非
  网关当前状态判定——规则升级后，对历史消息重新判定结果仍可复现。

## 可区分的失败原因

`unknown_device` / `invalid_credentials` / `timestamp_out_of_range` /
`duplicate_telemetry` / `unsupported_version` / `unknown_command` /
`invalid_state` / `result_unsupported_version` / `result_before_ack` /
`result_after_timeout` / `result_after_cancel` / `conflicting_result` /
`stale_result_attempt` / `unknown_result_attempt` / `queue_full`

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q -s     # 运行测试；-s 可看到关键输入与判定依据日志
uv run ruff check .     # 代码风格检查
uv run uvicorn main:app # 启动 HTTP 服务，文档见 http://127.0.0.1:8000/docs
```

新增/重点测试：

- `tests/test_results.py`：成功/失败结果、缺错误码、重复与矛盾结论、
  结果先于确认、超时/取消后报结果、v1.0 缺省语义，以及
  **24 ack + 24 结果 + 24 取消 + 8 超时并发交错**只落一个终态。
- `tests/test_retries.py`：attempt 递增、重连补发不重复、成功提前结束、
  到顶 `RETRIES_EXHAUSTED`、重排队尾顺序、容量×重试交织不丢、
  **旧轮次结果在重试 `QUEUED`/`ACKED` 阶段重传（含相同与矛盾结论）只算一次**、
  v1.0 即使配置了策略也不重试、策略持久化。
- `tests/test_result_ordering.py`（本次修复新增）：失败重排→重新下发并再次
  确认后旧失败才到达；只允许重试一次时被旧结果提前判死；旧结果与当前轮结果
  不一致；新一轮未确认前旧结果反复重传；上报未投递轮次/已关闭轮次。
- `tests/test_queue.py`：容量边界（含 0）、满/排空、按设备独立、占用查询、
  调小容量不丢命令。

HTTP 示例（结果上报 + 重试 + 容量）：

```bash
# v2.0 设备：最多重试 1 次，离线队列容量 2
curl -X POST localhost:8000/devices -H 'content-type: application/json' \
  -d '{"device_id":"dev-1","secret":"pw","protocol_version":"2.0",
       "max_retries":1,"queue_capacity":2}'
curl -X POST localhost:8000/devices/dev-1/connect
curl -X POST localhost:8000/devices/dev-1/commands \
  -H 'content-type: application/json' -d '{"name":"reboot"}'   # -> sent
CID=cmd-1
curl -X POST localhost:8000/devices/dev-1/commands/$CID/ack \
  -H 'content-type: application/json' -d '{"secret":"pw"}'
# 执行失败：重排重试（retried=true, terminal=false），该结果属于第 1 轮
curl -X POST localhost:8000/devices/dev-1/commands/$CID/result \
  -H 'content-type: application/json' \
  -d '{"secret":"pw","success":false,"duration_ms":120,"attempt":1,"error_code":"E_BUSY"}'
curl -X POST localhost:8000/devices/dev-1/flush                 # 补发 attempt=2
# 网络抖动：第 1 轮的旧失败此刻才重传到 -> duplicate=true，不重排、不改结论
curl -X POST localhost:8000/devices/dev-1/commands/$CID/result \
  -H 'content-type: application/json' \
  -d '{"secret":"pw","success":false,"duration_ms":120,"attempt":1,"error_code":"E_BUSY"}'
# 第 2 轮真正执行成功 -> accepted=true, terminal=true，命令落 SUCCEEDED
curl -X POST localhost:8000/devices/dev-1/commands/$CID/result \
  -H 'content-type: application/json' \
  -d '{"secret":"pw","success":true,"duration_ms":80,"attempt":2}'
curl localhost:8000/devices/dev-1/queue                         # queued/capacity/full
curl localhost:8000/devices/dev-1/state                         # 状态 + 每次结果

# 运行中调整策略（也可在注册时给出）
curl -X POST localhost:8000/devices/dev-1/policy \
  -H 'content-type: application/json' -d '{"max_retries":3,"queue_capacity":10}'
```

HTTP 服务默认将状态持久化到 `gateway_state.json`（重启后状态、去重集合、
离线队列均保留）；测试使用临时路径与注入时钟，互不干扰。

### 本地复现“旧结果迟到”问题并确认已修复

不依赖网络，用注入时钟在进程内模拟“失败重排 → 重新下发并确认 →
旧失败重传 → 当前轮真正成功”的乱序：

```bash
uv run python - << 'EOF'
from tests.conftest import FakeClock
from gateway.service import Gateway

g = Gateway(clock=FakeClock())           # max_retries=1：最容易被旧结果提前判死
g.register("d1", "s", protocol_version="2.0", max_retries=1)
g.connect("d1")
cmd, _ = g.dispatch("d1", "reboot"); cid = cmd.command_id

g.ack("d1", "s", cid)                                                 # 第 1 轮确认
print(g.report_result("d1", "s", cid, False, 100, attempt=1, error_code="E_BUSY"))  # 失败重排
g.flush("d1"); g.ack("d1", "s", cid")                                  # 补发第 2 轮并确认
stale = g.report_result("d1", "s", cid, False, 100, attempt=1, error_code="E_BUSY") # 旧失败迟到
print("stale:", stale.accepted, stale.duplicate, stale.retried)       # 期望 True/True/False
real = g.report_result("d1", "s", cid, True, 80, attempt=2)           # 第 2 轮真正成功
print("real: ", real.accepted, real.terminal)                         # 期望 True/True
print(g.store.get_command("d1", cid).status)                           # 期望 CommandStatus.SUCCEEDED
EOF
```

判定依据：旧结果用 `attempt=1` 上报，因第 1 轮已有**相同**记录而返回
`duplicate=true` 且 `retried=false`，命令仍是第 2 轮 `ACKED`；随后
`attempt=2` 的成功被正常接受，最终 `SUCCEEDED`、每轮结果各一条。
修复前同样的乱序会把旧失败当成第 2 轮新失败，命令被判
`RETRIES_EXHAUSTED`，随后的成功反被 `conflicting_result` 拒绝。
稳定回归用例见 `tests/test_result_ordering.py`（`uv run pytest -q -s`
可看到每一步的关键输入与判定依据）。
