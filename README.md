# IoT Device Gateway (Python)

本地可完整运行的设备接入与命令下发网关：内存存储 + 可选 JSON 本地持久化，
模拟设备、遥测与命令的完整状态流转，保证并发上报、离线重连与协议版本
差异下结论一致且可解释。

## 结构

```
gateway/
  models.py     # 类型定义：Device / TelemetryRecord / Command / 拒绝原因枚举
  protocol.py   # 协议版本兼容规则（纯函数，可复现）
  store.py      # 线程安全内存存储 + JSON 持久化（全局锁 + 每设备锁）
  devices.py    # 注册、鉴权、上下线
  telemetry.py  # 遥测摄入：幂等、时间戳窗口、乱序容忍
  commands.py   # 命令状态机、离线队列、按序补发、超时/取消
  service.py    # Gateway 门面（可注入时钟，便于确定性测试）
main.py         # FastAPI HTTP 前端
tests/          # pytest：并发、重复确认、乱序、版本不匹配、持久化
```

## 状态机

### 设备

```
OFFLINE --connect--> ONLINE --disconnect--> OFFLINE
```

`connect` 会触发离线队列的按序补发（FIFO）。

### 命令

```
QUEUED --deliver--> SENT --ack--> ACKED            (终态)
  |                  |
  |cancel            +--expire(超过 ack_deadline)--> TIMED_OUT  (终态)
  |                  |
  v                  +--cancel--> CANCELLED                     (终态)
CANCELLED (终态)
```

- 设备离线时 `dispatch` 进入 `QUEUED`；重连后 `flush` 按 FIFO 补发，
  每条命令只投递一次，**不会重复执行**。
- `ack` 幂等：对已 `ACKED` 的命令重复确认返回 `duplicate=True`，状态不变；
  对 `TIMED_OUT`/`CANCELLED`/未知命令的确认被拒绝并给出可区分原因。
- `cancel` 仅允许从 `QUEUED`/`SENT` 迁移；终态上的操作返回 `INVALID_STATE`。
- 超时由注入时钟驱动的 `expire()` 显式推进（无后台线程），判定确定、可复现。
- 同一设备的所有生命周期操作由每设备锁串行化，并发操作不会互相覆盖。

## 幂等规则

- 遥测按 `(device_id, message_id)` 去重：重复上报返回
  `duplicate=True` + `DUPLICATE_TELEMETRY`，**绝不重复计数**；
  检查-插入在每设备锁内完成，64 线程并发同一 message_id 仅 1 条被接受。
- 乱序时间戳：落在窗口 `[now-24h, now+5min]` 内即接受并缓冲；
  超出窗口拒绝 `TIMESTAMP_OUT_OF_RANGE`。
- 命令确认按 `command_id` 幂等（见上）。

## 协议版本兼容

- 支持版本：`1.0`、`2.0`（`SUPPORTED_VERSIONS`）。
- 未知版本（如 `0.9`/`3.0`）**明确拒绝** `UNSUPPORTED_VERSION`，绝不静默降级，
  也不会写入任何状态。
- 缺省语义（确定且稳定）：
  - `1.0`：缺省 `seq=0`、`qos=0`；
  - `2.0`：必须提供 `seq`，缺省 `qos=0`。
- 归一化是 `(version, message)` 的纯函数，规则按**消息声明的版本**而非
  网关当前状态判定——规则升级后，对历史消息重新判定结果仍可复现。

## 可区分的失败原因

`unknown_device` / `invalid_credentials` / `timestamp_out_of_range` /
`duplicate_telemetry` / `unsupported_version` / `unknown_command` /
`invalid_state`

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q -s     # 运行测试；-s 可看到关键输入与判定依据日志
uv run uvicorn main:app # 启动 HTTP 服务，文档见 http://127.0.0.1:8000/docs
```

HTTP 示例：

```bash
curl -X POST localhost:8000/devices -H 'content-type: application/json' \
  -d '{"device_id":"dev-1","secret":"pw","protocol_version":"2.0"}'
curl -X POST localhost:8000/devices/dev-1/commands \
  -H 'content-type: application/json' -d '{"name":"reboot"}'   # 离线 -> queued
curl -X POST localhost:8000/devices/dev-1/connect              # 重连 -> 按序补发
curl localhost:8000/devices/dev-1/state                        # 查看状态
```

HTTP 服务默认将状态持久化到 `gateway_state.json`（重启后状态、去重集合、
离线队列均保留）；测试使用临时路径与注入时钟，互不干扰。
