# IoT 设备接入与命令下发网关（本地可运行）

单进程 Python 网关，用内存状态模拟设备、遥测与命令的完整状态流转。重点保证：
并发上报不重复计数、离线命令 FIFO 补发不重复执行、协议版本缺省语义确定且不静默降级，
所有失败均有**稳定可区分的错误码**。

## 快速开始

```bash
uv sync
uv run python -m uvicorn main:app --port 8000
# 交互文档： http://localhost:8000/docs
```

端到端冒烟：

```bash
# 1. 注册（返回 secret）
SECRET=$(curl -s -X POST localhost:8000/devices/register \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"demo","protocol_version":"v2"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["secret"])')

# 2. 离线状态下发两条命令 -> QUEUED
curl -s -X POST localhost:8000/devices/demo/commands -H 'Content-Type: application/json' -d '{"payload":"A"}'
curl -s -X POST localhost:8000/devices/demo/commands -H 'Content-Type: application/json' -d '{"payload":"B"}'

# 3. 连接（重连补发，返回 redelivered=[A,B]，每条恰好一次）
curl -s -X POST localhost:8000/devices/demo/connect \
  -H 'Content-Type: application/json' -d "{\"secret\":\"$SECRET\",\"version\":\"v2\"}"
```

## 测试

```bash
uv run pytest -q          # 并发上报 / 重复确认 / 乱序 / 版本不匹配等 33 个用例
uv run ruff check gateway tests main.py
```

## 文档

- [docs/DESIGN.md](docs/DESIGN.md)：设备与命令状态机、遥测幂等与摄入顺序、
  版本兼容规则、错误码表、接口清单与本地验证方法。
