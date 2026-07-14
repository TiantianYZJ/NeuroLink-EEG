# NeuroLink 第三方接入文档

## 概述

NeuroLink 平台通过 WebSocket 对外提供实时 EEG 数据流。第三方应用可以以**纯监视端**角色接入，无需硬件采集设备即可获取脑电波形、频带功率和指标数据。

- **服务器**: `wss://eeg.yzjtiantian.cn/ws`
- **协议**: JSON over WebSocket (RFC 6455)
- **采样率**: ~120 Hz（Ganglion 4 通道蓝牙限速）
- **加速度计**: 3 轴 ±8g，需 OpenBCI GUI 额外配置 UDP Accel/Aux 输出到端口 12346

---

## 快速开始

```js
// Node.js 示例
const WebSocket = require('ws');
const ws = new WebSocket('wss://eeg.yzjtiantian.cn/ws');

// 1. 发送身份标识
ws.send(JSON.stringify({
  type: 'hello',
  role: 'pending',
  session_id: 'my-app-' + Date.now(),
  device_info: { platform: 'MyApp', userAgent: 'SDK v1.0', nickname: 'My Monitor' }
}));

// 2. 收到 room_info 后加入房间
ws.on('message', (raw) => {
  const msg = JSON.parse(raw);
  if (msg.type === 'room_info') {
    // 加入已有房间（4位数字房间号）
    ws.send(JSON.stringify({
      type: 'join_room',
      code: '7550',  // 替换为实际房间号
      session_id: 'my-app-' + Date.now()
    }));
  }

  if (msg.type === 'room_joined') {
    // 3. 认领监视端角色
    ws.send(JSON.stringify({
      type: 'claim_role',
      role: 'monitor',
      session_id: msg.session_id
    }));
  }

  if (msg.type === 'role_claimed' && msg.role === 'monitor') {
    // 4. 开始接收数据
    console.log('已连接为监视端');
  }

  if (msg.type === 'eeg_frame') {
    // 5. 处理脑电数据帧
    const { seq, channels, ts } = msg;
    // channels: [ch0, ch1, ch2, ch3]  单位 μV
    console.log(`采样 #${seq}:`, channels.map(v => v.toFixed(2)));
  }

  if (msg.type === 'metrics_snapshot') {
    // 6. 处理指标快照（每秒一次）
    const {
      band_power,        // { delta, theta, alpha, beta, gamma }
      theta_alpha_ratio, // 心流指标
      spectral_entropy,  // 谱熵
      cognitive_load_index, // 认知负载
    } = msg;
  }
});
```

### 连接流程

```
Client                          Server
  │                               │
  │──── hello({session_id}) ──────→│
  │←─── room_info(occupants) ─────│
  │──── join_room({code}) ───────→│
  │←─── room_joined(session_id) ──│
  │──── claim_role(monitor) ─────→│
  │←─── role_claimed(monitor) ────│
  │←─── eeg_frame(channels) ──────│  ← 200次/秒
  │←─── metrics_snapshot(...) ────│  ← 1次/秒
```

---

## EEG 数据帧 (`eeg_frame`)

采集设备以 ~120 Hz（Ganglion 实际）采样，每帧包含 4 通道的瞬时微伏值。

```json
{
  "type": "eeg_frame",
  "seq": 12345,
  "channels": [1.234, -0.567, 0.891, -2.345],
  "ts": 1710400000000
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | string | 固定为 `"eeg_frame"` |
| `seq` | number | 采样序号（单调递增，可用来检测丢包） |
| `channels` | number[] | 4 通道微伏值 `[CH1, CH2, CH3, CH4]` |
| `ts` | number | Unix 毫秒时间戳 |

**通道对应关系（OpenBCI Ganglion）**:
| 索引 | 电极位置 | 脑区 |
|------|----------|------|
| 0 | Fp1 | 左前额 |
| 1 | Fp2 | 右前额 |
| 2 | C3 | 左中央 |
| 3 | C4 | 右中央 |

---

## 指标快照 (`metrics_snapshot`)

服务器每秒计算一次，包含频带功率和复合指标。

```json
{
  "type": "metrics_snapshot",
  "ts": 1710400000000,
  "session_id": "room-7550-xxxx",
  "phase_index": 1,
  "phase_id": "flow1",
  "band_power": {
    "delta": 12.34,
    "theta": 8.91,
    "alpha": 5.67,
    "beta": 3.21,
    "gamma": 1.09
  },
  "band_power_per_ch": [
    { "delta": 12.34, "theta": 8.91, "alpha": 5.67, "beta": 3.21, "gamma": 1.09 },
    { "delta": 11.22, "theta": 7.65, "alpha": 4.32, "beta": 2.10, "gamma": 0.87 },
    { "delta": 9.87, "theta": 6.54, "alpha": 3.21, "beta": 1.98, "gamma": 0.76 },
    { "delta": 10.11, "theta": 7.23, "alpha": 4.56, "beta": 2.34, "gamma": 0.95 }
  ],
  "theta_alpha_ratio": 1.57,
  "spectral_entropy": 2.34,
  "cognitive_load_index": 0.19
}
```

### 频带定义

| 频带 | 频率范围 | 说明 |
|------|----------|------|
| δ (delta) | 0.5–4 Hz | 深睡眠 |
| θ (theta) | 4–8 Hz | 放松/冥想 |
| α (alpha) | 8–13 Hz | 放松闭眼 |
| β (beta) | 13–32 Hz | 专注/思考 |
| γ (gamma) | 32–100 Hz | 高级认知 |

### 复合指标

| 指标 | 公式 | 说明 |
|------|------|------|
| θ/α | P_θ / P_α | 比值稳定→心流稳态 |
| 谱熵 | -Σ(p_i·log₂(p_i)) | 频带分布不确定性 |
| 认知负载 | P_γ / P_α | ↑ 增大 = 负载增高 |

---

## 实验阶段同步 (`phase_sync`)

```json
{
  "type": "phase_sync",
  "phase_index": 1,
  "phase_id": "flow1",
  "phase_name": "心流诱导阶段",
  "round": 1,
  "time_left": 450,
  "time_in_phase": 30,
  "phase_duration": 480,
  "running": true,
  "auto_mode": true,
  "completed": false,
  "task_type": "math"
}
```

---

## 加速度计帧 (`accel_frame`)

需在 OpenBCI GUI 添加第二个 UDP 输出，Data Type 选 `"Accel/Aux"`，目标端口 `12346`。

```json
{
  "type": "accel_frame",
  "axes": [0.12, -0.05, 0.98],
  "ts": 1710400000000
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | string | 固定为 `"accel_frame"` |
| `axes` | number[] | `[X, Y, Z]`，单位 g（±8g 范围） |
| `ts` | number | Unix 毫秒时间戳 |

---

## 原始字段映射

OpenBCI Ganglion 通过 UDP 输出的完整列映射：

| 列 | 字段 | 单位 | UDP 来源 | WSS 消息 |
|:---|:---|:---:|:---|:---|
| 0 | 包索引 (0-99) | — | timeSeriesRaw (不包含) | `eeg_frame.seq`（由桥接生成） |
| 1-4 | EXG 通道 0-3 | µV | timeSeriesRaw JSON | `eeg_frame.channels[0..3]` |
| 5-7 | 加速度 X/Y/Z | g | Accel/Aux 端口 12346 | `accel_frame.axes[0..2]` |
| 8-12 | 数字/模拟通道 | — | 不在 UDP 输出中 | — |
| 13 | 时间戳 | Unix µs | timeSeriesRaw (不包含) | `eeg_frame.ts`（由桥接添加） |
| 14 | 其他 | — | 不在 UDP 输出中 | — |

---

## 标记 (`marker`)

```json
{
  "type": "marker",
  "code": 3,
  "source": "operator",
  "label": "心流进入",
  "phase": "flow1",
  "ts": 1710400000000
}
```

预定义 code:
| Code | 含义 |
|------|------|
| 1 | 阶段开始 |
| 2 | 阶段结束 |
| 3 | 心流进入（操作员） |
| 4 | 心流脱离（操作员） |
| 5 | 外部干扰 |
| 6 | 自定义 |
| 7 | 心流进入（受试者自评） |
| 8 | 心流脱离（受试者自评） |
| 9 | 分心（受试者自评） |

---

## Python 接入示例

```python
import json
import websocket
import threading

def on_message(ws, raw):
    msg = json.loads(raw)
    if msg['type'] == 'eeg_frame':
        ch = msg['channels']
        print(f"CH1={ch[0]:.2f} CH2={ch[1]:.2f} CH3={ch[2]:.2f} CH4={ch[3]:.2f}")
    elif msg['type'] == 'metrics_snapshot':
        bp = msg['band_power']
        print(f"θ/α={msg['theta_alpha_ratio']:.3f} 熵={msg['spectral_entropy']:.3f}")
    elif msg['type'] == 'room_info':
        ws.send(json.dumps({"type": "join_room", "code": "7550", "session_id": sid}))
    elif msg['type'] == 'room_joined':
        ws.send(json.dumps({"type": "claim_role", "role": "monitor", "session_id": msg['session_id']}))
    elif msg['type'] == 'role_claimed':
        print("监视端就绪")

sid = 'py-monitor-' + str(int(__import__('time').time()))
ws = websocket.WebSocketApp('wss://eeg.yzjtiantian.cn/ws',
    on_message=on_message,
    on_open=lambda ws: ws.send(json.dumps({
        "type": "hello", "role": "pending", "session_id": sid,
        "device_info": {"platform": "Python", "userAgent": "websocket-client", "nickname": "Python Monitor"}
    })))
ws.run_forever()
```

---

## 注意事项

1. **采样率**: Ganglion 固定 200 Hz，每帧 1 个采样点，4 通道
2. **数据格式**: 微伏（μV），浮点数，openBCI GUI 输出 JSON 格式时的原始值
3. **房间号**: 4 位数字，由创建者生成，24 小时有效
4. **监视端无数量限制**: 多个第三方客户端可以同时以 monitor 角色接入
5. **重连**: `hello` 后如果服务端返回 `role_denied(reconnect)`，5 秒后发送 `reconnect` 请求
6. **心跳**: 服务端每 30 秒 ping，客户端无需额外心跳
