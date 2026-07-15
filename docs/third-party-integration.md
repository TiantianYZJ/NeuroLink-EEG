# NeuroLink 第三方接入文档

## 概述

NeuroLink 平台通过 WebSocket 对外提供实时 EEG 数据流。第三方应用可以以**纯监视端**角色接入，无需硬件采集设备即可获取脑电波形、频带功率和指标数据。

- **服务器**: `wss://eeg.yzjtiantian.cn/ws`
- **协议**: JSON over WebSocket (RFC 6455)
- **采样率**: 标称 200 Hz（OpenBCI Ganglion 4 通道），实际蓝牙传输约 120–200 Hz，服务端实时测量并通过 `room_info.frame_rate` 广播
- **加速度计**: 3 轴 ±8g，需 OpenBCI GUI 额外配置 UDP Accel/Aux 输出到端口 **12347**（注意：12346 是 Marker 回传端口，勿混淆）

---

## 设备身份标识

每个连接在 `hello` 时必须通过 `device_info` 字段声明自己的身份。平台据此在房间设备列表中显示你的应用名称和类型。

```json
{
  "type": "hello",
  "role": "pending",
  "session_id": "my-app-1710400000",
  "device_info": {
    "platform": "MyApp",          // 应用名称（必填）
    "userAgent": "MyApp/1.0",     // 版本标识（必填）
    "nickname": "监视器-实验室A", // 自定义昵称（必填）
    "isBridge": false              // 是否为硬件桥接（第三方固定 false）
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|:----:|------|
| `platform` | string | ✅ | 应用/平台名称，如 `"PythonMonitor"`、`"AndroidApp"` |
| `userAgent` | string | ✅ | 版本号，如 `"v1.0.0"` |
| `nickname` | string | ✅ | 在房间设备列表中显示的自定义名称 |
| `isBridge` | boolean | ❌ | 固定传 `false`（仅硬件桥接脚本传 `true`） |

**注意**: 服务端会记录每台设备的 `nickname` 并在房间面板中展示。建议设为有辨识度的名称（如 `"Python数据分析-张三"`），方便实验员区分。

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

  if (msg.type === 'accel_frame') {
    // 7. 处理加速度计数据
    const { axes } = msg;
    console.log('±8g:', axes.map(v => v.toFixed(3)));
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
  │←─── eeg_frame(channels) ──────│  ← ~120-200 次/秒
  │←─── accel_frame(axes) ────────│  ← ~50 次/秒
  │←─── metrics_snapshot(...) ────│  ← 1 次/秒
  │←─── phase_sync(...) ──────────│  ← 1 次/秒
  │←─── marker(...) ──────────────│  ← 事件驱动
  │←─── room_info(occupants) ─────│  ← 每 2 秒 + 变更时
```

---

## EEG 数据帧 (`eeg_frame`)

采集设备（Ganglion 标称 200 Hz，实际蓝牙限速约 120–200 Hz）逐采样点上报，每帧包含 4 通道的瞬时微伏值。

**转发规则**: 仅 `master` 与 `monitor` 角色接收（`console` 不接收原始波形以节省带宽）。数据由硬件桥接脚本上行，服务端原样广播。

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

服务器每秒计算一次（基于 256 采样点滑动窗口 + Goertzel 算法），包含频带功率、复合指标和信号质量。

**转发规则**: `master`、`monitor`、`console` 三种角色均接收。注意：指标计算不受实验计时器 `running` 状态限制，即使未点"开始"也会持续输出（便于提前观察热力图）。

```json
{
  "type": "metrics_snapshot",
  "ts": 1710400000000,
  "session_id": "room-7550-xxxx",
  "phase_index": 1,
  "phase_id": "flow1",
  "sample_rate": 178,
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
  "cognitive_load_index": 0.19,
  "signal_quality": [2.15, 1.87, 3.02, 2.44]
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `sample_rate` | number | 服务端实测采样率（Hz），由实际帧计数推算，上限 500 |
| `band_power` | object | CH1 频带功率（向后兼容字段） |
| `band_power_per_ch` | number[4] | 4 通道独立频带功率，地形图数据源 |
| `theta_alpha_ratio` | number | θ/α 比值（基于 CH1） |
| `spectral_entropy` | number | 谱熵（基于 CH1 频带功率分布） |
| `cognitive_load_index` | number | 认知负载 P_γ / P_α（基于 CH1） |
| `signal_quality` | number[4] | 4 通道信号稳定性（32 样本窗口 RMS，单位 μV），越小越稳定 |

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

## 房间配置 (`room_config`)

控制台开始/结束实验时广播，通知所有角色房间锁定状态。

```json
{
  "type": "room_config",
  "locked": true,
  "config": {
    "template": "1",
    "switch_type": "math_art"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `locked` | bool | true=锁定(等待配置), false=已解锁(实验进行中) |
| `config` | object | 实验配置, null 或 {template, switch_type} |

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

需在 OpenBCI GUI 添加第二个 UDP 输出，Data Type 选 `"Accel/Aux"`，目标端口 **12347**（桥接脚本 `ACCEL_UDP_PORT` 默认值，可通过环境变量覆盖）。

**转发规则**: 仅 `master` 与 `monitor` 角色接收，服务端原样转发桥接脚本上行的 JSON 帧。上报频率约 50 Hz。

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
| 1-4 | EXG 通道 0-3 | µV | timeSeriesRaw JSON（端口 12345） | `eeg_frame.channels[0..3]` |
| 5-7 | 加速度 X/Y/Z | g | Accel/Aux 端口 12347 | `accel_frame.axes[0..2]` |
| 8-12 | 数字/模拟通道 | — | 不在 UDP 输出中 | — |
| 13 | 时间戳 | Unix µs | timeSeriesRaw (不包含) | `eeg_frame.ts`（由桥接添加） |
| 14 | 其他 | — | 不在 UDP 输出中 | — |

> **端口约定**: `12345` = EEG 数据 UDP（桥接监听）；`12346` = Marker 回传 UDP（OpenBCI GUI 接收，float32 big-endian）；`12347` = 加速度计 UDP（桥接监听）。

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

**转发规则**: `marker` 广播给房间内**所有角色**，并由服务端通过 UDP（float32 big-endian）回传给硬件桥接的 `master`，写入 OpenBCI GUI 的 Marker 列（端口 12346）。`source === 'master'` 的标记不回传，避免循环。

---

## 操作通知 (`action_notice`)

控制台/主控执行实验控制动作（开始、暂停、下一阶段、重置等）或受试者自评时，服务端向**所有角色**广播此通知，便于监视端同步显示操作日志。

```json
{
  "type": "action_notice",
  "action": "start",
  "actor": "实验员-张三",
  "label": "▶ 开始实验",
  "role": "console",
  "ts": 1710400000000
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `action` | string | 动作类型：`start`/`pause`/`next_phase`/`reset`/`set_auto`/`flow_enter`/`flow_exit`/`distracted` |
| `actor` | string | 操作者昵称 |
| `label` | string | 人类可读标签（含 emoji） |
| `role` | string | 操作者角色 |
| `ts` | number | Unix 毫秒时间戳 |

---

## 告警 (`alert`)

硬件数据源（非桥接的 `master`）断开时，服务端向房间内所有角色广播此告警，监视端可据此提示数据中断。

```json
{ "type": "alert", "level": "warning", "message": "数据源已断开" }
```

---

## 房间信息 (`room_info`)

连接 `hello` 后、加入/离开房间、角色认领/释放、设备断开时，以及每 2 秒定期广播一次。`occupants` 包含实时 `frame_rate`（服务端实测采样率，上限 500 Hz）和设备列表。

```json
{
  "type": "room_info",
  "occupants": {
    "frame_rate": 178,
    "master": true,
    "locked": false,
    "hasConsole": true,
    "monitor": 2,
    "subject": true,
    "console": 1,
    "bridge": 1,
    "devices": [
      { "role": "master", "nickname": "实验主控", "isBridge": false, "info": {} },
      { "role": "monitor", "nickname": "Python Monitor", "info": {} }
    ]
  }
}
```

---

## 消息转发总览

监视端（`monitor`）可接收的全部下行消息：

| 消息类型 | 频率 | 说明 |
|----------|------|------|
| `eeg_frame` | ~120-200 次/秒 | 4 通道脑电波形（仅 master/monitor） |
| `accel_frame` | ~50 次/秒 | 3 轴加速度（仅 master/monitor） |
| `metrics_snapshot` | 1 次/秒 | 频带功率/复合指标/信号质量（master/monitor/console） |
| `phase_sync` | 1 次/秒 | 实验阶段同步（全角色） |
| `room_info` | 每 2 秒 + 变更时 | 房间占用与实时采样率（全角色） |
| `room_config` | 事件驱动 | 房间锁定状态/实验配置（全角色） |
| `marker` | 事件驱动 | 实验标记（全角色） |
| `action_notice` | 事件驱动 | 操作动作通知（全角色） |
| `alert` | 事件驱动 | 数据源断开告警（全角色） |

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
    elif msg['type'] == 'accel_frame':
        print(f"accel X={msg['axes'][0]:.3f} Y={msg['axes'][1]:.3f} Z={msg['axes'][2]:.3f}")
    elif msg['type'] == 'metrics_snapshot':
        print(f"采样率={msg.get('sample_rate')}Hz θ/α={msg['theta_alpha_ratio']:.3f} "
              f"熵={msg['spectral_entropy']:.3f} 信号质量={msg.get('signal_quality')}")
    elif msg['type'] == 'action_notice':
        print(f"[操作] {msg.get('actor')} → {msg.get('label')}")
    elif msg['type'] == 'alert':
        print(f"[告警] {msg.get('message')}")
    elif msg['type'] == 'room_info':
        occ = msg.get('occupants', {})
        print(f"房间: master={occ.get('master')} monitor={occ.get('monitor')} 帧率={occ.get('frame_rate')}Hz")
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

1. **采样率**: Ganglion 标称 200 Hz，实际蓝牙限速约 120–200 Hz，每帧 1 个采样点、4 通道。实测值见 `room_info.frame_rate` 与 `metrics_snapshot.sample_rate`
2. **数据格式**: 微伏（μV），浮点数，OpenBCI GUI 输出 JSON 格式时的原始值
3. **端口约定**: `12345` EEG 数据 / `12346` Marker 回传 / `12347` 加速度计（均为桥接脚本侧）
4. **房间号**: 4 位数字，由创建者生成，24 小时有效
5. **监视端无数量限制**: 多个第三方客户端可以同时以 monitor 角色接入
6. **重连**: `hello` 后如果服务端返回 `role_denied(reconnect)`，5 秒后发送 `reconnect` 请求
7. **心跳**: 服务端每 30 秒 ping，客户端需响应 pong（多数 WebSocket 库自动处理），10 秒内未收到 pong 则终止连接
8. **指标持续输出**: `metrics_snapshot` 不受实验"开始/暂停"影响，接入后即可持续获取
