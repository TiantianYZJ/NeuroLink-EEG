# EEG 心流实验平台 · 架构设计

## 1. 概述

### 1.1 目的

将现有单机 EEG 监视面板重构为基于云端的三角色实时协作平台（上传监视端 / 纯监视端 / 受试者端），支撑 PLAN.md 定义的跨学科心流实验范式。

### 1.2 设计原则

- **需求决定设计**：所有功能取舍以 PLAN.md 实验方案为唯一依据
- **YAGNI**：不做实验不需要的功能
- **实时优先**：所有端（主控/监视/受试者）通过 WebSocket 实时同步
- **轻后端**：ECS 仅做消息中继、状态仲裁、指标聚合、问卷存储；原始 EEG 数据由 OpenBCI GUI 本地 CSV 录制

### 1.3 设备规格

| 参数 | 值 |
|------|-----|
| 设备 | OpenBCI Ganglion |
| 采样率 | **200 Hz** |
| 通道数 | 4 (CH1–CH4) |
| 数据传输 | UDP → 主控机本地桥接 |
| 数据录制 | OpenBCI GUI CSV（含标记列） |

### 1.4 实验流程速览（来自 PLAN.md）

```
准备 5min → 心流诱导 8min → 切换干预 2min → 恢复观测 10min → 问卷+休息 3min
                                              × 3 轮
```

被试间设计：对照组（同学科连续） vs 实验组（跨学科切换：文理/理艺/文艺）。

---

## 2. 系统架构

### 2.1 角色定义

| 角色 | 设备 | 职责 | 数量 | WEB 可视化 |
|------|------|------|------|-------------|
| **上传监视端 (master)** | 接 OpenBCI 的笔记本 | 本地 UDP↔WS 桥接、上传 EEG 实时数据、录制 CSV+标记、全功能控制面板 | 1 | ✅ master-panel.html |
| **纯监视端 (monitor)** | 手机/平板/浏览器 | 实时波形查看、可发控制指令（开始/暂停/下一阶段），**无上传** | 0-N | ✅ monitor-panel.html |
| **受试者端 (subject)** | 被试面前设备 | 显示任务信息、自评标记（进入/脱离心流）、FSS 问卷 | 1 | ✅ subject-panel.html |
| **实验控制台 (console)** | 操作员浏览器 | 创建/配置实验、管理受试者、查看历史场次 | 1 | ✅ console-panel.html |
| **ECS 云端** | 2C2G / MySQL 5.5 | WebSocket 中继、计时器仲裁、实时指标聚合、标记存档、问卷/基线存储 | 1 | — |

### 2.2 数据流

```
┌─ 上传监视端 ──────────────────────────────────────┐
│  OpenBCI Ganglion ─UDP─→ bridge/index.js          │
│  WebSocket Server (本地端口 8080)                   │
│    ↓ 转发 EEG + 标记                                 │
│  WebSocket Client → ECS                             │
│  角色面板: master-panel.html                         │
└───────────────────────┬───────────────────────────┘
                        │
                        ▼
┌─ ECS ────────────────────────────────────────────┐
│  WebSocket Server (端口 8080)                      │
│  ─ role_claim → 角色分配 + 房间管理                 │
│  ─ eeg_frame → 广播监视端 + 指标引擎 → MySQL        │
│  ─ marker → 广播 + UDP 回传主控机写入 CSV            │
│  ─ cmd → 计时器引擎 → 广播 phase_sync               │
│  ─ fss_submit → MySQL                              │
│  ─ 基线引擎 + 恢复判定引擎                           │
│  Timer: 每秒 phase_sync                            │
│  nginx: 静态页 (index.html 统一入口) + WS 反代       │
└──┬──────────┬──────────────┬──────────────────────┘
   │          │              │
   ▼          ▼              ▼
 纯监视端     受试者端       实验控制台
 monitor-    subject-      console-
 panel.html  panel.html    panel.html
 (Revolut)   (Revolut)     (Revolut)
  0-N 台     1 台           1 台
 可发控制    自评/问卷      配置/管理
 无上传      —              —
```

### 2.3 计时器架构

计时器运行在 ECS 端，所有设备以 `phase_sync` 广播为准，消除设备间时差。

- ECS 每 1 秒广播 `{ type: "phase_sync", phase_index, phase_id, time_left, running, auto_mode, task_type }`
- 主控机和纯监视端收到后更新本地 UI
- 受试者端收到后切换显示内容

### 2.4 统一入口与角色选择

所有设备访问同一 URL（`https://eeg.yzjtiantian.cn/`），统一入口页面完成 WebSocket 配对后进入角色选择。

#### 统一入口与 SPA 架构

所有设备访问 `https://eeg.yzjtiantian.cn/`。页面是一个 **SPA（单页应用）**，所有角色面板（上传监视端 / 纯监视端 / 受试者端 / 控制台）写在同一个 HTML 中，通过 DOM 切换显示。角色选择后**不跳转页面**，WebSocket 连接始终保留。

### 角色选择流程

```
设备打开 eeg.yzjtiantian.cn
        │
        ▼
┌────────────────────────────────┐
│  NEUROLINK · 实验平台           │
│  连接中...                      │  ← 自动连接 WebSocket
└────────────────────────────────┘
        │ 发送 hello(role:pending)
        ▼
┌────────────────────────────────┐
│  选择角色                       │
│                                │
│  [ 上传监视端 ]  — 可用/占用   │  ← 已选则灰化
│  [ 纯监视端   ]  — 不限        │  ← 始终可选
│  [ 受试者端   ]  — 可用/占用   │
│  [ 控制台     ]  — 可用/占用   │
│                                │
│  在线设备: 上传监视端 + 纯×2    │
└────────────────────────────────┘
        │ claim_role
        ▼
┌────────────────────────────────┐
│  切换 DOM 到对应角色面板         │
│  (WS 连接不变, 不断开)          │
└────────────────────────────────┘
```

#### 角色槽位规则

| 角色 | 最大数量 | 灰化条件 |
|------|---------|---------|
| 上传监视端 (master) | 1 | 已被其他设备占用 |
| 纯监视端 (monitor) | 不限 | 永不灰化 |
| 受试者端 (subject) | 1 | 已被其他设备占用 |
| 控制台 (console) | 1 | 已被其他设备占用 |

#### 角色认领 WebSocket 协议

```
1. 所有设备连接 ws://eeg.yzjtiantian.cn/ws
2. 发送 → { type: "hello", role: "pending", session_id: "xxx" }
   ← ECS: { type: "room_info",
       occupants: { master: null, monitor: 2, subject: null, console: null } }
3. 用户选择角色后发送 → { type: "claim_role", role: "master" }
   ← ECS: 检查 slot
      - 可用: { type: "role_claimed", role: "master" }
      - 占用: { type: "role_denied", role: "master", reason: "已被占用" }
4. claim 成功后页面过渡到对应角色面板
```

#### 页面结构变更

```
public/
├── index.html              # 统一入口（角色选择界面）
├── master-panel.html       # 上传监视端面板
├── monitor-panel.html      # 纯监视端面板
├── subject-panel.html      # 受试者端面板
└── console-panel.html      # 实验控制台
```

---

## 3. 各页面结构与可视化

### 3.1 上传监视端 (master-panel.html) — 页面布局

```
┌──────────────────────────────────────────────────────────┐
│  Nav Bar (#000000)                                        │
│  ⧩ NEUROLINK · GANGLION 4CH · 200HZ   [● 已连接]        │
├───────────────────────────┬──────────────────────────────┤
│  左列 (窄, 固定宽度)       │  右列 (弹性, 主操作区)        │
│                           │                               │
│  ┌─ 实时脑电波波形 ──────┐│  ┌─ 实验控制面板 ───────────┐ │
│  │  Canvas 折线图        ││  │  当前阶段: 心流诱导阶段    │ │
│  │  CH1 ●━━━ (δ蓝)      ││  │  07:30 (Aeonik Pro 64px) │ │
│  │  CH2 ●━━━ (θ紫)      ││  │  [继续] [下一阶段]       │ │
│  │  CH3 ●━━━ (α橙)      ││  │  [重置] [自动 ●]        │ │
│  │  CH4 ●━━━ (β暖橙)    ││  └─────────────────────────┘ │
│  │  µVrms               ││                               │
│  └───────────────────────┘│  ┌─ 频带功率 + 指标 ───────┐ │
│                           │  │  δ ████ 10.2 ?         │ │
│  ┌─ 系统状态 ───────────┐  │  │  θ ██████████ 24.5    │ │
│  │  采样率    200 Hz    │  │  │  α ██████ 15.6        │ │
│  │  数据包    15,234    │  │  │  β ████████████ 28.1  │ │
│  │  显示窗口  4.0 s     │  │  │  γ ███ 5.4            │ │
│  │  CH1 ● CH2 ● CH3 ●  │  │  │  ───────────────────  │ │
│  └───────────────────────┘  │  │  θ/α  0.57 ?         │ │
│                           │  │  熵   0.82 ?          │ │
│  ┌─ 信号稳定性 ─────────┐  │  │  负载 0.35 ?          │ │
│  │  CH1 ████████ 正常   │  │  └─────────────────────────┘ │
│  │  CH2 ██████  正常    │  │                               │
│  │  CH3 ███     微弱    │  │  ┌─ 实验流程 ──────────────┐ │
│  │  CH4 ████████ 活跃   │  │  │  ● R1 心流诱导 (当前)   │ │
│  └───────────────────────┘  │  │  ○ R1 任务切换         │ │
│                           │  │  ○ R1 恢复观测           │ │
│                           │  │  ○ R1 休息+问卷          │ │
│                           │  │  ...                     │ │
│                           │  └─────────────────────────┘ │
│                           │                               │
│                           │  ┌─ 恢复状态 (recover阶段) ─┐ │
│                           │  │  🟡 未恢复                │ │
│                           │  │  当前 0.61 | 基线 0.57   │ │
│                           │  │  稳定 12/30 秒            │ │
│                           │  └─────────────────────────┘ │
├───────────────────────────┴──────────────────────────────┤
│  底部提示行: Z 心流进入 · X 心流脱离 · C 外部干扰 · V 自定义 │
│             Space 下一阶段 · Enter 暂停/继续               │
└──────────────────────────────────────────────────────────┘
```

**可视化元素清单：**

| 可视化 | 类型 | 数据源 | 更新频率 |
|--------|------|--------|---------|
| 波形 Canvas | 折线图/通道 | eeg_frame | 60fps RAF |
| 频带柱 δ/θ/α/β/γ | CSS bar + 数值 | metrics_snapshot | 1s |
| θ/α 比值 | 大数字 + 三色状态 | metrics_snapshot | 1s |
| 谱熵 | 数字 + `?` tooltip | metrics_snapshot | 1s |
| 认知负载指数 | 数字 + `?` tooltip | metrics_snapshot | 1s |
| 信号稳定性 CH1-4 | 色条 + 标签 | eeg_frame 滑动窗 | 0.5s |
| 大倒计时 | Aeonik Pro 大字 | phase_sync | 1s |
| 进度条 | 14 段色条 | phase_sync | 阶段变更 |
| 时间线列表 | 图标+文字 | phase_sync | 阶段变更 |
| 恢复状态 | 状态卡片 | baseline.js → WS | 1s |

`?` 实现方式：指标值后跟 `<span class="hint-icon" title="计算公式...">ⓘ</span>`，CSS hover 显示 tooltip（白底黑字浮层）。

纯监视端 (monitor-panel.html) 与 master-panel.html 共用同一套布局，唯一差异：右上角没有"上传状态"指示、不需要连接本地 UDP 桥接。

### 3.2 受试者端 (subject-panel.html) — 页面布局

```
┌──────────────────────────────────────────────────────────┐
│  Nav Bar (#000000)                                        │
│  ⧩ NEUROLINK · 第 2 轮 · 心流诱导阶段                    │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────── 内容区 (max 720px, 居中) ───────────────┐ │
│  │                                                      │ │
│  │  阶段标题 (Aeonik Pro 40px, 白色)                    │ │
│  │  心流诱导阶段                                         │ │
│  │  数理推理任务                                         │ │
│  │                                                      │ │
│  │  ┌──────────── 倒计时 ───────────────┐               │ │
│  │  │        07:32                       │  Aeonik Pro  │
│  │  │        剩余时间                    │  80px         │
│  │  └───────────────────────────────────┘               │ │
│  │                                                      │ │
│  │  ┌─────────────────┬──────────────────────┐         │ │
│  │  │  [ 进入心流 ]   │   [ 脱离心流 ]       │         │ │
│  │  │  button-primary │   button-outline-dark │         │ │
│  │  │  钴紫 #494fdf   │   白边透明           │         │ │
│  │  └─────────────────┴──────────────────────┘         │ │
│  │          ↑ 点击后冷却 10 秒                          │ │
│  │                                                      │ │
│  │  我的标记：进入 14:32 · 脱离 15:10 · 进入 15:45      │ │
│  │                                                      │ │
│  │  底部提示：请继续在纸上完成数理推理题目                 │ │
│  │                                                      │ │
│  └──────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

**各阶段界面与交互：**

| 阶段 | 标题 | 倒计时 | 自评按钮 | 底部文字 |
|------|------|--------|---------|---------|
| prep | 准备开始 | 05:00 | 隐藏 | 请佩戴EEG头环，等待实验员确认 |
| flow | 心流诱导阶段 · 数理推理任务 | 任务时长 | 显示 x2 | 请自行完成题目 |
| switch | 切换任务中 | 02:00 | 隐藏 | 即将切换至新任务… |
| recover | 状态恢复观测 | 10:00 | 显示 x2 | 请继续原任务 |
| rest | 休息与问卷 | — | 🡺 显示 FSS 7 题量表 + 提交 | 请根据刚才的任务体验作答 |

受试者端**不显示**任何 EEG 波形或指标数据。核心交互只有自评按钮和问卷。

### 3.3 实验控制台 (console-panel.html) — 页面布局

```
┌──────────────────────────────────────────────────────────┐
│  Nav Bar (#000000)                                        │
│  ⧩ NEUROLINK · 实验控制台                                 │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  ┌── 新建实验 ────────────────────────────────────────┐  │
│  │  受试者  [下拉框: 张三 / 李四 /...]    [+ 新增]    │  │
│  │  姓名: [____]  年龄: [__]  性别: [M/F]             │  │
│  │  实验模板  [对照组 / 文理切换 / 理艺切换 / 文艺切换]│  │
│  │  操作员    [________________] (56px input)         │  │
│  │  备注      [文本域]                                │  │
│  │  [开始实验 →]  [保存为草稿]                        │  │
│  └────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌── 受试者管理 ─────────────────────────────────────┐  │
│  │  ID │ 姓名 │ 年龄 │ 性别 │ 场次数 │ 操作         │  │
│  │  1  │ 张三 │  22  │  M   │   3   │ 编辑 ✕      │  │
│  │  2  │ 李四 │  19  │  F   │   0   │ 编辑 ✕      │  │
│  │  [+ 添加受试者] (pill-sm)                       │  │
│  └────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌── 场次历史 ───────────────────────────────────────┐  │
│  │  场次ID │ 受试者 │ 模板 │ 状态 │ 日期 │ 操作    │  │
│  │  7ab..  │ 张三   │ 文理 │ ✔完成│ 07-12│ [查看]  │  │
│  │  8cd..  │ 李四   │ 对照 │ ⏳进行│ 07-13│ [查看]  │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

**可配置选项：**

| 配置项 | UI 组件 | 说明 |
|--------|---------|------|
| 受试者 | 下拉框 + 创建弹窗 | 从已有列表选择或现场新增 |
| 实验模板 | 单选卡片 | 4 种模板，选中态 cobalt violet 边框 |
| 操作员 | 文本输入框 56px | 记录本次谁操作 |
| 备注 | 文本域 | 环境、异常情况记录 |
| 开始实验 | button-primary | 创建 session + 跳转 master.html |

**三个核心动作：** 创建实验（选人+模板）→ 开始实验（跳转监视面板）→ 实验后查看场次总结。

所有消息为 UTF-8 JSON，通过 ECS 中转。

### 3.1 连接握手（统一格式）

```json
// 所有设备首次连接 → ECS
{ "type": "hello",
  "role": "pending",                        // 始终为 "pending"，不在此处认领角色
  "session_id": "uuid-v4" }
```

连接后通过独立的 `claim_role` 消息认领角色，角色槽位规则见 §2.4。

主控机在 claim_role 成功后额外发送：
```json
{ "type": "set_udp_target",
  "target": "192.168.1.100:12345" }        // OpenBCI GUI Marker 端口
```

断线重连时不使用 hello/claim_role，使用专用消息：
```json
{ "type": "reconnect",
  "role": "master",                         // 原角色
  "session_id": "uuid-v4",
  "udp_target": "192.168.1.100:12345" }
```

### 3.2 EEG 实时数据

```json
// 主控机 → ECS → 所有监视端
{ "type": "eeg_frame",
  "channels": [123.4, 56.7, 89.0, 12.3],   // 4 floats
  "seq": 15234,
  "ts": 1706000101123 }
```

每秒 200 包，每包 ~200 字节。ECS 收到后立即广播给所有监视端。

### 3.3 标记事件

```json
// 任意端 → ECS → 所有端 + UDP 回传主控机 → OpenBCI GUI CSV
{ "type": "marker",
  "source": "operator" | "subject" | "auto",
  "code": 1,                                // 1-8
  "label": "心流进入",
  "phase": "flow1",
  "ts": 1706000101123 }
```

标记代码分配：

| 码 | 触发方式 | 含义 |
|----|---------|------|
| 1 | 自动 | phase_start |
| 2 | 自动 | phase_end |
| — | 自动 | **baseline_start**（flow 后 4 分钟起点） |
| — | 自动 | **baseline_end**（flow 结束） |
| — | 自动 | **recovered**（30s 回到基线±5%） |
| 3 | 操作员 Z | 心流进入 / 状态恢复 |
| 4 | 操作员 X | 心流脱离 / 状态异常 |
| 5 | 操作员 C | 外部干扰（噪音/中断） |
| 6 | 操作员 V | 预留自定义 |
| 7 | 受试者点击 | 心流进入（自评） |
| 8 | 受试者点击 | 心流脱离（自评） |

> 自动标记的 `code` 字段使用高位区（9+），OpenBCI GUI 的 UDP Marker 仅接收浮点值，自动标记采用 `code = 11, 12, 13...` 发送，不与 1-8 键位冲突。

### 3.4 控制指令

```json
// 任意端 → ECS → 所有端
{ "type": "cmd",
  "action": "start" | "pause" | "next_phase" | "reset" | "set_auto" | "set_task_type",
  "value": "math" }                         // 仅 set_task_type 携带
```

### 3.5 受试者自评

```json
// 受试者端 → ECS → 主控机 + 转 marker
{ "type": "self_report",
  "state": "flow_enter" | "flow_exit" | "distracted" }
```

### 3.6 状态同步

```json
// ECS → 所有端（每秒一次）
{ "type": "phase_sync",
  "phase_index": 2,
  "phase_id": "flow1",
  "phase_name": "心流诱导阶段",
  "round": 1,
  "time_left": 287,
  "running": true,
  "auto_mode": true,
  "task_type": "math" | "language" | "art" | null,
  "time_in_phase": 193 }                   // 当前阶段已过秒数，供基线/恢复判定用
```

### 3.7 FSS 问卷提交

```json
// 受试者端 → ECS → MySQL
{ "type": "fss_submit",
  "round": 2,
  "phase": "rest2",
  "answers": [6, 5, 7, 6, 5, 3, 2],          // 7 题，1-7 Likert
  "ts": 1706000101123 }
```

### 3.8 指标快照（ECS 广播 + MySQL 写入）

```json
// ECS → MySQL（每秒写入）+ 同时广播给主控机
{ "type": "metrics_snapshot",
  "ts": 1706000101123,
  "phase": "flow1",
  "phase_index": 2,
  "band_power": { "delta": 12.3, "theta": 8.9, "alpha": 15.6, "beta": 22.1, "gamma": 5.4 },
  "theta_alpha_ratio": 0.57,
  "spectral_entropy": 0.82,
  "cognitive_load_index": 0.35,
  "signal_quality": [0.92, 0.88, 0.95, 0.91] }
```

### 3.9 WebSocket 连接生命周期

#### 连接拓扑

```
                    ECS WebSocket Server
                   (ws://eeg.yzjtiantian.cn/ws)
                    ┌──────────────────────────┐
                    │  房间管理器               │
                    │  Map<session_id,         │
                    │    Set<WebSocket>>       │
                    │  每个 session 一个房间    │
                    │  同房间内 WS 互连广播     │
                    └──────────────────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
     master.html     monitor.html     subject.html
     (1 个)           (0-N 个)         (1 个)
```

#### 连接流程

```
所有设备通过 SPA 入口 `index.html` 启动：

```
index.html 启动时:
  1. new WebSocket("ws://eeg.yzjtiantian.cn/ws")
  2. 发送 hello: { type: "hello", role: "pending", session_id: "xxx" }
  3. ← ECS: { type: "room_info", occupants: {...} }
  4. 显示角色选择面板（已占用角色灰化）

用户选择角色后（SPA 同一页面，WS 不断开）:
  5. 发送 → { type: "claim_role", role: "master" }
  6. ← ECS: { type: "role_claimed", role: "master" }
     或 ← { type: "role_denied", role: "master", reason: "已被占用" }
  7. 成功 → DOM 切换到对应角色面板（WS 连接保留）
  8. master 额外发送:
     → { type: "set_udp_target", target: "192.168.1.100:12345" }

角色面板激活后:
  master → 开始转发 eeg_frame 到 ECS
  monitor → 收到 phase_sync / eeg_frame / metrics_snapshot 渲染
  subject → 收到 phase_sync 切换界面状态

断开时:
  1. ECS 将有限角色槽位（master/subject/console）保留 30 秒
  2. 30 秒内重连则发送 → { type: "reconnect", role: "master", session_id: "xxx" }
     不释放槽位，直接恢复
  3. 超过 30 秒 → 释放槽位，其他设备可抢占

重连时:
  1. 新 WebSocket 连接
  2. 发送 reconnect 消息（替代 hello+claim_role 两步）
  3. ECS 验证后恢复角色和计时器状态
```
```

#### 消息传递路径

```
eeg_frame:
  master → ECS → 广播房间内所有 monitor
            → ECS metrics.js 累积计算
            → metrics_snapshot → MySQL + 广播

marker:
  任意端 → ECS → 广播房间内所有端
            → 若 source ≠ master:
              ECS 发 UDP 到 master 的 udp_target
              → master 本地 bridge/index.js → OpenBCI GUI

cmd:
  任意端(有控制权限) → ECS timer.js 更新状态
            → 广播 phase_sync → 所有端

phase_sync:
  ECS timer.js 每秒 → 广播所有端
```

#### 延迟分析

| 路径 | 估计延迟 |
|------|---------|
| Ganglion → 本机 UDP | < 1ms |
| 本地 WS → master 本地面板 | < 5ms |
| 本机 → ECS (公网) | 10-30ms |
| ECS → 监视端 (公网) | 10-30ms |
| **总延迟（远端监视）** | **20-60ms** |

对人眼观看实时波形来说 20-60ms 完全不可感知。

---

## 4. EEG 核心指标定义

本平台在 ECS 端对实时 EEG 数据每秒进行一次指标计算，结果写入 MySQL `metrics_snapshots` 表并广播给主控机面板显示。面板上每个指标名称旁显示 **`?`** 图标，鼠标悬停时弹出计算公式。

### 4.1 θ/α 比值 — 心流稳态核心指标

**位置**：频带功率面板，显眼位置大字显示
**面板显示**：`θ/α 0.57 ?`
**悬停提示**：

```
θ/α = Pθ / Pα

Pθ = Goertzel(4-8 Hz)   θ 波段功率
Pα = Goertzel(8-13 Hz)  α 波段功率

比值稳定 → 心流稳态
比值↑ → 放松加深
比值↓ → 专注/紧张
```

### 4.2 脑电熵值（谱熵）— 认知损耗指标

**位置**：频带功率面板，θ/α 比值旁
**面板显示**：`熵 0.82 ?`
**悬停提示**：

```
H = -Σ(pi × log₂(pi))

pi = Pi / Σ(Pj)   各频带归一化功率占比
Pi ∈ {Pδ, Pθ, Pα, Pβ, Pγ}

熵值升高 → 认知复杂度增大 → 心流被破坏（切换后预期升高）
赫兹范围：δ(0.5-4) θ(4-8) α(8-13) β(13-32) γ(32-100)
```

### 4.3 认知负载指数 — 认知损耗指标

**位置**：频带功率面板
**面板显示**：`负载 0.35 ?`
**悬停提示**：

```
CLI = Pγ / Pα

Pγ = Goertzel(32-100 Hz)  Gamma 功率
Pα = Goertzel(8-13 Hz)    Alpha 功率

CLI↑ → 认知负载增大（切换干涉后预期升高）
CLI↓ → 放松/沉浸状态
```

### 4.4 监视面板 `?` 实现规范

- 每个指标值后跟一个 `?` 圆圈图标（`ℹ︎` 或 `ⓘ`）
- 使用 `title` 属性实现原生 hover 提示，或 CSS 自定义 tooltip
- 纯监视端收到 `metrics_snapshot` 后同样渲染 `?` 提示
- 颜色：`?` 图标使用 `var(--body-mid)`（`#8a8a8f`），hover 时变白

### 4.5 信號稳定性

**位置**：信号稳定性面板
**面板显示**：各通道 `标准差` 值 + 色条

```
σ = √( Σ(xi - x̄)² / N )    // N=32 滑动窗口

σ < 500µV  → 无信号（红色）
σ 5k-50kµV → 正常（绿色）
σ > 50kµV   → 活跃（蓝色）
```

> 注：该指标反映信号波动程度，**并非真实电极阻抗**。本设计未包含通过注入电流实现的硬件级阻抗检测。

---

## 5. 基线录制与恢复判定

这是实现 PLAN.md 中"精准测算心流被打断后的 EEG 恢复时长"的核心算法。

### 5.1 基线录制机制

在 flow 阶段（心流诱导）的最后 4 分钟自动启动：

| 时机 | 动作 |
|------|------|
| flow 阶段开始 (phase_index=2) | 重置基线缓存 |
| flow 阶段进行到 4min（第 241 秒） | 打 `baseline_start` marker (code 11) |
| 后 4 分钟（240s） | 每秒采集 `theta_alpha_ratio` 样本，持续维护滑动统计 |
| flow 阶段结束 | 打 `baseline_end` marker (code 12)，写入 MySQL `baselines` 表 |

基线记录写入 MySQL：

```json
{
  "session_id": "uuid",
  "phase_id": "flow1",
  "baseline_ratio_mean": 0.57,      // θ/α 均值
  "baseline_ratio_std": 0.04,       // θ/α 标准差
  "baseline_alpha_mean": 15.6,      // Alpha 均值
  "baseline_alpha_std": 2.1,
  "baseline_beta_mean": 22.1,
  "baseline_beta_std": 3.0,
  "samples": 240
}
```

### 5.2 恢复判定算法

在 recover 阶段（恢复观测）每秒执行：

```
1. 计算当前 30 秒 θ/α 滑动均值 current_mean
2. 与基线比较：
   - target_low  = baseline_ratio_mean × 0.95
   - target_high = baseline_ratio_mean × 1.05
3. 若连续 30 秒 current_mean ∈ [target_low, target_high]:
   → 判定为已恢复
   → 打 recovered marker (code 13)
   → 记录 recovery_time = 当前 time_in_recover (秒)
   → 广播不可忽视的 UI 通知到主控机和监视端
```

### 5.3 恢复面板显示

主控机面板 recover 阶段额外显示：

```
恢复状态：[ 未恢复 / 已恢复 (4分32秒) ]
当前比值：0.61（基线 0.57 ±5%）
稳定时长：12 / 30 秒    ← 已连续处于±5%窗口内的秒数
```

---

## 6. FSS 心流问卷（7 题）

7 级李克特（1=完全不同意 → 7=完全同意），每轮休息阶段弹出。

| # | 维度 | 题目 |
|---|------|------|
| 1 | 专注度 | 我在任务期间完全沉浸在当前所做的事情中 |
| 2 | 忘我感 | 我忘记了周围的环境和自我的存在 |
| 3 | 时间扭曲 | 我感觉时间过得比平时快 |
| 4 | 投入度 | 我感到自己非常投入，不需要额外努力就能集中注意 |
| 5 | 流畅度 | 我的思考和行动非常流畅、自然，不需要刻意控制 |
| 6 | 切换损耗 | 切换到新任务后，我感到思维被打断、需要时间重新进入状态 |
| 7 | 中断感知 | 我感觉到任务进行中出现了明显的思维中断 |

题 6 仅在跨学科切换轮显示（对照组隐藏），题 7 所有轮次显示。

---

## 7. 受试者面板设计

### 7.1 全流程状态表

| 阶段 | 受试者界面 | 交互 |
|------|-----------|------|
| 准备 (prep) | "准备开始" + 佩戴说明 + 倒计时 | —（静默等待） |
| 心流诱导 (flow) | 任务标题 + 大倒计时 + 自评按钮 x2 | ✅ 自评：进入/脱离 |
| 任务切换 (switch) | "切换任务" + 新任务名称 + 3 秒后自动继续 | —（短暂过渡） |
| 恢复观测 (recover) | "请继续原任务" + 同 flow 样式 | ✅ 自评：进入/脱离 |
| 休息+问卷 (rest) | FSS 量表 7 题 + 提交按钮 | ✅ 必须提交才继续 |

### 7.2 自评按钮

- **"进入心流"** — 发送 self_report: flow_enter → marker code 7
- **"脱离心流"** — 发送 self_report: flow_exit → marker code 8
- 每次点击后 10 秒冷却，防止误触连点

### 7.3 操作员面板标记快捷键

快捷键仅在主控机和监视端有效（受试者端无键盘操作）：

| 键 | 功能 | Marker Code |
|----|------|-------------|
| Z | 心流进入 / 状态恢复 | 3 |
| X | 心流脱离 / 状态异常 | 4 |
| C | 外部干扰（噪音/中断） | 5 |
| V | 预留自定义 | 6 |

标记快捷键对应的面板 UI 在底部提示行显示（`Z 心流进入 · X 心流脱离 · C 外部干扰 · V 自定义`），每个键名前用对应彩色圆点标识。

---

## 8. MySQL 表结构

### 8.1 subjects — 受试者

```sql
CREATE TABLE subjects (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(32) NOT NULL,
  age         TINYINT,
  gender      CHAR(1),
  notes       TEXT,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 8.2 experiment_templates — 实验模板

```sql
CREATE TABLE experiment_templates (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(64) NOT NULL,           -- "对照组", "文理切换", "理艺切换", "文艺切换"
  group_type  ENUM('control','experiment') NOT NULL,
  switch_type ENUM('none','math_lang','lang_art','math_art'),
  phases_json JSON NOT NULL,                   -- 14 阶段配置（duration, id, task_type 等）
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 8.3 sessions — 实验场次

```sql
CREATE TABLE sessions (
  id              VARCHAR(36) PRIMARY KEY,     -- UUID
  subject_id      INT NOT NULL,
  template_id     INT NOT NULL,
  operator_name   VARCHAR(32),
  status          ENUM('pending','running','paused','completed') DEFAULT 'pending',
  started_at      TIMESTAMP NULL,
  ended_at        TIMESTAMP NULL,
  notes           TEXT,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (subject_id) REFERENCES subjects(id),
  FOREIGN KEY (template_id) REFERENCES experiment_templates(id)
);
```

### 8.4 fss_results — 问卷结果

```sql
CREATE TABLE fss_results (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(36) NOT NULL,
  round       TINYINT NOT NULL,
  phase       VARCHAR(16) NOT NULL,
  answers     JSON NOT NULL,                   -- [7] int array
  submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id)
);
```

### 8.5 markers — 标记存档

```sql
CREATE TABLE markers (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(36) NOT NULL,
  code        TINYINT NOT NULL,
  source      ENUM('operator','subject','auto'),
  label       VARCHAR(64),
  phase       VARCHAR(16),
  ts          BIGINT NOT NULL,                 -- 设备毫秒时间戳
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id)
);
```

### 8.6 metrics_snapshots — 每秒聚合指标

```sql
CREATE TABLE metrics_snapshots (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(36) NOT NULL,
  ts          BIGINT NOT NULL,
  phase_index TINYINT NOT NULL,
  phase_id    VARCHAR(16) NOT NULL,
  delta       FLOAT,                           -- δ 功率
  theta       FLOAT,                           -- θ 功率
  alpha       FLOAT,                           -- α 功率
  beta        FLOAT,                           -- β 功率
  gamma       FLOAT,                           -- γ 功率
  theta_alpha_ratio FLOAT,                     -- θ/α
  spectral_entropy   FLOAT,                    -- 谱熵
  cognitive_load_index FLOAT,                  -- γ/α
  sq_ch1      FLOAT,                           -- 信号稳定性 CH1（标准差）
  sq_ch2      FLOAT,
  sq_ch3      FLOAT,
  sq_ch4      FLOAT,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id),
  INDEX idx_session_phase (session_id, phase_index),
  INDEX idx_ts (ts)
);
```

1 小时实验 ≈ 3600 条记录，整张表在 MySQL 5.5 上完全无压力。

### 8.7 baselines — 心流基线记录

```sql
CREATE TABLE baselines (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(36) NOT NULL,
  phase_id    VARCHAR(16) NOT NULL,            -- flow1/flow2/flow3
  ratio_mean  FLOAT,                           -- θ/α 均值
  ratio_std   FLOAT,                           -- θ/α 标准差
  alpha_mean  FLOAT,
  alpha_std   FLOAT,
  beta_mean   FLOAT,
  beta_std    FLOAT,
  samples     INT,                             -- 参与计算的样本数
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id)
);
```

### 8.8 Backend 与 MySQL 交互

`db.js` — 极小连接池封装（使用 `mysql` npm 包）：

```javascript
const mysql = require('mysql');
const pool = mysql.createPool({
  host: config.DB_HOST,
  user: config.DB_USER,
  password: config.DB_PASS,
  database: config.DB_NAME,
  connectionLimit: 10
});

module.exports = {
  query(sql, params) {
    return new Promise((resolve, reject) => {
      pool.query(sql, params, (err, rows) => {
        if (err) reject(err); else resolve(rows);
      });
    });
  }
};
```

各模块的 SQL 调用一览：

| 模块 | 触发时机 | SQL |
|------|---------|-----|
| router.js | 收到 fss_submit | `INSERT INTO fss_results (session_id, round, phase, answers) VALUES (?, ?, ?, ?)` |
| router.js | 收到 marker | `INSERT INTO markers (session_id, code, source, label, phase, ts) VALUES (?, ?, ?, ?, ?, ?)` |
| metrics.js | 每秒计算完成 | `INSERT INTO metrics_snapshots (session_id, ts, phase_index, phase_id, delta, theta, alpha, beta, gamma, theta_alpha_ratio, spectral_entropy, cognitive_load_index, sq_ch1, sq_ch2, sq_ch3, sq_ch4) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)` |
| baseline.js | flow 结束写入基线 | `INSERT INTO baselines (session_id, phase_id, ratio_mean, ratio_std, alpha_mean, alpha_std, beta_mean, beta_std, samples) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)` |
| console API | 获取受试者列表 | `SELECT * FROM subjects ORDER BY created_at DESC` |
| console API | 创建受试者 | `INSERT INTO subjects (name, age, gender, notes) VALUES (?, ?, ?, ?)` |
| console API | 获取模板列表 | `SELECT * FROM experiment_templates` |
| console API | 创建实验场次 | `INSERT INTO sessions (id, subject_id, template_id, operator_name, status) VALUES (?, ?, ?, ?, 'pending')` |
| console API | 获取场次历史 | `SELECT s.*, t.name AS template_name FROM sessions s JOIN experiment_templates t ON s.template_id = t.id ORDER BY s.created_at DESC` |
| console API | 查看场次详情 | `SELECT * FROM markers WHERE session_id = ? ORDER BY ts` + `SELECT * FROM fss_results WHERE session_id = ?` + `SELECT * FROM metrics_snapshots WHERE session_id = ? ORDER BY ts` + `SELECT * FROM baselines WHERE session_id = ?` |

> **MySQL 5.5 兼容说明：** MySQL 5.5 不支持 JSON 数据类型。`answers`（fss_results）和 `phases_json`（experiment_templates）使用 `TEXT` 存储 JSON 字符串，应用层 `JSON.parse()` / `JSON.stringify()` 处理。MySQL 5.5 的 `utf8` 支持 3 字节编码，中文字符兼容。

---

## 9. 实验模板阶段 JSON Schema

14 阶段的精确 JSON 结构，每个模板对应一种实验配置。

```json
{
  "template_name": "理艺切换组",
  "group_type": "experiment",
  "switch_type": "math_art",
  "phases": [
    { "id": "prep",   "round": 0, "duration": 300, "task_type": null,     "name": "准备阶段" },
    { "id": "flow1",  "round": 1, "duration": 480, "task_type": "math",   "name": "心流诱导阶段（数理）" },
    { "id": "switch1","round": 1, "duration": 120, "task_type": "art",    "name": "任务切换阶段（数理→艺术）" },
    { "id": "recover1","round":1, "duration": 600, "task_type": "math",   "name": "心流恢复观测" },
    { "id": "rest1",  "round": 1, "duration": 180, "task_type": null,     "name": "休息与问卷" },
    { "id": "flow2",  "round": 2, "duration": 480, "task_type": "math",   "name": "心流诱导阶段（数理）" },
    { "id": "switch2","round": 2, "duration": 120, "task_type": "art",    "name": "任务切换阶段（数理→艺术）" },
    { "id": "recover2","round":2, "duration": 600, "task_type": "math",   "name": "心流恢复观测" },
    { "id": "rest2",  "round": 2, "duration": 180, "task_type": null,     "name": "休息与问卷" },
    { "id": "flow3",  "round": 3, "duration": 480, "task_type": "math",   "name": "心流诱导阶段（数理）" },
    { "id": "switch3","round": 3, "duration": 120, "task_type": "art",    "name": "任务切换阶段（数理→艺术）" },
    { "id": "recover3","round":3, "duration": 600, "task_type": "math",   "name": "心流恢复观测" },
    { "id": "rest3",  "round": 3, "duration": 180, "task_type": null,     "name": "休息与问卷" }
  ]
}
```

对照组模板：`switch` 阶段 `task_type` 与 flow 相同（同学科），`name` 改为"任务继续阶段"。

---

## 10. 服务端模块职责

### 10.1 主控机本地桥接 (bridge/index.js)

```
输入:  Ganglion UDP 数据包 (端口 12345, 200包/秒)
处理:  解析二进制 → { channels: [4 floats], seq, ts }
输出1: 本地 WebSocket (端口 9080) → master-panel.html 面板
输出2: WebSocket Client → ECS ws://eeg.yzjtiantian.cn/ws
```

职责：
- 监听本地 UDP 12345 端口接收 Ganglion 数据包
- 将 UDP 包解析为 JSON eeg_frame
- 通过本地 WebSocket Server (8080) 推送到本地面板
- **同时**通过 WebSocket Client 推送到 ECS `ws://eeg.yzjtiantian.cn/ws`
- 监听本地 UDP 端口接收 ECS 回传的 marker → 转发到 OpenBCI GUI Marker 端口

### 10.2 ECS WebSocket 中继 (cloud/index.js)

```
输入:  WebSocket 连接 (所有角色)
处理:  接收 WS 消息 → 调用 router.js 分发
      房间管理器: Map<session_id, Set<WebSocket>>
输出:  广播给同房间其他客户端
      定时器启动: 每秒 phase_sync
```

职责：
- WebSocket Server 监听 8080 端口（nginx 反代 `/ws`）
- 维护房间映射（session → 客户端列表）
- 收到 eeg_frame 时立即转发给同房间的 monitor 端
- 收到 cmd 时调用 timer.js 更新状态
- 收到 marker/fss_submit 时执行对应动作

### 10.3 计时器引擎 (cloud/timer.js)

```
输入:  cmd (start/pause/next_phase/reset/set_auto)
处理:  阶段时间线管理
      每秒 time_left--，检测阶段结束
输出:  phase_sync 广播 (每秒)
      自动 marker (phase_start/phase_end)
```

职责：
- 管理 phase_index、time_left、running 状态
- 每秒 tick：time_left--，如果归零则自动切换到下一阶段
- 如果 autoMode=true 则继续，否则暂停等待操作员确认
- 阶段切换时自动触发生成 marker：phase_end → phase_start

### 10.4 指标计算引擎 (cloud/metrics.js)

```
输入:  eeg_frame (200 包/秒，持续流入)
处理:  滑动窗口缓冲区 (256 采样点 ~1.28秒)
      每秒计算一次:
        - Goertzel(δ/θ/α/β/γ) → band_power
        - θ/α 比值
        - 谱熵 H = -Σ(pi·log₂(pi))
        - 认知负载指数 CLI = Pγ / Pα
        - 标准差 σ 每通道
输出:  metrics_snapshot → MySQL (每秒写入)
      metrics_snapshot → 广播给所有端
```

职责：
- 维护 eeg_frame 的环形缓冲区
- 每秒对齐时间窗口执行 FFT/Goertzel 计算
- 产出 5 频带功率 + 3 个复合指标 + 4 通道信号质量
- 写入 MySQL + 广播面板

### 10.5 基线录制与恢复判定 (cloud/baseline.js)

```
输入:  phase_sync (阶段变更事件)
处理:  在 flow 阶段后 4 分钟启动基线录制
      在 recover 阶段每秒执行恢复判定
输出:  baseline_start/end marker
      recovered marker + recovery_time
      baselines INSERT → MySQL
```

职责：
- 监听 phase_id 变化，识别 flow/recover 阶段
- flow 阶段的后 4 分钟：滑动窗口录制 θ/α 均值 ± 标准差
- flow 结束时写入 baselines 表
- recover 阶段：每秒计算 30 秒滑动均值，判定是否回到基线 ±5%
- 连续 30 秒达标 → 打 recovered marker

### 10.6 MySQL 连接池 (cloud/db.js)

```
输入:  SQL 查询字符串 + 参数数组
处理:  mysql.createPool → 异步 query
输出:  查询结果 Promise<rows>
```

职责：
- 连接池管理 (10 连接)
- 提供 `query(sql, params)` 统一接口

### 10.7 消息路由 (cloud/router.js)

```
输入:  { type, ... } 任意 WS 消息
处理:  switch(type):
         hello → 注册房间 + 存储元信息
         eeg_frame → metrics.js + 广播
         cmd → timer.js
         marker → 广播 + UDP 回传 + MySQL
         fss_submit → MySQL
         self_report → 广播 + 转 marker
输出:  调用对应模块
```

职责：
- type 分派
- 参数校验与错误处理
- UDP 回传 master（针对标记事件）
- 日志记录

### 10.8 配置文件 (cloud/config.js)

```javascript
module.exports = {
  WS_PORT: 8080,
  DB_HOST: 'localhost',
  DB_USER: 'eeg',
  DB_PASS: '***',
  DB_NAME: 'eeg_platform',
  UDP_DEFAULT_TARGET: '127.0.0.1:12345',  // OpenBCI GUI Marker 端口模板
};
```

---

## 11. 部署方案

| 项目 | 值 |
|------|-----|
| 服务器 | 2C2G ECS / MySQL 5.5 |
| 域名 | **eeg.yzjtiantian.cn** |
| Node 进程管理 | **PM2** |
| WebSocket 端口 | 8080（通过 nginx 反代 `/ws` 路径） |
| 静态页面 | nginx 直接托管 `public/` 目录 |
| 前端框架 | 纯原生 HTML/CSS/JS，无构建步骤 |

### 部署拓扑

```
客户端浏览器                           ECS
  │                                   │
  └── eeg.yzjtiantian.cn/              ├── nginx :80/:443
      │                                │   ├── / → index.html (SPA)
      ├── index.html (SPA, 所有面板)    │   ├── /master → master-panel.html (deprecated)
      └── ws://eeg.yzjtiantian.cn/ws    │   └── /ws → 反代 → Node :8080
                                        │
                                        ├── PM2 → cloud/index.js
                                        └── MySQL 5.5 :3306
```

### 项目文件树

```
eeg-platform/
├── bridge/                 # 主控机本地桥接 (端口 9080)
│   ├── index.js            # UDP 监听 + 本地 WS + ECS 上行
│   ├── config.js           # 端口/设备/ECS 地址配置
│   └── package.json
├── cloud/                  # ECS 云端 (端口 8080)
│   ├── index.js            # WS 中继 + 房间管理 + 计时器 + cmd 权限
│   ├── metrics.js          # 指标计算 (Goertzel/熵/负载)
│   ├── baseline.js         # 基线录制 + 恢复判定
│   ├── db.js               # MySQL 连接池
│   ├── config.js           # 云端配置
│   ├── ecosystem.config.js # PM2 配置
│   ├── nginx.conf          # nginx 反代配置
│   ├── migrations/
│   │   └── 001_init.sql    # 7 张建表语句
│   └── package.json
├── web/                    # 前端 SPA（nginx 静态托管）
│   └── index.html          # 统一入口 + 4 角色面板 (SPA)
├── docs/
│   └── specs/
│       └── 2026-07-13-eeg-experiment-platform-design.md
├── DESIGN.md               # Revolut 设计语言
├── OpenBCI_README.md       # OpenBCI GUI 使用指南
├── PLAN.md                 # 实验方案
└── .gitignore
```

---

## 12. 边界情况处理

### 12.1 网络断线重连

**主控机断网：**

```
事件: master WebSocket 断开
动作:
  1. ECS 检测到 master WS close → 暂停计时器 (running=false)
  2. 向房间内广播 { type: "alert", level: "warning", message: "数据源已断开" }
  3. 角色槽位保留 30 秒，不释放
  4. 所有端显示"数据源断开"指示器
恢复:
  1. master 重连后发送 → { type: "reconnect", role: "master", session_id: "xxx" }
  2. ECS 验证 identity，恢复角色槽位和计时器
  3. 继续广播 phase_sync
  4. 断线期间的 eeg_frame 丢失（WS 队列被清空）
  5. 但 OpenBCI GUI 本地一直在录 CSV，原始数据没丢
  6. 若 30 秒内未重连 → 释放槽位，其他设备可抢占
```

**监视端断网：**

```
事件: monitor WebSocket 断开
动作:
  1. 页面 JS 检测到 onclose → 显示"连接断开，5秒后重连"
  2. 自动 setTimeout(connectWS, 5000)
  3. 重连后发送 hello → 重新加入房间
  4. 收到最新 phase_sync 恢复界面状态
  5. 断线期间的 eeg_frame 丢失 → 波形画布空档期黑屏
```

**受试者端断网：**

```
事件: subject WebSocket 断开
动作:
  1. 同上自动重连
  2. 自评按钮在断网期间点击 → 缓存到本地队列
  3. 重连后队列内标记一次性发送 → ECS 依次处理
```

### 12.2 多人同时发送控制指令

```
cmd 冲突:
  1. ECS timer.js 串行处理所有收到的 cmd（单线程 Node.js 天然保证）
  2. 每次 cmd 执行后立即广播 phase_sync 同步所有端

  举例: monitor A 发 cmd: pause
        monitor B 发 cmd: start（几乎同时）

        ECS 按收到顺序执行:
        → pause  (timer 暂停, 广播 running=false)
        → start  (timer 恢复, 广播 running=true)

  3. 最终状态 = 最后一条 cmd 的结果
  4. 所有端看到的 phase_sync 完全一致，不存在不一致窗口

  边界: 如果 master 和 monitor 同时发相反的 cmd
        → 各端 UI 按钮状态可能短暂不一致
        → 但在下一次 phase_sync (1 秒内) 全部同步
        → 不在客户端做乐观锁定，以 ECS 仲裁为准
```

### 12.3 Phase 自动切换与标记时序

```
第 480 秒 (time_left=0) 时的精确时序:

  1. timer.js tick() 检测到 time_left=0
  2. 同一事件循环内依次执行:
     a. 生成 marker: { code: 2, label: "phase_end:flow1", source: "auto" }
     → 广播 + UDP 回传 + MySQL INSERT
     b. 更新 phase_index = flow1→switch1
     c. 生成 marker: { code: 1, label: "phase_start:switch1", source: "auto" }
     → 广播 + UDP 回传 + MySQL INSERT
     d. 广播 phase_sync { phase_index: 3, ... }
     e. 如果 autoMode=false → timer 暂停，等待 cmd: next_phase

  所有端在同一 tick 内收到 phase_end → phase_start → phase_sync，
  保证 CSV 中 end/start 标记顺序正确、时间戳单调递增。
```

### 12.4 恢复判定时序竞争

```
recover 阶段秒级判定:

  recover 阶段持续 600 秒，每秒执行一次判定
  恢复条件: 连续 30 秒 θ/α 均值在基线 ±5% 窗口

  竞争: 如果在第 29 秒时满足条件，第 30 秒不满足
        → 计数器清零，从第 31 秒重新开始计数

  防止"刚达标就波动"导致的误判:
  → 严格 30 秒连续，不采用"30 秒内多数达标"策略
  → 因为 PLAN.md 明确写了"连续30s回归"，学术规范决定
```

### 12.5 标记去重与乱序

```
同一标记事件的多次广播:
  - 受试者自评按钮有 10 秒冷却 → 人工防连点
  - WS 层不做消息去重，前端冷却是最可靠防重

UDP 回传丢失:
  - ECS → master 的 UDP marker 可能丢包（UDP 无确认）
  - OpenBCI GUI CSV 标记列可能缺失极少数标记
  - 但 markers 表在 ECS 的 MySQL 中完整记录
  - 事后分析时以 markers 表为准，CSV 标记列仅作补充参考
```

### 12.6 ECS 崩溃与计时器持久化

```
计时器状态每秒写入 MySQL timer_state 表:

  每 1 秒 tick → saveTimerState(sessionId)
  阶段变更 → saveTimerState(sessionId)
  重置 → saveTimerState(sessionId)

ECS 崩溃重启后:
  1. 第一个客户端连接时, 尝试重建计时器
  2. 先查询 timer_state 表是否有该 session 的记录
  3. 有记录 → 恢复 phase_index/time_left, 设为暂停状态等待操作员确认
  4. 无记录 → 从 phase 0 开始全新计时

timer_state 表结构:
  session_id, phase_index, time_left, time_in_phase,
  running, auto_mode, template_type, updated_at

注意: 断线期间(30 秒保留期内)的时间不会补算,
      phase_sync 从恢复时刻继续计时。
```

### 12.7 ECS 过载保护

```
eeg_frame 频率: 200 包/秒
metrics_snapshot 写入: 1 行/秒 (轻量 INSERT)

保护措施:
  1. metrics.js 的缓冲队列固定大小为 512 包
  2. 如果 ECS CPU 打满导致队列堆积 → 丢弃最旧的数据包
  3. 每秒计算时始终取最新的 256 个采样点
  4. MySQL 写入使用连接池，INSERT 超时 1 秒
  5. phase_sync / marker 等控制消息优先级最高，优于 eeg_frame 转发
```

---

## 14. UI 风格指南

所有页面统一遵循 `DESIGN.md`（Revolut · 金融消费设计语言）。

### 设计令牌

| 令牌 | 值 | 应用 |
|------|-----|------|
| 画布 | `#000000` | 所有页面、所有区域 |
| 表面色 | `#16181a` (surface-elevated) | 卡片/面板背景 |
| 表面色-软 | `#0a0a0a` (surface-deep) | 略微抬升的子面板 |
| 品牌色 | `#494fdf` (Cobalt Violet) | 强调按钮、激活态标签、品牌标识 |
| 字体：标题 | Aeonik Pro 500 | 所有 20px+ 显示文本 |
| 字体：正文 | Inter 400 / 600 | 所有正文、按钮、标签、图表注释 |
| 按钮形状 | `rounded: 9999px` (pill) | 所有按钮 |
| 卡片圆角 | `rounded: lg` (20px) | 数据卡片、面板容器 |
| 输入框圆角 | `rounded: md` (12px) | 表单输入、下拉选择 |
| 按钮高度 | 48px (primary) / 36px (pill-sm) | 主操作/次要操作 |
| 输入框高度 | 56px | 表单输入 |
| 无阴影 | — | 深度靠表面亮度分层 |
| 内容区宽度 | max 1200px | 页面内容容器 |
| 文本色 | `#ffffff` (on-dark) | 主文本 |
| 文本次色 | `rgba(255,255,255,0.72)` (on-dark-mute) | 辅助文本 |
| 发线 | `rgba(255,255,255,0.12)` (hairline-dark) | 卡片边框/分隔线 |

### 全局规则

1. **深色画布统一** — 所有页面 `#000000` 画布，卡片使用 `#16181a`/`#0a0a0a` 分层
2. **Cobalt Violet 稀缺** — 仅用于品牌标识、选中态、主 CTA，不上色大面积区域
3. **Pill 形状统一** — 所有按钮 `rounded: full`
4. **脑波彩色数据** — δ `#a0c3ec`、θ `#c4b5fd`、α `#ff7a17`、β `#ffc285`、γ `#7c3aed` 在黑画布上保持饱和度
5. **阶段色** — prep/flow/switch/recover/rest 保持 PLAN.md 定义的颜色映射

### 各页面适配要点

| 页面 | 主画布 | 关键差异 |
|------|--------|---------|
| index.html | 深色 `#000000` | 角色选择面板 + 房间状态 |
| master-panel.html | 深色 `#000000` | 全深色，含波形画布+EEG 数据卡片 |
| monitor-panel.html | 深色 `#000000` | 同 master，去掉上传状态指示 |
| subject-panel.html | 深色 `#000000` | 全深色，大倒计时+自评按钮 |
| console-panel.html | 深色 `#000000` | 全深色，表单表格全部在深色背景上 |

---

## 15. 变更范围总结

### 新增模块

| 模块 | 位置 | 工作量 |
|------|------|--------|
| ECS WebSocket 中继 + 房间管理 + 计时器 | cloud/index.js | ~400 行 |
| 实时指标计算引擎 | cloud/metrics.js | ~150 行 |
| 基线录制+恢复判定 | cloud/baseline.js | ~120 行 |
| MySQL 连接池 | cloud/db.js | ~60 行 |
| nginx 配置 | cloud/nginx.conf | 1 文件 |
| PM2 配置 | cloud/ecosystem.config.js | 1 文件 |
| MySQL 迁移脚本 | cloud/migrations/001_init.sql | 1 文件 (7 表) |
| **SPA 统一入口（4 角色面板）** | **web/index.html** | 全新 SPA（角色选择 + 上传监视 + 纯监视 + 受试者 + 控制台） |
| 主控机→ECS 转发模块 | bridge/index.js | ~150 行 |

### 修改模块

| 模块 | 变更 |
|------|------|
| public/index.html | 改造为统一入口 + 角色选择 |
| bridge/index.js | 增加 WS 客户端 → ECS 连接 + eeg_frame 上行 + UDP 标记接收 |

### 移除

| 模块 | 原因 |
|------|------|
| 加速度计面板 | Ganglion 无此硬件 |
| 阻抗模拟面板 | 改为"信号稳定性"（基于标准差） |
| FFT 频谱条 | 与频带功率信息重复 |
