# NeuroLink · PEYT-Studio · EEG BCI

> 基于云端三角色实时协作的 EEG 心流实验平台 —— 量化跨学科任务切换对心流状态的破坏程度与脑电恢复时长。

平台以 OpenBCI Ganglion 4 通道便携式 EEG 头环为采集前端，通过 UDP → 本地桥接 → 云端 WebSocket 中继的链路，将实时脑电波形、频带功率、认知负载指标同步到主控端、监视端、受试者端与实验控制台，支撑"诱发 → 切换 → 恢复"递进式心流实验范式。

---

## 目录

- [研究背景](#研究背景)
- [核心特性](#核心特性)
- [系统架构](#系统架构)
- [技术栈](#技术栈)
- [目录结构](#目录结构)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [部署](#部署)
- [WebSocket 协议](#websocket-协议)
- [相关文档](#相关文档)

---

## 研究背景

本项目落地 [PLAN.md](./PLAN.md) 中定义的跨学科心流实验：

- **核心问题**：跨学科任务切换是否显著破坏心流稳态？不同切换类型（文理 / 理艺 / 文艺）破坏程度是否不同？EEG 指标恢复需多久？
- **实验范式**：稳态心流诱发（8min）→ 任务切换干预（2min）→ 心流恢复观测（10min）× 3 轮
- **核心对比**：同学科连续任务（对照组） vs. 跨学科切换任务（实验组）
- **量化指标**：Theta/Alpha 比值、Alpha/Beta/Gamma 波能量、谱熵、认知负载指数、EEG 恢复时长（连续 30s 回归基线 ±5%）

## 核心特性

- **三角色实时协作** —— 上传监视端（master）、纯监视端（monitor）、受试者端（subject）+ 实验控制台（console），统一入口 SPA，WebSocket 全程在线。
- **实时 EEG 数据流** —— 4 通道 × 200Hz 采集，UDP → 本地桥接 → 云端广播，端到端低延迟。
- **实时指标引擎** —— Goertzel 算法计算 δ/θ/α/β/γ 五频带功率（每通道独立）、谱熵、认知负载指数，每秒输出快照。
- **基线与恢复判定** —— flow 阶段后 4 分钟录制 θ/α 基线，recover 阶段每秒判定是否连续 30s 回归基线 ±5%，自动写入 marker。
- **计时器仲裁** —— 计时器运行于 ECS 端，每秒广播 `phase_sync`，消除设备间时差。
- **Marker 双向流转** —— 操作员 / 受试者 / 自动 marker 经云端广播并 UDP 回传主控机写入 OpenBCI CSV。
- **FSS 心流量表** —— 受试者端电子问卷，结果入库交叉验证。
- **Revolut 设计语言** —— 真黑 `#000000` 画布 + 钴紫 `#494fdf` 品牌印记，Inter 字体，全胶囊按钮，无投影色块层次（详见 [DESIGN.md](./DESIGN.md)）。
- **多 session 隔离** —— 缓冲区、基线、恢复状态均以 `sessionId` 为 key，杜绝并发跨会话覆盖。
- **OpenBCI Ganglion 模拟器** —— 内置 `simulate-ganglion.js`，无硬件也可联调。

## 系统架构

```
┌─ 上传监视端 (master) ────────────────────────────┐
│  OpenBCI Ganglion ─UDP─→ bridge/index.js         │
│  本地 WS Server (9080) → 本地面板低延迟渲染        │
│  WS Client → ECS 云端 (上传 EEG + 标记)            │
└───────────────────────┬──────────────────────────┘
                        │
                        ▼
┌─ ECS 云端 (cloud/) ──────────────────────────────┐
│  WebSocket Server (8080) + nginx 反代 + HTTPS     │
│  · role_claim → 角色分配 + 房间管理 (断线 30s 保留) │
│  · eeg_frame → 广播 + 指标引擎 → MySQL            │
│  · marker → 广播 + UDP 回传主控机                 │
│  · cmd → 计时器引擎 → phase_sync 每秒广播          │
│  · 基线引擎 + 恢复判定引擎                         │
│  · fss_submit / 控制台 CRUD → MySQL               │
└──┬──────────────┬──────────────┬─────────────────┘
   ▼              ▼              ▼
 纯监视端       受试者端       实验控制台
 (monitor)     (subject)      (console)
 0-N 台         1 台           1 台
```

| 角色 | 设备 | 职责 | 数量 |
|------|------|------|------|
| **master** | 接 OpenBCI 的笔记本 | UDP↔WS 桥接、上传 EEG、录制 CSV+标记、全功能控制面板 | 1 |
| **monitor** | 手机 / 平板 / 浏览器 | 实时波形查看、可发控制指令，**无上传** | 0-N |
| **subject** | 受试者面前设备 | 显示任务信息、自评标记、FSS 问卷 | 1 |
| **console** | 操作员浏览器 | 创建 / 配置实验、管理受试者、查看历史场次 | 1 |

## 技术栈

| 层 | 技术 |
|----|------|
| 采集硬件 | OpenBCI Ganglion（4 通道，200Hz，蓝牙） |
| 采集软件 | OpenBCI GUI（UDP OUT + Marker Port） |
| 桥接 (bridge) | Node.js + `ws` + `dgram`（UDP→WS） |
| 云端 (cloud) | Node.js + `ws` + `mysql` + nginx + PM2 |
| 数据库 | MySQL 5.5（subjects / sessions / markers / fss_results / experiment_templates） |
| 前端 (web) | 原生 HTML/CSS/JS + Chart.js，SPA 单页应用 |
| 部署 | 阿里云 ECS 2C2G + Let's Encrypt HTTPS |
| 设计系统 | Revolut Design Language（详见 [DESIGN.md](./DESIGN.md)） |

## 目录结构

```
AI for EEG/
├── bridge/                   # 主控机本地桥接（UDP → WS）
│   ├── config.js             # 桥接配置（端口 / ECS 地址 / 设备类型）
│   ├── index.js              # 桥接主程序 + 本地 WS Server
│   └── package.json
├── cloud/                    # ECS 云端（中继 + 计算 + 存储）
│   ├── config.js             # 云端配置（WS 端口 / DB / 采样率）
│   ├── db.js                 # MySQL 连接池封装
│   ├── index.js              # WS 中继 + 房间 + 计时器 + marker 路由
│   ├── metrics.js            # 实时指标引擎（Goertzel 频带功率 / 谱熵 / 负载）
│   ├── baseline.js           # 基线录制 + 恢复判定引擎
│   ├── migrations/001_init.sql  # 建表语句
│   ├── ecosystem.config.js   # PM2 进程配置
│   └── nginx.conf            # nginx 反代配置
├── web/                      # 前端 SPA
│   ├── index.html            # 统一入口 + 4 角色面板
│   ├── chart.min.js          # Chart.js 本地副本
│   └── PEYT-Studio-LOGO.png
├── docs/                     # 设计与接入文档
│   ├── specs/                # 架构与迭代设计稿
│   ├── CODE_WIKI.md          # 代码百科
│   └── third-party-integration.md  # 第三方接入指南
├── scripts/
│   └── validate-css-classes.py  # HTML class 定义校验
├── simulate-ganglion.js      # OpenBCI Ganglion 模拟器
├── PLAN.md                   # 实验方案
├── DESIGN.md                 # 设计系统规范
├── OpenBCI_README.md         # OpenBCI GUI 使用指南
└── package.json
```

## 快速开始

### 1. 环境准备

- Node.js ≥ 16
- MySQL ≥ 5.5
- OpenBCI GUI（真实硬件）或直接使用内置模拟器

### 2. 初始化数据库

```bash
mysql -u root -p < cloud/migrations/001_init.sql
```

### 3. 启动云端

```bash
cd cloud
npm install
export DB_PASS='your_password'   # 生产环境必填
export EEG_SAMPLE_RATE=120
npm start                        # 监听 8080
```

### 4. 启动桥接（主控机）

```bash
cd bridge
npm install
# 真实硬件：先启动 OpenBCI GUI 并配置 UDP OUT 到 12345
# 模拟数据：另开终端运行 node simulate-ganglion.js
npm start                        # 本地 WS 9080 + 连接 ECS
```

### 5. 打开前端

访问云端 nginx 暴露的地址（生产环境为 `https://eeg.yzjtiantian.cn/`），本地开发可直接打开 `web/index.html` 并修改 WS 地址。

### 6. 无硬件联调（模拟器）

```bash
node simulate-ganglion.js --rate 200 --port 12345
```

## 配置说明

### bridge/config.js

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `UDP_PORT` | 12345 | OpenBCI UDP 数据端口 |
| `LOCAL_WS_PORT` | 9080 | 本地面板 WS 端口 |
| `ECS_WS_URL` | `wss://eeg.yzjtiantian.cn/ws` | ECS 云端 WS 地址 |
| `DEVICE_TYPE` | `ganglion` | 设备类型 |
| `SAMPLE_RATE` | 200 | 标称采样率 |
| `GUI_MARKER_PORT` | 12346 | OpenBCI GUI Marker 回传端口 |
| `ACCEL_UDP_PORT` | 12347 | 加速度计 UDP 端口 |
| `SESSION_ID` | （自动生成） | 固定 session_id |

### cloud/config.js

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `WS_PORT` | 8080 | WS 监听端口 |
| `EEG_SAMPLE_RATE` | 120 | 指标计算采样率 |
| `DB_HOST` / `DB_USER` / `DB_NAME` | localhost / eeg / eeg_platform | 数据库连接 |
| `DB_PASS` | — | **生产环境必填**，缺失即拒绝启动 |

## 部署

生产环境使用 PM2 + nginx：

```bash
# 云端
cd cloud
export DB_PASS='your_password'
pm2 start ecosystem.config.js

# nginx 配置（见 cloud/nginx.conf）
# - 静态文件 root → /var/www/eeg/web
# - /ws 反代 → 127.0.0.1:8080（proxy_buffering off 保证低延迟）
# - HTTP 80 强制跳转 HTTPS 443
```

**打包发布**（按项目规范生成 `deploy.zip`，仅含 12 个运行时文件）：

```bash
python scripts/validate-css-classes.py   # 打包前必做 CSS 校验
# 然后按 project_memory 中的打包脚本生成 deploy.zip
```

## WebSocket 协议

云端 WS Server 处理 26 类顶层消息，主要分类：

| 类别 | 消息类型 |
|------|----------|
| 连接 / 心跳 | `hello` · `reconnect` · `ping` · `pong` |
| 角色与房间 | `claim_role` · `release_role` · `leave_room` · `create_room` · `join_room` |
| EEG 数据 | `eeg_frame` · `accel_frame` |
| 实验控制 | `cmd`（start / pause / next_phase / reset / set_auto / set_task_type）· `start_experiment` · `end_experiment` |
| 标记与问卷 | `marker` · `self_report` · `fss_submit` |
| 设备与配置 | `set_udp_target` · `update_device_info` · `refresh` · `update_config_meta` |
| 控制台 CRUD | `list_subjects` · `create_subject` · `list_templates` · `create_session` · `list_sessions` · `get_session_detail` · `update_session_status` |

完整字段定义与第三方接入示例见 [docs/third-party-integration.md](./docs/third-party-integration.md)。

## 相关文档

- [PLAN.md](./PLAN.md) —— 实验方案（研究问题、变量、流程、分析方法）
- [DESIGN.md](./DESIGN.md) —— Revolut 设计系统规范（颜色 / 字体 / 组件）
- [OpenBCI_README.md](./OpenBCI_README.md) —— OpenBCI GUI 使用指南
- [docs/specs/](./docs/specs/) —— 架构设计与迭代稿
- [docs/third-party-integration.md](./docs/third-party-integration.md) —— 第三方接入指南
- [docs/CODE_WIKI.md](./docs/CODE_WIKI.md) —— 代码百科
