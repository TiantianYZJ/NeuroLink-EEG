# NeuroLink 协作流程统一优化

## 设计决策

### 1. 控制台按钮合并
- 移除"创建实验场次"和"解锁房间并开始"两个独立按钮
- 合并为单个钴紫 pill 按钮"创建并解锁房间"
- 后端 `create_session` 执行后自动调用 `start_experiment`（一次性流程）

### 2. 锁蒙版全覆盖
- 当前：仅挡实验控制按钮
- 改为：所有面板（master/monitor/subject）在房间锁定状态下，dashboard 内容区覆盖半透明遮罩
- 蒙版样式：`rgba(0,0,0,0.8)` 背景、圆角 16px
- 居中元素：64px 白色 LOCK SVG → "实验未就绪"（uppercase 600）→ "请等待控制台配置实验" → 钴紫 spinner
- 房间页面不显示蒙版

### 3. Space 键切换
- `sendCmd` 根据 `timer.running` 状态自动选择：
  - 未运行 → `start`
  - 运行中 → `pause`

### 4. Header 实验元数据
- 上传端/监视端顶部栏靠右区域，在系统数据之前：
  - `[模板名] [受试者]` 两个小药丸标签
  - 使用 DESIGN.md `sub-nav-pill` 风格
