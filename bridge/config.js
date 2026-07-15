/**
 * 主控机本地桥接配置
 */

// 端口解析 helper：避免 NaN 与非法值
function parsePort(env, def) {
  const n = parseInt(process.env[env] || String(def), 10);
  return Number.isFinite(n) && n > 0 ? n : def;
}

module.exports = {
  // OpenBCI UDP 数据监听端口（OpenBCI GUI 的 UDP_OUT 需设为此端口）
  // 注意: GUI_MARKER_PORT 默认 12346 避免与此冲突
  UDP_LISTEN_PORT: parsePort('UDP_PORT', 12345),

  // 本地 WebSocket 端口（供本地面板连接，低延迟渲染波形）
  LOCAL_WS_PORT: parsePort('LOCAL_WS_PORT', 9088), // 与 ECS 8080 区分（避免 NahimicService 占用 9080）

  // 设备类型
  DEVICE_TYPE: (process.env.DEVICE_TYPE || 'ganglion').toLowerCase(),

  // 真实采样率（上报给云端，运行时统计仅作校验）
  SAMPLE_RATE: parseInt(process.env.SAMPLE_RATE || '200', 10) || 200,

  // ECS 云端 WebSocket 地址
  ECS_WS_URL: process.env.ECS_WS_URL || 'wss://eeg.yzjtiantian.cn/ws',

  // OpenBCI GUI Marker 端口（ECS 回传标记写入目标）
  // 默认 12346 — OpenBCI GUI 的 "UDP Marker Port" 需设为此值, 与 UDP 数据端口(12345) 区分
  GUI_MARKER_PORT: parsePort('GUI_MARKER_PORT', 12346),

  // OpenBCI GUI Marker 目标主机（默认 127.0.0.1）
  GUI_MARKER_HOST: process.env.GUI_MARKER_HOST || '127.0.0.1',

  // 加速度计 UDP 端口（与 simulate-ganglion.js 默认 12347 保持一致）
  ACCEL_PORT: parsePort('ACCEL_UDP_PORT', 12347),

  // 指定 LAN IP（多网卡环境下避免选错，留空则自动检测）
  LAN_IP: process.env.LAN_IP || '',

  // 固定 session_id（不设置则自动生成）
  SESSION_ID: process.env.SESSION_ID || '',
};
