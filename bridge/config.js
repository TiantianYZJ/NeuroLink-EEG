/**
 * 主控机本地桥接配置
 */
module.exports = {
  // OpenBCI UDP 数据监听端口（OpenBCI GUI 的 UDP_OUT 需设为此端口）
  UDP_LISTEN_PORT: parseInt(process.env.UDP_PORT || '12345', 10),

  // 本地 WebSocket 端口（供本地面板连接，低延迟渲染波形）
  LOCAL_WS_PORT: parseInt(process.env.LOCAL_WS_PORT || '9080', 10), // 与 ECS 8080 区分

  // 设备类型
  DEVICE_TYPE: (process.env.DEVICE_TYPE || 'ganglion').toLowerCase(),

  // ECS 云端 WebSocket 地址
  ECS_WS_URL: process.env.ECS_WS_URL || 'ws://eeg.yzjtiantian.cn/ws',

  // OpenBCI GUI Marker 端口（ECS 回传标记写入目标）
  GUI_MARKER_PORT: parseInt(process.env.GUI_MARKER_PORT || '12345', 10),

  // 固定 session_id（不设置则自动生成）
  SESSION_ID: process.env.SESSION_ID || '',
};
