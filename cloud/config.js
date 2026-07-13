/**
 * ECS 云端配置
 */
module.exports = {
  WS_PORT: parseInt(process.env.WS_PORT || '8080', 10),

  DB: {
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'eeg',
    password: process.env.DB_PASS || '',
    database: process.env.DB_NAME || 'eeg_platform',
    connectionLimit: 10,
  },

  // 主控机默认 UDP Marker 端口模板（以 hello 中上报的为准）
  UDP_MARKER_PORT: 12345,
};
