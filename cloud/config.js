/**
 * ECS 云端配置
 */

// 数据库密码必须通过环境变量注入，缺失则启动失败（避免明文泄露）
const DB_PASS = process.env.DB_PASS || '';
if (!DB_PASS && process.env.NODE_ENV === 'production') {
  throw new Error('[FATAL] DB_PASS 环境变量未设置，拒绝启动');
}

module.exports = {
  WS_PORT: parseInt(process.env.WS_PORT || '8080', 10),

  // EEG 采样率（统一在此管理，metrics.js 引用）
  EEG_SAMPLE_RATE: parseInt(process.env.EEG_SAMPLE_RATE || '120', 10),

  DB: {
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'eeg',
    password: DB_PASS,
    database: process.env.DB_NAME || 'eeg_platform',
    connectionLimit: 10,
    waitForConnections: true,
    queueLimit: 100,
    acquireTimeout: 10000,
  },

  // 主控机默认 UDP Marker 端口模板（以 hello 中上报的为准）
  UDP_MARKER_PORT: 12345,
};
