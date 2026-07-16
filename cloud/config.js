/**
 * ECS 云端配置
 * 支持从 .env 文件读取环境变量（与系统环境变量合并，.env 优先级更高）
 * .env 文件格式: KEY=VALUE，每行一个，放在 cloud/ 目录下
 */

// ── 加载 .env 文件（如果存在） ──
try {
  const fs = require('fs');
  const path = require('path');
  const envPath = path.join(__dirname, '.env');
  if (fs.existsSync(envPath)) {
    const lines = fs.readFileSync(envPath, 'utf8').split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#') || !trimmed.includes('=')) continue;
      const eq = trimmed.indexOf('=');
      const key = trimmed.substring(0, eq).trim();
      let val = trimmed.substring(eq + 1).trim();
      // 移除首尾引号
      if ((val.startsWith("'") && val.endsWith("'")) || (val.startsWith('"') && val.endsWith('"')))
        val = val.slice(1, -1);
      if (!process.env[key]) process.env[key] = val;
    }
  }
} catch (_) {}

// 数据库密码必须通过环境变量注入，缺失则启动失败（避免明文泄露）
const DB_PASS = process.env.DB_PASS || '';
if (!DB_PASS && process.env.NODE_ENV === 'production') {
  throw new Error('[FATAL] DB_PASS 环境变量未设置，拒绝启动');
}

module.exports = {
  WS_PORT: parseInt(process.env.WS_PORT || '8080', 10),

  // EEG 采样率（Ganglion = 200Hz，统一在此管理，metrics.js 引用）
  EEG_SAMPLE_RATE: parseInt(process.env.EEG_SAMPLE_RATE || '200', 10),

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
