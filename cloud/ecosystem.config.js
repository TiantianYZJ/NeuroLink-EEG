module.exports = {
  apps: [{
    name: 'eeg-cloud',
    script: 'index.js',
    instances: 1,
    exec_mode: 'fork',
    max_memory_restart: '512M',
    error_file: '/var/log/eeg-cloud/err.log',
    out_file: '/var/log/eeg-cloud/out.log',
    merge_logs: true,
    autorestart: true,
    env: {
      NODE_ENV: 'production',
      WS_PORT: 8080,
      DB_HOST: 'localhost',
      DB_USER: 'eeg',
      // 密码通过外部环境注入（pm2 启动前 export DB_PASS=xxx），不在配置中硬编码
      DB_PASS: process.env.DB_PASS || '',
      DB_NAME: 'eeg_platform',
      EEG_SAMPLE_RATE: 120,
    },
  }],
};
