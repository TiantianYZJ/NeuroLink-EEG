module.exports = {
  apps: [{
    name: 'eeg-cloud',
    script: 'index.js',
    instances: 1,
    exec_mode: 'fork',
    env: {
      NODE_ENV: 'production',
      WS_PORT: 8080,
      DB_HOST: 'localhost',
      DB_USER: 'eeg',
      DB_PASS: 'fz4Kp3aCkHGYHzZJ',
      DB_NAME: 'eeg_platform',
    },
  }],
};
