/**
 * MySQL 连接池封装
 */
const mysql = require('mysql');
const config = require('./config');

const pool = mysql.createPool(config.DB);

// 连接池事件监听（debug 日志，便于排查连接泄漏）
pool.on('connection', (connection) => {
  console.debug('[DB] pool: new connection id=%d', connection.threadId);
});
pool.on('acquire', (connection) => {
  console.debug('[DB] pool: acquire connection id=%d', connection.threadId);
});
pool.on('release', (connection) => {
  console.debug('[DB] pool: release connection id=%d', connection.threadId);
});

module.exports = {
  query(sql, params) {
    return new Promise((resolve, reject) => {
      pool.query(sql, params, (err, rows) => {
        if (err) reject(err); else resolve(rows);
      });
    });
  },
};
