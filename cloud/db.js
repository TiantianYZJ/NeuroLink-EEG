/**
 * MySQL 连接池封装
 */
const mysql = require('mysql');
const config = require('./config');

const pool = mysql.createPool(config.DB);

module.exports = {
  query(sql, params) {
    return new Promise((resolve, reject) => {
      pool.query(sql, params, (err, rows) => {
        if (err) reject(err); else resolve(rows);
      });
    });
  },
};
