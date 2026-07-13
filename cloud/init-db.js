/**
 * 数据库初始化脚本
 * 用法: node init-db.js [db_password]
 */
const mysql = require('mysql');
const config = require('./config');
const fs = require('fs');
const path = require('path');

const password = process.argv[2] || config.DB.password || '';
const DB_NAME = config.DB.database;

// 先创建数据库（无 DB 连接）
const conn = mysql.createConnection({
  host: config.DB.host,
  user: config.DB.user,
  password: password,
  multipleStatements: true,
});

conn.connect(err => {
  if (err) {
    console.error('连接 MySQL 失败:', err.message);
    process.exit(1);
  }
  console.log('已连接到 MySQL');

  conn.query(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` DEFAULT CHARSET utf8`, err => {
    if (err) { console.error('创建数据库失败:', err.message); process.exit(1); }
    console.log(`数据库 ${DB_NAME} 已就绪`);
    conn.changeUser({ database: DB_NAME }, err => {
      if (err) { console.error('切换数据库失败:', err.message); process.exit(1); }

      // 执行迁移脚本
      const sqlPath = path.join(__dirname, 'migrations', '001_init.sql');
      const sql = fs.readFileSync(sqlPath, 'utf8');
      conn.query(sql, (err, results) => {
        if (err) { console.error('建表失败:', err.message); process.exit(1); }
        console.log('所有表已创建完成');
        conn.end();
        console.log('数据库初始化完成 ✓');
      });
    });
  });
});
