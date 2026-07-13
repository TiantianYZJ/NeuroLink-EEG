/**
 * 数据库初始化 + 种子数据脚本
 * 用法: node init-db.js [db_password]
 */
const mysql = require('mysql');
const config = require('./config');
const fs = require('fs');
const path = require('path');

const password = process.argv[2] || config.DB.password || '';
const DB_NAME = config.DB.database;

const conn = mysql.createConnection({
  host: config.DB.host,
  user: config.DB.user,
  password: password,
  multipleStatements: true,
});

conn.connect(err => {
  if (err) { console.error('连接 MySQL 失败:', err.message); process.exit(1); }
  console.log('已连接到 MySQL');

  conn.query(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` DEFAULT CHARSET utf8`, err => {
    if (err) { console.error('创建数据库失败:', err.message); process.exit(1); }
    console.log(`数据库 ${DB_NAME} 已就绪`);
    conn.changeUser({ database: DB_NAME }, err => {
      if (err) { console.error('切换数据库失败:', err.message); process.exit(1); }

      const sqlPath = path.join(__dirname, 'migrations', '001_init.sql');
      const sql = fs.readFileSync(sqlPath, 'utf8');
      conn.query(sql, (err) => {
        if (err) { console.error('建表失败:', err.message); process.exit(1); }
        console.log('所有表已创建完成');

        // ── 种子数据：4 种实验模板 ──
        const templates = [
          {
            name: '对照组',
            group_type: 'control', switch_type: 'none',
            phases: [
              {id:'prep',duration:300,round:0,task_type:null,name:'准备阶段'},
              {id:'flow1',duration:480,round:1,task_type:'math',name:'心流诱导阶段·数理'},
              {id:'switch1',duration:120,round:1,task_type:'math',name:'任务继续阶段'},
              {id:'recover1',duration:600,round:1,task_type:'math',name:'状态恢复观测'},
              {id:'rest1',duration:180,round:1,task_type:null,name:'休息与问卷'},
              {id:'flow2',duration:480,round:2,task_type:'math',name:'心流诱导阶段·数理'},
              {id:'switch2',duration:120,round:2,task_type:'math',name:'任务继续阶段'},
              {id:'recover2',duration:600,round:2,task_type:'math',name:'状态恢复观测'},
              {id:'rest2',duration:180,round:2,task_type:null,name:'休息与问卷'},
              {id:'flow3',duration:480,round:3,task_type:'math',name:'心流诱导阶段·数理'},
              {id:'switch3',duration:120,round:3,task_type:'math',name:'任务继续阶段'},
              {id:'recover3',duration:600,round:3,task_type:'math',name:'状态恢复观测'},
              {id:'rest3',duration:180,round:3,task_type:null,name:'休息与问卷'},
            ],
          },
          {
            name: '文理切换组',
            group_type: 'experiment', switch_type: 'math_lang',
            phases: [
              {id:'prep',duration:300,round:0,task_type:null,name:'准备阶段'},
              {id:'flow1',duration:480,round:1,task_type:'math',name:'心流诱导·数理'},
              {id:'switch1',duration:120,round:1,task_type:'language',name:'切换数理→语文'},
              {id:'recover1',duration:600,round:1,task_type:'math',name:'状态恢复观测'},
              {id:'rest1',duration:180,round:1,task_type:null,name:'休息与问卷'},
              {id:'flow2',duration:480,round:2,task_type:'math',name:'心流诱导·数理'},
              {id:'switch2',duration:120,round:2,task_type:'language',name:'切换数理→语文'},
              {id:'recover2',duration:600,round:2,task_type:'math',name:'状态恢复观测'},
              {id:'rest2',duration:180,round:2,task_type:null,name:'休息与问卷'},
              {id:'flow3',duration:480,round:3,task_type:'math',name:'心流诱导·数理'},
              {id:'switch3',duration:120,round:3,task_type:'language',name:'切换数理→语文'},
              {id:'recover3',duration:600,round:3,task_type:'math',name:'状态恢复观测'},
              {id:'rest3',duration:180,round:3,task_type:null,name:'休息与问卷'},
            ],
          },
          {
            name: '理艺切换组',
            group_type: 'experiment', switch_type: 'math_art',
            phases: [
              {id:'prep',duration:300,round:0,task_type:null,name:'准备阶段'},
              {id:'flow1',duration:480,round:1,task_type:'math',name:'心流诱导·数理'},
              {id:'switch1',duration:120,round:1,task_type:'art',name:'切换数理→艺术'},
              {id:'recover1',duration:600,round:1,task_type:'math',name:'状态恢复观测'},
              {id:'rest1',duration:180,round:1,task_type:null,name:'休息与问卷'},
              {id:'flow2',duration:480,round:2,task_type:'math',name:'心流诱导·数理'},
              {id:'switch2',duration:120,round:2,task_type:'art',name:'切换数理→艺术'},
              {id:'recover2',duration:600,round:2,task_type:'math',name:'状态恢复观测'},
              {id:'rest2',duration:180,round:2,task_type:null,name:'休息与问卷'},
              {id:'flow3',duration:480,round:3,task_type:'math',name:'心流诱导·数理'},
              {id:'switch3',duration:120,round:3,task_type:'art',name:'切换数理→艺术'},
              {id:'recover3',duration:600,round:3,task_type:'math',name:'状态恢复观测'},
              {id:'rest3',duration:180,round:3,task_type:null,name:'休息与问卷'},
            ],
          },
          {
            name: '文艺切换组',
            group_type: 'experiment', switch_type: 'lang_art',
            phases: [
              {id:'prep',duration:300,round:0,task_type:null,name:'准备阶段'},
              {id:'flow1',duration:480,round:1,task_type:'language',name:'心流诱导·语文'},
              {id:'switch1',duration:120,round:1,task_type:'art',name:'切换语文→艺术'},
              {id:'recover1',duration:600,round:1,task_type:'language',name:'状态恢复观测'},
              {id:'rest1',duration:180,round:1,task_type:null,name:'休息与问卷'},
              {id:'flow2',duration:480,round:2,task_type:'language',name:'心流诱导·语文'},
              {id:'switch2',duration:120,round:2,task_type:'art',name:'切换语文→艺术'},
              {id:'recover2',duration:600,round:2,task_type:'language',name:'状态恢复观测'},
              {id:'rest2',duration:180,round:2,task_type:null,name:'休息与问卷'},
              {id:'flow3',duration:480,round:3,task_type:'language',name:'心流诱导·语文'},
              {id:'switch3',duration:120,round:3,task_type:'art',name:'切换语文→艺术'},
              {id:'recover3',duration:600,round:3,task_type:'language',name:'状态恢复观测'},
              {id:'rest3',duration:180,round:3,task_type:null,name:'休息与问卷'},
            ],
          },
        ];

        conn.query('SELECT COUNT(*) AS cnt FROM experiment_templates', (err, rows) => {
          if (err) { console.warn('查询模板数失败:', err.message); }
          if (!err && rows[0].cnt === 0) {
            const stmt = 'INSERT INTO experiment_templates (name, group_type, switch_type, phases_json) VALUES ?';
            const values = templates.map(t => [t.name, t.group_type, t.switch_type, JSON.stringify(t.phases)]);
            conn.query(stmt, [values], (err) => {
              if (err) console.warn('插入模板种子数据失败:', err.message);
              else console.log('已插入 ' + templates.length + ' 组实验模板');
              finish();
            });
          } else {
            console.log('模板数据已存在，跳过');
            finish();
          }
        });
      });
    });
  });
});

function finish() {
  conn.end();
  console.log('数据库初始化完成 ✓');
}
