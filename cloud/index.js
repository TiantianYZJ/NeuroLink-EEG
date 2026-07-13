/**
 * ECS 云端 WebSocket 中继
 *
 * 职责:
 *   - 统一入口 → 角色选择 → 各角色面板 (SPA)
 *   - 角色认领 + 房间管理 (断线保留 30 秒, 基于时间戳)
 *   - eeg_frame 广播 + 指标计算 (多 session 安全)
 *   - 计时器仲裁 + phase_sync (广播顺序: end→start→sync)
 *   - marker 广播 + 持久化 + UDP 回传 (过滤 master 来源)
 *   - 基线录制 + 恢复判定
 *   - cmd 权限白名单
 *   - 控制台 CRUD
 *   - 计时器状态持久化 (INSERT ... ON DUPLICATE KEY UPDATE)
 */

const { WebSocketServer } = require('ws');
const dgram = require('dgram');
const config = require('./config');
const db = require('./db');
const metrics = require('./metrics');
const baseline = require('./baseline');

// ── 共享 UDP socket（避免每次 marker 创建新 socket） ──
const udpSock = dgram.createSocket('udp4');
udpSock.on('error', () => {});

// ── 定期清理已完成的空房间（每 10 分钟） ──
setInterval(() => {
  for (const [sid, room] of rooms) {
    if (room.sockets.size === 0) {
      const timer = timers.get(sid);
      if (timer && timer.completed) cleanupSession(sid);
    }
  }
}, 600000).unref();

// ── 房间 ──
const rooms = new Map(); // sessionId → room

// 角色槽位锁: sessionId → { role: lockExpireAt }
const roleLocks = new Map();

function getRoom(sessionId) {
  if (!rooms.has(sessionId)) {
    rooms.set(sessionId, {
      sockets: new Set(),
      occupants: { master: null, monitor: [], subject: null, console: null },
      udpTargets: new Map(),
    });
  }
  return rooms.get(sessionId);
}

function broadcast(room, msg, exclude = null) {
  const payload = typeof msg === 'string' ? msg : JSON.stringify(msg);
  room.sockets.forEach(ws => {
    if (ws !== exclude && ws.readyState === 1) ws.send(payload);
  });
}

function broadcastToRoles(room, msg, roles) {
  const payload = typeof msg === 'string' ? msg : JSON.stringify(msg);
  room.sockets.forEach(ws => {
    if (ws.readyState === 1 && roles.includes(ws.role)) ws.send(payload);
  });
}

function getOccupantSummary(room) {
  return {
    master: !!room.occupants.master,
    monitor: room.occupants.monitor.filter(s => s.readyState === 1 || s.readyState === 2).length,
    subject: !!room.occupants.subject,
    console: !!room.occupants.console,
  };
}

// ── 角色锁管理 ──
function isRoleLocked(sessionId, role) {
  const locks = roleLocks.get(sessionId);
  if (!locks || !locks[role]) return false;
  if (Date.now() < locks[role]) return true;
  delete locks[role];
  return false;
}

function lockRole(sessionId, role, ms) {
  if (!roleLocks.has(sessionId)) roleLocks.set(sessionId, {});
  roleLocks.get(sessionId)[role] = Date.now() + ms;
}

function unlockRole(sessionId, role) {
  const locks = roleLocks.get(sessionId);
  if (locks) delete locks[role];
}

// ── 计时器持久化 ──
function saveTimerState(sessionId) {
  const timer = timers.get(sessionId);
  if (!timer) return;
  const templateType = Object.keys(PHASE_TEMPLATES).find(k => PHASE_TEMPLATES[k] === timer.phases) || 'control';
  db.query(
    `INSERT INTO timer_state (session_id, phase_index, time_left, time_in_phase, running, auto_mode, template_type)
     VALUES (?,?,?,?,?,?,?)
     ON DUPLICATE KEY UPDATE phase_index=VALUES(phase_index), time_left=VALUES(time_left),
       time_in_phase=VALUES(time_in_phase), running=VALUES(running),
       auto_mode=VALUES(auto_mode), template_type=VALUES(template_type)`,
    [sessionId, timer.phaseIndex, timer.timeLeft, timer.timeInPhase,
     timer.running ? 1 : 0, timer.autoMode ? 1 : 0, templateType]
  ).catch(e => console.warn('[DB]', e.message));
}

// ── 阶段模板 ──
const PHASE_TEMPLATES = {
  control: [
    { id: 'prep',    duration: 300, round: 0, name: '准备阶段',        taskType: null },
    { id: 'flow1',   duration: 480, round: 1, name: '心流诱导阶段',    taskType: 'math' },
    { id: 'switch1', duration: 120, round: 1, name: '任务继续阶段',    taskType: 'math' },
    { id: 'recover1',duration: 600, round: 1, name: '状态恢复观测',    taskType: 'math' },
    { id: 'rest1',   duration: 180, round: 1, name: '休息与问卷',      taskType: null },
    { id: 'flow2',   duration: 480, round: 2, name: '心流诱导阶段',    taskType: 'math' },
    { id: 'switch2', duration: 120, round: 2, name: '任务继续阶段',    taskType: 'math' },
    { id: 'recover2',duration: 600, round: 2, name: '状态恢复观测',    taskType: 'math' },
    { id: 'rest2',   duration: 180, round: 2, name: '休息与问卷',      taskType: null },
    { id: 'flow3',   duration: 480, round: 3, name: '心流诱导阶段',    taskType: 'math' },
    { id: 'switch3', duration: 120, round: 3, name: '任务继续阶段',    taskType: 'math' },
    { id: 'recover3',duration: 600, round: 3, name: '状态恢复观测',    taskType: 'math' },
    { id: 'rest3',   duration: 180, round: 3, name: '休息与问卷',        taskType: null },
  ],
  math_art: [
    { id: 'prep',    duration: 300, round: 0, name: '准备阶段',          taskType: null },
    { id: 'flow1',   duration: 480, round: 1, name: '心流诱导·数理',    taskType: 'math' },
    { id: 'switch1', duration: 120, round: 1, name: '切换数理→艺术',    taskType: 'art' },
    { id: 'recover1',duration: 600, round: 1, name: '状态恢复观测',     taskType: 'math' },
    { id: 'rest1',   duration: 180, round: 1, name: '休息与问卷',        taskType: null },
    { id: 'flow2',   duration: 480, round: 2, name: '心流诱导·数理',    taskType: 'math' },
    { id: 'switch2', duration: 120, round: 2, name: '切换数理→艺术',    taskType: 'art' },
    { id: 'recover2',duration: 600, round: 2, name: '状态恢复观测',     taskType: 'math' },
    { id: 'rest2',   duration: 180, round: 2, name: '休息与问卷',        taskType: null },
    { id: 'flow3',   duration: 480, round: 3, name: '心流诱导·数理',    taskType: 'math' },
    { id: 'switch3', duration: 120, round: 3, name: '切换数理→艺术',    taskType: 'art' },
    { id: 'recover3',duration: 600, round: 3, name: '状态恢复观测',     taskType: 'math' },
    { id: 'rest3',   duration: 180, round: 3, name: '休息与问卷',        taskType: null },
  ],
  math_lang: [
    { id: 'prep',    duration: 300, round: 0, name: '准备阶段',          taskType: null },
    { id: 'flow1',   duration: 480, round: 1, name: '心流诱导·数理',    taskType: 'math' },
    { id: 'switch1', duration: 120, round: 1, name: '切换数理→语文',    taskType: 'language' },
    { id: 'recover1',duration: 600, round: 1, name: '状态恢复观测',     taskType: 'math' },
    { id: 'rest1',   duration: 180, round: 1, name: '休息与问卷',        taskType: null },
    { id: 'flow2',   duration: 480, round: 2, name: '心流诱导·数理',    taskType: 'math' },
    { id: 'switch2', duration: 120, round: 2, name: '切换数理→语文',    taskType: 'language' },
    { id: 'recover2',duration: 600, round: 2, name: '状态恢复观测',     taskType: 'math' },
    { id: 'rest2',   duration: 180, round: 2, name: '休息与问卷',        taskType: null },
    { id: 'flow3',   duration: 480, round: 3, name: '心流诱导·数理',    taskType: 'math' },
    { id: 'switch3', duration: 120, round: 3, name: '切换数理→语文',    taskType: 'language' },
    { id: 'recover3',duration: 600, round: 3, name: '状态恢复观测',     taskType: 'math' },
    { id: 'rest3',   duration: 180, round: 3, name: '休息与问卷',        taskType: null },
  ],
  lang_art: [
    { id: 'prep',    duration: 300, round: 0, name: '准备阶段',          taskType: null },
    { id: 'flow1',   duration: 480, round: 1, name: '心流诱导·语文',    taskType: 'language' },
    { id: 'switch1', duration: 120, round: 1, name: '切换语文→艺术',    taskType: 'art' },
    { id: 'recover1',duration: 600, round: 1, name: '状态恢复观测',     taskType: 'language' },
    { id: 'rest1',   duration: 180, round: 1, name: '休息与问卷',        taskType: null },
    { id: 'flow2',   duration: 480, round: 2, name: '心流诱导·语文',    taskType: 'language' },
    { id: 'switch2', duration: 120, round: 2, name: '切换语文→艺术',    taskType: 'art' },
    { id: 'recover2',duration: 600, round: 2, name: '状态恢复观测',     taskType: 'language' },
    { id: 'rest2',   duration: 180, round: 2, name: '休息与问卷',        taskType: null },
    { id: 'flow3',   duration: 480, round: 3, name: '心流诱导·语文',    taskType: 'language' },
    { id: 'switch3', duration: 120, round: 3, name: '切换语文→艺术',    taskType: 'art' },
    { id: 'recover3',duration: 600, round: 3, name: '状态恢复观测',     taskType: 'language' },
    { id: 'rest3',   duration: 180, round: 3, name: '休息与问卷',        taskType: null },
  ],
};

// ── 计时器 ──
const timers = new Map();

function initTimer(sessionId, templateType) {
  const phases = PHASE_TEMPLATES[templateType] || PHASE_TEMPLATES.control;
  const timer = {
    phases, phaseIndex: 0, timeLeft: phases[0].duration,
    timeInPhase: 0, running: false, autoMode: true, completed: false,
    tickTimer: null, templateType, taskType: phases[0].taskType,
  };
  timers.set(sessionId, timer);
  saveTimerState(sessionId);
  return timer;
}

function getCurrentPhase(timer) {
  return timer.phases[timer.phaseIndex] || timer.phases[0];
}

function advancePhase(sessionId, timer, room) {
  if (timer.phaseIndex >= timer.phases.length - 1) {
    timer.completed = true; timer.running = false;
    clearInterval(timer.tickTimer); timer.tickTimer = null;
    saveTimerState(sessionId);
    return;
  }
  timer.phaseIndex++;
  const phase = getCurrentPhase(timer);
  timer.timeLeft = phase.duration;
  timer.timeInPhase = 0;
  timer.taskType = phase.taskType;
  saveTimerState(sessionId);
}

function resetTimer(sessionId, timer, room) {
  clearInterval(timer.tickTimer);
  timer.phaseIndex = 0; timer.timeLeft = timer.phases[0].duration;
  timer.timeInPhase = 0; timer.running = false; timer.completed = false;
  timer.taskType = timer.phases[0].taskType;
  baseline.reset(sessionId);
  saveTimerState(sessionId);
}

function broadcastSync(sessionId, timer, room) {
  const phase = getCurrentPhase(timer);
  broadcast(room, {
    type: 'phase_sync',
    phase_index: timer.phaseIndex, phase_id: phase.id,
    phase_name: phase.name, round: phase.round,
    time_left: timer.timeLeft, time_in_phase: timer.timeInPhase,
    running: timer.running, auto_mode: timer.autoMode,
    completed: timer.completed, task_type: timer.taskType,
  });
}

function startTick(sessionId, timer, room) {
  if (timer.tickTimer) clearInterval(timer.tickTimer);
  timer.tickTimer = setInterval(async () => {
    const room = rooms.get(sessionId);
    if (!room) { clearInterval(timer.tickTimer); return; }

    if (timer.running && !timer.completed) {
      timer.timeLeft--;
      timer.timeInPhase++;
      saveTimerState(sessionId);

      const now = Date.now();
      const phase = getCurrentPhase(timer);

      // 1. 指标计算
      const snapshot = metrics.tick(now, sessionId, timer.phaseIndex, phase.id);

      if (snapshot) {
        // 2. metrics_snapshot → MySQL
        db.query(
          `INSERT INTO metrics_snapshots
           (session_id, ts, phase_index, phase_id, delta, theta, alpha, beta, gamma,
            theta_alpha_ratio, spectral_entropy, cognitive_load_index,
            sq_ch1, sq_ch2, sq_ch3, sq_ch4)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
          [sessionId, snapshot.ts, snapshot.phase_index, snapshot.phase_id,
           snapshot.band_power.delta, snapshot.band_power.theta,
           snapshot.band_power.alpha, snapshot.band_power.beta, snapshot.band_power.gamma,
           snapshot.theta_alpha_ratio, snapshot.spectral_entropy, snapshot.cognitive_load_index,
           snapshot.signal_quality[0], snapshot.signal_quality[1],
           snapshot.signal_quality[2], snapshot.signal_quality[3]]
        ).catch(e => console.warn('[DB]', e.message));

        // 3. metrics 广播 (master + monitor)
        broadcastToRoles(room, { type: 'metrics_snapshot', ...snapshot }, ['master', 'monitor']);

        // 4. 基线录制 + 恢复判定
        const msgs = await baseline.tick(sessionId, phase.id, timer.timeInPhase, snapshot);
        for (const m of msgs) {
          if (m.type === 'marker') {
            db.query(
              'INSERT INTO markers (session_id, code, source, label, phase, ts) VALUES (?,?,?,?,?,?)',
              [sessionId, m.code, m.source, m.label, m.phase, m.ts]
            ).catch(e => console.warn('[DB]', e.message));
            broadcast(room, m);
            sendMarkerUDP(room, m);
          } else {
            broadcastToRoles(room, m, ['master', 'monitor']);
          }
        }
      }

      // 5. 阶段切换: 先发 phase_end, 再 advance, 再 phase_start, 最后 phase_sync
      if (timer.timeLeft <= 0) {
        broadcast(room, {
          type: 'marker', code: 2, source: 'auto',
          label: 'phase_end:' + phase.id, phase: phase.id, ts: now,
        });
        advancePhase(sessionId, timer, room);
        if (!timer.completed) {
          const nextPhase = getCurrentPhase(timer);
          broadcast(room, {
            type: 'marker', code: 1, source: 'auto',
            label: 'phase_start:' + nextPhase.id, phase: nextPhase.id, ts: Date.now(),
          });
          if (!timer.autoMode) timer.running = false;
        }
      }
    }

    broadcastSync(sessionId, timer, room);
  }, 1000);
}

// ── UDP Marker 回传（仅非 master 来源） ──
function sendMarkerUDP(room, markerMsg) {
  if (markerMsg.source === 'master') return; // master 本地的标记不重复回传
  const target = room.udpTargets.get('master');
  if (!target) return;
  const [host, port] = target.split(':');
  if (!host || !port) return;
  try {
    const buf = Buffer.alloc(4);
    buf.writeFloatBE(markerMsg.code || 0);
    udpSock.send(buf, 0, 4, parseInt(port), host);
  } catch (_) {}
}

// ── Session 清理 ──
function cleanupSession(sessionId) {
  const timer = timers.get(sessionId);
  if (timer) { clearInterval(timer.tickTimer); }
  timers.delete(sessionId);
  metrics.cleanup(sessionId);
  baseline.cleanup(sessionId);
  rooms.delete(sessionId);
  roleLocks.delete(sessionId);
}

// ── 消息路由 ──
function handleMessage(ws, raw, room, sessionId) {
  let msg;
  try { msg = JSON.parse(raw); } catch (_) { return; }

  switch (msg.type) {

    case 'hello': {
      room.sockets.add(ws);
      ws.role = 'pending';
      ws.sessionId = sessionId;
      ws.send(JSON.stringify({ type: 'room_info', occupants: getOccupantSummary(room) }));
      break;
    }

    case 'reconnect': {
      const targetRole = msg.role;
      // 时间戳锁: 允许同 sessionId 重连, 拒绝被抢占的新声明
      if (!['master', 'subject', 'console'].includes(targetRole)) {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '不支持的重连角色' }));
        break;
      }
      // 立即清除旧锁, 允许重连
      unlockRole(sessionId, targetRole);
      // 如果槽位还有旧 socket, 释放它
      if (targetRole === 'master' && room.occupants.master) room.occupants.master = null;
      if (targetRole === 'subject' && room.occupants.subject) room.occupants.subject = null;
      if (targetRole === 'console' && room.occupants.console) room.occupants.console = null;

      room.sockets.add(ws);
      ws.role = targetRole;
      ws.sessionId = sessionId;
      ws.roleLock = targetRole;
      if (targetRole === 'master' && msg.udpTarget) {
        room.udpTargets.set('master', msg.udpTarget);
      }
      // 恢复槽位
      if (targetRole === 'master') room.occupants.master = ws;
      else if (targetRole === 'subject') room.occupants.subject = ws;
      else if (targetRole === 'console') room.occupants.console = ws;

      ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });

      const timer = timers.get(sessionId);
      if (timer && targetRole === 'master') { timer.running = false; broadcastSync(sessionId, timer, room); }
      break;
    }

    case 'claim_role': {
      const targetRole = msg.role;
      const occ = room.occupants;

      // 检查角色锁
      if (isRoleLocked(sessionId, targetRole)) {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '该角色正在重连保留中' }));
        break;
      }

      let canClaim = false;
      if (targetRole === 'master' && !occ.master) canClaim = true;
      else if (targetRole === 'subject' && !occ.subject) canClaim = true;
      else if (targetRole === 'console' && !occ.console) canClaim = true;
      else if (targetRole === 'monitor') canClaim = true;

      if (canClaim) {
        if (targetRole === 'master') { occ.master = ws; ws.roleLock = 'master'; }
        else if (targetRole === 'subject') { occ.subject = ws; ws.roleLock = 'subject'; }
        else if (targetRole === 'console') { occ.console = ws; ws.roleLock = 'console'; }
        else { occ.monitor.push(ws); ws.roleLock = 'monitor'; }
        ws.role = targetRole;
        ws.sessionId = sessionId;
        if (msg.udpTarget) { room.udpTargets.set('master', msg.udpTarget); }
        ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
      } else {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '已被占用' }));
      }
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
      break;
    }

    case 'eeg_frame': {
      if (ws.role !== 'master') return;
      room.sockets.forEach(s => {
        if (s !== ws && s.role === 'monitor' && s.readyState === 1) s.send(raw);
      });
      metrics.pushFrame(msg, ws.sessionId || sessionId);
      break;
    }

    case 'cmd': {
      const ALLOW_ALL = ['master', 'console'];
      const ALLOW_AUTO = ['monitor'];
      if (!ALLOW_ALL.includes(ws.role) && !ALLOW_AUTO.includes(ws.role)) return;
      if (ws.role === 'monitor' && msg.action !== 'set_auto') return;

      const timer = timers.get(sessionId);
      if (!timer) return;

      switch (msg.action) {
        case 'start':        timer.running = true; break;
        case 'pause':        timer.running = false; break;
        case 'next_phase':   advancePhase(sessionId, timer, room); break;
        case 'reset':        resetTimer(sessionId, timer, room); break;
        case 'set_auto':     timer.autoMode = !timer.autoMode; break;
        case 'set_task_type': if (msg.value) { timer.taskType = msg.value; } break;
      }
      broadcastSync(sessionId, timer, room);
      break;
    }

    case 'marker': {
      db.query(
        'INSERT INTO markers (session_id, code, source, label, phase, ts) VALUES (?,?,?,?,?,?)',
        [sessionId, msg.code || 0, msg.source || 'operator', msg.label || '', msg.phase || '', msg.ts || Date.now()]
      ).catch(e => console.warn('[DB]', e.message));
      broadcast(room, msg);
      sendMarkerUDP(room, msg);
      break;
    }

    case 'self_report': {
      let code, label;
      if (msg.state === 'flow_enter') { code = 7; label = '心流进入（自评）'; }
      else if (msg.state === 'flow_exit') { code = 8; label = '心流脱离（自评）'; }
      else if (msg.state === 'distracted') { code = 9; label = '分心（自评）'; }
      else {
        ws.send(JSON.stringify({ type: 'error', message: '未知的自评状态: ' + msg.state }));
        return;
      }
      const markerMsg = {
        type: 'marker', code, source: 'subject', label,
        phase: msg.phase || '', ts: Date.now(),
      };
      db.query(
        'INSERT INTO markers (session_id, code, source, label, phase, ts) VALUES (?,?,?,?,?,?)',
        [sessionId, code, 'subject', label, markerMsg.phase, markerMsg.ts]
      ).catch(e => console.warn('[DB]', e.message));
      broadcast(room, markerMsg);
      sendMarkerUDP(room, markerMsg);
      ws.send(JSON.stringify({ type: 'self_report_ack', state: msg.state }));
      break;
    }

    case 'fss_submit': {
      db.query(
        'INSERT INTO fss_results (session_id, round, phase, answers) VALUES (?,?,?,?)',
        [sessionId, msg.round || 0, msg.phase || '', JSON.stringify(msg.answers || [])]
      ).then(() => {
        ws.send(JSON.stringify({ type: 'fss_submit_ack', success: true }));
      }).catch(err => {
        ws.send(JSON.stringify({ type: 'fss_submit_ack', success: false, error: err.message }));
      });
      break;
    }

    case 'set_udp_target': {
      if (ws.role === 'master') {
        room.udpTargets.set('master', msg.target);
      }
      break;
    }

    // ── 控制台 CRUD ──

    case 'list_subjects': {
      db.query('SELECT * FROM subjects ORDER BY created_at DESC')
        .then(rows => ws.send(JSON.stringify({ type: 'subjects_list', subjects: rows })))
        .catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    case 'create_subject': {
      db.query(
        'INSERT INTO subjects (name, age, gender, notes) VALUES (?,?,?,?)',
        [msg.name || '', msg.age || null, msg.gender || null, msg.notes || null]
      ).then(result => ws.send(JSON.stringify({ type: 'subject_created', id: result.insertId })))
       .catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    case 'list_templates': {
      db.query('SELECT * FROM experiment_templates')
        .then(rows => ws.send(JSON.stringify({ type: 'templates_list', templates: rows })))
        .catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    case 'create_session': {
      const sid = msg.sessionId || ('sess-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6));
      db.query(
        'INSERT INTO sessions (id, subject_id, template_id, operator_name, status, notes) VALUES (?,?,?,?,?,?)',
        [sid, msg.subject_id || 1, msg.template_id || 1, msg.operator_name || '', 'pending', msg.notes || null]
      ).then(() => ws.send(JSON.stringify({ type: 'session_created', session_id: sid })))
       .catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    case 'list_sessions': {
      db.query(
        'SELECT s.*, t.name AS template_name, t.group_type, t.switch_type ' +
        'FROM sessions s LEFT JOIN experiment_templates t ON s.template_id = t.id ' +
        'ORDER BY s.created_at DESC'
      ).then(rows => ws.send(JSON.stringify({ type: 'sessions_list', sessions: rows })))
       .catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    case 'get_session_detail': {
      const sid = msg.session_id;
      Promise.all([
        db.query('SELECT * FROM markers WHERE session_id = ? ORDER BY ts', [sid]),
        db.query('SELECT * FROM fss_results WHERE session_id = ? ORDER BY round', [sid]),
        db.query('SELECT * FROM metrics_snapshots WHERE session_id = ? ORDER BY ts', [sid]),
        db.query('SELECT * FROM baselines WHERE session_id = ?', [sid]),
        db.query('SELECT * FROM sessions WHERE id = ?', [sid]),
      ]).then(([markers, fss, metricsRows, baselines, sessions]) => {
        ws.send(JSON.stringify({
          type: 'session_detail',
          session: sessions[0] || null,
          markers, fss_results: fss, metrics_snapshots: metricsRows, baselines,
        }));
      }).catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    case 'update_session_status': {
      db.query("UPDATE sessions SET status = ? WHERE id = ?", [msg.status, msg.session_id])
        .then(() => {
          ws.send(JSON.stringify({ type: 'session_status_updated', session_id: msg.session_id, status: msg.status }));
          // completed → 清理内存
          if (msg.status === 'completed') cleanupSession(msg.session_id);
        })
        .catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }
  }
}

// ── WS 服务 ──
const wss = new WebSocketServer({ port: config.WS_PORT, maxPayload: 1024 * 1024 });

wss.on('connection', (ws, req) => {
  ws.role = 'pending';
  ws.sessionId = null;
  ws.roleLock = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }

    const sessionId = msg.session_id || ws.sessionId || 'default';
    const room = getRoom(sessionId);

    if (msg.type === 'hello' || msg.type === 'reconnect') {
      if (!timers.has(sessionId)) {
        db.query('SELECT * FROM timer_state WHERE session_id = ?', [sessionId])
          .then(rows => {
            if (rows.length > 0 && !timers.has(sessionId)) {
              const row = rows[0];
              const phases = PHASE_TEMPLATES[row.template_type] || PHASE_TEMPLATES.control;
              const timer = {
                phases, phaseIndex: row.phase_index, timeLeft: row.time_left,
                timeInPhase: row.time_in_phase, running: false,
                autoMode: row.auto_mode === 1, completed: false,
                tickTimer: null, templateType: row.template_type,
                taskType: (phases[row.phase_index] || phases[0]).taskType,
              };
              timers.set(sessionId, timer);
              startTick(sessionId, timer, room);
            } else if (!timers.has(sessionId)) {
              initTimer(sessionId, msg.template_type || 'control');
              startTick(sessionId, timers.get(sessionId), room);
            }
          })
          .catch(() => {
            if (!timers.has(sessionId)) {
              initTimer(sessionId, msg.template_type || 'control');
              startTick(sessionId, timers.get(sessionId), room);
            }
          });
      }
      handleMessage(ws, raw, room, sessionId);
    } else {
      const sid = ws.sessionId || 'default';
      handleMessage(ws, raw, getRoom(sid), sid);
    }
  });

  ws.on('close', () => {
    const sid = ws.sessionId;
    const roleLock = ws.roleLock;
    const room = sid ? rooms.get(sid) : null;

    if (room) {
      room.sockets.delete(ws);

      if (roleLock === 'master' || roleLock === 'subject' || roleLock === 'console') {
        // 立即释放槽位, 但加 30 秒角色锁阻止新声明
        if (roleLock === 'master') {
          if (room.occupants.master === ws) room.occupants.master = null;
          // master 断开 → 暂停计时器 + 持久化 + 广播告警
          const timer = timers.get(sid);
          if (timer) { timer.running = false; saveTimerState(sid); broadcastSync(sid, timer, room); }
          broadcast(room, { type: 'alert', level: 'warning', message: '数据源已断开' });
        }
        if (roleLock === 'subject' && room.occupants.subject === ws) room.occupants.subject = null;
        if (roleLock === 'console' && room.occupants.console === ws) room.occupants.console = null;

        lockRole(sid, roleLock, 30000);
        setTimeout(() => {
          const r = rooms.get(sid);
          if (r) broadcast(r, { type: 'room_info', occupants: getOccupantSummary(r) });
        }, 30000);
      } else if (roleLock === 'monitor') {
        room.occupants.monitor = room.occupants.monitor.filter(s => s !== ws);
      }
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
    }
  });

  ws.on('error', () => {});
});

console.log(`[WS] EEG Cloud 中继启动 :${config.WS_PORT}`);
