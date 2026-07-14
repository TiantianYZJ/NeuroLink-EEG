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

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const dgram = require('dgram');
const config = require('./config');
const db = require('./db');
const metrics = require('./metrics');
const baseline = require('./baseline');

// ── 共享 UDP socket（避免每次 marker 创建新 socket） ──
const udpSock = dgram.createSocket('udp4');
udpSock.on('error', (err) => console.warn('[UDP] socket error:', err.message));

// ── 定期清理已完成的空房间（每 10 分钟） ──
setInterval(() => {
  for (const [sid, room] of rooms) {
    if (room.sockets.size === 0) {
      const timer = timers.get(sid);
      if (!timer || timer.completed) cleanupSession(sid);
    }
  }
}, 600000).unref();

// ── 房间 ──
const rooms = new Map(); // sessionId → room

// 角色槽位锁: sessionId → { role: lockExpireAt }
const roleLocks = new Map();

// Room code mapping: 4-digit code -> sessionId
const roomCodes = new Map();
const CODE_EXPIRE = 86400000;

function generateRoomCode() {
  let code, attempts = 0;
  do {
    code = String(Math.floor(1000 + Math.random() * 9000));
    if (++attempts > 100) return null;
  } while (roomCodes.has(code));
  return code;
}

setInterval(() => {
  const now = Date.now();
  for (const [code, entry] of roomCodes) {
    if (now - entry.createdAt > CODE_EXPIRE || !rooms.has(entry.sessionId)) roomCodes.delete(code);
  }
}, 600000).unref();

function getRoom(sessionId) {
  if (!rooms.has(sessionId)) {
    rooms.set(sessionId, {
      sockets: new Set(),
      occupants: { master: null, monitor: [], subject: null, console: null },
      udpTargets: new Map(),
      locked: true,
      config: null,
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
    locked: room.locked !== false,
    hasConsole: !!room.occupants.console,
    monitor: room.occupants.monitor.filter(s => s.readyState === 1 || s.readyState === 2).length,
    subject: !!room.occupants.subject,
    console: !!room.occupants.console,
    devices: [
      ...(room.occupants.master ? [{ role: 'master', nickname: room.occupants.master.nickname || '', isBridge: room.occupants.master._bridge || false, info: room.occupants.master.deviceInfo || {} }] : []),
      ...room.occupants.monitor.filter(s => s.readyState === 1 || s.readyState === 2).map(s => ({ role: 'monitor', nickname: s.nickname || '', info: s.deviceInfo || {} })),
      ...(room.occupants.subject ? [{ role: 'subject', nickname: room.occupants.subject.nickname || '', info: room.occupants.subject.deviceInfo || {} }] : []),
      ...(room.occupants.console ? [{ role: 'console', nickname: room.occupants.console.nickname || '', info: room.occupants.console.deviceInfo || {} }] : []),
    ],
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
  timer.tickTimer = null;
  timer.phaseIndex = 0; timer.timeLeft = timer.phases[0].duration;
  timer.timeInPhase = 0; timer.running = false; timer.completed = false;
  timer.taskType = timer.phases[0].taskType;
  baseline.reset(sessionId);
  saveTimerState(sessionId);
  startTick(sessionId, timer, room);
}

function broadcastSync(sessionId, timer, room) {
  const phase = getCurrentPhase(timer);
  broadcast(room, {
    type: 'phase_sync',
    phase_index: timer.phaseIndex, phase_id: phase.id,
    phase_name: phase.name, round: phase.round,
    time_left: timer.timeLeft, time_in_phase: timer.timeInPhase,
    phase_duration: phase.duration,
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
      ws.deviceInfo = msg.device_info || {};
      ws.nickname = msg.device_info && msg.device_info.nickname ? msg.device_info.nickname : 'Anonymous';
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
      ws.deviceInfo = msg.device_info || ws.deviceInfo || {};
      if (targetRole === 'master' && msg.udpTarget) {
        room.udpTargets.set('master', msg.udpTarget);
      }
      // 恢复槽位
      if (targetRole === 'master') room.occupants.master = ws;
      else if (targetRole === 'subject') room.occupants.subject = ws;
      else if (targetRole === 'console') room.occupants.console = ws;

      ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
      if (room.config) ws.send(JSON.stringify({ type: 'room_config', locked: room.locked !== false, config: room.config }));
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
        // Clear old role lock for same socket so it can re-claim
        if (ws.roleLock) unlockRole(sessionId, ws.roleLock);
        if (ws.role !== 'pending') {
          ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '该设备已注册角色' }));
          break;
        }
        if (targetRole === 'master') { occ.master = ws; ws.roleLock = 'master'; }
        else if (targetRole === 'subject') { occ.subject = ws; ws.roleLock = 'subject'; }
        else if (targetRole === 'console') { occ.console = ws; ws.roleLock = 'console'; }
        else { occ.monitor.push(ws); ws.roleLock = 'monitor'; }
        ws.role = targetRole;
        ws.sessionId = sessionId;
        ws.deviceInfo = msg.device_info || ws.deviceInfo || {};
        if (msg.udpTarget) { room.udpTargets.set('master', msg.udpTarget); }
        ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
        if (room.config) ws.send(JSON.stringify({ type: 'room_config', locked: room.locked !== false, config: room.config }));
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

      const now = Date.now();
      // Lock check for experiment control actions
      if (room.locked !== false && (msg.action === 'start' || msg.action === 'reset' || msg.action === 'next_phase' || msg.action === 'pause')) {
        ws.send(JSON.stringify({ type: 'error', message: '房间已锁定，请等待控制台配置' }));
        break;
      }
      switch (msg.action) {
        case 'start': {
          timer.running = true;
          const sp = getCurrentPhase(timer);
          broadcast(room, { type: 'marker', code: 1, source: 'auto', label: 'phase_start:' + sp.id, phase: sp.id, ts: now });
          break;
        }
        case 'pause':        timer.running = false; break;
        case 'next_phase': {
          const cp = getCurrentPhase(timer);
          broadcast(room, { type: 'marker', code: 2, source: 'auto', label: 'phase_end:' + cp.id, phase: cp.id, ts: now });
          advancePhase(sessionId, timer, room);
          if (!timer.completed) {
            const np = getCurrentPhase(timer);
            broadcast(room, { type: 'marker', code: 1, source: 'auto', label: 'phase_start:' + np.id, phase: np.id, ts: Date.now() });
          }
          break;
        }
        case 'reset': {
          const rp = getCurrentPhase(timer);
          broadcast(room, { type: 'marker', code: 2, source: 'auto', label: 'phase_end:' + rp.id + '(reset)', phase: rp.id, ts: now });
          resetTimer(sessionId, timer, room);
          break;
        }
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

    case 'update_device_info': {
      if (msg.nickname) ws.nickname = msg.nickname;
      if (msg.device_info) ws.deviceInfo = msg.device_info;
      break;
    }

    case 'leave_room': {
      if (ws.roleLock) unlockRole(sessionId, ws.roleLock);
      if (ws.sessionId) { const r = rooms.get(ws.sessionId); if (r) { r.sockets.delete(ws); ws.role = 'pending'; ws.roleLock = null; } }
      ws.send(JSON.stringify({ type: 'room_left' }));
      break;
    }

    case 'release_role': {
      const occ = room.occupants;
      if (ws.roleLock === 'master' && occ.master === ws) occ.master = null;
      else if (ws.roleLock === 'subject' && occ.subject === ws) occ.subject = null;
      else if (ws.roleLock === 'console' && occ.console === ws) occ.console = null;
      else if (ws.roleLock === 'monitor') { occ.monitor = occ.monitor.filter(s => s !== ws); }
      // Clean role lock for this socket so it can re-claim
      if (ws.roleLock) unlockRole(sessionId, ws.roleLock);
      ws.role = 'pending'; ws.roleLock = null;
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
      ws.send(JSON.stringify({ type: 'role_released' }));
      break;
    }

    // ── 房间锁管理 ──

    case 'start_experiment': {
      room.locked = false;
      room.config = msg.config || {};
      broadcast(room, { type: 'room_config', locked: false, config: room.config });
      // Initialize timer with chosen template
      if (!timers.has(sessionId) && msg.template_type) {
        initTimer(sessionId, msg.template_type);
        startTick(sessionId, timers.get(sessionId), room);
      }
      ws.send(JSON.stringify({ type: 'experiment_started' }));
      break;
    }

    case 'end_experiment': {
      room.locked = true;
      const timer = timers.get(sessionId);
      if (timer) { clearInterval(timer.tickTimer); timer.running = false; timer.completed = true; }
      broadcast(room, { type: 'room_config', locked: true, config: room.config });
      break;
    }

    // ── 房间码管理 ──

    case 'create_room': {
      const code = generateRoomCode();
      if (!code) { ws.send(JSON.stringify({ type: 'error', message: '所有房间号已被占用' })); break; }
      const sid = 'room-' + code + '-' + Date.now().toString(36);
      roomCodes.set(code, { sessionId: sid, createdAt: Date.now() });
      getRoom(sid);
      // Remove from old room, add to new room
      if (ws.sessionId) { const r = rooms.get(ws.sessionId); if (r) r.sockets.delete(ws); }
      getRoom(sid).sockets.add(ws);
      ws.sessionId = sid;
      ws.send(JSON.stringify({ type: 'room_created', code, session_id: sid, url: '/?room=' + code }));
      break;
    }

    case 'join_room': {
      const entry = roomCodes.get(msg.code);
      if (entry && rooms.has(entry.sessionId)) {
        // Remove from old room, add to new room
        if (ws.sessionId && ws.sessionId !== entry.sessionId) { const r = rooms.get(ws.sessionId); if (r) r.sockets.delete(ws); }
        getRoom(entry.sessionId).sockets.add(ws);
        ws.sessionId = entry.sessionId;
        ws.send(JSON.stringify({ type: 'room_joined', session_id: entry.sessionId, code: msg.code }));
      } else {
        ws.send(JSON.stringify({ type: 'error', message: '房间不存在或已过期' }));
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
        'SELECT s.*, t.name AS template_name, t.group_type, t.switch_type, sub.name AS subject_name ' +
        'FROM sessions s LEFT JOIN experiment_templates t ON s.template_id = t.id ' +
        'LEFT JOIN subjects sub ON s.subject_id = sub.id ' +
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

// ── HTTP + WS 服务（共享端口） ──
const server = http.createServer((req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Bridge 文件下载 (内联内容, 不依赖 bridge/ 目录)
  if (req.method === 'GET' && req.url.startsWith('/api/download/bridge/')) {
    const file = req.url.replace('/api/download/bridge/', '');
    const files = {
      'package.json': JSON.stringify({ name: 'neurolink-bridge', version: '1.0.0', private: true,
        dependencies: { ws: '^8.0.0' } }),
      'config.js': `/**
 * 主控机本地桥接配置
 */
module.exports = {
  // OpenBCI UDP 数据监听端口（OpenBCI GUI 的 UDP_OUT 需设为此端口）
  // 注意: GUI_MARKER_PORT 默认 12346 避免与此冲突
  UDP_LISTEN_PORT: parseInt(process.env.UDP_PORT || '12345', 10),

  // 本地 WebSocket 端口（供本地面板连接，低延迟渲染波形）
  LOCAL_WS_PORT: parseInt(process.env.LOCAL_WS_PORT || '9080', 10), // 与 ECS 8080 区分

  // 设备类型
  DEVICE_TYPE: (process.env.DEVICE_TYPE || 'ganglion').toLowerCase(),

  // ECS 云端 WebSocket 地址
  ECS_WS_URL: process.env.ECS_WS_URL || 'wss://eeg.yzjtiantian.cn/ws',

  // OpenBCI GUI Marker 端口（ECS 回传标记写入目标）
  // 默认 12346 — OpenBCI GUI 的 "UDP Marker Port" 需设为此值, 与 UDP 数据端口(12345) 区分
  GUI_MARKER_PORT: parseInt(process.env.GUI_MARKER_PORT || '12346', 10),

  // 固定 session_id（不设置则自动生成）
  SESSION_ID: process.env.SESSION_ID || '',
};
`,
      'index.js': `/**
 * OpenBCI UDP → WebSocket 本地桥接
 *
 * 职责:
 *   1. 监听 UDP 端口接收 Ganglion/Cyton 数据包
 *   2. 解析二进制协议为 JSON
 *   3. 推送到本地 WebSocket（本地面板低延迟渲染）
 *   4. 推送到 ECS 云端 WebSocket（远端监视端转发）
 *   5. 监听 ECS 回传的 marker → 写入 OpenBCI GUI Marker 端口
 */

const dgram = require('dgram');
const WebSocket = require('ws');
const config = require('./config');

// ── 状态 ──
let packetCount = 0;
let lastStatsTime = Date.now();
let sampleRate = 0;
const isGanglion = config.DEVICE_TYPE === 'ganglion';
const chCount = isGanglion ? 4 : 8;

// ── 1. UDP 监听（接收 OpenBCI 数据） ──
const udpServer = dgram.createSocket('udp4');

function parseOpenBCIPacket(msg) {
  const len = msg.length;
  if (len < 5) return null;
  let offset = 0;
  while (offset < len && msg[offset] !== 0xA0) offset++;
  if (offset >= len) return null;
  if (offset + 4 > len) return null;
  const sampleNumber = msg[offset + 1] | (msg[offset + 2] << 8) | (msg[offset + 3] << 16);
  offset += 4;
  const dataLen = chCount * 3;
  if (offset + dataLen + 1 > len) return null;
  const channels = [];
  for (let i = 0; i < chCount; i++) {
    let val = msg[offset] | (msg[offset + 1] << 8) | (msg[offset + 2] << 16);
    if (val & 0x800000) val |= ~0xFFFFFF;
    channels.push(val);
    offset += 3;
  }
  return { sampleNumber, channels };
}

const frameBroadcast = (parsed) => {
  const payload = JSON.stringify({ type: 'eeg_frame', seq: parsed.sampleNumber || 0, channels: parsed.channels, ts: Date.now() });
  localWss.clients.forEach(c => { if (c.readyState === 1) c.send(payload); });
  if (ecsWs && ecsWs.readyState === 1 && ecsConnected) ecsWs.send(payload);
};

udpServer.on('message', (msg) => {
  const parsed = parseOpenBCIPacket(msg);
  if (!parsed) return;
  packetCount++;
  const now = Date.now();
  if (now - lastStatsTime >= 1000) {
    sampleRate = Math.round(packetCount * 1000 / (now - lastStatsTime));
    packetCount = 0;
    lastStatsTime = now;
  }
  frameBroadcast(parsed);
});

udpServer.on('error', (err) => { console.error('[UDP]', err.message); });
udpServer.on('listening', () => {
  const addr = udpServer.address();
  console.log(\`[UDP] 监听 \${addr.address}:\${addr.port}\`);
});

// ── 2. 本地 WebSocket 服务（供本地面板连接） ──
const localWss = new WebSocket.Server({ port: config.LOCAL_WS_PORT });
localWss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'status',
    deviceType: config.DEVICE_TYPE,
    channels: chCount,
    sampleRate: sampleRate || (isGanglion ? 200 : 250),
  }));
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'set_session' && msg.session_id) {
        ecsSessionId = msg.session_id;
        try{require('fs').writeFileSync(require('path').join(__dirname,'ecs-session.id'),ecsSessionId,'utf8')}catch(e){};

        if (ecsWs) ecsWs._reconnectOnClose = false;
        ecsWs.close();
        setTimeout(() => connectECS(), 1000);
      }
    } catch (_) {}
  });
  ws.on('close', () => {});
});

// ── 3. ECS 上行 WebSocket 客户端 ──
let ecsWs = null;
let ecsSessionId = (function(){try{var p=require('path').join(__dirname,'ecs-session.id'),d=require('fs').readFileSync(p,'utf8').trim();if(d)return d}catch(e){}return config.SESSION_ID||('bridge-'+Date.now()+'-'+Math.random().toString(36).slice(2,6))})();
let ecsConnected = false;
let pendingRoomCode = process.env.ROOM_CODE || '';
let roomJoinAttempted = false;

/** 获取本机 LAN IP */
function getLANIP() {
  try {
    const os = require('os');
    const ifaces = os.networkInterfaces();
    for (const name of Object.keys(ifaces)) {
      for (const iface of ifaces[name]) {
        if (iface.family === 'IPv4' && !iface.internal) return iface.address;
      }
    }
  } catch (_) {}
  return '127.0.0.1';
}

function connectECS() {
  const ws = new WebSocket(config.ECS_WS_URL);
  ws.on('open', () => {
    console.log('[ECS] 已连接');
    ws.send(JSON.stringify({ type: 'hello', role: 'pending', session_id: ecsSessionId }));
    if (pendingRoomCode && !roomJoinAttempted) {
      roomJoinAttempted = true;
      console.log('[ECS] join room:', pendingRoomCode);
      ws.send(JSON.stringify({ type: 'join_room', code: pendingRoomCode, session_id: ecsSessionId }));
    }
  });
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'role_claimed' && msg.role === 'master') {
        console.log('[ECS] 已认领 master 角色');
        ecsConnected = true;
        const lanIP = getLANIP();
        ws.send(JSON.stringify({ type: 'set_udp_target', target: lanIP + ':' + config.GUI_MARKER_PORT }));
      }
      if (msg.type === 'role_denied' && !ecsConnected) {
        console.warn('[ECS] 角色被拒，5 秒后重试:', msg.reason);
        setTimeout(() => {
          if (ws.readyState === 1)
            ws.send(JSON.stringify({ type: 'reconnect', role: 'master', session_id: ecsSessionId }));
        }, 5000);
      }
      if (msg.type === 'marker') {
        const buf = Buffer.alloc(4);
        buf.writeFloatBE(msg.code || 0);
        udpServer.send(buf, 0, 4, config.GUI_MARKER_PORT, '127.0.0.1');
      }
      if (msg.type === 'room_info' && !ecsConnected) {
        ws.send(JSON.stringify({ type: 'reconnect', role: 'master', session_id: ecsSessionId }));
      }
      if (msg.type === 'room_joined') {
        console.log('[ECS] room joined:', msg.code);
        pendingRoomCode = '';
        ecsSessionId = msg.session_id;
        try{require('fs').writeFileSync(require('path').join(__dirname,'ecs-session.id'),ecsSessionId,'utf8')}catch(e){};

        if (ecsWs) ecsWs._reconnectOnClose = false;
        ecsWs.close();
        setTimeout(() => connectECS(), 1000);
      }
      if (msg.type === 'error' && pendingRoomCode && msg.message) {
        console.error('[ECS] room error:', msg.message);
        pendingRoomCode = '';
        process.exit(1);
      }
    } catch (e) {}
  });
  ws.on('close', () => {
    console.log('[ECS] 断开，5 秒后重连');
    ecsConnected = false;
    if (ws._reconnectOnClose !== false) setTimeout(connectECS, 5000);
  });
  ws.on('error', () => ws.close());
  ecsWs = ws;
}

// ââ å¯å¨ ââ
udpServer.bind(config.UDP_LISTEN_PORT);
connectECS();

function sendLeaveRoom() {
  pendingRoomCode = '';
  roomJoinAttempted = false;
  if (ecsWs && ecsWs.readyState === 1) {
    console.log('[ECS] \xe7\xa6\xbb\xe5\xbc\x80\xe6\x88\xbf\xe9\x97\xb4...');
    ecsWs.send(JSON.stringify({ type: 'leave_room', session_id: ecsSessionId }));
    ecsWs._reconnectOnClose = false;
    ecsWs.close();
  }
  setTimeout(() => {
    const rl = require('readline').createInterface({ input: process.stdin, output: process.stdout });
    rl.question('\xe6\x88\xbf\xe9\x97\xb4\xe5\x8f\xb7: ', (code) => {
      code = code.trim();
      if (code.length === 4 && /^\\d{4}$/.test(code)) {
        pendingRoomCode = code;
        roomJoinAttempted = false;
        rl.close();
        connectECS();
      } else {
        console.log('\xe6\x97\xa0\xe6\x95\x88\xe6\x88\xbf\xe9\x97\xb4\xe5\x8f\xb7');
        rl.close();
        process.exit(0);
      }
    });
  }, 1500);
}

const stdinex = process.stdin;
stdinex.setEncoding('utf8');
stdinex.setRawMode && stdinex.setRawMode(true);
console.log('  \xe8\xbe\x93\xe5\x85\xa5 l \xe9\x80\x80\xe5\x87\xba\xe5\xbd\x93\xe5\x89\x8d\xe6\x88\xbf\xe9\x97\xb4 \xc2\xb7 Ctrl+C \xe9\x80\x80\xe5\x87\xba\xe6\xa1\xa5\xe6\x8e\xa5');
stdinex.on("data", (key) => {
  const k = key.toString().toLowerCase().trim();
  if (k === 'l' || k === 'leave') {
    sendLeaveRoom();
  } else if (k === '\\x03') {
    sendLeaveRoom();
    process.exit(0);
  }
});
process.on("SIGINT", () => { sendLeaveRoom(); setTimeout(() => process.exit(0), 500); });

console.log(\`── OpenBCI UDP → WebSocket Bridge ──\`);
console.log(\`  Device      : \${isGanglion ? 'Ganglion (4ch 200Hz)' : 'Cyton'}\`);
console.log(\`  UDP 监听    : \${config.UDP_LISTEN_PORT}\`);
console.log(\`  本地面板 WS : :\${config.LOCAL_WS_PORT}\`);
console.log(\`  ECS 上行    : \${config.ECS_WS_URL}\`);
console.log('  \u8fd0\u884c\u4e2d: \u8f93\u5165 l \u9000\u51fa\u623f\u95f4 \u00b7 Ctrl+C \u9000\u51fa\u6865\u63a5');
    `,
    };
    const data = files[file];
    if (!data) { res.writeHead(403); res.end('Forbidden'); return; }
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(data);
    return;
  }

  // 桥接状态检查 (前端用于判断本地桥接是否已连接 ECS)
  if (req.method === 'GET' && req.url.startsWith('/api/bridge-status')) {
    const url = new URL(req.url, 'http://localhost');
    const sid = url.searchParams.get('session_id') || '';
    const room = sid ? rooms.get(sid) : null;
    const masterAlive = room && room.occupants && !!room.occupants.master;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ master: masterAlive }));
    return;
  }

  // 健康检查
  if (req.url === '/api/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
    return;
  }

  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: 1024 * 1024 });
server.listen(config.WS_PORT);

// WS 心跳检测（每 30 秒 ping，超时 10 秒终止）
function heartbeat(ws) {
  if (ws._alive === false) { ws.terminate(); return; }
  ws._alive = false;
  ws.ping();
}
const hbTimer = setInterval(() => wss.clients.forEach(heartbeat), 30000).unref();
wss.on('connection', (ws, req) => {
  ws._alive = true;
  ws.role = 'pending';
  ws.sessionId = null;
  ws.roleLock = null;
  ws.on('pong', () => { ws._alive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }

    const sessionId = msg.session_id || ws.sessionId || 'default';
    const room = getRoom(sessionId);

    if (msg.type === 'hello' || msg.type === 'reconnect') {
      if (!timers.has(sessionId)) {
        // Auto-create sessions row to prevent FK failures on data tables
        db.query('INSERT IGNORE INTO sessions (id, subject_id, template_id, status) VALUES (?, 1, 1, "running")', [sessionId])
          .catch(() => {});
        // Try restoring timer from timer_state first, else derive template from DB
        db.query('SELECT * FROM timer_state WHERE session_id = ?', [sessionId])
          .then(rows => {
            if (rows.length > 0 && !timers.has(sessionId)) {
              const row = rows[0];
              const phases = PHASE_TEMPLATES[row.template_type] || PHASE_TEMPLATES.control;
              timers.set(sessionId, {
                phases, phaseIndex: row.phase_index, timeLeft: row.time_left,
                timeInPhase: row.time_in_phase, running: false,
                autoMode: row.auto_mode === 1, completed: row.phase_index >= phases.length - 1 && row.time_left <= 0,
                tickTimer: null, templateType: row.template_type,
                taskType: (phases[row.phase_index] || phases[0]).taskType,
              });
              startTick(sessionId, timers.get(sessionId), room);
            } else if (!timers.has(sessionId)) {
              // Derive template type from DB session record
              db.query('SELECT t.switch_type FROM sessions s JOIN experiment_templates t ON s.template_id = t.id WHERE s.id = ?', [sessionId])
                .then(rows2 => {
                  if (!timers.has(sessionId)) {
                    initTimer(sessionId, (rows2.length > 0 ? rows2[0].switch_type : 'control'));
                    startTick(sessionId, timers.get(sessionId), room);
                  }
                })
                .catch(() => {
                  if (!timers.has(sessionId)) {
                    initTimer(sessionId, 'control');
                    startTick(sessionId, timers.get(sessionId), room);
                  }
                });
            }
          })
          .catch(() => {
            if (!timers.has(sessionId)) {
              initTimer(sessionId, 'control');
              startTick(sessionId, timers.get(sessionId), room);
            }
          });
      }
      handleMessage(ws, raw, room, sessionId);
    } else {
      const sid = msg.session_id || ws.sessionId || 'default';
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

console.log(`[OK] EEG Cloud 服务启动 :${config.WS_PORT} (WS + HTTP API)`);
