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

// ── 定期清理僵尸 socket 和空房间（每 60 秒） ──
setInterval(() => {
  for (const [sid, room] of rooms) {
    // 清理僵尸 socket
    const alive = (s) => s.readyState === 1 || s.readyState === 2;
    const dead = Array.from(room.sockets).filter(s => !alive(s) && !s._bridge);
    for (const s of dead) {
      room.sockets.delete(s);
      const lock = s.roleLock;
      if (lock === 'master' && room.occupants.master === s) room.occupants.master = null;
      if (lock === 'subject' && room.occupants.subject === s) room.occupants.subject = null;
      if (lock === 'console') room.occupants.console = room.occupants.console.filter(x => x !== s);
      if (lock === 'monitor') room.occupants.monitor = room.occupants.monitor.filter(x => x !== s);
    }
    // 清理已完成的空房间
    if (room.sockets.size === 0) {
      const timer = timers.get(sid);
      if (!timer || timer.completed) cleanupSession(sid);
    }
  }
}, 60000).unref();

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
      occupants: { master: null, monitor: [], subject: null, console: [] },
      udpTargets: new Map(),
      locked: true,
      config: null,
    });
  }
  return rooms.get(sessionId);
}

// Per-room frame rate tracking (always active, no timer needed)
const frameRateTracker = {
  _counts: new Map(), // sessionId -> {count, lastTime}
  count(sessionId) {
    let t = this._counts.get(sessionId);
    if (!t) { t = { count: 0, lastTime: Date.now() }; this._counts.set(sessionId, t); }
    t.count++;
  },
  getRate(sessionId) {
    const t = this._counts.get(sessionId);
    if (!t) return 0;
    const elapsed = Date.now() - t.lastTime;
    if (elapsed < 800) return 0;
    const rate = Math.round(t.count * 1000 / elapsed);
    t.count = 0;
    t.lastTime = Date.now();
    return Math.min(rate, 500);
  }
};

function broadcast(room, msg, exclude = null) {
  const payload = typeof msg === 'string' ? msg : JSON.stringify(msg);
  room.sockets.forEach(ws => {
    if (ws !== exclude && ws.readyState === 1) ws.send(payload);
  });
}

function broadcastToRoles(room, msg, roles) {
  const payload = typeof msg === 'string' ? msg : JSON.stringify(msg);
  room.sockets.forEach(ws => {
    try { if (ws.readyState === 1 && roles.includes(ws.role)) ws.send(payload); } catch(e) {
      if (e && e.message && e.message !== 'write EPIPE') console.warn('[BROADCAST]', e.message);
    }
  });
}

function getOccupantSummary(room, sessionId) {
  const alive = (s) => s.readyState === 1 || s.readyState === 2;
  // 清理僵尸 socket：TCP 断开但 on('close') 未触发的残留连接
  const deadSockets = Array.from(room.sockets).filter(s => !alive(s));
  for (const s of deadSockets) {
    room.sockets.delete(s);
    const lock = s.roleLock;
    if (lock === 'master' && room.occupants.master === s) room.occupants.master = null;
    if (lock === 'subject' && room.occupants.subject === s) room.occupants.subject = null;
    if (lock === 'console') room.occupants.console = room.occupants.console.filter(x => x !== s);
    if (lock === 'monitor') room.occupants.monitor = room.occupants.monitor.filter(x => x !== s);
  }
  // Collect all bridge sockets from room
  const bridgeSockets = Array.from(room.sockets).filter(s => s._bridge && alive(s));
  return {
    frame_rate: room._lastFrameRate || 0,
    master: !!(room.occupants.master && !room.occupants.master._bridge),
    locked: room.locked !== false,
    hasConsole: room.occupants.console.filter(alive).length > 0,
    monitor: room.occupants.monitor.filter(alive).length,
    subject: !!room.occupants.subject,
    console: room.occupants.console.filter(alive).length,
    bridge: bridgeSockets.length,
    devices: [
      ...(room.occupants.master && !room.occupants.master._bridge ? [{ role: 'master', nickname: room.occupants.master.nickname || '', isBridge: false, info: room.occupants.master.deviceInfo || {} }] : []),
      ...room.occupants.monitor.filter(alive).map(s => ({ role: 'monitor', nickname: s.nickname || '', info: s.deviceInfo || {} })),
      ...(room.occupants.subject ? [{ role: 'subject', nickname: room.occupants.subject.nickname || '', info: room.occupants.subject.deviceInfo || {} }] : []),
      ...room.occupants.console.filter(alive).map(s => ({ role: 'console', nickname: s.nickname || '', info: s.deviceInfo || {} })),
      ...bridgeSockets.map(s => ({ role: s.role || 'bridge', nickname: s.nickname || ('Bridge ' + (s.sessionId || '').slice(-6)), isBridge: true, info: s.deviceInfo || {} })),
      // 未认领角色但已进入房间的设备
      ...Array.from(room.sockets).filter(s => alive(s) && (s.role === 'pending' || !s.role)).map(s => ({ role: 'pending', nickname: s.nickname || '', info: s.deviceInfo || {} })),
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

// 角色锁时长带随机抖动（27.5s-32.5s），避免多客户端同时抢占（W6）
function lockRoleWithJitter(sessionId, role) {
  const ms = 30000 + Math.floor(Math.random() * 5000) - 2500;
  lockRole(sessionId, role, ms);
}

// ── Marker 持久化辅助（S6: 统一所有 marker 产生点的 INSERT） ──
function persistMarker(sessionId, msg) {
  if (!msg || msg.type !== 'marker') return;
  db.query(
    'INSERT INTO markers (session_id, code, source, label, phase, ts) VALUES (?,?,?,?,?,?)',
    [sessionId, msg.code || 0, msg.source || 'auto', msg.label || '', msg.phase || '', msg.ts || Date.now()]
  ).catch(e => console.warn('[DB] marker persist failed:', e.message));
}

// ── 从房间移除 socket（清 sockets + occupants 槽位），用于切房场景（B2） ──
// BUG2-fix: 同时重置 ws.role/roleLock，避免切房后 close 事件用旧角色锁错房间
function removeSocketFromRoom(ws, room) {
  if (!room) return;
  room.sockets.delete(ws);
  const lock = ws.roleLock;
  if (lock === 'master' && room.occupants.master === ws) room.occupants.master = null;
  if (lock === 'subject' && room.occupants.subject === ws) room.occupants.subject = null;
  if (lock === 'console') room.occupants.console = room.occupants.console.filter(s => s !== ws);
  if (lock === 'monitor') room.occupants.monitor = room.occupants.monitor.filter(s => s !== ws);
  // 释放旧房间角色锁，重置 ws 状态为 pending（与 leave_room 行为一致）
  if (lock) unlockRole(ws.sessionId, lock);
  ws.role = 'pending';
  ws.roleLock = null;
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
  return timer;
}

function getCurrentPhase(timer) {
  return timer.phases[timer.phaseIndex] || timer.phases[0];
}

function advancePhase(sessionId, timer, room) {
  if (timer.phaseIndex >= timer.phases.length - 1) {
    timer.completed = true; timer.running = false;
    clearInterval(timer.tickTimer); timer.tickTimer = null;
    return;
  }
  timer.phaseIndex++;
  const phase = getCurrentPhase(timer);
  timer.timeLeft = phase.duration;
  timer.timeInPhase = 0;
  timer.taskType = phase.taskType;
}

function resetTimer(sessionId, timer, room) {
  clearInterval(timer.tickTimer);
  timer.tickTimer = null;
  // BUG2-fix: 确保旧 tick 的 finally 不会覆盖新 _ticking 状态
  // 使用计数器而非 boolean 让旧 tick 的 finally 自减不影响新 tick
  if (typeof timer._tickGen === 'undefined') timer._tickGen = 0;
  timer._tickGen++;
  timer._ticking = false;
  timer.phaseIndex = 0; timer.timeLeft = timer.phases[0].duration;
  timer.timeInPhase = 0; timer.running = false; timer.completed = false;
  timer.taskType = timer.phases[0].taskType;
  baseline.reset(sessionId);
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
    // B7: 重叠守卫，避免上一轮 async 未完成时下一轮进入
    if (timer._ticking) return;
    timer._ticking = true;
    const thisGen = timer._tickGen || 0;
    try {
      const room = rooms.get(sessionId);
      if (!room) { clearInterval(timer.tickTimer); return; }

      // 指标计算（不受 timer.running 限制，点"开始"前也能看热力图）
      const now = Date.now();
      const phase = getCurrentPhase(timer);
      const snapshot = metrics.tick(now, sessionId, timer.phaseIndex, phase.id);

      if (snapshot) {
        broadcastToRoles(room, { type: 'metrics_snapshot', ...snapshot }, ['master', 'monitor', 'console']);
      }

      if (timer.running && !timer.completed) {
        timer.timeLeft--;
        timer.timeInPhase++;

        // S3: metrics 失败时 snapshot 为 null，不传给 baseline 避免 undefined 污染
        let baselineFailed = false;
        let msgs = [];
        try {
          msgs = await baseline.tick(sessionId, phase.id, timer.timeInPhase, snapshot || null) || [];
        } catch (e) {
          baselineFailed = true;
          console.warn('[BASELINE] tick failed:', e.message);
        }
        if (!baselineFailed) {
          for (const m of msgs) {
            if (m.type === 'marker') {
              persistMarker(sessionId, m);
              broadcast(room, m);
              sendMarkerUDP(room, m);
            } else {
              broadcastToRoles(room, m, ['master', 'monitor']);
            }
          }
        }

        // 5. 阶段切换
        if (timer.timeLeft <= 0) {
          const endMarker = {
            type: 'marker', code: 2, source: 'auto',
            label: 'phase_end:' + phase.id, phase: phase.id, ts: now,
          };
          persistMarker(sessionId, endMarker);
          broadcast(room, endMarker);
          advancePhase(sessionId, timer, room);
          if (!timer.completed) {
            const nextPhase = getCurrentPhase(timer);
            const startMarker = {
              type: 'marker', code: 1, source: 'auto',
              label: 'phase_start:' + nextPhase.id, phase: nextPhase.id, ts: Date.now(),
            };
            persistMarker(sessionId, startMarker);
            broadcast(room, startMarker);
            if (!timer.autoMode) timer.running = false;
          }
        }
      }

      broadcastSync(sessionId, timer, room);
    } finally {
      // BUG2-fix: 仅当 generation 未变时才重置 _ticking，避免 resetTimer 后旧 async tick 覆盖新状态
      if ((timer._tickGen || 0) === thisGen) timer._ticking = false;
    }
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
    udpSock.send(buf, 0, 4, parseInt(port, 10), host);
  } catch (_) {}
}

// ── Session 清理 ──
function cleanupSession(sessionId) {
  const timer = timers.get(sessionId);
  if (timer) { clearInterval(timer.tickTimer); }
  timers.delete(sessionId);
  metrics.cleanup(sessionId);
  baseline.cleanup(sessionId);
  // W5: 清理重连定时器
  const room = rooms.get(sessionId);
  if (room && room._reconnectTimer) { clearTimeout(room._reconnectTimer); room._reconnectTimer = null; }
  rooms.delete(sessionId);
  roleLocks.delete(sessionId);
}

// ── 消息路由 ──
function handleMessage(ws, raw, room, sessionId) {
  let msg;
  try { msg = JSON.parse(raw); } catch (_) { return; }

  switch (msg.type) {

    // W1: 应用层心跳分支
    case 'ping': ws.send(JSON.stringify({ type: 'pong' })); return;
    case 'pong': ws._alive = true; return;

    case 'hello': {
      // BUG3-fix: hello 前清理旧房间（避免残留 sockets/occupants 槽位）
      if (ws.sessionId && ws.sessionId !== sessionId) removeSocketFromRoom(ws, rooms.get(ws.sessionId));
      room.sockets.add(ws);
      ws.role = 'pending';
      ws.sessionId = sessionId;
      ws.deviceInfo = msg.device_info || {};
      ws.nickname = msg.device_info && msg.device_info.nickname ? msg.device_info.nickname : 'Anonymous';
      ws._bridge = !!(msg.device_info && msg.device_info.isBridge);
      ws.send(JSON.stringify({ type: 'room_info', occupants: getOccupantSummary(room, sessionId) }));
      break;
    }

    case 'reconnect': {
      const targetRole = msg.role;
      // 时间戳锁: 允许同 sessionId 重连, 拒绝被抢占的新声明
      if (!['master', 'subject', 'console'].includes(targetRole)) {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '不支持的重连角色' }));
        break;
      }
      // BUG1-fix: 重连场景必须同步 ws._bridge 标志，否则 B1 桥接隔离失效（bridge 重连会被当作普通 master）
      ws._bridge = !!(msg.device_info && msg.device_info.isBridge) || ws._bridge;
      // 检查房间码是否存在（服务器重启后房间已丢失）
      const hasRoomCode = Array.from(roomCodes.entries()).some(([code, entry]) => entry.sessionId === sessionId);
      if (!hasRoomCode) {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '房间已不存在，请重新加入' }));
        break;
      }
      // 立即清除旧锁, 允许重连
      unlockRole(sessionId, targetRole);
      // 如果槽位还有旧 socket, 释放它
      if (targetRole === 'master' && !ws._bridge && room.occupants.master) room.occupants.master = null;
      if (targetRole === 'subject' && room.occupants.subject) room.occupants.subject = null;

      room.sockets.add(ws);
      ws.role = targetRole;
      ws.sessionId = sessionId;
      ws.roleLock = targetRole;
      ws.deviceInfo = msg.device_info || ws.deviceInfo || {};
      if (targetRole === 'master' && msg.udpTarget) {
        room.udpTargets.set('master', msg.udpTarget);
      }
      // 恢复槽位
      if (targetRole === 'master' && !ws._bridge) room.occupants.master = ws;
      else if (targetRole === 'subject') room.occupants.subject = ws;
      else if (targetRole === 'console') { room.occupants.console.push(ws); }

      ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
      ws.send(JSON.stringify({ type: 'room_config', locked: room.locked !== false, config: room.config }));
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room, sessionId) });

      const timer = timers.get(sessionId);
      if (timer && targetRole === 'master' && !ws._bridge) { timer.running = false; broadcastSync(sessionId, timer, room); }
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
      // Bridge 连接不占用 master 槽位: 浏览器仍可认领 master
      if (targetRole === 'master' && (!occ.master || occ.master._bridge)) canClaim = true;
      else if (targetRole === 'subject' && !occ.subject) canClaim = true;
      else if (targetRole === 'console') canClaim = true;
      else if (targetRole === 'monitor') canClaim = true;

      if (canClaim) {
        // Clear old role lock for same socket so it can re-claim
        if (ws.roleLock) unlockRole(sessionId, ws.roleLock);
        if (ws.role !== 'pending') {
          ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '该设备已注册角色' }));
          break;
        }
        if (targetRole === 'master' && !ws._bridge) { occ.master = ws; ws.roleLock = 'master'; }
        else if (targetRole === 'master' && ws._bridge) { ws.roleLock = 'master'; } // B1: bridge 仅锁角色，不占 master 槽位
        else if (targetRole === 'subject') { occ.subject = ws; ws.roleLock = 'subject'; }
        else if (targetRole === 'console') { occ.console.push(ws); ws.roleLock = 'console'; }
        else { occ.monitor.push(ws); ws.roleLock = 'monitor'; }
        ws.role = targetRole;
        ws.sessionId = sessionId;
        ws.deviceInfo = msg.device_info || ws.deviceInfo || {};
        if (msg.udpTarget) { room.udpTargets.set('master', msg.udpTarget); }
        ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
        // Console-fix: 控制台是配置者，永远收到 locked:false（不受房间锁限制，否则控制台无法配置）
        const lockForRole = targetRole === 'console' ? false : room.locked !== false;
        ws.send(JSON.stringify({ type: 'room_config', locked: lockForRole, config: room.config }));
      } else {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '已被占用' }));
      }
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room, sessionId) });
      break;
    }

        case 'eeg_frame': {
      if (ws.role !== 'master' && !ws._bridge) return;
      if (!ws._bridge) {
        const hasAliveBridge = Array.from(room.sockets).some(s => s._bridge && (s.readyState === 1 || s.readyState === 2));
        if (hasAliveBridge) return;
      }
      frameRateTracker.count(ws.sessionId || sessionId);
      metrics.pushFrame(msg, ws.sessionId || sessionId);
      broadcastToRoles(room, msg, ['master', 'monitor']);
      break;
    }

case 'accel_frame': {
      if (ws.role !== 'master' && !ws._bridge) return;
      room.sockets.forEach(s => {
        if (s !== ws && (s.role === 'master' || s.role === 'monitor') && s.readyState === 1) s.send(raw);
      });
      break;
    }

    case 'cmd': {
      const ALLOW_ALL = ['master', 'console', 'monitor'];
      if (!ALLOW_ALL.includes(ws.role)) return;

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
          if (timer.running) break; // BUG7-fix: 防止重复 start 产生冗余 phase_start marker
          timer.running = true;
          const sp = getCurrentPhase(timer);
          const m = { type: 'marker', code: 1, source: 'auto', label: 'phase_start:' + sp.id, phase: sp.id, ts: now };
          persistMarker(sessionId, m);
          broadcast(room, m);
          break;
        }
        case 'pause':        timer.running = false; break;
        case 'next_phase': {
          const cp = getCurrentPhase(timer);
          const em = { type: 'marker', code: 2, source: 'auto', label: 'phase_end:' + cp.id, phase: cp.id, ts: now };
          persistMarker(sessionId, em);
          broadcast(room, em);
          advancePhase(sessionId, timer, room);
          if (!timer.completed) {
            const np = getCurrentPhase(timer);
            const sm = { type: 'marker', code: 1, source: 'auto', label: 'phase_start:' + np.id, phase: np.id, ts: Date.now() };
            persistMarker(sessionId, sm);
            broadcast(room, sm);
          }
          break;
        }
        case 'reset': {
          const rp = getCurrentPhase(timer);
          const rm = { type: 'marker', code: 2, source: 'auto', label: 'phase_end:' + rp.id + '(reset)', phase: rp.id, ts: now };
          persistMarker(sessionId, rm);
          broadcast(room, rm);
          resetTimer(sessionId, timer, room);
          break;
        }
        case 'set_auto':     timer.autoMode = !timer.autoMode; break;
        case 'set_task_type': if (msg.value) { timer.taskType = msg.value; } break;
      }
      // Broadcast action notice to all roles
      const actor = ws.nickname || ws.role || 'unknown';
      const actionLabels = { start:'▶ 开始实验', pause:'⏸ 暂停', next_phase:'⏭ 下一阶段', reset:'⟳ 重置', set_auto:'🔄 切换自动模式' };
      broadcast(room, { type: 'action_notice', action: msg.action, actor, label: actionLabels[msg.action] || msg.action, role: ws.role, ts: Date.now() });
      broadcastSync(sessionId, timer, room);
      break;
    }

    case 'marker': {
      persistMarker(sessionId, msg);
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
      // BUG7-fix: 统一走 persistMarker（与其他 marker 一致：先持久化再广播，DB 失败不阻断广播）
      persistMarker(sessionId, markerMsg);
      broadcast(room, markerMsg);
      sendMarkerUDP(room, markerMsg);
      broadcast(room, { type: 'action_notice', action: msg.state, actor: ws.nickname || '受试者', label: label, role: 'subject', ts: Date.now() });
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

    case 'refresh': {
      // 重新广播 room_info，不改变任何 socket 状态
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room, sessionId) });
      break;
    }

    case 'update_config_meta': {
      // 广播配置元数据（模板名、受试者名）给所有客户端
      if (msg.config) {
        room.config = { ...(room.config || {}), ...msg.config };
        broadcast(room, { type: 'update_config_meta', config: msg.config });
      }
      break;
    }

    case 'leave_room': {
      const occ = room.occupants;
      if (ws.roleLock === 'master' && occ.master === ws) occ.master = null;
      else if (ws.roleLock === 'subject' && occ.subject === ws) occ.subject = null;
      else if (ws.roleLock === 'console') occ.console = occ.console.filter(s => s !== ws);
      else if (ws.roleLock === 'monitor') occ.monitor = occ.monitor.filter(s => s !== ws);
      if (ws.roleLock) unlockRole(sessionId, ws.roleLock);
      room.sockets.delete(ws);
      ws.role = 'pending'; ws.roleLock = null;
      // 房间空了 → 重置锁和配置，下一个进入的人得到全新房间
      if (room.sockets.size === 0) { room.locked = true; room.config = null; }
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room, sessionId) });
      ws.send(JSON.stringify({ type: 'room_left' }));
      break;
    }

    case 'release_role': {
      const occ = room.occupants;
      if (ws.roleLock === 'master' && occ.master === ws) occ.master = null;
      else if (ws.roleLock === 'subject' && occ.subject === ws) occ.subject = null;
      else if (ws.roleLock === 'console') { occ.console = occ.console.filter(s => s !== ws); }
      else if (ws.roleLock === 'monitor') { occ.monitor = occ.monitor.filter(s => s !== ws); }
      // Clean role lock for this socket so it can re-claim
      if (ws.roleLock) unlockRole(sessionId, ws.roleLock);
      ws.role = 'pending'; ws.roleLock = null;
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room, sessionId) });
      ws.send(JSON.stringify({ type: 'role_released' }));
      break;
    }

    // ── 房间锁管理 ──

    case 'start_experiment': {
      // B4: 仅控制台/主控可操作
      if (ws.role !== 'console' && ws.role !== 'master') {
        ws.send(JSON.stringify({ type: 'role_denied', reason: 'permission denied' }));
        break;
      }
      if (!msg.template_type) {
        ws.send(JSON.stringify({ type: 'error', message: '实验模板类型不能为空' }));
        break;
      }
      room.locked = false;
      room.config = msg.config || {};
      broadcast(room, { type: 'room_config', locked: false, config: room.config });
      if (timers.has(sessionId)) {
        const t = timers.get(sessionId);
        clearInterval(t.tickTimer);
        timers.delete(sessionId);
      }
      metrics.cleanup(sessionId);
      baseline.cleanup(sessionId);
      initTimer(sessionId, msg.template_type);
      startTick(sessionId, timers.get(sessionId), room);
      ws.send(JSON.stringify({ type: 'experiment_started' }));
      break;
    }

    case 'end_experiment': {
      // B4: 仅控制台/主控可操作
      if (ws.role !== 'console' && ws.role !== 'master') {
        ws.send(JSON.stringify({ type: 'role_denied', reason: 'permission denied' }));
        break;
      }
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
      // B2: 切房时统一清理旧房间的 sockets + occupants 槽位
      if (ws.sessionId) removeSocketFromRoom(ws, rooms.get(ws.sessionId));
      getRoom(sid).sockets.add(ws);
      ws.sessionId = sid;
      broadcast(getRoom(sid), { type: 'room_info', occupants: getOccupantSummary(getRoom(sid), sid) });
      ws.send(JSON.stringify({ type: 'room_created', code, session_id: sid, url: '/?room=' + code }));
      break;
    }

    case 'join_room': {
      const entry = roomCodes.get(msg.code);
      if (entry && rooms.has(entry.sessionId)) {
        // B2: 切房时统一清理旧房间的 sockets + occupants 槽位
        if (ws.sessionId && ws.sessionId !== entry.sessionId) removeSocketFromRoom(ws, rooms.get(ws.sessionId));
        getRoom(entry.sessionId).sockets.add(ws);
        ws.sessionId = entry.sessionId;
        broadcast(getRoom(entry.sessionId), { type: 'room_info', occupants: getOccupantSummary(getRoom(entry.sessionId), entry.sessionId) });
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
      const baseId = msg.session_id || ('sess-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6));
      const sid = baseId + '-' + Date.now().toString(36);
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
        db.query('SELECT * FROM sessions WHERE id = ?', [sid]),
      ]).then(([markers, fss, sessions]) => {
        ws.send(JSON.stringify({
          type: 'session_detail',
          session: sessions[0] || null,
          markers, fss_results: fss,
        }));
      }).catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    case 'update_session_status': {
      // B4: 仅控制台/主控可操作
      if (ws.role !== 'console' && ws.role !== 'master') {
        ws.send(JSON.stringify({ type: 'role_denied', reason: 'permission denied' }));
        break;
      }
      db.query("UPDATE sessions SET status = ? WHERE id = ?", [msg.status, msg.session_id])
        .then(() => {
          ws.send(JSON.stringify({ type: 'session_status_updated', session_id: msg.session_id, status: msg.status }));
          // B9: completed → 先通知房间内在线客户端并主动关闭，延迟 500ms 再清理，避免在线客户端失去 timer
          if (msg.status === 'completed') {
            const targetRoom = rooms.get(msg.session_id);
            if (targetRoom) {
              broadcast(targetRoom, { type: 'session_completed', reason: 'session completed', session_id: msg.session_id });
              targetRoom.sockets.forEach(s => { if (s.readyState === 1) s.close(1000, 'session completed'); });
            }
            setTimeout(() => cleanupSession(msg.session_id), 500).unref();
          }
        })
        .catch(err => ws.send(JSON.stringify({ type: 'error', message: err.message })));
      break;
    }

    // W3: 未知消息类型回送 error
    default:
      ws.send(JSON.stringify({ type: 'error', message: 'unknown message type: ' + msg.type }));
      break;
  }
}

// ── HTTP + WS 服务（共享端口） ──
// B12: CORS 收紧为白名单，默认 * 兼容
const ALLOWED_ORIGINS = (process.env.CORS_ALLOWED_ORIGINS || '*').split(',').map(s => s.trim()).filter(Boolean);

function applyCORS(req, res) {
  const origin = req.headers.origin;
  if (origin && (ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(origin))) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  } else if (ALLOWED_ORIGINS.includes('*')) {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
}

const server = http.createServer((req, res) => {
  applyCORS(req, res);

  // B12: OPTIONS 预检
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.writeHead(204);
    res.end();
    return;
  }

  // Bridge 文件下载 (从磁盘读取，确保下载内容永远最新)
  if (req.method === 'GET' && req.url.startsWith('/api/download/bridge/')) {
    const file = req.url.replace('/api/download/bridge/', '');
    const bridgeDir = path.join(__dirname, '..', 'bridge');
    // S5-fix: config.js 已无密码(敏感信息移至环境变量)，重新加入白名单
    const safe = ['index.js', 'package.json', 'config.js'];
    if (!safe.includes(file)) { res.writeHead(403); res.end('Forbidden'); return; }
    const filePath = path.join(bridgeDir, file);
    try {
      const data = fs.readFileSync(filePath, 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end(data);
    } catch (err) {
      console.warn('[Download] bridge/%s not found:', file, err.message);
      res.writeHead(404); res.end('Not found');
    }
    return;
  }

  // 桥接状态检查 (前端用于判断本地桥接是否已连接 ECS)
  if (req.method === 'GET' && req.url.startsWith('/api/bridge-status')) {
    const url = new URL(req.url, 'http://localhost');
    const sid = url.searchParams.get('session_id') || '';
    const room = sid ? rooms.get(sid) : null;
    const masterAlive = room && room.occupants && !!(room.occupants.master || Array.from(room.sockets).some(s => s._bridge));
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

// S7: WS Origin 校验，防止跨站劫持
const wss = new WebSocketServer({
  server,
  maxPayload: 1024 * 1024,
  verifyClient: (info) => {
    const origin = info.req.headers.origin;
    if (!origin) return true; // 非浏览器客户端无 Origin
    const allowed = (process.env.WS_ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
    if (allowed.length === 0) return true; // 未配置则全放行
    try {
      const u = new URL(origin);
      return allowed.includes(u.hostname) || allowed.includes(origin);
    } catch (_) {
      return false;
    }
  },
});

// D4: server.listen 增加启动日志与错误处理
server.on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.error('[FATAL] 端口 %d 已被占用', config.WS_PORT);
    process.exit(1);
  }
  throw e;
});
server.listen(config.WS_PORT, () => {
  console.log(`[OK] EEG Cloud 服务启动 :${config.WS_PORT} (WS + HTTP API)`);
});

// 实时采样率广播（2 秒间隔，确保前端定期收到 frame_rate 更新）
setInterval(() => {
  rooms.forEach((room, sid) => {
    if (!room || room.sockets.size === 0) return;
    // B10: getRate 仅由 2 秒定时广播调用，结果缓存到 room._lastFrameRate 供其他路径复用
    room._lastFrameRate = frameRateTracker.getRate(sid);
    const summary = getOccupantSummary(room, sid);
    broadcast(room, { type: 'room_info', occupants: summary });
  });
}, 2000).unref();

// WS 心跳检测（每 30 秒 ping，超时 30 秒(一个 ping 周期)终止）
function heartbeat(ws) {
  // BUG8-fix: 使用 _pendingPong 单周期检测，避免两个周期间的 pong 竞态
  if (ws._pendingPong) { ws.close(1011, 'heartbeat timeout'); return; }
  ws._pendingPong = true;
  try { ws.ping(); } catch (_) { ws._pendingPong = false; }
}
const hbTimer = setInterval(() => wss.clients.forEach(heartbeat), 30000).unref();
wss.on('connection', (ws, req) => {
  ws._pendingPong = false;
  ws.role = 'pending';
  ws.sessionId = null;
  ws.roleLock = null;
  ws.on('pong', () => { ws._pendingPong = false; });

  // W4: 根据 isBinary 分别处理；二进制走 accel_frame 转发，文本走 JSON.parse
  ws.on('message', (raw, isBinary) => {
    if (isBinary) {
      // 二进制帧：作为 accel_frame 原样转发给 master/monitor
      const sid = ws.sessionId;
      if (!sid) { ws.send(JSON.stringify({ type: 'error', message: 'session_id required' })); return; }
      const room = rooms.get(sid);
      if (!room) return;
      if (ws.role !== 'master' && !ws._bridge) return;
      room.sockets.forEach(s => {
        if (s !== ws && (s.role === 'master' || s.role === 'monitor') && s.readyState === 1) s.send(raw);
      });
      return;
    }

    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }

    const sessionId = msg.session_id || ws.sessionId;
    // B5: 未携带 session_id 的非 hello/create_room/join_room/ping/pong 消息直接拒绝，避免 'default' 串扰
    // BUG4-fix: ping/pong 加入豁免名单，避免应用层心跳被拦截
    if (!sessionId && !['hello', 'create_room', 'join_room', 'ping', 'pong'].includes(msg.type)) {
      ws.send(JSON.stringify({ type: 'error', message: 'session_id required' }));
      return;
    }
    const sid = sessionId || 'default';
    const room = getRoom(sid);

    if (msg.type === 'hello' || msg.type === 'reconnect') {
      // BUG5-fix: 用 sid（已兜底为 'default'）而非可能为 undefined 的 sessionId 作为 timer key
      // B3: 删除冗余二次检查，将 handleMessage 移到 timer 初始化的 then 回调中执行
      if (!timers.has(sid)) {
        db.query('SELECT t.switch_type FROM sessions s JOIN experiment_templates t ON s.template_id = t.id WHERE s.id = ?', [sid])
          .then(rows2 => {
            if (!timers.has(sid)) {
              initTimer(sid, (rows2.length > 0 ? rows2[0].switch_type : 'control'));
              startTick(sid, timers.get(sid), room);
            }
            handleMessage(ws, raw, room, sid);
          })
          .catch(() => {
            if (!timers.has(sid)) {
              initTimer(sid, 'control');
              startTick(sid, timers.get(sid), room);
            }
            handleMessage(ws, raw, room, sid);
          });
      } else {
        // timer 已存在，直接处理
        handleMessage(ws, raw, room, sid);
      }
    } else {
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
        // 立即释放槽位; 桥接连接不锁（浏览器接管）
        if (roleLock === 'master') {
          if (room.occupants.master === ws) room.occupants.master = null;
          if (!ws._bridge) {
            const timer = timers.get(sid);
            if (timer) { timer.running = false; broadcastSync(sid, timer, room); }
            broadcast(room, { type: 'alert', level: 'warning', message: '数据源已断开' });
          }
        }
        if (roleLock === 'subject' && room.occupants.subject === ws) room.occupants.subject = null;
        if (roleLock === 'console') { room.occupants.console = room.occupants.console.filter(s => s !== ws); }

        if (ws._bridge) {
          broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room, sid) });
        } else {
          // W6: 角色锁时长带随机抖动 27.5s-32.5s
          lockRoleWithJitter(sid, roleLock);
          // W5: 重连 setTimeout 保存到 room._reconnectTimer，cleanupSession 中 clearTimeout
          if (room._reconnectTimer) clearTimeout(room._reconnectTimer);
          room._reconnectTimer = setTimeout(() => {
            const r = rooms.get(sid);
            if (r) broadcast(r, { type: 'room_info', occupants: getOccupantSummary(r, sid) });
          }, 30000);
          room._reconnectTimer.unref();
        }
      } else if (roleLock === 'monitor') {
        room.occupants.monitor = room.occupants.monitor.filter(s => s !== ws);
      }
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room, sid) });
    }
  });

  // B6: 不再吞掉错误，记录日志便于排查
  ws.on('error', (err) => {
    console.warn('[WS] error:', err.message);
  });
});
