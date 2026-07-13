/**
 * ECS 云端 WebSocket 中继
 *
 * 职责:
 *   - 统一入口: 所有设备先 hello + pending, 再 claim_role
 *   - 角色认领 + 房间管理 (断线保留 30 秒)
 *   - eeg_frame 广播 + 指标计算
 *   - 计时器仲裁 + phase_sync
 *   - marker 广播 + 持久化 + UDP 回传 (所有 source)
 *   - 基线录制 + 恢复判定
 *   - cmd 权限白名单
 */

const { WebSocketServer } = require('ws');
const config = require('./config');
const db = require('./db');
const metrics = require('./metrics');
const baseline = require('./baseline');

// ── 房间状态 ──
const rooms = new Map(); // sessionId → room

function getRoom(sessionId) {
  if (!rooms.has(sessionId)) {
    rooms.set(sessionId, { sockets: new Set(), occupants: { master: null, monitor: [], subject: null, console: null }, udpTargets: new Map() });
  }
  return rooms.get(sessionId);
}

function broadcast(room, msg, exclude = null) {
  const payload = JSON.stringify(msg);
  room.sockets.forEach(ws => {
    if (ws !== exclude && ws.readyState === 1) ws.send(payload);
  });
}

// 只广播给特定角色
function broadcastToRoles(room, msg, roles) {
  const payload = JSON.stringify(msg);
  room.sockets.forEach(ws => {
    if (ws.readyState === 1 && roles.includes(ws.role)) ws.send(payload);
  });
}

// ── 计时器持久化 ──
function saveTimerState(sessionId) {
  const timer = timers.get(sessionId);
  if (!timer) return;
  const templateType = Object.keys(PHASE_TEMPLATES).find(k => PHASE_TEMPLATES[k] === timer.phases) || 'control';
  db.query(
    'REPLACE INTO timer_state (session_id, phase_index, time_left, time_in_phase, running, auto_mode, template_type) VALUES (?,?,?,?,?,?,?)',
    [sessionId, timer.phaseIndex, timer.timeLeft, timer.timeInPhase, timer.running ? 1 : 0, timer.autoMode ? 1 : 0, templateType]
  ).catch(() => {});
}

async function restoreTimerState(sessionId) {
  try {
    const rows = await db.query('SELECT * FROM timer_state WHERE session_id = ?', [sessionId]);
    if (rows.length === 0) return null;
    const row = rows[0];
    const phases = PHASE_TEMPLATES[row.template_type] || PHASE_TEMPLATES.control;
    const timer = {
      phases, phaseIndex: row.phase_index, timeLeft: row.time_left,
      timeInPhase: row.time_in_phase, running: false,
      autoMode: row.auto_mode === 1, completed: false, tickTimer: null,
    };
    timers.set(sessionId, timer);
    return timer;
  } catch (e) { return null; }
}

// ── 计时器 ──
const timers = new Map();
const PHASE_TEMPLATES = {
  control: [
    { id: 'prep',    duration: 300, round: 0, name: '准备阶段',          taskType: null },
    { id: 'flow1',   duration: 480, round: 1, name: '心流诱导阶段',      taskType: 'math' },
    { id: 'switch1', duration: 120, round: 1, name: '任务继续阶段',      taskType: 'math' },
    { id: 'recover1',duration: 600, round: 1, name: '状态恢复观测阶段',   taskType: 'math' },
    { id: 'rest1',   duration: 180, round: 1, name: '休息与问卷',        taskType: null },
    { id: 'flow2',   duration: 480, round: 2, name: '心流诱导阶段',      taskType: 'math' },
    { id: 'switch2', duration: 120, round: 2, name: '任务继续阶段',      taskType: 'math' },
    { id: 'recover2',duration: 600, round: 2, name: '状态恢复观测阶段',   taskType: 'math' },
    { id: 'rest2',   duration: 180, round: 2, name: '休息与问卷',        taskType: null },
    { id: 'flow3',   duration: 480, round: 3, name: '心流诱导阶段',      taskType: 'math' },
    { id: 'switch3', duration: 120, round: 3, name: '任务继续阶段',      taskType: 'math' },
    { id: 'recover3',duration: 600, round: 3, name: '状态恢复观测阶段',   taskType: 'math' },
    { id: 'rest3',   duration: 180, round: 3, name: '休息与问卷',        taskType: null },
  ],
  math_art: [
    { id: 'prep',    duration: 300, round: 0, name: '准备阶段',              taskType: null },
    { id: 'flow1',   duration: 480, round: 1, name: '心流诱导阶段（数理）',  taskType: 'math' },
    { id: 'switch1', duration: 120, round: 1, name: '任务切换（数理→艺术）', taskType: 'art' },
    { id: 'recover1',duration: 600, round: 1, name: '状态恢复观测阶段',       taskType: 'math' },
    { id: 'rest1',   duration: 180, round: 1, name: '休息与问卷',            taskType: null },
    { id: 'flow2',   duration: 480, round: 2, name: '心流诱导阶段（数理）',  taskType: 'math' },
    { id: 'switch2', duration: 120, round: 2, name: '任务切换（数理→艺术）', taskType: 'art' },
    { id: 'recover2',duration: 600, round: 2, name: '状态恢复观测阶段',       taskType: 'math' },
    { id: 'rest2',   duration: 180, round: 2, name: '休息与问卷',            taskType: null },
    { id: 'flow3',   duration: 480, round: 3, name: '心流诱导阶段（数理）',  taskType: 'math' },
    { id: 'switch3', duration: 120, round: 3, name: '任务切换（数理→艺术）', taskType: 'art' },
    { id: 'recover3',duration: 600, round: 3, name: '状态恢复观测阶段',       taskType: 'math' },
    { id: 'rest3',   duration: 180, round: 3, name: '休息与问卷',            taskType: null },
  ],
};

// ── 消息路由 ──
function handleMessage(ws, raw, room, sessionId) {
  let msg;
  try { msg = JSON.parse(raw); } catch (e) { return; }

  switch (msg.type) {
    case 'hello': {
      // 统一: hello 只注册为 pending, 不分配角色
      room.sockets.add(ws);
      ws.role = 'pending';
      ws.sessionId = sessionId;
      ws.pendingRole = null;
      ws.releaseTimer = null;
      ws.send(JSON.stringify({ type: 'room_info', occupants: getOccupantSummary(room) }));
      break;
    }

    case 'reconnect': {
      // 断线重连: 验证 session_id + role, 恢复槽位
      const targetRole = msg.role;
      const existing = findExistingSlot(room, targetRole);
      if (existing === ws) {
        // 同一 socket 重连自己
        room.sockets.add(ws);
        ws.role = targetRole;
        ws.sessionId = sessionId;
        if (msg.udpTarget) room.udpTargets.set('master', msg.udpTarget);
        if (msg.udpTarget) ws.udpTarget = msg.udpTarget;
        if (ws.releaseTimer) { clearTimeout(ws.releaseTimer); ws.releaseTimer = null; }
        ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
        broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
        // 恢复计时器
        const timer = timers.get(sessionId);
        if (timer && targetRole === 'master') { timer.running = false; broadcastSync(sessionId, timer, room); }
      } else {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '槽位已被其他设备占用' }));
      }
      break;
    }

    case 'claim_role': {
      const targetRole = msg.role;
      const occupations = room.occupants;
      let canClaim = false;
      if (targetRole === 'master' && !occupations.master) canClaim = true;
      else if (targetRole === 'subject' && !occupations.subject) canClaim = true;
      else if (targetRole === 'console' && !occupations.console) canClaim = true;
      else if (targetRole === 'monitor') canClaim = true;

      if (canClaim) {
        if (targetRole === 'master')       { occupations.master = ws; ws.roleLock = 'master'; }
        else if (targetRole === 'subject') { occupations.subject = ws; ws.roleLock = 'subject'; }
        else if (targetRole === 'console') { occupations.console = ws; ws.roleLock = 'console'; }
        else if (targetRole === 'monitor') { occupations.monitor.push(ws); ws.roleLock = 'monitor'; }
        ws.role = targetRole;
        ws.sessionId = sessionId;
        if (msg.udpTarget) { room.udpTargets.set('master', msg.udpTarget); ws.udpTarget = msg.udpTarget; }
        ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
      } else {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '已被占用' }));
      }
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
      break;
    }

    case 'eeg_frame': {
      if (ws.role !== 'master') return;
      // 转发到所有 monitor, 不转发回 master
      room.sockets.forEach(s => {
        if (s !== ws && s.role === 'monitor' && s.readyState === 1) s.send(raw);
      });
      metrics.pushFrame(msg);
      break;
    }

    case 'cmd': {
      // cmd 权限白名单: master + console 可发所有; monitor 可发 set_auto; subject 禁止
      const ALLOW_ALL = ['master', 'console'];
      const ALLOW_AUTO = ['monitor'];
      if (!ALLOW_ALL.includes(ws.role) && !ALLOW_AUTO.includes(ws.role)) return;
      if (ws.role === 'monitor' && msg.action !== 'set_auto') return;

      const timer = timers.get(sessionId);
      if (!timer) return;
      if (msg.action === 'start')       timer.running = true;
      else if (msg.action === 'pause')  timer.running = false;
      else if (msg.action === 'next_phase') advancePhase(sessionId, timer, room);
      else if (msg.action === 'reset')  resetTimer(sessionId, timer, room);
      else if (msg.action === 'set_auto') timer.autoMode = !timer.autoMode;
      broadcastSync(sessionId, timer, room);
      break;
    }

    case 'marker': {
      db.query(
        'INSERT INTO markers (session_id, code, source, label, phase, ts) VALUES (?,?,?,?,?,?)',
        [sessionId, msg.code || 0, msg.source || 'operator', msg.label || '', msg.phase || '', msg.ts || Date.now()]
      ).catch(() => {});
      // 广播所有端
      broadcast(room, msg);
      // UDP 回传 master → OpenBCI GUI (所有 source 都回传, 包括 master 自身)
      sendMarkerUDP(room, msg);
      break;
    }

    case 'self_report': {
      const code = msg.state === 'flow_enter' ? 7 : (msg.state === 'flow_exit' ? 8 : 9);
      const label = msg.state === 'flow_enter' ? '心流进入（自评）' : (msg.state === 'flow_exit' ? '心流脱离（自评）' : '状态异常（自评）');
      const markerMsg = { type: 'marker', code, source: 'subject', label, phase: msg.phase || '', ts: Date.now() };
      // 走完整 marker 路径 (含 MySQL + UDP 回传)
      db.query(
        'INSERT INTO markers (session_id, code, source, label, phase, ts) VALUES (?,?,?,?,?,?)',
        [sessionId, code, 'subject', label, markerMsg.phase, markerMsg.ts]
      ).catch(() => {});
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
      }).catch((err) => {
        ws.send(JSON.stringify({ type: 'fss_submit_ack', success: false, error: err.message }));
      });
      break;
    }

    case 'set_udp_target': {
      if (ws.role === 'master') {
        room.udpTargets.set('master', msg.target);
        ws.udpTarget = msg.target;
      }
      break;
    }
  }
}

function sendMarkerUDP(room, markerMsg) {
  // 向 master 的 udpTarget 发送 UDP marker 数据包
  const target = room.udpTargets.get('master');
  if (!target) return;
  const [host, port] = target.split(':');
  if (!host || !port) return;
  try {
    const dgram = require('dgram');
    const sock = dgram.createSocket('udp4');
    const buf = Buffer.alloc(4);
    buf.writeFloatBE(markerMsg.code || 0);
    sock.send(buf, 0, 4, parseInt(port), host, () => sock.close());
  } catch (e) { /* UDP 丢包可接受 */ }
}

function findExistingSlot(room, role) {
  if (role === 'master') return room.occupants.master;
  if (role === 'subject') return room.occupants.subject;
  if (role === 'console') return room.occupants.console;
  return null;
}

function getOccupantSummary(room) {
  return {
    master: room.occupants.master ? true : false,
    monitor: room.occupants.monitor.filter(s => s.readyState === 1 || s.readyState === 2).length,
    subject: room.occupants.subject ? true : false,
    console: room.occupants.console ? true : false,
  };
}

// ── 计时器 ── (同原有逻辑, 略精简)
function initTimer(sessionId, templateType) {
  const phases = PHASE_TEMPLATES[templateType] || PHASE_TEMPLATES.control;
  const timer = { phases, phaseIndex: 0, timeLeft: phases[0].duration, timeInPhase: 0, running: false, autoMode: true, completed: false, tickTimer: null };
  timers.set(sessionId, timer);
  saveTimerState(sessionId);
  return timer;
}

function getCurrentPhase(timer) { return timer.phases[timer.phaseIndex] || timer.phases[0]; }

function advancePhase(sessionId, timer, room) {
  if (timer.phaseIndex >= timer.phases.length - 1) { timer.completed = true; timer.running = false; clearInterval(timer.tickTimer); timer.tickTimer = null; saveTimerState(sessionId); broadcastSync(sessionId, timer, room); return; }
  timer.phaseIndex++;
  const phase = getCurrentPhase(timer);
  timer.timeLeft = phase.duration;
  timer.timeInPhase = 0;
  saveTimerState(sessionId);
  broadcastSync(sessionId, timer, room);
}

function resetTimer(sessionId, timer, room) {
  clearInterval(timer.tickTimer);
  timer.phaseIndex = 0; timer.timeLeft = timer.phases[0].duration; timer.timeInPhase = 0;
  timer.running = false; timer.completed = false;
  saveTimerState(sessionId);
  broadcastSync(sessionId, timer, room);
}

function broadcastSync(sessionId, timer, room) {
  const phase = getCurrentPhase(timer);
  broadcast(room, { type: 'phase_sync', phase_index: timer.phaseIndex, phase_id: phase.id, phase_name: phase.name, round: phase.round, time_left: timer.timeLeft, time_in_phase: timer.timeInPhase, running: timer.running, auto_mode: timer.autoMode, completed: timer.completed, task_type: phase.taskType });
}

function startTick(sessionId, timer, room) {
  if (timer.tickTimer) clearInterval(timer.tickTimer);
  timer.tickTimer = setInterval(() => {
    const room = rooms.get(sessionId);
    if (!room) { clearInterval(timer.tickTimer); return; }
    if (timer.running && !timer.completed) {
      timer.timeLeft--; timer.timeInPhase++;
      // 每秒持久化计时器状态
      saveTimerState(sessionId);
      const now = Date.now();
      const phase = getCurrentPhase(timer);
      const snapshot = metrics.tick(now, sessionId, timer.phaseIndex, phase.id);
      if (snapshot) {
        db.query('INSERT INTO metrics_snapshots (session_id, ts, phase_index, phase_id, delta, theta, alpha, beta, gamma, theta_alpha_ratio, spectral_entropy, cognitive_load_index, sq_ch1, sq_ch2, sq_ch3, sq_ch4) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
          [sessionId, snapshot.ts, snapshot.phase_index, snapshot.phase_id,
           snapshot.band_power.delta, snapshot.band_power.theta, snapshot.band_power.alpha,
           snapshot.band_power.beta, snapshot.band_power.gamma,
           snapshot.theta_alpha_ratio, snapshot.spectral_entropy, snapshot.cognitive_load_index,
           snapshot.signal_quality[0], snapshot.signal_quality[1], snapshot.signal_quality[2], snapshot.signal_quality[3]]
        ).catch(() => {});
        // metrics 只广播给 master + monitor
        broadcastToRoles(room, { type: 'metrics_snapshot', ...snapshot }, ['master', 'monitor']);
      }
      if (timer.timeLeft <= 0) {
        broadcast(room, { type: 'marker', code: 2, source: 'auto', label: 'phase_end:' + phase.id, ts: now });
        advancePhase(sessionId, timer, room);
        const nextPhase = getCurrentPhase(timer);
        if (!timer.completed) broadcast(room, { type: 'marker', code: 1, source: 'auto', label: 'phase_start:' + nextPhase.id, ts: Date.now() });
        if (!timer.autoMode && !timer.completed) timer.running = false;
      }
    }
    broadcastSync(sessionId, timer, room);
  }, 1000);
}

// ── WebSocket 服务 ──
const wss = new WebSocketServer({ port: config.WS_PORT });

wss.on('connection', (ws, req) => {
  ws.role = 'pending';
  ws.sessionId = null;
  ws.roleLock = null;
  ws.releaseTimer = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    const sessionId = msg.session_id || ws.sessionId || 'default';
    const room = getRoom(sessionId);

    if (msg.type === 'hello' || msg.type === 'reconnect') {
      if (!timers.has(sessionId)) {
        initTimer(sessionId, msg.template_type || 'control');
        startTick(sessionId, timers.get(sessionId), room);
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
      // 有限角色位: 延迟 30 秒释放, 给重连机会
      if (roleLock === 'master' || roleLock === 'subject' || roleLock === 'console') {
        if (ws.releaseTimer) clearTimeout(ws.releaseTimer);
        ws.releaseTimer = setTimeout(() => {
          const r = rooms.get(sid);
          if (!r) return;
          if (roleLock === 'master' && r.occupants.master === ws) r.occupants.master = null;
          if (roleLock === 'subject' && r.occupants.subject === ws) r.occupants.subject = null;
          if (roleLock === 'console' && r.occupants.console === ws) r.occupants.console = null;
          broadcast(r, { type: 'room_info', occupants: getOccupantSummary(r) });
        }, 30000);
      } else if (roleLock === 'monitor') {
        room.occupants.monitor = room.occupants.monitor.filter(s => s !== ws);
      }
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
    }
  });
});

console.log(`[WS] EEG Cloud 中继启动 :${config.WS_PORT}`);
