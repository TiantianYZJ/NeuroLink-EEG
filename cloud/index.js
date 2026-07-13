/**
 * ECS 云端 WebSocket 中继
 *
 * 职责:
 *   - 角色认领 + 房间管理
 *   - eeg_frame 广播 + 指标计算
 *   - 计时器仲裁 + phase_sync
 *   - marker 广播 + 持久化
 *   - 基线录制 + 恢复判定
 */

const { WebSocketServer } = require('ws');
const config = require('./config');
const db = require('./db');
const metrics = require('./metrics');
const baseline = require('./baseline');

// ── 房间状态 ──
const rooms = new Map(); // sessionId → { sockets: Set<Ws>, occupants: { master, monitor[], subject, console } }

function getRoom(sessionId) {
  if (!rooms.has(sessionId)) {
    rooms.set(sessionId, { sockets: new Set(), occupants: { master: null, monitor: [], subject: null, console: null } });
  }
  return rooms.get(sessionId);
}

function broadcast(room, msg, exclude = null) {
  const payload = JSON.stringify(msg);
  room.sockets.forEach(ws => {
    if (ws !== exclude && ws.readyState === 1) ws.send(payload);
  });
}

// ── 计时器状态 ──
const timers = new Map(); // sessionId → { phaseIndex, timeLeft, running, autoMode, tickTimer }

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
      const role = msg.role || 'monitor';
      if (role === 'master')       room.occupants.master = ws;
      else if (role === 'subject') room.occupants.subject = ws;
      else if (role === 'console') room.occupants.console = ws;
      else if (role === 'monitor') room.occupants.monitor.push(ws);
      room.sockets.add(ws);
      ws.role = role;
      ws.sessionId = sessionId;

      ws.send(JSON.stringify({ type: 'room_info', occupants: getOccupantSummary(room) }));

      // 主控机注册 UDP target
      if (role === 'master' && msg.udpTarget) {
        ws.udpTarget = msg.udpTarget;
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
        if (targetRole === 'master')       occupations.master = ws;
        else if (targetRole === 'subject') occupations.subject = ws;
        else if (targetRole === 'console') occupations.console = ws;
        else if (targetRole === 'monitor') occupations.monitor.push(ws);
        ws.role = targetRole;
        ws.sessionId = sessionId;
        if (msg.udpTarget) ws.udpTarget = msg.udpTarget;
        ws.send(JSON.stringify({ type: 'role_claimed', role: targetRole }));
      } else {
        ws.send(JSON.stringify({ type: 'role_denied', role: targetRole, reason: '已被占用' }));
      }

      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
      break;
    }

    case 'eeg_frame': {
      // 仅 master 可发送
      if (ws.role !== 'master') return;
      // 转发到所有 monitor
      room.sockets.forEach(s => {
        if (s !== ws && s.role === 'monitor' && s.readyState === 1) s.send(raw);
      });
      // 指标引擎
      metrics.pushFrame(msg);
      break;
    }

    case 'cmd': {
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
      broadcast(room, msg);
      // 回传 master → OpenBCI GUI 通过 UDP
      if (ws.role !== 'master' && room.occupants.master && room.occupants.master.readyState === 1) {
        room.occupants.master.send(JSON.stringify(msg));
      }
      break;
    }

    case 'self_report': {
      const code = msg.state === 'flow_enter' ? 7 : 8;
      const label = msg.state === 'flow_enter' ? '心流进入（自评）' : '心流脱离（自评）';
      const markerMsg = { type: 'marker', code, source: 'subject', label, ts: Date.now() };
      handleMessage(ws, JSON.stringify(markerMsg), room, sessionId);
      break;
    }

    case 'fss_submit': {
      db.query(
        'INSERT INTO fss_results (session_id, round, phase, answers) VALUES (?,?,?,?)',
        [sessionId, msg.round || 0, msg.phase || '', JSON.stringify(msg.answers || [])]
      ).catch(() => {});
      ws.send(JSON.stringify({ type: 'fss_submit_ack' }));
      break;
    }

    case 'set_udp_target': {
      if (ws.role === 'master') ws.udpTarget = msg.target;
      break;
    }
  }
}

function getOccupantSummary(room) {
  return {
    master: room.occupants.master ? true : false,
    monitor: room.occupants.monitor.length,
    subject: room.occupants.subject ? true : false,
    console: room.occupants.console ? true : false,
  };
}

// ── 计时器 ──
function initTimer(sessionId, templateType) {
  const phases = PHASE_TEMPLATES[templateType] || PHASE_TEMPLATES.control;
  const timer = {
    phases,
    phaseIndex: 0,
    timeLeft: phases[0].duration,
    timeInPhase: 0,
    running: false,
    autoMode: true,
    completed: false,
    recoverSteady: 0,
    tickTimer: null,
  };
  timers.set(sessionId, timer);
  return timer;
}

function getCurrentPhase(timer) {
  return timer.phases[timer.phaseIndex] || timer.phases[0];
}

function advancePhase(sessionId, timer, room) {
  if (timer.phaseIndex >= timer.phases.length - 1) {
    timer.completed = true;
    timer.running = false;
    clearInterval(timer.tickTimer);
    timer.tickTimer = null;
    broadcastSync(sessionId, timer, room);
    return;
  }
  timer.phaseIndex++;
  const phase = getCurrentPhase(timer);
  timer.timeLeft = phase.duration;
  timer.timeInPhase = 0;
  timer.recoverSteady = 0;
  baseline.onPhaseChange(sessionId, phase.id, 0, null);
  broadcastSync(sessionId, timer, room);
}

function resetTimer(sessionId, timer, room) {
  clearInterval(timer.tickTimer);
  timer.phaseIndex = 0;
  timer.timeLeft = timer.phases[0].duration;
  timer.timeInPhase = 0;
  timer.running = false;
  timer.completed = false;
  timer.recoverSteady = 0;
  broadcastSync(sessionId, timer, room);
}

function broadcastSync(sessionId, timer, room) {
  const phase = getCurrentPhase(timer);
  const payload = {
    type: 'phase_sync',
    phase_index: timer.phaseIndex,
    phase_id: phase.id,
    phase_name: phase.name,
    round: phase.round,
    time_left: timer.timeLeft,
    time_in_phase: timer.timeInPhase,
    running: timer.running,
    auto_mode: timer.autoMode,
    completed: timer.completed,
    task_type: phase.taskType,
  };
  broadcast(room, payload);
}

function startTick(sessionId, timer, room) {
  if (timer.tickTimer) clearInterval(timer.tickTimer);
  timer.tickTimer = setInterval(() => {
    const room = rooms.get(sessionId);
    if (!room) { clearInterval(timer.tickTimer); return; }

    if (timer.running && !timer.completed) {
      timer.timeLeft--;
      timer.timeInPhase++;

      // 指标计算
      const now = Date.now();
      const phase = getCurrentPhase(timer);
      const snapshot = metrics.tick(now, sessionId, timer.phaseIndex, phase.id);
      if (snapshot) {
        // 写入 MySQL
        db.query(
          'INSERT INTO metrics_snapshots (session_id, ts, phase_index, phase_id, delta, theta, alpha, beta, gamma, theta_alpha_ratio, spectral_entropy, cognitive_load_index, sq_ch1, sq_ch2, sq_ch3, sq_ch4) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
          [sessionId, snapshot.ts, snapshot.phase_index, snapshot.phase_id,
           snapshot.band_power.delta, snapshot.band_power.theta, snapshot.band_power.alpha,
           snapshot.band_power.beta, snapshot.band_power.gamma,
           snapshot.theta_alpha_ratio, snapshot.spectral_entropy, snapshot.cognitive_load_index,
           snapshot.signal_quality[0], snapshot.signal_quality[1], snapshot.signal_quality[2], snapshot.signal_quality[3]]
        ).catch(() => {});
        // 广播指标
        broadcast(room, { type: 'metrics_snapshot', ...snapshot });
      }

      // 基线录制 + 恢复判定
      baseline.onPhaseChange(sessionId, phase.id, timer.timeInPhase, snapshot);

      // 判定阶段结束
      if (timer.timeLeft <= 0) {
        // marker: phase_end
        broadcast(room, { type: 'marker', code: 2, source: 'auto', label: 'phase_end:' + phase.id, ts: now });
        advancePhase(sessionId, timer, room);
        // marker: phase_start
        const nextPhase = getCurrentPhase(timer);
        if (!timer.completed) {
          broadcast(room, { type: 'marker', code: 1, source: 'auto', label: 'phase_start:' + nextPhase.id, ts: Date.now() });
        }
        // autoMode 下自动继续
        if (timer.autoMode && !timer.completed) {
          // timer 已经在 advancePhase 中推进了，继续 tick
        } else if (!timer.completed) {
          timer.running = false;
        }
      }
    }
    broadcastSync(sessionId, timer, room);
  }, 1000);
}

// ── WebSocket 服务 ──
const wss = new WebSocketServer({ port: config.WS_PORT });

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  ws.role = 'pending';

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === 'hello' || msg.type === 'claim_role') {
      const sessionId = msg.session_id || 'default';
      const room = getRoom(sessionId);

      // 首次连接时初始化计时器
      if (!timers.has(sessionId) && msg.type === 'hello') {
        const templateType = msg.template_type || 'control';
        const timer = initTimer(sessionId, templateType);
        startTick(sessionId, timer, room);
      }

      handleMessage(ws, raw, room, sessionId);
    } else {
      // 已注册的 ws 应该有关联的 sessionId
      const sessionId = ws.sessionId || 'default';
      const room = getRoom(sessionId);
      handleMessage(ws, raw, room, sessionId);
    }
  });

  ws.on('close', () => {
    // 从房间移除
    for (const [sid, room] of rooms) {
      room.sockets.delete(ws);
      if (room.occupants.master === ws) room.occupants.master = null;
      if (room.occupants.subject === ws) room.occupants.subject = null;
      if (room.occupants.console === ws) room.occupants.console = null;
      room.occupants.monitor = room.occupants.monitor.filter(s => s !== ws);
      // 通知变更
      broadcast(room, { type: 'room_info', occupants: getOccupantSummary(room) });
    }
  });
});

console.log(`[WS] EEG Cloud 中继启动 :${config.WS_PORT}`);
console.log(`[WS] 等待连接...`);
