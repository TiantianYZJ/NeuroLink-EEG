/**
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
const fs = require('fs');
const path = require('path');
const config = require('./config');

// ── 状态 ──
let packetCount = 0;
let lastStatsTime = Date.now();
let sampleRate = 0;
let frameCount = 0;         // 实际 EEG 帧计数器（emitBatch 拆分后）
const isGanglion = config.DEVICE_TYPE === 'ganglion';
const chCount = isGanglion ? 4 : 8;

// ── 1. UDP 监听（接收 OpenBCI 数据） ──
const udpServer = dgram.createSocket('udp4');

let udpFormat = null; // null=undetected, 'json', 'float32_be', 'binary'

function parseBinaryPacket(msg) {
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

function parseJSONPacket(msg) {
  // OpenBCI GUI JSON — timeSeriesRaw/timeSeriesFilt:
  //   {"type":"timeSeriesRaw","data":[[ch0_s0,ch0_s1,...],[ch1_s0,...],...]}
  //   data[channel][sample] — take the LAST sample from each channel
  // bandPower:
  //   {"type":"bandPower","data":[[ch0_d,t,a,b,g],[ch1_d,t,a,b,g],...]}
  // averageBandPower:
  //   {"type":"averageBandPower","data":[d,t,a,b,g]}
  try {
    const text = Buffer.from(msg).toString('utf8').trim();
    if (text[0] !== '{' && text[0] !== '[') return null;
    const obj = JSON.parse(text);
    const raw = obj.data || obj.channels || obj;
    if (!Array.isArray(raw) || raw.length < Math.min(chCount, 1)) return null;

    // Case 1: 2D array — data[channel][sample] → return ALL samples
    if (Array.isArray(raw[0])) {
      const allChannels = raw.slice(0, chCount).map(ch =>
        Array.isArray(ch) ? ch.map(v => typeof v === 'number' ? v : 0) : [0]
      );
      return { sampleNumber: obj.sample || obj.sampleNumber || 0, channels: allChannels };
    }

    // Case 2: 1D flat array — data[0..N]
    const channels = raw.slice(0, chCount).map(v => typeof v === 'number' ? v : 0);
    return { sampleNumber: obj.sample || obj.sampleNumber || 0, channels };
  } catch (_) {}
  return null;
}

function parseFloat32BEPacket(msg) {
  // OpenBCI raw float32 big-endian: [sample(f32), ch1(f32), ch2(f32), ...]
  // struct.unpack('>%df' % N, data)
  const len = msg.length;
  const recordBytes = (1 + chCount) * 4;
  if (len < recordBytes) return null;
  const dv = new DataView(msg.buffer, msg.byteOffset, msg.byteLength);
  // Big-endian (!) — OpenBCI GUI uses '>' format
  const sampleNumber = Math.round(dv.getFloat32(0, false));
  const channels = [];
  for (let i = 0; i < chCount; i++) {
    channels.push(dv.getFloat32((1 + i) * 4, false));
  }
  return { sampleNumber, channels };
}

function parseOpenBCIPacket(msg) {
  if (udpFormat === 'json') return parseJSONPacket(msg);
  if (udpFormat === 'float32_be') return parseFloat32BEPacket(msg);
  if (udpFormat === 'binary') return parseBinaryPacket(msg);
  // Auto-detect: JSON first (GUI default), then big-endian float32, then binary
  let p = parseJSONPacket(msg);
  if (p) { udpFormat = 'json'; console.log('[UDP] OpenBCI JSON 格式'); return p; }
  p = parseFloat32BEPacket(msg);
  if (p) { udpFormat = 'float32_be'; console.log('[UDP] float32 big-endian 格式'); return p; }
  p = parseBinaryPacket(msg);
  if (p) { udpFormat = 'binary'; console.log('[UDP] 二进制 0xA0 格式'); return p; }
  return null;
}

const frameBroadcast = (parsed) => {
  const payload = JSON.stringify({ type: 'eeg_frame', seq: parsed.sampleNumber || 0, channels: parsed.channels, ts: Date.now() });
  localWss.clients.forEach(c => { if (c.readyState === 1) c.send(payload); });
  if (ecsWs && ecsWs.readyState === 1 && ecsConnected) ecsWs.send(payload);
};

// Emit all samples in a 2D batch as individual eeg_frames
const emitBatch = (parsed) => {
  if (!Array.isArray(parsed.channels) || !Array.isArray(parsed.channels[0])) {
    frameCount++;
    frameBroadcast(parsed);
    return;
  }
  const chBatch = parsed.channels;
  const n = Math.min(chBatch[0] ? chBatch[0].length : 1, 30);
  if (n <= 1) { frameCount++; frameBroadcast(parsed); return; }
  const now = Date.now();
  for (let s = 0; s < n; s++) {
    const sample = chBatch.map(ch => (Array.isArray(ch) && s < ch.length) ? ch[s] : 0);
    const payload = JSON.stringify({ type: 'eeg_frame', seq: parsed.sampleNumber * n + s || 0, channels: sample, ts: now });
    localWss.clients.forEach(c => { if (c.readyState === 1) c.send(payload); });
    if (ecsWs && ecsWs.readyState === 1 && ecsConnected) ecsWs.send(payload);
    frameCount++;
  }
};

const getLastSample = (parsed) => {
  if (!Array.isArray(parsed.channels)) return parsed.channels || [];
  if (Array.isArray(parsed.channels[0])) {
    return parsed.channels.map(ch => (Array.isArray(ch) && ch.length) ? ch[ch.length - 1] : 0);
  }
  return parsed.channels;
};

let lastParsed = null;

udpServer.on('message', (msg) => {
  if (packetCount === 0) {
    const hex = Buffer.from(msg).slice(0, 32).toString('hex');
    console.log('[UDP] 收到首包 ' + msg.length + 'B hex=' + hex);
  }
  const parsed = parseOpenBCIPacket(msg);
  if (!parsed) {
    if (packetCount === 0) console.log('[UDP] ⚠ 无法解析，等待更多包...');
    packetCount++;
    const now = Date.now();
    if (now - lastStatsTime >= 200) {
      const hex = Buffer.from(msg).slice(0, 32).toString('hex');
      console.log('[UDP] ⚠ 无法解析: ' + hex);
      packetCount = 0;
      lastStatsTime = now;
    }
    return;
  }
  lastParsed = parsed;
  packetCount++;
  const now = Date.now();
  if (now - lastStatsTime >= 200) {
    const uPkt = Math.round(packetCount * 1000 / (now - lastStatsTime));
    const uFrameRate = Math.round(frameCount * 1000 / (now - lastStatsTime));
    sampleRate = uFrameRate; // actual sample/frame rate for status messages
    const ch = getLastSample(lastParsed);
    const chStr = ch.slice(0,4).map(v => typeof v === 'number' ? v.toFixed(0).padStart(6) : '     0').join(' ');
    console.log('[' + uFrameRate.toString().padStart(3) + ' fps | ' + uPkt.toString().padStart(2) + ' pkt] ' + chStr + ' | ECS: ' + (ecsConnected ? '●' : '○'));
    frameCount = 0;
    packetCount = 0;
    lastStatsTime = now;
  }
  emitBatch(parsed);
});

udpServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`[UDP] 端口 ${config.UDP_LISTEN_PORT} 被占用，请先关闭旧进程`);
    process.exit(1);
  } else {
    console.error('[UDP]', err.message);
  }
});
udpServer.on('listening', () => {
  const addr = udpServer.address();
  console.log(`[UDP] 监听 ${addr.address}:${addr.port}`);
});

// ── 1b. 加速度计 UDP 监听（额外端口，OpenBCI GUI Accel/Aux 类型） ──
const ACCEL_PORT = parseInt(process.env.ACCEL_UDP_PORT || '12347', 10);
const accelServer = dgram.createSocket('udp4');
accelServer.on('message', (msg) => {
  // Format: {"type":"accelerometer","data":[[x0,x1,...],[y0,y1,...],[z0,z1,...]]}
  try {
    const text = Buffer.from(msg).toString('utf8').trim();
    if (text[0] !== '{') return;
    const obj = JSON.parse(text);
    const raw = obj.data || obj;
    if (Array.isArray(raw) && raw.length >= 3 && Array.isArray(raw[0])) {
      const accel = raw.slice(0, 3).map(axis => {
        if (!Array.isArray(axis) || axis.length === 0) return 0;
        return axis[axis.length - 1]; // last sample per axis
      });
      const payload = JSON.stringify({ type: 'accel_frame', axes: accel, ts: Date.now() });
      localWss.clients.forEach(c => { if (c.readyState === 1) c.send(payload); });
      if (ecsWs && ecsWs.readyState === 1 && ecsConnected) ecsWs.send(payload);
    }
  } catch (_) {}
});
accelServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`[ACCEL UDP] 端口 ${ACCEL_PORT} 被占用，加速度计通道不可用（不影响脑电图）`);
  } else {
    console.error('[ACCEL UDP]', err.message);
  }
});
accelServer.bind(ACCEL_PORT, () => {
  console.log(`[ACCEL UDP] 监听 0.0.0.0:${ACCEL_PORT}`);
});

// ── 2. 本地 WebSocket 服务（供本地面板连接） ──
let localWss;
try {
  localWss = new WebSocket.Server({ port: config.LOCAL_WS_PORT });
} catch (err) {
  if (err.code === 'EADDRINUSE') {
    console.error(`[本地WS] 端口 ${config.LOCAL_WS_PORT} 被占用，请先关闭旧桥接进程`);
    console.log('  Windows: taskkill /F /IM node.exe  (或找到并终止旧进程)');
    process.exit(1);
  }
  throw err;
}
localWss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'status',
    deviceType: config.DEVICE_TYPE,
    channels: chCount,
    sampleRate: sampleRate || (isGanglion ? 200 : 250),
    session_id: ecsSessionId,
  }));
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'set_session' && msg.session_id) {
        console.log('[本地WS] 收到房间 sessionId:', msg.session_id);
        companionMode = true;
        ecsSessionId = msg.session_id;
        saveSessionId(ecsSessionId);
        // 断开 ECS 连接以新 sessionId 重连
        if (ecsWs) {
          if (ecsWs) ecsWs._reconnectOnClose = false; // 阻止旧重连
          ecsWs.close();
        }
        setTimeout(() => connectECS(), 1000);
      }
    } catch (e) {}
  });
  ws.on('close', () => {});
});

// ── 3. ECS 上行 WebSocket 客户端 ──
let ecsWs = null;
const SESSION_FILE = path.join(__dirname, 'ecs-session.id');
function loadSessionId() {
  try { return fs.readFileSync(SESSION_FILE, 'utf8').trim(); } catch (_) {}
  return '';
}
function saveSessionId(id) {
  try { fs.writeFileSync(SESSION_FILE, id, 'utf8'); } catch (_) {}
}
let ecsSessionId = config.SESSION_ID || loadSessionId() || ('bridge-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6));
if (!config.SESSION_ID && !loadSessionId()) saveSessionId(ecsSessionId);
let ecsConnected = false;
let companionMode = false;
let pendingRoomCode = process.env.ROOM_CODE || '';  // 从环境变量读取房间号
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
    ws.send(JSON.stringify({ type: 'hello', role: 'pending', session_id: ecsSessionId, device_info: { platform: 'Node.js Bridge', userAgent: 'bridge', nickname: 'Bridge ' + ecsSessionId.slice(-6), isBridge: true } }));
    // 如果有房间号，hello 后立即尝试加入房间
    if (pendingRoomCode && !roomJoinAttempted) {
      roomJoinAttempted = true;
      console.log('[ECS] 加入房间:', pendingRoomCode);
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
      if (msg.type === 'role_denied' && !ecsConnected && !companionMode) {
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
        if (companionMode) {
          // 伴生模式：浏览器负责 master 角色，桥接仅转发数据
          ecsConnected = true;
          console.log('[ECS] 桥接伴生模式已就绪');
          const lanIP = getLANIP();
          ws.send(JSON.stringify({ type: 'set_udp_target', target: lanIP + ':' + config.GUI_MARKER_PORT }));
        } else {
          ws.send(JSON.stringify({ type: 'reconnect', role: 'master', session_id: ecsSessionId }));
        }
      }
      // 房间号响应
      if (msg.type === 'room_joined') {
        console.log('[ECS] 加入房间成功:', msg.code);
        pendingRoomCode = '';
        // 使用房间 sessionId 重新连接并持久化
        ecsSessionId = msg.session_id;
        saveSessionId(ecsSessionId);
        if (ecsWs) ecsWs._reconnectOnClose = false;
        ecsWs.close();
        setTimeout(() => connectECS(), 1000);
      }
      if (msg.type === 'error' && pendingRoomCode && msg.message && msg.message.includes('房间')) {
        console.error('[ECS] 房间号无效或已过期:', pendingRoomCode);
        pendingRoomCode = '';
        process.exit(1);
      }
    } catch (e) {}
  });
  ws.on('close', () => {
    if (ws._reconnectOnClose === false) return; // 主动关闭(room_joined/set_session), 不污染状态
    console.log('[ECS] 断开，5 秒后重连');
    ecsConnected = false;
    setTimeout(connectECS, 5000);
  });
  ws.on('error', () => ws.close());
  ecsWs = ws;
}

// ── 启动 ──
udpServer.bind(config.UDP_LISTEN_PORT);

// ── 命令行交互 ──
const stdin = process.stdin;
stdin.setEncoding('utf8');
stdin.setRawMode && stdin.setRawMode(true);
console.log('  输入 l 退出当前房间 · Ctrl+C 退出桥接');
stdin.on('data', (key) => {
  const k = key.toString().toLowerCase().trim();
  if (k === 'l' || k === 'leave') {
    sendLeaveRoom();
  } else if (k === '\x03') {
    sendLeaveRoom();
    process.exit(0);
  }
});
process.on('SIGINT', () => { sendLeaveRoom(); setTimeout(() => process.exit(0), 500); });

function sendLeaveRoom() {
  pendingRoomCode = '';
  roomJoinAttempted = false;
  if (ecsWs && ecsWs.readyState === 1) {
    console.log('[ECS] 离开房间...');
    ecsWs.send(JSON.stringify({ type: 'leave_room', session_id: ecsSessionId }));
    ecsWs._reconnectOnClose = false;
    ecsWs.close();
  }
  setTimeout(() => {
    console.log('  输入 4 位房间号重新加入，或 Ctrl+C 退出');
    stdin.setRawMode && stdin.setRawMode(false);
    const rl = require('readline').createInterface({ input: process.stdin, output: process.stdout });
    rl.question('房间号: ', (code) => {
      code = code.trim();
      if (code.length === 4 && /^\d{4}$/.test(code)) {
        pendingRoomCode = code;
        roomJoinAttempted = false;
        stdin.setRawMode && stdin.setRawMode(true);
        rl.close();
        connectECS();
      } else {
        console.log('无效房间号');
        rl.close();
        process.exit(0);
      }
    });
  }, 1500);
}

connectECS();

console.log(`── OpenBCI UDP → WebSocket Bridge ──`);
console.log(`  Device      : ${isGanglion ? 'Ganglion (4ch 200Hz)' : 'Cyton'}`);
console.log(`  UDP 监听    : ${config.UDP_LISTEN_PORT}`);
console.log(`  本地面板 WS : :${config.LOCAL_WS_PORT}`);
console.log(`  ECS 上行    : ${config.ECS_WS_URL}`);
