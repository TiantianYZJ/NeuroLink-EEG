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
  console.log(`[UDP] 监听 ${addr.address}:${addr.port}`);
});

// ── 2. 本地 WebSocket 服务（供本地面板连接） ──
const localWss = new WebSocket.Server({ port: config.LOCAL_WS_PORT });
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
        ecsSessionId = msg.session_id;
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
let ecsSessionId = config.SESSION_ID || ('bridge-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6));
let ecsConnected = false;
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
    ws.send(JSON.stringify({ type: 'hello', role: 'pending', session_id: ecsSessionId }));
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
      if (msg.type === 'role_denied' && !ecsConnected) {
        console.warn('[ECS] 角色被拒，5 秒后重试:', msg.reason);
        setTimeout(() => {
          if (ws.readyState === 1)
            ws.send(JSON.stringify({ type: 'claim_role', role: 'master', session_id: ecsSessionId }));
        }, 5000);
      }
      if (msg.type === 'marker') {
        const buf = Buffer.alloc(4);
        buf.writeFloatBE(msg.code || 0);
        udpServer.send(buf, 0, 4, config.GUI_MARKER_PORT, '127.0.0.1');
      }
      if (msg.type === 'room_info' && !ecsConnected) {
        ws.send(JSON.stringify({ type: 'claim_role', role: 'master', session_id: ecsSessionId }));
      }
      // 房间号响应
      if (msg.type === 'room_joined') {
        console.log('[ECS] 加入房间成功:', msg.code);
        pendingRoomCode = '';
        // 使用房间 sessionId 重新连接
        ecsSessionId = msg.session_id;
        if (ecsWs) ecsWs._reconnectOnClose = false;
        ecsWs.close();
        setTimeout(() => connectECS(), 1000);
      }
      if (msg.type === 'error' && pendingRoomCode && msg.message && msg.message.includes('房间号')) {
        console.error('[ECS] 房间号无效或已过期:', pendingRoomCode);
        pendingRoomCode = '';
        process.exit(1);
      }
    } catch (e) {}
  });
  ws.on('close', () => {
    console.log('[ECS] 断开，5 秒后重连');
    ecsConnected = false;
    if (ws._reconnectOnClose !== false) {
      setTimeout(connectECS, 5000);
    }
  });
  ws.on('error', () => ws.close());
  ecsWs = ws;
}

// ── 启动 ──
udpServer.bind(config.UDP_LISTEN_PORT);
connectECS();

console.log(`── OpenBCI UDP → WebSocket Bridge ──`);
console.log(`  Device      : ${isGanglion ? 'Ganglion (4ch 200Hz)' : 'Cyton'}`);
console.log(`  UDP 监听    : ${config.UDP_LISTEN_PORT}`);
console.log(`  本地面板 WS : :${config.LOCAL_WS_PORT}`);
console.log(`  ECS 上行    : ${config.ECS_WS_URL}`);
