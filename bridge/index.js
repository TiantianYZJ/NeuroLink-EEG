/**
 * NeuroLink Bridge — 朋友圈级终端输出
 * chcp 65001 确保中文不乱码，ANSI 颜色流光溢彩
 */
// 强制终端 UTF-8（Windows 解决中文乱码）
if (process.platform === 'win32') {
  try { require('child_process').execSync('chcp 65001 >NUL 2>NUL'); } catch (e) {}
}

const dgram = require('dgram');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const config = require('./config');

// ── ANSI 调色板 ──
const C = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  // 霓虹色系
  cyan: '\x1b[38;2;0;255;200m',
  pink: '\x1b[38;2;255;20;147m',
  violet: '\x1b[38;2;180;130;255m',
  blue: '\x1b[38;2;80;180;255m',
  green: '\x1b[38;2;0;255;128m',
  yellow: '\x1b[38;2;255;200;0m',
  orange: '\x1b[38;2;255;140;0m',
  red: '\x1b[38;2;255;60;60m',
  gray: '\x1b[38;2;100;100;120m',
  // 背景
  bgDark: '\x1b[48;2;20;20;30m',
  bgCard: '\x1b[48;2;25;25;40m',
};
const R = C.reset;

// ── 美化打印 ──
const LOG = {
  _ts() {
    const d = new Date();
    return C.gray + String(d.getHours()).padStart(2,'0') + ':' +
      String(d.getMinutes()).padStart(2,'0') + ':' +
      String(d.getSeconds()).padStart(2,'0') + R;
  },
  info(tag, msg) {
    console.log('  ' + this._ts() + ' ' + C.cyan + C.bold + tag + R + ' ' + msg);
  },
  ok(tag, msg) {
    console.log('  ' + this._ts() + ' ' + C.green + C.bold + tag + R + ' ' + msg);
  },
  warn(tag, msg) {
    console.log('  ' + this._ts() + ' ' + C.orange + C.bold + tag + R + ' ' + msg);
  },
  err(tag, msg) {
    console.log('  ' + this._ts() + ' ' + C.red + C.bold + tag + R + ' ' + msg);
  },
  // 纯色强调
  line(color, str) {
    const c = C[color] || '';
    console.log('  ' + this._ts() + ' ' + c + str + R);
  },
  // 原始输出（无时间戳）
  raw(str) {
    console.log(str);
  },
};

// C._ts 时间戳 helper（必须在 LOG 之后定义，供帧统计行直接调用）
C._ts = () => {
  const d = new Date();
  return C.gray + String(d.getHours()).padStart(2,'0') + ':' +
    String(d.getMinutes()).padStart(2,'0') + ':' +
    String(d.getSeconds()).padStart(2,'0') + R;
};

// ── 开场 Logo（PEYT 块字符堆砌） ──
const B = C.bold, D = C.dim;
const BC = C.cyan, BV = C.violet, BG = C.gray;
const LOGO_BG = D;       // ░ 用 dim
const LOGO_FG = BC + B;  // █ 用亮青加粗
const L = (line) => {
  // ░ → dim gray, █ → bright cyan bold, preserve spaces
  return line.replace(/░/g, `${BG}${D}░${R}`).replace(/█/g, `${BC}${B}█${R}`);
};
const BANNER = `
  ${L('             ░█████████  ░██████████ ░██     ░██ ░██████████     ░██████      ░██                      ░██ ░██           ')}${R}
  ${L('             ░██                    ░██     ░██ ░██          ░██   ░██      ░██        ░██   ░██     ░██                      ░██                ')}${R}
  ${L('              ░██                   ░██     ░██ ░██           ░██ ░██       ░██       ░██         ░████████ ░██    ░██  ░████████ ░██ ░███████  ')}${R}
  ${L('               ░██                  ░█████████  ░█████████     ░████        ░██        ░████████     ░██    ░██    ░██ ░██    ░██ ░██░██    ░██ ')}${R}
  ${L('              ░██                   ░██         ░██             ░██         ░██               ░██    ░██    ░██    ░██ ░██    ░██ ░██░██    ░██ ')}${R}
  ${L('             ░██                    ░██         ░██             ░██         ░██        ░██   ░██     ░██    ░██   ░███ ░██   ░███ ░██░██    ░██ ')}${R}
  ${L('                  ░██████████       ░██         ░██████████     ░██         ░██         ░██████       ░████  ░█████░██  ░█████░██ ░██ ░███████  ')}${R}
  ${D}${BG}  ───────────────────────────────────────────────────────────────────────────${R}
  ${D}${BG}  ▌ @TiantianYZJ                                    OpenBCI ▸ Bridge ▸ EEG Stream${R}
`;

// ── 状态 ──
let packetCount = 0;
let lastStatsTime = Date.now();
let sampleRate = 0;
let frameCount = 0;
const isGanglion = config.DEVICE_TYPE === 'ganglion';
const chCount = isGanglion ? 4 : 8;
const CH_LABELS = isGanglion ? ['Fp1', 'Fp2', 'C3', 'C4'] : ['Fp1','Fp2','C3','C4','P3','P4','O1','O2'];
const CH_COLORS = [
  '\x1b[38;2;160;195;236m', // 淡蓝
  '\x1b[38;2;196;181;253m', // 淡紫
  '\x1b[38;2;255;122;23m',  // 橙
  '\x1b[38;2;255;194;133m', // 淡橙
  '\x1b[38;2;120;200;120m', // 绿
  '\x1b[38;2;255;100;150m', // 粉
  '\x1b[38;2;130;200;255m', // 天蓝
  '\x1b[38;2;200;150;255m', // 紫
];

console.log(BANNER);

// ── 1. UDP 监听 ──
const udpServer = dgram.createSocket('udp4');

let udpFormat = null;
let frameUnit = 'uv'; // W13: 当前帧单位 — binary 模式为 'raw', 其他模式为 'uv'

function parseBinaryPacket(msg) {
  // 完整帧 = 0xA0 + sampleNumber(3B) + channelData(chCount*3B) + 0xC0
  const len = msg.length;
  if (len < 5) return null;
  let offset = 0;
  while (offset < len && msg[offset] !== 0xA0) offset++;
  if (offset >= len) return null;
  if (offset + 4 > len) return null;
  const sampleNumber = msg[offset + 1] | (msg[offset + 2] << 8) | (msg[offset + 3] << 16);
  offset += 4;
  const dataLen = chCount * 3;
  if (offset + dataLen + 1 > len) return null; // +1 预留 0xC0 结束字节
  const channels = [];
  for (let i = 0; i < chCount; i++) {
    let val = msg[offset] | (msg[offset + 1] << 8) | (msg[offset + 2] << 16);
    if (val & 0x800000) val |= ~0xFFFFFF;
    channels.push(val);
    offset += 3;
  }
  // H2/H3: 校验末字节为 0xC0 结束边界，否则跳过该帧避免数据误触发
  if (msg[offset] !== 0xC0) return null;
  return { sampleNumber, channels };
}

function parseJSONPacket(msg) {
  try {
    const text = Buffer.from(msg).toString('utf8').trim();
    if (text[0] !== '{' && text[0] !== '[') return null;
    const obj = JSON.parse(text);
    const raw = obj.data || obj.channels || obj;
    if (!Array.isArray(raw) || raw.length < Math.min(chCount, 1)) return null;
    if (Array.isArray(raw[0])) {
      const allChannels = raw.slice(0, chCount).map(ch =>
        Array.isArray(ch) ? ch.map(v => typeof v === 'number' ? v : 0) : [0]
      );
      return { sampleNumber: obj.sample ?? obj.sampleNumber ?? 0, channels: allChannels };
    }
    const channels = raw.slice(0, chCount).map(v => typeof v === 'number' ? v : 0);
    return { sampleNumber: obj.sample ?? obj.sampleNumber ?? 0, channels };
  } catch (e) { LOG.warn('▸ UDP', 'JSON 解析失败: ' + e.message); }
  return null;
}

function parseFloat32BEPacket(msg) {
  const len = msg.length;
  const recordBytes = (1 + chCount) * 4;
  if (len < recordBytes) return null;
  const dv = new DataView(msg.buffer, msg.byteOffset, msg.byteLength);
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
  let p = parseJSONPacket(msg);
  if (p) { udpFormat = 'json'; frameUnit = 'uv'; LOG.ok('▸ 格式', 'OpenBCI JSON'); return p; }
  p = parseFloat32BEPacket(msg);
  if (p) { udpFormat = 'float32_be'; frameUnit = 'uv'; LOG.ok('▸ 格式', 'float32 big-endian'); return p; }
  p = parseBinaryPacket(msg);
  if (p) { udpFormat = 'binary'; frameUnit = 'raw'; LOG.ok('▸ 格式', '0xA0 二进制'); return p; }
  return null;
}

const frameBroadcast = (parsed) => {
  const payload = JSON.stringify({ type: 'eeg_frame', seq: parsed.sampleNumber ?? 0, channels: parsed.channels, unit: frameUnit, ts: Date.now() });
  // H4: 背压控制 — 客户端缓冲超过 1MB 则跳过该慢客户端
  localWss.clients.forEach(c => { if (c.readyState === 1 && c.bufferedAmount <= 1024 * 1024) c.send(payload); });
  if (ecsWs && ecsWs.readyState === 1 && ecsConnected && ecsWs.bufferedAmount <= 1024 * 1024) ecsWs.send(payload);
};

const emitBatch = (parsed) => {
  if (!Array.isArray(parsed.channels) || !Array.isArray(parsed.channels[0])) {
    frameCount++;
    frameBroadcast(parsed);
    return;
  }
  const chBatch = parsed.channels;
  const rawLen = chBatch[0] ? chBatch[0].length : 1;
  if (rawLen > 30 && !global._warnedBatchCap) { global._warnedBatchCap = true; LOG.warn('▸ UDP', '每包样本数 ' + rawLen + ' 超过上限 30，已截断'); }
  const n = Math.min(rawLen, 30);
  if (n <= 1) {
    // W14: n<=1 时嵌套数组需 flatten 为单值数组
    parsed.channels = parsed.channels.map(ch => Array.isArray(ch) ? (ch[0] !== undefined ? ch[0] : 0) : ch);
    frameCount++;
    frameBroadcast(parsed);
    return;
  }
  const now = Date.now();
  for (let s = 0; s < n; s++) {
    const sample = chBatch.map(ch => (Array.isArray(ch) && s < ch.length) ? ch[s] : 0);
    // W15: seq 应为 sampleNumber + s（每包内偏移），而非 sampleNumber * n + s
    const payload = JSON.stringify({ type: 'eeg_frame', seq: (parsed.sampleNumber + s) || 0, channels: sample, unit: frameUnit, ts: now });
    localWss.clients.forEach(c => { if (c.readyState === 1 && c.bufferedAmount <= 1024 * 1024) c.send(payload); });
    if (ecsWs && ecsWs.readyState === 1 && ecsConnected && ecsWs.bufferedAmount <= 1024 * 1024) ecsWs.send(payload);
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

// H1: UDP 数据流中断超时检测
let lastDataTime = Date.now();
let deviceTimeoutReported = false;
let dataEverReceived = false;
setInterval(() => {
  // 仅在已经收到过首包后才检测（避免启动时空闲误报）
  if (!dataEverReceived) return;
  const idle = Date.now() - lastDataTime;
  if (idle > 3000 && !deviceTimeoutReported) {
    deviceTimeoutReported = true;
    LOG.err('▸ UDP', '数据流中断 ' + C.yellow + idle + 'ms' + R + '，上报 device_timeout');
    if (ecsWs && ecsWs.readyState === 1 && ecsConnected) {
      try { ecsWs.send(JSON.stringify({ type: 'device_timeout', idle_ms: idle, ts: Date.now() })); } catch (e) { LOG.warn('▸ ECS', e.message); }
    }
  }
}, 2000);

udpServer.on('message', (msg) => {
  lastDataTime = Date.now();
  dataEverReceived = true;
  if (deviceTimeoutReported) {
    deviceTimeoutReported = false;
    LOG.ok('▸ UDP', '数据流恢复');
  }
  if (packetCount === 0) {
    const hex = Buffer.from(msg).slice(0, 32).toString('hex');
    LOG.info('▸ UDP', '收到首包 ' + C.yellow + msg.length + 'B' + R + ' hex=' + C.dim + hex + R);
  }
  const parsed = parseOpenBCIPacket(msg);
  if (!parsed) {
    if (packetCount === 0) LOG.warn('▸ UDP', '等待有效数据包...');
    packetCount++;
    const now = Date.now();
    if (now - lastStatsTime >= 200) {
      const hex = Buffer.from(msg).slice(0, 32).toString('hex');
      LOG.err('▸ UDP', '无法解析: ' + C.dim + hex + R);
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
    sampleRate = uFrameRate;
    const ch = getLastSample(lastParsed);
    const chStr = ch.slice(0, Math.min(chCount, 4)).map((v, i) => {
      const idx = i;
      const cv = typeof v === 'number' ? v : 0;
      const sign = cv >= 0 ? ' ' : '';
      return CH_COLORS[idx] + sign + cv.toFixed(0).padStart(6) + R;
    }).join(' ');
    const fpsColor = uFrameRate > 150 ? C.green : uFrameRate > 80 ? C.yellow : C.red;
    const ecsDot = ecsConnected ? C.green + '●' + R : C.red + '○' + R;
    const barLen = Math.min(Math.floor(uFrameRate / 10), 20);
    const bar = C.violet + '█'.repeat(barLen) + C.gray + '░'.repeat(Math.max(0, 20 - barLen)) + R;
    console.log('  ' + C._ts() + '  ' + bar + ' ' + fpsColor + C.bold + String(uFrameRate).padStart(3) + ' fps' + R + '  ' +
      chStr + '  ECS ' + ecsDot);
    frameCount = 0;
    packetCount = 0;
    lastStatsTime = now;
  }
  emitBatch(parsed);
});

udpServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    LOG.err('▸ UDP', '端口 ' + C.yellow + config.UDP_LISTEN_PORT + R + ' 被占用，请先关闭旧进程');
    process.exit(1);
  } else {
    LOG.err('▸ UDP', err.message);
  }
});

udpServer.on('listening', () => {
  const addr = udpServer.address();
  LOG.ok('▸ UDP', '监听 ' + C.cyan + addr.address + ':' + addr.port + R);
});

// ── 1b. 加速度计 ──
const ACCEL_PORT = config.ACCEL_PORT;
const accelServer = dgram.createSocket('udp4');
accelServer.on('message', (msg) => {
  try {
    const text = Buffer.from(msg).toString('utf8').trim();
    if (text[0] !== '{') return;
    const obj = JSON.parse(text);
    const raw = obj.data || obj;
    if (Array.isArray(raw) && raw.length >= 3 && Array.isArray(raw[0])) {
      const accel = raw.slice(0, 3).map(axis => {
        if (!Array.isArray(axis) || axis.length === 0) return 0;
        return axis[axis.length - 1];
      });
      const payload = JSON.stringify({ type: 'accel_frame', axes: accel, ts: Date.now() });
      // H4: 背压控制
      localWss.clients.forEach(c => { if (c.readyState === 1 && c.bufferedAmount <= 1024 * 1024) c.send(payload); });
      if (ecsWs && ecsWs.readyState === 1 && ecsConnected && ecsWs.bufferedAmount <= 1024 * 1024) ecsWs.send(payload);
    }
  } catch (e) { LOG.warn('▸ ACCEL', e.message); }
});
accelServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    LOG.warn('▸ ACCEL', '端口 ' + C.yellow + ACCEL_PORT + R + ' 被占用（不影响脑电图）');
  } else {
    LOG.err('▸ ACCEL', err.message);
  }
});
accelServer.bind(ACCEL_PORT, () => {
  LOG.info('▸ ACCEL', '监听 0.0.0.0:' + C.cyan + ACCEL_PORT + R);
});

// ── 2. 本地 WebSocket ──
let localWss;
// W12: 显式绑定 127.0.0.1，避免 0.0.0.0 暴露无鉴权
// EADDRINUSE-fix: WebSocket.Server 的 listen 是异步的，try/catch 无法捕获；必须用 error 事件
localWss = new WebSocket.Server({ host: '127.0.0.1', port: config.LOCAL_WS_PORT });
localWss.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    LOG.err('▸ FATAL', '本地WS端口 ' + C.yellow + config.LOCAL_WS_PORT + R + ' 被占用');
    console.log('  ' + C.dim + '  请关闭其他正在运行的桥接程序，或执行:' + R);
    console.log('  ' + C.yellow + '  taskkill /F /IM node.exe' + R);
    console.log('  ' + C.dim + '  然后重新启动桥接。' + R);
    process.exit(1);
  } else {
    LOG.err('▸ 本地WS', err.message);
    process.exit(1);
  }
});
localWss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'status',
    deviceType: config.DEVICE_TYPE,
    channels: chCount,
    // H6: 优先使用 config.SAMPLE_RATE，运行时统计仅作校验
    sampleRate: config.SAMPLE_RATE,
    session_id: ecsSessionId,
  }));
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'set_session' && msg.session_id) {
        LOG.info('▸ 本地WS', '收到房间 ' + C.violet + msg.session_id.slice(0,12) + '...' + R);
        companionMode = true;
        ecsSessionId = msg.session_id;
        saveSessionId(ecsSessionId);
        // W9: 新建连接前清理旧的重连定时器并终止旧 ws
        if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
        if (ecsWs) {
          ecsWs._reconnectOnClose = false;
          ecsWs.terminate();
        }
        reconnectTimer = setTimeout(() => { reconnectTimer = null; connectECS(); }, 1000);
      }
    } catch (e) { LOG.warn('▸ 本地WS', e.message); }
  });
  ws.on('close', () => {});
});

// ── 3. ECS 上行 ──
let ecsWs = null;
const SESSION_FILE = path.join(__dirname, 'ecs-session.id');
function loadSessionId() {
  try { return fs.readFileSync(SESSION_FILE, 'utf8').trim(); } catch (e) { LOG.warn('▸ SESSION', 'load failed: ' + e.message); }
  return '';
}
function saveSessionId(id) {
  // D10: 失败时记录警告而非静默
  try { fs.writeFileSync(SESSION_FILE, id, 'utf8'); } catch (e) { LOG.warn('▸ SESSION', 'save failed: ' + e.message); }
}
let ecsSessionId = config.SESSION_ID || loadSessionId() || ('bridge-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6));
if (!config.SESSION_ID && !loadSessionId()) saveSessionId(ecsSessionId);
let ecsConnected = false;
let companionMode = false;
let pendingRoomCode = process.env.ROOM_CODE || '';
let roomJoinAttempted = false;
// W9: 全局重连定时器引用，避免多 ws 实例并存
let reconnectTimer = null;
// W8: 重连次数（连接成功后重置）
let reconnectAttempts = 0;

// D11: 优先使用 config.LAN_IP，无配置时保持现有自动检测逻辑
function getLANIP() {
  if (config.LAN_IP) return config.LAN_IP;
  try {
    const os = require('os');
    const ifaces = os.networkInterfaces();
    for (const name of Object.keys(ifaces)) {
      for (const iface of ifaces[name]) {
        if (iface.family === 'IPv4' && !iface.internal) return iface.address;
      }
    }
  } catch (e) { LOG.warn('▸ NET', e.message); }
  return '127.0.0.1';
}

// W8: 指数退避 + 随机抖动 (±20%)
function getReconnectDelay() {
  const steps = [1000, 2000, 4000, 8000, 16000, 30000, 60000];
  const base = steps[Math.min(reconnectAttempts, steps.length - 1)];
  const jitter = base * 0.2 * (Math.random() * 2 - 1); // ±20%
  return Math.max(500, Math.round(base + jitter));
}

function scheduleReconnect() {
  if (reconnectTimer) clearTimeout(reconnectTimer);
  const delay = getReconnectDelay();
  reconnectAttempts++;
  LOG.warn('▸ ECS', '断开，' + C.yellow + Math.round(delay / 1000) + ' 秒' + R + '后重连（第 ' + reconnectAttempts + ' 次）');
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectECS(); }, delay);
}

function connectECS() {
  // W9: 新建连接前清理旧的重连定时器并终止旧 ws，避免多实例并存
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  if (ecsWs) { try { ecsWs.terminate(); } catch (e) {} ecsWs = null; }

  const ws = new WebSocket(config.ECS_WS_URL);
  // W7: 心跳 — 30s ping，监听 pong 设置 _alive；超时则 terminate
  ws._alive = true;
  ws._pingTimer = null;

  ws.on('open', () => {
    // B14: 校验当前 ws 仍是全局 ecsWs，避免旧 ws 修改全局状态
    if (ws !== ecsWs) return;
    LOG.ok('▸ ECS', '已连接 ' + C.cyan + config.ECS_WS_URL + R);
    // W8: 连接成功，重置重连次数
    reconnectAttempts = 0;
    ws.send(JSON.stringify({ type: 'hello', role: 'pending', session_id: ecsSessionId, device_info: { platform: 'Node.js Bridge', userAgent: 'bridge', nickname: 'Bridge ' + ecsSessionId.slice(-6), isBridge: true } }));
    if (pendingRoomCode && !roomJoinAttempted) {
      roomJoinAttempted = true;
      LOG.info('▸ ECS', '加入房间 ' + C.violet + pendingRoomCode + R);
      ws.send(JSON.stringify({ type: 'join_room', code: pendingRoomCode, session_id: ecsSessionId }));
    }
    // W7: 启动心跳定时器
    ws._pingTimer = setInterval(() => {
      if (ws !== ecsWs) { clearInterval(ws._pingTimer); return; }
      if (!ws._alive) {
        LOG.warn('▸ ECS', '心跳超时，强制断开重连');
        try { ws.terminate(); } catch (e) {}
        return;
      }
      ws._alive = false;
      try { ws.ping(); } catch (e) {}
    }, 30000);
  });
  ws.on('pong', () => { ws._alive = true; });
  // W10: 监听 unexpected-response
  ws.on('unexpected-response', (req, res) => {
    LOG.err('▸ ECS', 'HTTP ' + res.statusCode);
  });
  ws.on('message', (raw) => {
    if (ws !== ecsWs) return;
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'role_claimed' && msg.role === 'master') {
        LOG.ok('▸ ECS', '已认领 ' + C.violet + C.bold + 'master' + R + ' 角色');
        ecsConnected = true;
        const lanIP = getLANIP();
        ws.send(JSON.stringify({ type: 'set_udp_target', target: lanIP + ':' + config.GUI_MARKER_PORT }));
      }
      if (msg.type === 'role_denied' && !ecsConnected && !companionMode) {
        LOG.warn('▸ ECS', '角色被拒: ' + msg.reason + '，5 秒后重试');
        setTimeout(() => {
          if (ws === ecsWs && ws.readyState === 1)
            ws.send(JSON.stringify({ type: 'reconnect', role: 'master', session_id: ecsSessionId }));
        }, 5000);
      }
      if (msg.type === 'marker') {
        const buf = Buffer.alloc(4);
        // B18: 避免 code=0 被 || 吞掉
        buf.writeFloatBE(typeof msg.code === 'number' ? msg.code : 0);
        // H5: 增加 send callback + 使用 config.GUI_MARKER_HOST
        udpServer.send(buf, 0, 4, config.GUI_MARKER_PORT, config.GUI_MARKER_HOST, (err) => {
          if (err) LOG.warn('▸ UDP', err.message);
        });
      }
      if (msg.type === 'room_info' && !ecsConnected) {
        if (companionMode) {
          ecsConnected = true;
          LOG.ok('▸ ECS', '桥接伴生模式 ' + C.green + C.bold + '已就绪' + R);
          const lanIP = getLANIP();
          ws.send(JSON.stringify({ type: 'set_udp_target', target: lanIP + ':' + config.GUI_MARKER_PORT }));
        } else {
          ws.send(JSON.stringify({ type: 'reconnect', role: 'master', session_id: ecsSessionId }));
        }
      }
      if (msg.type === 'room_joined') {
        LOG.ok('▸ ECS', '加入房间 ' + C.violet + msg.code + R + ' ' + C.green + '✓' + R);
        pendingRoomCode = '';
        ecsSessionId = msg.session_id;
        saveSessionId(ecsSessionId);
        // W9: 用 reconnectTimer 管理重连，terminate 旧连接
        if (ws._pingTimer) { clearInterval(ws._pingTimer); ws._pingTimer = null; }
        ws._reconnectOnClose = false;
        try { ws.terminate(); } catch (e) {}
        reconnectTimer = setTimeout(() => { reconnectTimer = null; connectECS(); }, 1000);
      }
      if (msg.type === 'error' && pendingRoomCode && msg.message && msg.message.includes('房间')) {
        LOG.err('▸ ECS', '房间号无效或已过期: ' + C.yellow + pendingRoomCode + R);
        pendingRoomCode = '';
      }
    } catch (e) { LOG.warn('▸ ECS', e.message); }
  });
  ws.on('close', () => {
    if (ws._pingTimer) { clearInterval(ws._pingTimer); ws._pingTimer = null; }
    ecsConnected = false;
    if (ws._reconnectOnClose === false) return;
    // B14: 仅当前 ecsWs 触发重连
    if (ws !== ecsWs) return;
    scheduleReconnect();
  });
  // W11: error 事件改用 terminate 强制断开
  ws.on('error', (e) => { LOG.warn('▸ ECS', e && e.message ? e.message : String(e)); try { ws.terminate(); } catch (_) {} });
  ecsWs = ws;
}

// ── 启动 ──
udpServer.bind(config.UDP_LISTEN_PORT);

// ── 命令行交互 ──
const stdin = process.stdin;
stdin.setEncoding('utf8');
stdin.setRawMode && stdin.setRawMode(true);
LOG.raw('  ' + C.dim + '  ──────────────────────────────────────────────────────────' + R);
LOG.raw('  ' + C.gray + '  ⌨  ' + C.violet + 'l' + R + C.gray + '  离开当前房间  ·  ' + C.violet + 'Ctrl+C' + R + C.gray + '  退出桥接' + R);
LOG.raw('  ' + C.dim + '  ──────────────────────────────────────────────────────────' + R);
let _leaving = false;
// B16: stdin 中处理 Ctrl+C（raw mode 下不触发 SIGINT，必须在 data 中处理），用 _leaving flag 防重入
stdin.on('data', (key) => {
  const k = key.toString().toLowerCase().trim();
  if (!_leaving && (k === 'l' || k === 'leave')) {
    _leaving = true;
    sendLeaveRoom();
    setTimeout(() => { _leaving = false; }, 1000);
  } else if (key[0] === 0x03) {
    // Ctrl+C in raw mode — 不触发 SIGINT，需在此处理
    if (!_leaving) {
      _leaving = true;
      sendLeaveRoom();
      setTimeout(() => process.exit(0), 500);
    }
  }
});
process.on('SIGINT', () => {
  // B16: 用 _leaving flag 防重入
  if (_leaving) return;
  _leaving = true;
  sendLeaveRoom();
  setTimeout(() => process.exit(0), 500);
});

function sendLeaveRoom() {
  pendingRoomCode = '';
  roomJoinAttempted = false;
  // B13: 重置 companionMode，避免下次连接仍处于伴生模式
  companionMode = false;
  if (ecsWs && ecsWs.readyState === 1) {
    LOG.info('▸ ECS', '离开房间...');
    try { ecsWs.send(JSON.stringify({ type: 'leave_room', session_id: ecsSessionId })); } catch (e) { LOG.warn('▸ ECS', e.message); }
    if (ecsWs._pingTimer) { clearInterval(ecsWs._pingTimer); ecsWs._pingTimer = null; }
    ecsWs._reconnectOnClose = false;
    // W11: 改用 terminate
    try { ecsWs.terminate(); } catch (e) {}
  }
  setTimeout(() => {
    LOG.raw('  ' + C.gray + '  输入 4 位房间号重新加入，或 Ctrl+C 退出' + R);
    stdin.setRawMode && stdin.setRawMode(false);
    const rl = require('readline').createInterface({ input: process.stdin, output: process.stdout });
    const askRoom = () => {
      rl.question('  房间号: ', (code) => {
        code = code.trim();
        if (code.length === 4 && /^\d{4}$/.test(code)) {
          pendingRoomCode = code;
          roomJoinAttempted = false;
          stdin.setRawMode && stdin.setRawMode(true);
          rl.close();
          connectECS();
        } else {
          // B17: 无效房间号提示重新输入，不退出
          LOG.err('输入', '无效房间号（需 4 位数字），请重新输入');
          askRoom();
        }
      });
    };
    askRoom();
  }, 1500);
}

connectECS();

// D8: 全局异常兜底
process.on('uncaughtException', (e) => {
  LOG.err('▸ FATAL', e && e.stack ? e.stack : String(e));
  process.exit(1);
});
process.on('unhandledRejection', (e) => {
  LOG.err('▸ REJECT', e && e.message ? e.message : String(e));
});

// ── 启动摘要 ──
LOG.raw('');
LOG.raw('  ' + C.bgDark + C.cyan + C.bold + '  设备    ' + R + C.bgDark + ' ' + (isGanglion ? 'Ganglion (4ch 200Hz)' : 'Cyton (8ch)') + '                   ' + R);
LOG.raw('  ' + C.bgDark + C.cyan + C.bold + '  UDP     ' + R + C.bgDark + ' ' + String(config.UDP_LISTEN_PORT) + ' → WebSocket 转发           ' + R);
LOG.raw('  ' + C.bgDark + C.cyan + C.bold + '  本地WS  ' + R + C.bgDark + ' ws://localhost:' + String(config.LOCAL_WS_PORT) + '               ' + R);
LOG.raw('  ' + C.bgDark + C.cyan + C.bold + '  ECS     ' + R + C.bgDark + ' ' + config.ECS_WS_URL + R);
console.log('');
