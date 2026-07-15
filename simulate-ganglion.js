/**
 * OpenBCI Ganglion 模拟器 v3 — 朋友圈级终端输出
 *
 * 模拟真实 OpenBCI GUI UDP 输出，float32 200Hz 双端口。
 *
 * 使用: node simulate-ganglion.js [选项]
 *
 * 选项:
 *   --format float32   数据格式: json | float32 | binary (默认 float32)
 *   --rate 200         EEG 采样率 (Hz), 默认 200
 *   --port 12345       EEG UDP 端口, 默认 12345
 *   --accel-port 12347 加速度计 UDP 端口, 默认 12347
 *   --host 127.0.0.1   UDP 目标地址, 默认 127.0.0.1
 *   --amp 1000         信号幅值 (µVrms), 默认 1000
 *   --noise 300        噪声水平 (µV), 默认 300
 *   --batch 10         每包样本数 (JSON 模式), 默认 10
 *   --chirp true|false 启用频率扫描 (默认 false)
 */

const dgram = require('dgram');

// ANSI 调色板
const C = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[2m',
  cyan: '\x1b[38;2;0;255;200m', pink: '\x1b[38;2;255;20;147m',
  violet: '\x1b[38;2;180;130;255m', blue: '\x1b[38;2;80;180;255m',
  green: '\x1b[38;2;0;255;128m', yellow: '\x1b[38;2;255;200;0m',
  orange: '\x1b[38;2;255;140;0m', red: '\x1b[38;2;255;60;60m',
  gray: '\x1b[38;2;100;100;120m',
};
const R = C.reset;

// ── 协议定义 ──
const PROTO = {
  EEG_F32: { code: 0xA0, label: 'EEG float32', /* port: 12345 */ color: C.cyan },
  ACCEL_JSON: { code: 0xA1, label: 'Accel JSON', /* port: 12347 */ color: C.pink },
};

// ── 解析参数 ──
const args = {};
const argv = process.argv.slice(2);
argv.forEach((a, i) => {
  if (a.startsWith('--')) {
    const n = argv[i + 1];
    // 显式处理 'true'/'false' 字符串，避免 'false' 被当作 truthy
    let val;
    if (n && !n.startsWith('--')) {
      val = (n === 'true' ? true : n === 'false' ? false : n);
    } else {
      val = true;
    }
    args[a.slice(2)] = val;
  }
});

const FORMAT = (args.format || 'float32').toLowerCase();
const RATE = parseInt(args.rate, 10) || 200;
const EEG_PORT = parseInt(args.port, 10) || 12345;
const ACCEL_PORT = parseInt(args['accel-port'], 10) || 12347;
const HOST = args.host || '127.0.0.1';
const AMP = parseFloat(args.amp) || 1000;
const NOISE = parseFloat(args.noise) || 300;
const BATCH = parseInt(args.batch, 10) || 10;
const CHIRP = args.chirp === true;

const INTERVAL = 1000 / RATE;

// ── 通道配置 ──
const CH_CONFIG = [
  { name: 'Fp1', bands: [{f:10.5,a:0.6},{f:22,a:0.2},{f:6,a:0.15}] },
  { name: 'Fp2', bands: [{f:10.5,a:0.4},{f:22,a:0.35},{f:6,a:0.2},{f:40,a:0.05}] },
  { name: 'C3',  bands: [{f:10,a:0.25},{f:3,a:0.3},{f:6,a:0.3},{f:22,a:0.15}] },
  { name: 'C4',  bands: [{f:10,a:0.25},{f:3,a:0.3},{f:6,a:0.3},{f:22,a:0.15}] },
];
const CH_COLORS = [
  '\x1b[38;2;160;195;236m', '\x1b[38;2;196;181;253m',
  '\x1b[38;2;255;122;23m', '\x1b[38;2;255;194;133m',
];

let sampleIdx = 0;
let t = 0;
let chirpFreq = 3;
let eegPacketCount = 0;
let accelPacketCount = 0;
let lastStats = Date.now();

// ── 单个样本 (4 通道) ──
function genSample() {
  const vals = [];
  for (let ch = 0; ch < 4; ch++) {
    const cfg = CH_CONFIG[ch];
    let val = 0;
    for (const b of cfg.bands) {
      const freq = CHIRP ? chirpFreq + b.f * 0.3 : b.f;
      val += Math.sin(2 * Math.PI * freq * t) * b.a * AMP;
    }
    val += (Math.random() - 0.5) * NOISE * 2;
    vals.push(val);
  }
  t += 1 / RATE;
  if (CHIRP) {
    chirpFreq += 0.3 / RATE;
    if (chirpFreq > 45) chirpFreq = 3;
  }
  return vals;
}

// ── Float32 Big-Endian 包 (OpenBCI GUI 兼容, 200Hz 逐条) ──
function buildFloat32Packet() {
  const vals = genSample();
  const buf = Buffer.alloc((1 + 4) * 4); // sample + 4ch float32
  buf.writeFloatBE(sampleIdx++, 0);
  for (let ch = 0; ch < 4; ch++) buf.writeFloatBE(vals[ch], (1 + ch) * 4);
  return buf;
}

// ── Binary 0xA0 包 (末尾追加 0xC0 结束字节) ──
function buildBinaryPacket() {
  const vals = genSample();
  const buf = Buffer.alloc(5 + 4 * 3 + 1); // 头 4B + 4ch*3B + 0xC0 结束字节
  buf[0] = 0xA0;
  const seq = sampleIdx++;
  buf[1] = seq & 0xFF; buf[2] = (seq >> 8) & 0xFF; buf[3] = (seq >> 16) & 0xFF;
  let off = 4;
  for (let ch = 0; ch < 4; ch++) {
    let v = Math.round(vals[ch]);
    if (v < 0) v = v & 0xFFFFFF;
    buf[off] = v & 0xFF; buf[off+1] = (v >> 8) & 0xFF; buf[off+2] = (v >> 16) & 0xFF;
    off += 3;
  }
  buf[off] = 0xC0; // 结束字节
  return buf;
}

// ── JSON 包 ──
function buildJSONPacket() {
  const allCh = [[], [], [], []];
  for (let s = 0; s < BATCH; s++) {
    const vals = genSample();
    for (let ch = 0; ch < 4; ch++) allCh[ch].push(Math.round(vals[ch] * 1000) / 1000);
  }
  const obj = { type: 'timeSeriesRaw', data: allCh, sample: sampleIdx, timestamp: Date.now() };
  sampleIdx += BATCH;
  return Buffer.from(JSON.stringify(obj), 'utf8');
}

function buildEEGPacket() {
  if (FORMAT === 'binary') return buildBinaryPacket();
  if (FORMAT === 'json') return buildJSONPacket();
  return buildFloat32Packet();
}

// ── 加速度计包 (JSON, OpenBCI GUI 兼容格式) ──
let accelT = 0;
const ACCEL_RATE = 50; // Hz
function buildAccelPacket() {
  const B = 5;
  const x = [], y = [], z = [];
  for (let i = 0; i < B; i++) {
    const ax = Math.sin(2 * Math.PI * 0.5 * accelT) * 0.5;
    const ay = Math.sin(2 * Math.PI * 0.3 * accelT) * 0.3;
    const az = Math.cos(2 * Math.PI * 0.4 * accelT) * 0.8 + 0.2;
    x.push(ax + (Math.random() - 0.5) * 0.05);
    y.push(ay + (Math.random() - 0.5) * 0.05);
    z.push(az + (Math.random() - 0.5) * 0.05);
    accelT += 1 / ACCEL_RATE;
  }
  return Buffer.from(JSON.stringify({ type: 'accelerometer', data: [x, y, z] }), 'utf8');
}

// ── UDP Sockets ──
const eegSock = dgram.createSocket('udp4');
const accelSock = dgram.createSocket('udp4');

// ── 统计 ──
function printStats() {
  const elapsed = (Date.now() - lastStats) / 1000 || 1;
  const eegRate = Math.round(eegPacketCount / elapsed);
  const accelRate = Math.round(accelPacketCount / elapsed);
  const pktSec = Math.round((eegPacketCount + accelPacketCount) / elapsed);
  const barLen = Math.min(Math.floor(eegRate / 10), 20);
  const bar = C.violet + '█'.repeat(barLen) + C.gray + '░'.repeat(Math.max(0, 20 - barLen)) + R;
  console.log(`  ${C.gray}${new Date().toLocaleTimeString()}${R} ${bar} ${C.cyan}${C.bold}${eegRate} EEG/s${R} ${C.pink}${accelRate} ACC/s${R} ${C.gray}(total ${pktSec}/s)${R}`);
  eegPacketCount = 0;
  accelPacketCount = 0;
  lastStats = Date.now();
}

// ── Banner ──
const BANNER = `
  ${C.cyan}${C.bold}┌──────────────────────────────────────────────────────┐${R}
  ${C.cyan}${C.bold}│${R}  ${C.violet}${C.bold}✦${R}  ${C.cyan}OpenBCI Ganglion Simulator v3${R}                     ${C.cyan}${C.bold}│${R}
  ${C.cyan}${C.bold}│${R}  ${C.gray}${C.dim}────────────────────────────────────────────────────${R}  ${C.cyan}${C.bold}│${R}
  ${C.cyan}${C.bold}│${R}  EEG  → ${C.cyan}${C.bold}${HOST}:${EEG_PORT}${R} (${FORMAT}, ${RATE}Hz)                      ${C.cyan}${C.bold}│${R}
  ${C.cyan}${C.bold}│${R}  ACC  → ${C.pink}${C.bold}${HOST}:${ACCEL_PORT}${R} (JSON, ${ACCEL_RATE}Hz)                    ${C.cyan}${C.bold}│${R}
  ${C.cyan}${C.bold}│${R}  ${C.yellow}Ctrl+C${R} to stop                                         ${C.cyan}${C.bold}│${R}
  ${C.cyan}${C.bold}└──────────────────────────────────────────────────────┘${R}
`;

console.log(BANNER);
console.log(`  ${C.gray}${C.dim}  通道: Fp1 α·β | Fp2 α·β·γ | C3 μ/δ·θ·β | C4 μ/δ·θ·β${R}`);
if (CHIRP) console.log(`  ${C.gray}${C.dim}  扫描: ${CHIRP ? '3→45Hz 频率扫描' : '稳态节律'}${R}`);

// ── 启动 ──
// 保存 interval 引用，便于 SIGINT 时清理
let eegInterval = null;
let accelInterval = null;
let statsInterval = null;

eegSock.bind(0, () => {
  accelSock.bind(0, () => {
    console.log(`  ${C.gray}UDP 发送端就绪, 按 Ctrl+C 停止${R}\n`);

    // EEG 定时器 — 基于 Date.now() 的补偿式调度，避免 setInterval 截断浮点 INTERVAL 导致采样率偏差
    let nextEegTime = Date.now();
    eegInterval = setInterval(() => {
      const now = Date.now();
      // BUG2-fix: 增加 catch-up 上限，系统休眠唤醒后大延迟时重置基准时间避免瞬间发送上千包
      if (now - nextEegTime > 1000) { nextEegTime = now; }
      let sent = 0;
      while (now >= nextEegTime && sent < 50) {
        const buf = buildEEGPacket();
        eegSock.send(buf, EEG_PORT, HOST);
        eegPacketCount++;
        nextEegTime += INTERVAL;
        sent++;
      }
    }, 1);

    // 加速度计定时器 (50Hz) — 同样使用补偿式调度
    const ACCEL_INTERVAL = 1000 / ACCEL_RATE;
    let nextAccelTime = Date.now();
    accelInterval = setInterval(() => {
      const now = Date.now();
      // BUG2-fix: 同 EEG 定时器
      if (now - nextAccelTime > 1000) { nextAccelTime = now; }
      let sent = 0;
      while (now >= nextAccelTime && sent < 50) {
        const buf = buildAccelPacket();
        accelSock.send(buf, ACCEL_PORT, HOST);
        accelPacketCount++;
        nextAccelTime += ACCEL_INTERVAL;
        sent++;
      }
    }, 1);

    // 统计
    statsInterval = setInterval(printStats, 5000);
  });
});

let _shuttingDown = false;
process.on('SIGINT', () => {
  if (_shuttingDown) return;
  _shuttingDown = true;
  console.log(`\n  ${C.cyan}${C.bold}✦${R} ${C.gray}模拟器已停止${R}\n`);
  if (eegInterval) clearInterval(eegInterval);
  if (accelInterval) clearInterval(accelInterval);
  if (statsInterval) clearInterval(statsInterval);
  try { eegSock.close(); } catch (e) {}
  try { accelSock.close(); } catch (e) {}
  // 等 close 完成后再退出
  setTimeout(() => process.exit(0), 500);
});
