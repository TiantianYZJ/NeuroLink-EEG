/**
 * OpenBCI Ganglion 模拟器 (v2)
 *
 * 模拟真实 OpenBCI GUI UDP 输出，支持三种协议格式 + 加速度计。
 * 用于无 EEG 设备时的桥接/ECS/前端全链路调试。
 *
 * 使用: node simulate-ganglion.js [选项]
 *
 * 选项:
 *   --format json     数据格式: json | float32 | binary (默认 json)
 *   --rate 120        EEG 采样率 (Hz), 默认 120 (Ganglion 实际)
 *   --port 12345      UDP 目标端口, 默认 12345
 *   --host 127.0.0.1  UDP 目标地址, 默认 127.0.0.1
 *   --amp 2000        信号幅值 (µVrms), 默认 2000
 *   --noise 500       噪声水平 (µV), 默认 500
 *   --batch 10        每包样本数 (JSON 模式), 默认 10
 *   --chirp           启用频率扫描
 *   --accel           同时模拟加速度计 (端口 12347)
 */

const dgram = require('dgram');
const sock = dgram.createSocket('udp4');
let accelSock = null;

// ── 解析参数 ──
const args = {};
process.argv.slice(2).forEach((a, i) => {
  const n = process.argv[i + 2];
  if (a.startsWith('--')) args[a.slice(2)] = (n && !n.startsWith('--') ? n : true);
});

const FORMAT = (args.format || 'json').toLowerCase();
const RATE = parseInt(args.rate, 10) || 120;
const PORT = parseInt(args.port, 10) || 12345;
const HOST = args.host || '127.0.0.1';
const AMP = parseFloat(args.amp) || 2000;
const NOISE = parseFloat(args.noise) || 500;
const BATCH = parseInt(args.batch, 10) || 10;
const CHIRP = args.chirp !== undefined && args.chirp !== false;
const DO_ACCEL = args.accel !== undefined && args.accel !== false;

const INTERVAL = 1000 / RATE;

// ── 通道配置 ──
const CH_CONFIG = [
  { name: 'Fp1', bands: [{f:10.5,a:0.6},{f:22,a:0.2},{f:6,a:0.15}] },
  { name: 'Fp2', bands: [{f:10.5,a:0.4},{f:22,a:0.35},{f:6,a:0.2},{f:40,a:0.05}] },
  { name: 'C3',  bands: [{f:10,a:0.25},{f:3,a:0.3},{f:6,a:0.3},{f:22,a:0.15}] },
  { name: 'C4',  bands: [{f:10,a:0.25},{f:3,a:0.3},{f:6,a:0.3},{f:22,a:0.15}] },
];

let seq = 0;
let t = 0;
let chirpFreq = 3;
let sampleIdx = 0;

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

// ── 格式 1: Binary (0xA0 + 24-bit) ──
function buildBinaryPacket() {
  const vals = genSample();
  const buf = Buffer.alloc(1 + 3 + 4 * 3);
  buf[0] = 0xA0;
  buf[1] = seq & 0xFF;
  buf[2] = (seq >> 8) & 0xFF;
  buf[3] = (seq >> 16) & 0xFF;
  seq = (seq + 1) & 0xFFFFFF;
  let off = 4;
  for (let ch = 0; ch < 4; ch++) {
    let v = Math.round(vals[ch]);
    if (v < 0) v = v & 0xFFFFFF;
    buf[off] = v & 0xFF;
    buf[off + 1] = (v >> 8) & 0xFF;
    buf[off + 2] = (v >> 16) & 0xFF;
    off += 3;
  }
  return buf;
}

// ── 格式 2: Float32 Big-Endian ──
function buildFloat32Packet() {
  const vals = genSample();
  const buf = Buffer.alloc((1 + 4) * 4);
  buf.writeFloatBE(sampleIdx++, 0);
  for (let ch = 0; ch < 4; ch++) buf.writeFloatBE(vals[ch], (1 + ch) * 4);
  return buf;
}

// ── 格式 3: JSON (OpenBCI GUI 兼容) ──
// 真实 GUI 每包含 BATCH 个样本 (10 @ 250Hz → 25 pkt/s)
function buildJSONPacket() {
  const allCh = [[], [], [], []];
  const baseT = t;
  for (let s = 0; s < BATCH; s++) {
    const vals = genSample();
    for (let ch = 0; ch < 4; ch++) allCh[ch].push(Math.round(vals[ch] * 1000) / 1000);
  }
  const obj = {
    type: 'timeSeriesRaw',
    data: allCh,
    sample: sampleIdx,
    timestamp: Date.now(),
  };
  sampleIdx += BATCH;
  return Buffer.from(JSON.stringify(obj), 'utf8');
}

function buildPacket() {
  if (FORMAT === 'binary') return buildBinaryPacket();
  if (FORMAT === 'float32') return buildFloat32Packet();
  return buildJSONPacket();
}

// ── 加速度计模拟 ──
let accelT = 0;
function buildAccelPacket() {
  const B = 5; // batch per axis
  const x = [], y = [], z = [];
  for (let i = 0; i < B; i++) {
    x.push(Math.sin(2 * Math.PI * 0.5 * accelT) * 0.5 + (Math.random() - 0.5) * 0.1);
    y.push(Math.sin(2 * Math.PI * 0.3 * accelT) * 0.3 + (Math.random() - 0.5) * 0.1);
    z.push(Math.cos(2 * Math.PI * 0.4 * accelT) * 0.8 + (Math.random() - 0.5) * 0.1);
    accelT += 1 / 50;
  }
  return Buffer.from(JSON.stringify({ type: 'accelerometer', data: [x, y, z] }), 'utf8');
}

// ── 统计 ──
let packetsSent = 0;
let framesSent = 0;
let lastStats = Date.now();

function printStats() {
  const elapsed = (Date.now() - lastStats) / 1000;
  const pktRate = packetsSent / elapsed;
  const frmRate = framesSent / elapsed;
  console.log(
    `[模拟器] ${FORMAT} | ${pktRate.toFixed(0).padStart(3)} pkt/s | ` +
    `${frmRate.toFixed(0).padStart(4)} sps | seq=${sampleIdx} | t=${t.toFixed(1)}s` +
    (CHIRP ? ` | chirp=${chirpFreq.toFixed(0)}Hz` : '')
  );
  packetsSent = 0;
  framesSent = 0;
  lastStats = Date.now();
}

// ── 主循环 ──
console.log(`╔══ OpenBCI Ganglion 模拟器 v2 ═══════════════════╗`);
console.log(`║  目标: ${HOST}:${PORT} (${FORMAT})`);
console.log(`║  速率: ${RATE} Hz | 批大小: ${FORMAT === 'json' ? BATCH : 1}`);
console.log(`║  幅值: ${AMP} µVrms | 噪声: ${NOISE} µV`);
console.log(`║  通道: Fp1 α主导 · Fp2 α+β · C3 μ+δ/θ · C4 μ+δ/θ`);
console.log(`║  模式: ${CHIRP ? '频率扫描 3-45Hz' : '稳态节律'}`);
if (DO_ACCEL) console.log(`║  加速度计: ${HOST}:12347`);
console.log(`╚${'═'.repeat(48)}╝`);

let timer, statsTimer, accelTimer;

function start() {
  timer = setInterval(() => {
    const buf = buildPacket();
    sock.send(buf, PORT, HOST, (err) => {
      if (err) console.error('[发送错误]', err.message);
    });
    packetsSent++;
    framesSent += (FORMAT === 'json' ? BATCH : 1);
  }, INTERVAL);

  statsTimer = setInterval(printStats, 5000);

  if (DO_ACCEL) {
    accelSock = dgram.createSocket('udp4');
    accelTimer = setInterval(() => {
      const buf = buildAccelPacket();
      accelSock.send(buf, 12347, HOST, (err) => {
        if (err) console.error('[ACCEL发送错误]', err.message);
      });
    }, 200); // 50Hz accel
  }
}

process.on('SIGINT', () => {
  clearInterval(timer);
  clearInterval(statsTimer);
  clearInterval(accelTimer);
  sock.close();
  if (accelSock) accelSock.close();
  console.log('\n[模拟器] 已停止');
  process.exit(0);
});

const freePort = 0;
sock.bind(freePort, () => {
  console.log(`[UDP] 发送端就绪: ${sock.address().address}:${sock.address().port} → ${HOST}:${PORT}`);
  start();
});
