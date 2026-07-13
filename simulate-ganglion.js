/**
 * OpenBCI Ganglion 模拟器
 *
 * 在无真实 EEG 设备时模拟 Ganglion UDP 数据流。
 * 发送格式与 bridge/index.js parseOpenBCIPacket() 完全一致:
 *   0xA0 + 24-bit LE sampleNumber + 4ch × 24-bit LE 电压值
 *
 * 使用: node simulate-ganglion.js [选项]
 *
 * 选项:
 *   --rate 200      采样率 (Hz), 默认 200
 *   --port 12345    UDP 目标端口, 默认 12345
 *   --host 127.0.0.1 UDP 目标地址, 默认 127.0.0.1
 *   --amp 2000      信号幅值 (µVrms), 默认 2000
 *   --noise 500     噪声水平 (µV), 默认 500
 *   --chirp         启用频率扫描 (模拟不同脑波频段)
 */

const dgram = require('dgram');
const sock = dgram.createSocket('udp4');

// ── 解析参数 ──
const args = {};
process.argv.slice(2).forEach((a, i) => {
  if (a.startsWith('--')) args[a.slice(2)] = process.argv[i + 3] && !process.argv[i + 3].startsWith('--') ? process.argv[i + 3] : true;
});

const RATE = parseInt(args.rate, 10) || 200;
const PORT = parseInt(args.port, 10) || 12345;
const HOST = args.host || '127.0.0.1';
const AMP = parseFloat(args.amp) || 2000;
const NOISE = parseFloat(args.noise) || 500;
const CHIRP = args.chirp === 'true' || args.chirp === true;

const INTERVAL = 1000 / RATE;  // 5ms @ 200Hz

// ── 模拟信号生成 ──
// 真实脑电各频段: δ(0.5-4) θ(4-8) α(8-13) β(13-32) γ(32-100)
// 每通道混合同: 基础节律 + 独立噪声
// CH1 Fp1: α 主导 (放松/专注基线)
// CH2 Fp2: α + β
// CH3 C3:  μ 节律 (感觉运动), δ+θ
// CH4 C4:  μ 节律, δ+θ

const CH_CONFIG = [
  { name: 'Fp1', bands: [{f:10.5,a:0.6},{f:22,a:0.2},{f:6,a:0.15}] },  // α 主导
  { name: 'Fp2', bands: [{f:10.5,a:0.4},{f:22,a:0.35},{f:6,a:0.2},{f:40,a:0.05}] },  // α + β
  { name: 'C3',  bands: [{f:10,a:0.25},{f:3,a:0.3},{f:6,a:0.3},{f:22,a:0.15}] },  // μ + δ/θ
  { name: 'C4',  bands: [{f:10,a:0.25},{f:3,a:0.3},{f:6,a:0.3},{f:22,a:0.15}] },  // μ + δ/θ
];

let seq = 0;
let t = 0;
let t0 = Date.now();
let chirpFreq = 3; // chirp 模式下起始频率

function buildPacket() {
  const buf = Buffer.alloc(1 + 3 + 4 * 3); // 0xA0 + 3 + 12 = 16 bytes
  buf[0] = 0xA0;
  // 24-bit LE sampleNumber
  buf[1] = seq & 0xFF;
  buf[2] = (seq >> 8) & 0xFF;
  buf[3] = (seq >> 16) & 0xFF;
  seq = (seq + 1) & 0xFFFFFF;

  let offset = 4;
  for (let ch = 0; ch < 4; ch++) {
    const cfg = CH_CONFIG[ch];
    let val = 0;
    for (const b of cfg.bands) {
      const freq = CHIRP ? chirpFreq + b.f * 0.3 : b.f;
      val += Math.sin(2 * Math.PI * freq * t) * b.a * AMP;
    }
    // 独立噪声
    val += (Math.random() - 0.5) * NOISE * 2;
    val = Math.round(val);

    // 24-bit LE, 符号扩展
    if (val < 0) val = val & 0xFFFFFF;
    buf[offset] = val & 0xFF;
    buf[offset + 1] = (val >> 8) & 0xFF;
    buf[offset + 2] = (val >> 16) & 0xFF;
    offset += 3;
  }

  t += 1 / RATE;
  if (CHIRP) {
    chirpFreq += 0.3 / RATE;
    if (chirpFreq > 45) chirpFreq = 3;
  }

  return buf;
}

// ── 统计输出 ──
let packetsSent = 0;
let lastStats = Date.now();

function printStats() {
  const elapsed = (Date.now() - lastStats) / 1000;
  const rate = packetsSent / elapsed;
  console.log(`[模拟器] ${packetsSent} 包 | ${rate.toFixed(0)} pkt/s | seq=${seq} | t=${t.toFixed(1)}s${CHIRP ? ` | chirp=${chirpFreq.toFixed(1)}Hz` : ''}`);
  packetsSent = 0;
  lastStats = Date.now();
}

// ── 主循环 ──
console.log(`╔══ OpenBCI Ganglion 模拟器 ═══════════════════`);
console.log(`║  目标: ${HOST}:${PORT}`);
console.log(`║  速率: ${RATE} Hz (间隔 ${INTERVAL.toFixed(1)}ms)`);
console.log(`║  幅值: ${AMP} µVrms | 噪声: ${NOISE} µV`);
console.log(`║  通道: Fp1 α主导 · Fp2 α+β · C3 μ+δ/θ · C4 μ+δ/θ`);
console.log(`║  模式: ${CHIRP ? '频率扫描 3-45Hz' : '稳态节律'}`);
console.log(`╚${'═'.repeat(45)}`);
console.log('');

let timer, statsTimer;

function start() {
  timer = setInterval(() => {
    const buf = buildPacket();
    sock.send(buf, PORT, HOST, (err) => {
      if (err) console.error('[发送错误]', err.message);
    });
    packetsSent++;
  }, INTERVAL);

  statsTimer = setInterval(printStats, 5000);
}

// ── 优雅退出 ──
process.on('SIGINT', () => {
  clearInterval(timer);
  clearInterval(statsTimer);
  sock.close();
  console.log('\n[模拟器] 已停止');
  process.exit(0);
});

sock.on('listening', () => {
  console.log(`[UDP] 发送端就绪: ${sock.address().address}:${sock.address().port} → ${HOST}:${PORT}`);
  start();
});

// 绑定随机端口
sock.bind();
