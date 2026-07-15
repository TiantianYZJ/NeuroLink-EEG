/**
 * 实时指标计算引擎
 *
 * 输入: eeg_frame (滑动窗口 256 采样点)
 * 输出: metrics_snapshot (每秒: 频带功率/熵/负载/信号质量 + 每通道频带功率)
 *
 * 多 session 安全: 每个 session 独立缓冲区
 * 4 通道: 每通道独立 Goertzel → band_power_per_ch[4]
 */
const config = require('./config');

// 采样率统一由 config.js 管理
const SAMPLE_RATE = config.EEG_SAMPLE_RATE;

// gamma 频带上限需低于 Nyquist 频率 (SAMPLE_RATE/2)，否则产生混叠
// 120Hz 采样 → Nyquist=60Hz → gamma = 32-59Hz
const GAMMA_HI = Math.min(100, SAMPLE_RATE / 2 - 1);

const BANDS = [
  { id: 'delta', lo: 0.5, hi: 4 },
  { id: 'theta', lo: 4,   hi: 8 },
  { id: 'alpha', lo: 8,   hi: 13 },
  { id: 'beta',  lo: 13,  hi: 32 },
  { id: 'gamma', lo: 32,  hi: GAMMA_HI },
];

// 每个 session 独立缓冲区: sessionId → { buffer: [], lastCompute: 0 }
const sessions = new Map();

const POOL_SIZE = 256;    // ~2.13s @ 120Hz (频带功率用)
const SQ_WINDOW = 32;     // 信号稳定性窗口 (N=32, 文档 §4.5)

function ensureSession(sessionId) {
  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, { buffer: [], lastCompute: 0, frameCount: 0, lastFrameCount: 0, lastFrameTime: 0 });
  }
  return sessions.get(sessionId);
}

function goertzel(signal, freq, rate) {
  const n = signal.length;
  const k = Math.round(freq * n / rate);
  const omega = 2 * Math.PI * k / n;
  const cosine = 2 * Math.cos(omega);
  let s1 = 0, s2 = 0;
  for (let i = 0; i < n; i++) {
    const y = signal[i] + cosine * s1 - s2;
    s2 = s1;
    s1 = y;
  }
  return Math.sqrt(s2 * s2 + s1 * s1 - cosine * s1 * s2) / n;
}

function goertzelBand(signal, band, rate) {
  return goertzel(signal, (band.lo + band.hi) / 2, rate);
}

function spectralEntropy(bandPowers) {
  // 使用功率（幅度平方）计算谱熵, H = -Σ(pi·log₂(pi))
  const powers = bandPowers.map(v => v * v);
  const total = powers.reduce((a, b) => a + b, 0);
  if (total === 0) return 0;
  return -powers.reduce((sum, p) => {
    const pi = p / total;
    return sum + (pi > 0 ? pi * Math.log2(pi) : 0);
  }, 0);
}

function computeMetrics(sessionId) {
  const ss = sessions.get(sessionId);
  if (!ss || ss.buffer.length < 64) return null;

  // 频带功率: 使用完整 256 样本窗口
  const poolSamples = ss.buffer.slice(-POOL_SIZE);

  // 4 通道 Goertzel: 每通道计算 5 频带
  const chPower = [0, 1, 2, 3].map(chIdx => {
    const ch = poolSamples.map(s => s.channels[chIdx] || 0);
    return BANDS.map(b => goertzelBand(ch, b, SAMPLE_RATE));
  });

  // CH1 频带功率 (向后兼容)
  const ch1Powers = chPower[0];
  const bandPower = {};
  BANDS.forEach((b, i) => { bandPower[b.id] = ch1Powers[i]; });

  // 复合指标 (基于 CH1)
  const thetaAlpha = ch1Powers[1] / (ch1Powers[2] || 1);
  const entropy = spectralEntropy(ch1Powers);
  const cognitiveLoad = ch1Powers[4] / (ch1Powers[2] || 1);

  // 每通道频带功率 (地形图数据源)
  const bandPowerPerCh = chPower.map(ch => {
    const obj = {};
    BANDS.forEach((b, i) => { obj[b.id] = ch[i]; });
    return obj;
  });

  // 信号稳定性: 使用 32 样本窗口 (文档 §4.5)
  const sqSamples = ss.buffer.slice(-SQ_WINDOW);
  const sqCh = [0, 1, 2, 3].map(chIdx => {
    const ch = sqSamples.map(s => s.channels[chIdx] || 0);
    const m = ch.reduce((a, b) => a + b, 0) / ch.length;
    return Math.sqrt(ch.reduce((sum, v) => sum + (v - m) ** 2, 0) / ch.length);
  });

  return {
    band_power: bandPower,
    band_power_per_ch: bandPowerPerCh,
    theta_alpha_ratio: thetaAlpha,
    spectral_entropy: entropy,
    cognitive_load_index: cognitiveLoad,
    signal_quality: sqCh,
  };
}

module.exports = {
  pushFrame(frame, sessionId) {
    const ss = ensureSession(sessionId);
    ss.buffer.push(frame);
    ss.frameCount++;
    if (ss.buffer.length > POOL_SIZE * 2) ss.buffer = ss.buffer.slice(-POOL_SIZE);
  },

  tick(now, sessionId, phaseIndex, phaseId) {
    const ss = ensureSession(sessionId);
    if (now - ss.lastCompute < 1000) return null;
    ss.lastCompute = now;
    const metrics = computeMetrics(sessionId);
    if (!metrics) return null;

    // Real sample rate from actual frame count
    const elapsed = (now - ss.lastFrameTime) || 1000;
    const actualRate = Math.round((ss.frameCount - ss.lastFrameCount) * 1000 / elapsed);
    ss.lastFrameCount = ss.frameCount;
    ss.lastFrameTime = now;

    return {
      ts: now,
      session_id: sessionId,
      phase_index: phaseIndex,
      phase_id: phaseId,
      sample_rate: Math.min(actualRate, 500),
      ...metrics,
    };
  },

  /** 清理 session 数据（session 完成时调用） */
  cleanup(sessionId) {
    sessions.delete(sessionId);
  },
};
