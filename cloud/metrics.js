/**
 * 实时指标计算引擎
 *
 * 输入: eeg_frame (滑动窗口 256 采样点)
 * 输出: metrics_snapshot (每秒: 频带功率/熵/负载/信号质量)
 */
const BANDS = [
  { id: 'delta', lo: 0.5, hi: 4 },
  { id: 'theta', lo: 4,   hi: 8 },
  { id: 'alpha', lo: 8,   hi: 13 },
  { id: 'beta',  lo: 13,  hi: 32 },
  { id: 'gamma', lo: 32,  hi: 100 },
];

let buffer = [];          // [{ channels: [4 floats], ts }]
let lastCompute = 0;
const POOL_SIZE = 256;    // ~1.28s @ 200Hz
const SAMPLE_RATE = 200;

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

function spectralEntropy(bandPowers) {
  const total = bandPowers.reduce((a, b) => a + b, 0);
  if (total === 0) return 0;
  return -bandPowers.reduce((sum, p) => {
    const pi = p / total;
    return sum + (pi > 0 ? pi * Math.log2(pi) : 0);
  }, 0);
}

function computeMetrics() {
  if (buffer.length < 64) return null;
  const samples = buffer.slice(-POOL_SIZE);
  const ch1 = samples.map(s => s.channels[0] || 0);
  const ch2 = samples.map(s => s.channels[1] || 0);
  const ch3 = samples.map(s => s.channels[2] || 0);
  const ch4 = samples.map(s => s.channels[3] || 0);

  // 频带功率 (基于 CH1)
  const powers = BANDS.map(b => goertzel(ch1, (b.lo + b.hi) / 2, SAMPLE_RATE));
  const bandPower = {};
  BANDS.forEach((b, i) => { bandPower[b.id] = powers[i]; });

  // 复合指标
  const thetaAlpha = powers[1] / (powers[2] || 1);
  const entropy = spectralEntropy(powers);
  const cognitiveLoad = powers[4] / (powers[2] || 1);

  // 信号稳定性 (标准差)
  const sq = [ch1, ch2, ch3, ch4].map(ch => {
    const m = ch.reduce((a, b) => a + b, 0) / ch.length;
    return Math.sqrt(ch.reduce((sum, v) => sum + (v - m) ** 2, 0) / ch.length);
  });

  return {
    band_power: { delta: powers[0], theta: powers[1], alpha: powers[2], beta: powers[3], gamma: powers[4] },
    theta_alpha_ratio: thetaAlpha,
    spectral_entropy: entropy,
    cognitive_load_index: cognitiveLoad,
    signal_quality: sq,
  };
}

module.exports = {
  pushFrame(frame) {
    buffer.push(frame);
    if (buffer.length > POOL_SIZE * 2) buffer = buffer.slice(-POOL_SIZE);
  },

  tick(now, sessionId, phaseIndex, phaseId) {
    if (now - lastCompute < 1000) return null;
    lastCompute = now;
    const metrics = computeMetrics();
    if (!metrics) return null;

    const snapshot = {
      ts: now,
      session_id: sessionId,
      phase_index: phaseIndex,
      phase_id: phaseId,
      ...metrics,
    };
    return snapshot;
  },
};
