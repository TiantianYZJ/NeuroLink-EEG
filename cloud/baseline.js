/**
 * 基线录制 + 恢复判定引擎
 *
 * 基线: flow 阶段后 4 分钟录制 θ/α 均值±标准差
 * 恢复: recover 阶段每秒判定是否连续 30 秒回到基线±5%
 */
const db = require('./db');

// 每个 flow 阶段的基线缓存 (内存)
const baselines = {};

module.exports = {
  /**
   * 阶段变更时调用
   */
  onPhaseChange(sessionId, phaseId, timeInPhase, metricsSnapshot) {
    // 检测 flow 阶段后 4 分钟 (第 241 秒起)
    if (phaseId.startsWith('flow') && timeInPhase >= 241) {
      if (!baselines[sessionId]) baselines[sessionId] = { samples: [] };
      if (metricsSnapshot) {
        baselines[sessionId].samples.push(metricsSnapshot.theta_alpha_ratio);
      }
    }

    // flow 阶段结束 → 写入 MySQL
    if (phaseId.startsWith('flow') && timeInPhase === 0) {
      const b = baselines[sessionId];
      if (b && b.samples.length > 10) {
        const vals = b.samples;
        const mean = vals.reduce((a, v) => a + v, 0) / vals.length;
        const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / vals.length);
        db.query(
          'INSERT INTO baselines (session_id, phase_id, ratio_mean, ratio_std, samples) VALUES (?,?,?,?,?)',
          [sessionId, phaseId, mean, std, vals.length]
        ).catch(() => {});
        delete baselines[sessionId];
      }
    }
  },

  /**
   * recover 阶段每秒判定
   * 返回: { recovered: bool, recoveryTime: 秒, steadyCount: 连续秒数 } | null
   */
  checkRecovery(sessionId, phaseId, timeInRecover, currentRatio, baselinesForSession) {
    if (!phaseId.startsWith('recover')) return null;
    if (!baselinesForSession) return { recovered: false, steadyCount: 0 };

    const { ratio_mean: mean, ratio_std: std } = baselinesForSession;
    const targetLow = mean * 0.95;
    const targetHigh = mean * 1.05;

    const inWindow = currentRatio >= targetLow && currentRatio <= targetHigh;
    // 连续 30 秒判定逻辑由调用方维护 (timer.js)
    return {
      inWindow,
      targetLow,
      targetHigh,
      baselineMean: mean,
      currentRatio,
    };
  },
};
