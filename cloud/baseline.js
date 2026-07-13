/**
 * 基线录制 + 恢复判定引擎
 *
 * 基线: flow 阶段后 4 分钟录制 θ/α 均值±标准差
 * 恢复: recover 阶段每秒判定是否连续 30 秒回到基线±5%
 * 自动标记: baseline_start(11) / baseline_end(12) / recovered(13)
 */
const db = require('./db');

// ── 基线录制状态 ──
// sessionId → { samples: [], phaseId: null, baselineStarted: false }
const baselineBuf = {};

// ── 恢复判定状态 ──
// sessionId → { steadyCount: 0, recovered: false, baseline: null }
const recoveryState = {};

module.exports = {
  /**
   * 每秒调用一次，传入当前 metrics_snapshot
   * 返回需要广播的 auto 标记数组
   */
  async tick(sessionId, phaseId, timeInPhase, metricsSnapshot) {
    const markers = [];

    // ──── 基线录制 ────
    if (phaseId && String(phaseId).startsWith('flow') && metricsSnapshot) {
      if (!baselineBuf[sessionId] || baselineBuf[sessionId].phaseId !== phaseId) {
        baselineBuf[sessionId] = { samples: [], phaseId, baselineStarted: false };
      }
      const buf = baselineBuf[sessionId];

      // 第 241 秒 → baselines_start
      if (timeInPhase >= 241 && !buf.baselineStarted) {
        buf.baselineStarted = true;
        markers.push({
          type: 'marker', code: 11, source: 'auto',
          label: 'baseline_start:' + phaseId, phase: phaseId, ts: Date.now(),
        });
      }

      if (buf.baselineStarted && timeInPhase >= 241) {
        buf.samples.push(metricsSnapshot.theta_alpha_ratio);
      }
    }

    // flow 阶段切换出去 → 写入 DB 基线
    if (phaseId && String(phaseId).startsWith('flow') && timeInPhase === 0) {
      const buf = baselineBuf[sessionId];
      if (buf && buf.samples.length > 10) {
        const vals = buf.samples;
        const mean = vals.reduce((a, v) => a + v, 0) / vals.length;
        const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / vals.length);
        try {
          await db.query(
            'INSERT INTO baselines (session_id, phase_id, ratio_mean, ratio_std, samples) VALUES (?,?,?,?,?)',
            [sessionId, phaseId, mean, std, vals.length]
          );
        } catch (_) {}
        markers.push({
          type: 'marker', code: 12, source: 'auto',
          label: 'baseline_end:' + phaseId, phase: phaseId, ts: Date.now(),
        });
      }
      delete baselineBuf[sessionId];
    }

    // ──── 恢复判定 ────
    if (phaseId && String(phaseId).startsWith('recover') && metricsSnapshot) {
      if (!recoveryState[sessionId]) {
        recoveryState[sessionId] = { steadyCount: 0, recovered: false, baseline: null };
      }
      const rs = recoveryState[sessionId];

      // 已恢复就不再判定
      if (rs.recovered) return markers;

      // 懒加载基线（只查一次）
      if (!rs.baseline) {
        try {
          const rows = await db.query(
            'SELECT * FROM baselines WHERE session_id = ? AND phase_id = ? ORDER BY id DESC LIMIT 1',
            [sessionId, phaseId.replace(/recover(\d+)/, 'flow$1')] // recover1 → flow1
          );
          rs.baseline = rows.length > 0 ? rows[0] : false;
        } catch (_) { rs.baseline = false; }
      }

      if (rs.baseline) {
        const { ratio_mean: mean } = rs.baseline;
        const targetLow = mean * 0.95;
        const targetHigh = mean * 1.05;
        const inWindow = metricsSnapshot.theta_alpha_ratio >= targetLow &&
                         metricsSnapshot.theta_alpha_ratio <= targetHigh;

        if (inWindow) {
          rs.steadyCount++;
        } else {
          rs.steadyCount = 0; // 严格连续，不采用多数制
        }

        if (rs.steadyCount >= 30 && !rs.recovered) {
          rs.recovered = true;
          markers.push({
            type: 'marker', code: 13, source: 'auto',
            label: 'recovered:' + phaseId, phase: phaseId, ts: Date.now(),
          });
          // 发送恢复通知给前端
          markers.push({
            type: 'recovery_event',
            recovered: true,
            recovery_time: timeInPhase,
            steady_count: rs.steadyCount,
            current_ratio: metricsSnapshot.theta_alpha_ratio,
            baseline_mean: mean,
          });
        } else {
          markers.push({
            type: 'recovery_progress',
            recovered: false,
            steady_count: rs.steadyCount,
            current_ratio: metricsSnapshot.theta_alpha_ratio,
            baseline_mean: mean,
            target_low: targetLow,
            target_high: targetHigh,
          });
        }
      } else {
        // 无基线数据（例如直接进入 recover 无前序 flow）
        markers.push({
          type: 'recovery_progress',
          recovered: false,
          steady_count: 0,
          current_ratio: metricsSnapshot.theta_alpha_ratio,
          baseline_mean: 0,
          target_low: 0,
          target_high: 0,
          no_baseline: true,
        });
      }
    }

    // 离开 recover 阶段时清理状态
    if (phaseId && !String(phaseId).startsWith('recover')) {
      delete recoveryState[sessionId];
    }

    return markers;
  },

  /** 重置 session 的基线/恢复状态 */
  reset(sessionId) {
    delete baselineBuf[sessionId];
    delete recoveryState[sessionId];
  },
};
