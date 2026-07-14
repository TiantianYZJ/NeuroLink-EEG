/**
 * 基线录制 + 恢复判定引擎
 *
 * 基线: flow 阶段后 4 分钟录制 θ/α 均值±标准差
 * 恢复: recover 阶段每秒判定是否连续 30 秒回到基线±5%
 * 自动标记: baseline_start(11) / baseline_end(12) / recovered(13)
 */

// sessionId → { samples: [], thetaAlphaSamples: [], alphaSamples: [], betaSamples: [],
//               phaseId: null, baselineStarted: false, prevPhaseId: null }
const baselineBuf = {};

// sessionId → { steadyCount: 0, recovered: false, baseline: null }
const recoveryState = {};

// sessionId → { ratio_mean, ratio_std, alpha_mean, alpha_std, beta_mean, beta_std } 内存基线
const baselineResults = {};

module.exports = {
  /**
   * 每秒调用一次
   * 返回需要广播的消息数组（auto marker / recovery_progress / recovery_event）
   */
  tick(sessionId, phaseId, timeInPhase, metricsSnapshot) {
    const markers = [];
    const prevPhaseId = baselineBuf[sessionId] ? baselineBuf[sessionId].phaseId : null;

    // ──── 检测离开 flow 阶段 → 写入基线 ────
    if (prevPhaseId && String(prevPhaseId).startsWith('flow') &&
        (!phaseId || !String(phaseId).startsWith('flow'))) {
      const buf = baselineBuf[sessionId];
      if (buf && buf.samples.length > 10) {
        const vals = buf.samples;
        const alphaVals = buf.alphaSamples || [];
        const betaVals = buf.betaSamples || [];
        const ratioMean = vals.reduce((a, v) => a + v, 0) / vals.length;
        const ratioStd = Math.sqrt(vals.reduce((s, v) => s + (v - ratioMean) ** 2, 0) / vals.length);
        const alphaMean = alphaVals.length > 0 ? alphaVals.reduce((a, v) => a + v, 0) / alphaVals.length : 0;
        const alphaStd = alphaVals.length > 0
          ? Math.sqrt(alphaVals.reduce((s, v) => s + (v - alphaMean) ** 2, 0) / alphaVals.length) : 0;
        const betaMean = betaVals.length > 0 ? betaVals.reduce((a, v) => a + v, 0) / betaVals.length : 0;
        const betaStd = betaVals.length > 0
          ? Math.sqrt(betaVals.reduce((s, v) => s + (v - betaMean) ** 2, 0) / betaVals.length) : 0;
        // 内存存储基线结果，供 recover 阶段使用
        baselineResults[prevPhaseId] = { ratio_mean: ratioMean, ratio_std: ratioStd, alpha_mean: alphaMean, alpha_std: alphaStd, beta_mean: betaMean, beta_std: betaStd, samples: vals.length };
        markers.push({
          type: 'marker', code: 12, source: 'auto',
          label: 'baseline_end:' + prevPhaseId, phase: prevPhaseId, ts: Date.now(),
        });
      }
      delete baselineBuf[sessionId];
    }

    // ──── 基线录制（flow 阶段）────
    if (phaseId && String(phaseId).startsWith('flow') && metricsSnapshot) {
      if (!baselineBuf[sessionId] || baselineBuf[sessionId].phaseId !== phaseId) {
        baselineBuf[sessionId] = {
          samples: [], alphaSamples: [], betaSamples: [],
          phaseId, baselineStarted: false,
        };
      }
      const buf = baselineBuf[sessionId];

      if (timeInPhase >= 241 && !buf.baselineStarted) {
        buf.baselineStarted = true;
        markers.push({
          type: 'marker', code: 11, source: 'auto',
          label: 'baseline_start:' + phaseId, phase: phaseId, ts: Date.now(),
        });
      }

      if (buf.baselineStarted && timeInPhase >= 241) {
        buf.samples.push(metricsSnapshot.theta_alpha_ratio);
        buf.alphaSamples.push(metricsSnapshot.band_power ? metricsSnapshot.band_power.alpha : 0);
        buf.betaSamples.push(metricsSnapshot.band_power ? metricsSnapshot.band_power.beta : 0);
      }
    }

    // ──── 恢复判定（recover 阶段）────
    if (phaseId && String(phaseId).startsWith('recover') && metricsSnapshot) {
      if (!recoveryState[sessionId]) {
        recoveryState[sessionId] = { steadyCount: 0, recovered: false, baseline: null };
      }
      const rs = recoveryState[sessionId];

      if (rs.recovered) return markers;

      if (!rs.baseline) {
        // 从内存基线结果查找（对应 flow 阶段）
        const flowPhaseId = phaseId.replace(/recover(\d+)/, 'flow$1');
        rs.baseline = baselineResults[flowPhaseId] || false;
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
          rs.steadyCount = 0;
        }

        if (rs.steadyCount >= 30 && !rs.recovered) {
          rs.recovered = true;
          markers.push({
            type: 'marker', code: 13, source: 'auto',
            label: 'recovered:' + phaseId, phase: phaseId, ts: Date.now(),
          });
          markers.push({
            type: 'recovery_event',
            recovered: true,
            recovery_time: timeInPhase,
            steady_count: rs.steadyCount,
            current_ratio: metricsSnapshot.theta_alpha_ratio,
            baseline_mean: mean,
          });
        } else if (!rs.recovered) {
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

    // 离开 recover 阶段清理
    if (phaseId && !String(phaseId).startsWith('recover')) {
      delete recoveryState[sessionId];
    }

    return markers;
  },

  reset(sessionId) {
    delete baselineBuf[sessionId];
    delete recoveryState[sessionId];
    Object.keys(baselineResults).forEach(k => { if (k.startsWith('flow')) delete baselineResults[k]; });
  },

  cleanup(sessionId) {
    delete baselineBuf[sessionId];
    delete recoveryState[sessionId];
  },
};
