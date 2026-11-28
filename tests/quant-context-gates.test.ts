import test from "node:test";
import assert from "node:assert/strict";
import {
  evaluateContextGatesOos,
  evaluateLiveContextGates,
  type AdvancedQuantSetupRecord,
  type ContextGateRuleResult,
  type LiveContextGateInput,
} from "../src/quant-learning-analytics.js";
import { normalizeBlockReasonKey } from "../src/learning-engine.js";

function makeSetup(idx: number, overrides: Partial<AdvancedQuantSetupRecord> = {}): AdvancedQuantSetupRecord {
  return {
    entryTimeSec: 1_790_000_000 + idx * 3600,
    symbol: "SOLUSDT",
    pnlPct: 0.9,
    score: 60,
    buyVolRatio12: 2.0,
    rawRs24h: 1.5,
    betaAlpha24h: 1.2,
    rsi1h: 55,
    atrCompressionRatio: 1.0,
    distFromBottomLowPct: 2.5,
    upperWickRatioPct: 10,
    breadthDelta4h: 4,
    concurrentSignals: 3,
    usdRet5dPct: 0.0,
    sessionBucket: "US_MIDDAY",
    vwapDistPct: 0.4,
    overnightGapPct: 0.1,
    sectorName: "TECH",
    sectorBreadthPct: 60,
    takerBuyPct12: 55,
    oiDelta4hPct: 1.2,
    fundingRateBps: 0.5,
    perpBasisBps: 0.0,
    topLongShortRatio: 1.2,
    macroEventDay: false,
    macroEventInWindow: false,
    hitSl: false,
    mfePct: 1.4,
    subBarPathShape: "NEUTRAL_PATH",
    lateBarCloseStrength: 0.1,
    usedSubBar1m: true,
    autoBeReturns: {},
    autoBeHitSl: {},
    ...overrides,
  };
}

function baseLiveInput(overrides: Partial<LiveContextGateInput> = {}): LiveContextGateInput {
  return {
    usdRet5dPct: 0.1,
    betaAlpha24h: 1.0,
    atrCompressionRatio: 1.0,
    concurrentSignals: 4,
    vwapDistPct: 0.3,
    distFromBottomLowPct: 2.4,
    sessionBucket: "US_MIDDAY",
    macroEventDay: false,
    macroEventInWindow: false,
    oiDelta4hPct: 1.0,
    takerBuyPct12: 54,
    perpBasisBps: 0.0,
    overnightGapPct: 0.0,
    subBarPathShape: null,
    ...overrides,
  };
}

test("1. evaluateContextGatesOos evaluates ATR_SQUEEZE and FRESH_BOTTOM_LOW on 70/30 chronological split", () => {
  const records: AdvancedQuantSetupRecord[] = [];
  for (let i = 0; i < 100; i++) {
    const isSqueeze = i % 4 === 0;
    const isFresh = i % 5 === 0;
    const pnlPct = isSqueeze ? -1.4 : isFresh ? -0.9 : 0.85;
    records.push(
      makeSetup(i, {
        atrCompressionRatio: isSqueeze ? 0.6 : 1.05,
        distFromBottomLowPct: isFresh ? 0.9 : 2.8,
        pnlPct,
        hitSl: pnlPct < 0,
      }),
    );
  }

  const res = evaluateContextGatesOos(records, "binance", 2);
  const squeezeRule = res.rules.find((r) => r.id === "ATR_SQUEEZE");
  const freshRule = res.rules.find((r) => r.id === "FRESH_BOTTOM_LOW");

  assert.ok(squeezeRule);
  assert.equal(squeezeRule.status, "PROMOTE_READY");
  assert.equal(squeezeRule.recommendedAction, "HARD_GATE");
  assert.ok(squeezeRule.hardGate.trainDeltaPct > 0);
  assert.ok(squeezeRule.hardGate.testDeltaPct >= 0.05);

  assert.ok(freshRule);
  assert.equal(freshRule.status, "PROMOTE_READY");

  // Alpaca should exclude crypto-only FRESH_BOTTOM_LOW
  const alpRes = evaluateContextGatesOos(records, "alpaca", 3);
  assert.equal(alpRes.rules.some((r) => r.id === "FRESH_BOTTOM_LOW"), false);
});

test("2. evaluateLiveContextGates enforces modes (off, observe, auto_half_size, auto_enforce) and auto-demotion", () => {
  const promotedUsdHard: ContextGateRuleResult = {
    id: "USD_SURGE",
    label: "USD 5D Surge (>+0.5%)",
    affectedTrain: 30,
    affectedTest: 10,
    hardGate: {
      trainDeltaPct: 0.1,
      testDeltaPct: 0.4,
      test: { trades: 20, winRatePct: 80, avgPnlPct: 1.1, totalPnlPct: 22, profitFactor: 5, maxDrawdownPct: 1 },
    },
    halfSize: {
      trainDeltaPct: 0.05,
      testDeltaPct: 0.1,
      test: { trades: 30, winRatePct: 70, avgPnlPct: 0.8, totalPnlPct: 24, profitFactor: 3, maxDrawdownPct: 1.5 },
    },
    recommendedAction: "HARD_GATE",
    status: "PROMOTE_READY",
    reason: "Passed Train + OOS",
  };
  const demotedSqueeze: ContextGateRuleResult = {
    ...promotedUsdHard,
    id: "ATR_SQUEEZE",
    label: "ATR Squeeze (<0.75x ATR96)",
    status: "REJECT",
  };
  const cryptoOnlyFresh: ContextGateRuleResult = {
    ...promotedUsdHard,
    id: "FRESH_BOTTOM_LOW",
    label: "Unconfirmed Bottom (<1.5% from low)",
    status: "PROMOTE_READY",
  };

  const surgingInput = baseLiveInput({ usdRet5dPct: 0.85, atrCompressionRatio: 0.5, distFromBottomLowPct: 0.8 });

  // off -> ALLOW 1.0x
  const dOff = evaluateLiveContextGates({
    promotedRules: [promotedUsdHard],
    mode: "off",
    exchange: "alpaca",
    input: surgingInput,
  });
  assert.equal(dOff.action, "ALLOW");
  assert.equal(dOff.sizeMultiplier, 1.0);
  assert.equal(dOff.triggeredRules.length, 0);

  // observe -> ALLOW 1.0x + SHADOW_CTX_ tag
  const dObs = evaluateLiveContextGates({
    promotedRules: [promotedUsdHard],
    mode: "observe",
    exchange: "alpaca",
    input: surgingInput,
  });
  assert.equal(dObs.action, "ALLOW");
  assert.equal(dObs.sizeMultiplier, 1.0);
  assert.equal(dObs.primaryBlockReason, "SHADOW_CTX_USD_SURGE");

  // auto_half_size -> HALF_SIZE 0.5x even for HARD_GATE rules (safe production default)
  const dHalf = evaluateLiveContextGates({
    promotedRules: [promotedUsdHard, demotedSqueeze, cryptoOnlyFresh],
    mode: "auto_half_size",
    exchange: "alpaca",
    input: surgingInput,
  });
  assert.equal(dHalf.action, "HALF_SIZE");
  assert.equal(dHalf.sizeMultiplier, 0.5);
  assert.deepEqual(
    dHalf.triggeredRules.map((r) => r.id),
    ["USD_SURGE"], // demoted ATR_SQUEEZE & crypto-only FRESH_BOTTOM_LOW excluded
  );

  // auto_enforce -> BLOCK for HARD_GATE rule
  const dEnf = evaluateLiveContextGates({
    promotedRules: [promotedUsdHard],
    mode: "auto_enforce",
    exchange: "alpaca",
    input: surgingInput,
  });
  assert.equal(dEnf.action, "BLOCK");
  assert.equal(dEnf.sizeMultiplier, 0);
  assert.equal(dEnf.primaryBlockReason, "FILTER:CTX_USD_SURGE");

  // Auto-demotion: if rule status drops from PROMOTE_READY -> OBSERVE/REJECT, live gate allows full size
  const dDemoted = evaluateLiveContextGates({
    promotedRules: [demotedSqueeze],
    mode: "auto_enforce",
    exchange: "binance",
    input: surgingInput,
  });
  assert.equal(dDemoted.action, "ALLOW");
  assert.equal(dDemoted.sizeMultiplier, 1.0);
});

test("3. normalizeBlockReasonKey preserves Q6 FILTER:CTX_* and SHADOW_CTX_* buckets", () => {
  assert.equal(normalizeBlockReasonKey("FILTER:CTX_USD_SURGE"), "FILTER:CTX_USD_SURGE");
  assert.equal(normalizeBlockReasonKey("FILTER:CTX_ATR_SQUEEZE extra"), "FILTER:CTX_ATR_SQUEEZE");
  assert.equal(normalizeBlockReasonKey("SHADOW_CTX_USD_SURGE"), "SHADOW_CTX_USD_SURGE");
});
