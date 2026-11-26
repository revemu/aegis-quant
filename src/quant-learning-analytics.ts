import type { Candle } from "./market-structure.js";

export interface PerpBasisPoint {
  timeMs: number;
  basisBps: number;
}

export interface LongShortRatioPoint {
  timeMs: number;
  longShortRatio: number;
}

export interface FactorRankIcRow {
  factor: string;
  label: string;
  trainN: number;
  testN: number;
  fullIc: number;
  trainIc: number;
  testIc: number;
  verdict: "HIGH_SIGNAL" | "INVERTED" | "UNSTABLE" | "NOISE" | "LOW_N";
  hint: string;
}

export interface MfeAutoBeCandidate {
  triggerPct: number;
  label: string;
  trainAvgPnlPct: number;
  testTrades: number;
  testWinRatePct: number;
  testAvgPnlPct: number;
  testTotalPnlPct: number;
  testProfitFactor: number;
  testSlCount: number;
  testSavedSlCount: number;
}

export interface MfeGivebackAutoBeSummary {
  totalTrades: number;
  slTrades: number;
  slGivebackAt08Count: number;
  slGivebackAt10Count: number;
  slGivebackAt08Pct: number;
  slGivebackAt10Pct: number;
  avgMfeBeforeSlPct: number;
  bestTriggerPct: number | null;
  verdict: string;
  candidates: MfeAutoBeCandidate[];
}

export interface ContextGateMetrics {
  trades: number;
  winRatePct: number;
  avgPnlPct: number;
  totalPnlPct: number;
  profitFactor: number;
  maxDrawdownPct: number;
}

export interface ContextGateRuleResult {
  id: string;
  label: string;
  affectedTrain: number;
  affectedTest: number;
  hardGate: {
    trainDeltaPct: number;
    testDeltaPct: number;
    test: ContextGateMetrics;
  };
  halfSize: {
    trainDeltaPct: number;
    testDeltaPct: number;
    test: ContextGateMetrics;
  };
  recommendedAction: "HARD_GATE" | "HALF_SIZE" | "NONE";
  status: "PROMOTE_READY" | "OBSERVE" | "REJECT";
  reason: string;
}

export interface ContextGateOosSummary {
  baselineTest: ContextGateMetrics;
  promotableCount: number;
  verdict: string;
  rules: ContextGateRuleResult[];
}

export interface ConfidenceSizingAuditSummary {
  testTrades: number;
  equalWeight: ContextGateMetrics & { sharpeProxy: number; calmarProxy: number };
  confidenceScaled: ContextGateMetrics & { sharpeProxy: number; calmarProxy: number };
  deltaPf: number;
  deltaMaxDdPct: number;
  activeDownweightTags: string[];
  activeUpweightTags: string[];
  verdict: string;
}

export interface DecayedRegimeMemorySummary {
  halfLifeDays: number;
  fullWinRatePct: number;
  fullAvgPnlPct: number;
  fullProfitFactor: number;
  decayedWinRatePct: number;
  decayedAvgPnlPct: number;
  decayedProfitFactor: number;
  effectiveN: number;
  drift: "IMPROVING" | "STABLE" | "DETERIORATING";
}

export interface AdvancedQuantSetupRecord {
  entryTimeSec: number;
  symbol: string;
  pnlPct: number;
  score: number;
  buyVolRatio12: number;
  rawRs24h: number;
  betaAlpha24h: number | null;
  rsi1h: number;
  atrCompressionRatio: number | null;
  distFromBottomLowPct: number;
  upperWickRatioPct: number;
  breadthDelta4h: number;
  concurrentSignals: number;
  usdRet5dPct: number | null;
  sessionBucket: string;
  vwapDistPct: number | null;
  overnightGapPct: number | null;
  sectorName: string | null;
  sectorBreadthPct: number | null;
  takerBuyPct12: number | null;
  oiDelta4hPct: number | null;
  fundingRateBps: number | null;
  perpBasisBps: number | null;
  topLongShortRatio: number | null;
  macroEventDay: boolean;
  macroEventInWindow: boolean;
  hitSl: boolean;
  mfePct: number;
  autoBeReturns: Record<string, number>;
  autoBeHitSl: Record<string, boolean>;
}

export const AUTO_BE_TRIGGER_CANDIDATES = [0.6, 0.8, 1.0, 1.2, 1.5] as const;

export const US_EQUITY_SECTOR_MAP: Record<string, string> = {
  SPY: "INDEX_ETF",
  QQQ: "INDEX_ETF",
  DIA: "INDEX_ETF",
  IWM: "INDEX_ETF",
  SCHD: "INDEX_ETF",
  VYM: "INDEX_ETF",
  VTI: "INDEX_ETF",
  VOO: "INDEX_ETF",
  AAPL: "TECH",
  MSFT: "TECH",
  GOOGL: "TECH",
  AMZN: "TECH",
  META: "TECH",
  TSLA: "TECH",
  ORCL: "TECH",
  IBM: "TECH",
  CSCO: "TECH",
  NVDA: "SEMIS",
  AVGO: "SEMIS",
  AMD: "SEMIS",
  QCOM: "SEMIS",
  KO: "CONSUMER",
  PEP: "CONSUMER",
  MCD: "CONSUMER",
  WMT: "CONSUMER",
  COST: "CONSUMER",
  PG: "CONSUMER",
  HD: "CONSUMER",
  LOW: "CONSUMER",
  JNJ: "HEALTH",
  ABBV: "HEALTH",
  MRK: "HEALTH",
  PFE: "HEALTH",
  UNH: "HEALTH",
  JPM: "FINANCE",
  BAC: "FINANCE",
  WFC: "FINANCE",
  V: "FINANCE",
  MA: "FINANCE",
  CAT: "INDUSTRIAL",
  HON: "INDUSTRIAL",
  GE: "INDUSTRIAL",
  RTX: "INDUSTRIAL",
  CVX: "ENERGY",
  XOM: "ENERGY",
  O: "UTILITIES_REIT_TEL",
  T: "UTILITIES_REIT_TEL",
  VZ: "UTILITIES_REIT_TEL",
  NEE: "UTILITIES_REIT_TEL",
};

export function getEquitySector(symbol: string): string | null {
  const clean = (symbol || "").toUpperCase().trim();
  return US_EQUITY_SECTOR_MAP[clean] ?? null;
}

function clamp(v: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, v));
}

function roundTo(v: number, digits: number): number {
  const f = 10 ** digits;
  return Math.round(v * f) / f;
}

/**
 * Computes strictly causal rolling Beta (trailing `lookback` aligned 1-bar returns <= `coinC15[coinPrevIdx].time`)
 * and Beta-Adjusted 24h Alpha (`coinRet24h - beta * benchRet24h`).
 */
export function computeCausalBetaAlphaAt(
  coinC15: Candle[],
  coinPrevIdx: number,
  benchC15: Candle[],
  benchIdxByTime: Map<number, number>,
  coinRet24h: number,
  benchRet24h: number,
  lookback = 48,
): { beta: number; betaAlpha24h: number } | null {
  if (coinPrevIdx < 12 || coinPrevIdx >= coinC15.length || benchC15.length < 12) return null;
  if (!Number.isFinite(coinRet24h) || !Number.isFinite(benchRet24h)) return null;

  const coinRets: number[] = [];
  const benchRets: number[] = [];
  const start = Math.max(1, coinPrevIdx - lookback + 1);

  for (let i = start; i <= coinPrevIdx; i++) {
    const cur = coinC15[i];
    const prev = coinC15[i - 1];
    if (!cur || !prev || prev.close <= 0) continue;
    const bIdx = benchIdxByTime.get(cur.time);
    if (bIdx === undefined || bIdx < 1 || bIdx >= benchC15.length) continue;
    const bCur = benchC15[bIdx]!;
    const bPrev = benchC15[bIdx - 1]!;
    if (bPrev.close <= 0) continue;
    coinRets.push((cur.close - prev.close) / prev.close);
    benchRets.push((bCur.close - bPrev.close) / bPrev.close);
  }

  if (coinRets.length < 12) return null;
  const n = coinRets.length;
  let sumC = 0;
  let sumB = 0;
  for (let i = 0; i < n; i++) {
    sumC += coinRets[i]!;
    sumB += benchRets[i]!;
  }
  const meanC = sumC / n;
  const meanB = sumB / n;

  let cov = 0;
  let varB = 0;
  for (let i = 0; i < n; i++) {
    const dc = coinRets[i]! - meanC;
    const db = benchRets[i]! - meanB;
    cov += dc * db;
    varB += db * db;
  }
  if (varB <= 1e-12) return null;
  const beta = clamp(cov / varB, 0.2, 3.0);
  const betaAlpha24h = coinRet24h - beta * benchRet24h;
  if (!Number.isFinite(betaAlpha24h)) return null;
  return { beta: roundTo(beta, 3), betaAlpha24h: roundTo(betaAlpha24h, 3) };
}

function calcCausalAtrPctAt(c15: Candle[], endIdx: number, period: number): number | null {
  if (endIdx < period || endIdx >= c15.length) return null;
  const start = endIdx - period + 1;
  let trSum = 0;
  let count = 0;
  for (let i = Math.max(1, start); i <= endIdx; i++) {
    const cur = c15[i]!;
    const prev = c15[i - 1]!;
    const tr = Math.max(
      cur.high - cur.low,
      Math.abs(cur.high - prev.close),
      Math.abs(cur.low - prev.close),
    );
    if (Number.isFinite(tr) && tr >= 0) {
      trSum += tr;
      count++;
    }
  }
  const close = c15[endIdx]!.close;
  if (count === 0 || close <= 0) return null;
  return ((trSum / count) / close) * 100;
}

/**
 * Computes causal ATR compression ratio (`ATR(14)% / ATR(96)%`) strictly up to `endIdx`.
 * Values < 0.75 indicate volatility squeeze; > 1.25 indicate volatility expansion/exhaustion.
 */
export function computeAtrCompressionRatioAt(
  c15: Candle[],
  endIdx: number,
  fastPeriod = 14,
  slowPeriod = 96,
): number | null {
  if (endIdx < slowPeriod || endIdx >= c15.length) return null;
  const fastAtr = calcCausalAtrPctAt(c15, endIdx, fastPeriod);
  const slowAtr = calcCausalAtrPctAt(c15, endIdx, slowPeriod);
  if (fastAtr === null || slowAtr === null || slowAtr <= 1e-6) return null;
  const ratio = fastAtr / slowAtr;
  return Number.isFinite(ratio) ? roundTo(ratio, 3) : null;
}

/**
 * For US equities, finds the latest regular-session open gap (`c15[k].time - c15[k-1].time > 3600`)
 * at or before `idx` (within trailing 30 bars) and returns `((open - prevClose) / prevClose) * 100`.
 */
export function computeOvernightGapPctAt(c15: Candle[], idx: number): number | null {
  if (idx < 1 || idx >= c15.length) return null;
  const minK = Math.max(1, idx - 30);
  for (let k = idx; k >= minK; k--) {
    const cur = c15[k]!;
    const prev = c15[k - 1]!;
    if (cur.time - prev.time > 3600 && prev.close > 0 && cur.open > 0) {
      const gapPct = ((cur.open - prev.close) / prev.close) * 100;
      return Number.isFinite(gapPct) ? roundTo(gapPct, 3) : null;
    }
  }
  return null;
}

/**
 * Causal lookup for closed 1h Binance premium index klines (`p.timeMs + 3_600_000 <= barTimeSec * 1000`).
 */
export function lookupCausalPerpBasisBpsAt(
  series: PerpBasisPoint[] | undefined,
  barTimeSec: number,
): number | null {
  if (!series || series.length === 0) return null;
  const barMs = barTimeSec * 1000;
  let lo = 0;
  let hi = series.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (series[mid]!.timeMs + 3_600_000 <= barMs) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (best < 0) return null;
  if (barMs - (series[best]!.timeMs + 3_600_000) > 24 * 3_600_000) return null;
  return Number.isFinite(series[best]!.basisBps) ? roundTo(series[best]!.basisBps, 2) : null;
}

/**
 * Causal lookup for 1h Binance top-trader Long/Short account ratio (`p.timeMs <= barTimeSec * 1000`).
 */
export function lookupCausalLongShortRatioAt(
  series: LongShortRatioPoint[] | undefined,
  barTimeSec: number,
): number | null {
  if (!series || series.length === 0) return null;
  const barMs = barTimeSec * 1000;
  let lo = 0;
  let hi = series.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (series[mid]!.timeMs <= barMs) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (best < 0) return null;
  if (barMs - series[best]!.timeMs > 24 * 3_600_000) return null;
  return Number.isFinite(series[best]!.longShortRatio) ? roundTo(series[best]!.longShortRatio, 3) : null;
}

export function classifyBetaAlphaZone(alpha: number | null | undefined): string | null {
  if (alpha === null || alpha === undefined || !Number.isFinite(alpha)) return null;
  if (alpha < 0) return "NEG_ALPHA (<0%)";
  if (alpha < 2.0) return "NEUTRAL_ALPHA (0..+2%)";
  return "STRONG_ALPHA (>=+2%)";
}

export function classifyAtrSqueezeZone(ratio: number | null | undefined): string | null {
  if (ratio === null || ratio === undefined || !Number.isFinite(ratio)) return null;
  if (ratio < 0.75) return "SQUEEZE (<0.75x)";
  if (ratio <= 1.25) return "NORMAL (0.75-1.25x)";
  return "EXPANDING (>1.25x)";
}

export function classifyOvernightGapZone(gapPct: number | null | undefined): string | null {
  if (gapPct === null || gapPct === undefined || !Number.isFinite(gapPct)) return null;
  if (gapPct < -0.5) return "GAP_DOWN (<-0.5%)";
  if (gapPct <= 0.5) return "FLAT_OPEN (-0.5..+0.5%)";
  return "GAP_UP (>+0.5%)";
}

export function classifySectorBreadthZone(sectorBreadthPct: number | null | undefined): string | null {
  if (sectorBreadthPct === null || sectorBreadthPct === undefined || !Number.isFinite(sectorBreadthPct)) return null;
  if (sectorBreadthPct < 35) return "WEAK_SECTOR (<35%)";
  if (sectorBreadthPct < 65) return "MID_SECTOR (35-65%)";
  return "LEADING_SECTOR (>=65%)";
}

export function classifyPerpBasisZone(basisBps: number | null | undefined): string | null {
  if (basisBps === null || basisBps === undefined || !Number.isFinite(basisBps)) return null;
  if (basisBps < -5) return "DISCOUNT (<-5 bps)";
  if (basisBps <= 5) return "NEUTRAL (-5..+5 bps)";
  return "PREMIUM (>+5 bps)";
}

export function classifyLongShortZone(lsRatio: number | null | undefined): string | null {
  if (lsRatio === null || lsRatio === undefined || !Number.isFinite(lsRatio)) return null;
  if (lsRatio < 0.9) return "SHORT_CROWDED (<0.9)";
  if (lsRatio <= 1.5) return "BALANCED (0.9-1.5)";
  return "LONG_CROWDED (>1.5)";
}

/**
 * Causal bar-by-bar simulator for pre-TP1 Auto-Breakeven locking.
 * Checks SL against the active stop *before* promoting the stop on that bar's high.
 */
export function simulateAutoBeVariant(
  entryPrice: number,
  distTargetPct: number,
  slTriggerPct: number,
  futureBars: Array<{ high: number; low: number; close: number }>,
  costPct: number,
  exitOpts: {
    tp1FloorPct: number;
    runnerTrailPct: number;
    staleExitBars: number;
    staleExitMaxGainPct: number;
  },
  beTriggerPct: number,
): { finalRet: number; hitSl: boolean } {
  const minFloor = Math.max(0.5, exitOpts.tp1FloorPct);
  const tp1Pct =
    Number.isFinite(distTargetPct) && distTargetPct > 0
      ? Math.max(minFloor, Math.min(distTargetPct, 8.0))
      : Math.max(minFloor, 2.0);
  const runnerTrailPct = Math.max(0, exitOpts.runnerTrailPct);
  const staleExitBars = Math.max(0, Math.round(exitOpts.staleExitBars));
  const staleMaxGainPct = exitOpts.staleExitMaxGainPct;
  const beLockAfterTp1Pct = 0.25;
  const preTp1BeStopPct = 0.1;
  const trailFloorPct = 0.1;

  let tp1Hit = false;
  let preTp1BeLocked = false;
  let peakPctSinceTp1 = 0;
  let finalRet: number | null = null;
  let hitSl = false;

  for (let i = 0; i < futureBars.length; i++) {
    const b = futureBars[i]!;
    const barsHeld = i + 1;
    const lowPct = ((b.low - entryPrice) / entryPrice) * 100;
    const highPct = ((b.high - entryPrice) / entryPrice) * 100;
    const closePct = ((b.close - entryPrice) / entryPrice) * 100;

    if (!tp1Hit) {
      const activeStopPct = preTp1BeLocked ? preTp1BeStopPct : slTriggerPct;
      if (lowPct <= activeStopPct) {
        finalRet = activeStopPct;
        hitSl = !preTp1BeLocked;
        break;
      }
      if (highPct >= tp1Pct) {
        tp1Hit = true;
        peakPctSinceTp1 = Math.max(tp1Pct, closePct);
      } else {
        if (beTriggerPct > 0 && highPct >= beTriggerPct) {
          preTp1BeLocked = true;
        }
        if (staleExitBars > 0 && barsHeld >= staleExitBars && closePct < staleMaxGainPct) {
          finalRet = closePct;
          break;
        }
      }
    } else if (runnerTrailPct > 0) {
      const prevTrailStopPct = Math.max(trailFloorPct, peakPctSinceTp1 - runnerTrailPct);
      if (lowPct <= prevTrailStopPct) {
        finalRet = 0.5 * tp1Pct + 0.5 * prevTrailStopPct;
        break;
      }
      peakPctSinceTp1 = Math.max(peakPctSinceTp1, highPct);
      const nextTrailStopPct = Math.max(trailFloorPct, peakPctSinceTp1 - runnerTrailPct);
      if (closePct <= nextTrailStopPct) {
        finalRet = 0.5 * tp1Pct + 0.5 * nextTrailStopPct;
        break;
      }
    } else if (lowPct <= beLockAfterTp1Pct) {
      finalRet = 0.5 * tp1Pct + 0.5 * beLockAfterTp1Pct;
      break;
    }
  }

  if (finalRet === null) {
    const lastClose = futureBars[futureBars.length - 1]!.close;
    const closeRetPct = ((lastClose - entryPrice) / entryPrice) * 100;
    finalRet = tp1Hit ? 0.5 * tp1Pct + 0.5 * closeRetPct : closeRetPct;
  }

  return {
    finalRet: finalRet - Math.max(0, costPct),
    hitSl,
  };
}

function rankWithTies(values: number[]): number[] {
  const n = values.length;
  const indexed = values.map((v, i) => ({ v, i })).sort((a, b) => a.v - b.v);
  const ranks = new Array<number>(n);
  let i = 0;
  while (i < n) {
    let j = i;
    while (j + 1 < n && indexed[j + 1]!.v === indexed[i]!.v) j++;
    const avgRank = (i + j) / 2 + 1;
    for (let k = i; k <= j; k++) {
      ranks[indexed[k]!.i] = avgRank;
    }
    i = j + 1;
  }
  return ranks;
}

export function spearmanRankCorrelation(xs: number[], ys: number[]): number {
  const n = Math.min(xs.length, ys.length);
  if (n < 5) return 0;
  const rx = rankWithTies(xs.slice(0, n));
  const ry = rankWithTies(ys.slice(0, n));
  let sumX = 0;
  let sumY = 0;
  for (let i = 0; i < n; i++) {
    sumX += rx[i]!;
    sumY += ry[i]!;
  }
  const meanX = sumX / n;
  const meanY = sumY / n;
  let num = 0;
  let denX = 0;
  let denY = 0;
  for (let i = 0; i < n; i++) {
    const dx = rx[i]! - meanX;
    const dy = ry[i]! - meanY;
    num += dx * dy;
    denX += dx * dx;
    denY += dy * dy;
  }
  const den = Math.sqrt(denX * denY);
  if (den <= 1e-12) return 0;
  return clamp(num / den, -1, 1);
}

/**
 * Walk-forward Rank IC (Spearman correlation between causal entry feature and net PnL)
 * evaluated across Full, Train 70%, and OOS Test 30% splits.
 */
export function computeFactorRankIc(
  records: AdvancedQuantSetupRecord[],
  exchange: string,
): FactorRankIcRow[] {
  const sorted = records.slice().sort((a, b) => a.entryTimeSec - b.entryTimeSec);
  const splitIdx = Math.floor(sorted.length * 0.7);
  const train = sorted.slice(0, splitIdx);
  const test = sorted.slice(splitIdx);
  const isAlpaca = exchange.toLowerCase() === "alpaca";

  const factorDefs: Array<{
    factor: string;
    label: string;
    market: "all" | "alpaca" | "crypto";
    get: (r: AdvancedQuantSetupRecord) => number | null;
  }> = [
    { factor: "score", label: "Composite Score", market: "all", get: (r) => r.score },
    { factor: "buyVolRatio12", label: "Buy VolRatio (12b)", market: "all", get: (r) => r.buyVolRatio12 },
    { factor: "rawRs24h", label: "Raw RS 24h (%)", market: "all", get: (r) => r.rawRs24h },
    { factor: "betaAlpha24h", label: "Beta-Adjusted Alpha 24h", market: "all", get: (r) => r.betaAlpha24h },
    { factor: "rsi1h", label: "1h RSI (Closed)", market: "all", get: (r) => r.rsi1h },
    { factor: "atrCompressionRatio", label: "ATR Squeeze (14/96)", market: "all", get: (r) => r.atrCompressionRatio },
    { factor: "distFromBottomLowPct", label: "Dist From Bottom (%)", market: "all", get: (r) => r.distFromBottomLowPct },
    { factor: "upperWickRatioPct", label: "Upper Wick (%)", market: "all", get: (r) => r.upperWickRatioPct },
    { factor: "concurrentSignals", label: "Concurrent Signals", market: "all", get: (r) => r.concurrentSignals },
    { factor: "usdRet5dPct", label: "USD Proxy 5D (%)", market: "all", get: (r) => r.usdRet5dPct },
    { factor: "vwapDistPct", label: "Intraday VWAP Dist (%)", market: "alpaca", get: (r) => r.vwapDistPct },
    { factor: "overnightGapPct", label: "Overnight Gap (%)", market: "alpaca", get: (r) => r.overnightGapPct },
    { factor: "sectorBreadthPct", label: "Sector Breadth (%)", market: "alpaca", get: (r) => r.sectorBreadthPct },
    { factor: "takerBuyPct12", label: "Taker Buy % (12b)", market: "crypto", get: (r) => r.takerBuyPct12 },
    { factor: "oiDelta4hPct", label: "OI 4h Delta (%)", market: "crypto", get: (r) => r.oiDelta4hPct },
    { factor: "fundingRateBps", label: "Funding Rate (bps)", market: "crypto", get: (r) => r.fundingRateBps },
    { factor: "perpBasisBps", label: "Perp-Spot Basis (bps)", market: "crypto", get: (r) => r.perpBasisBps },
    { factor: "topLongShortRatio", label: "Top-Trader L/S Ratio", market: "crypto", get: (r) => r.topLongShortRatio },
  ];

  const calcIc = (subset: AdvancedQuantSetupRecord[], getFn: (r: AdvancedQuantSetupRecord) => number | null) => {
    const xs: number[] = [];
    const ys: number[] = [];
    for (const r of subset) {
      const val = getFn(r);
      if (val !== null && Number.isFinite(val) && Number.isFinite(r.pnlPct)) {
        xs.push(val);
        ys.push(r.pnlPct);
      }
    }
    return { n: xs.length, ic: xs.length >= 8 ? roundTo(spearmanRankCorrelation(xs, ys), 3) : 0 };
  };

  const rows: FactorRankIcRow[] = [];
  for (const def of factorDefs) {
    if (def.market === "alpaca" && !isAlpaca) continue;
    if (def.market === "crypto" && isAlpaca) continue;
    const full = calcIc(sorted, def.get);
    if (full.n < 8) continue;
    const tr = calcIc(train, def.get);
    const te = calcIc(test, def.get);

    let verdict: FactorRankIcRow["verdict"] = "NOISE";
    let hint = "ไม่มีอำนาจแยกผลใน OOS";
    if (tr.n < 15 || te.n < 10) {
      verdict = "LOW_N";
      hint = `รอตัวอย่างเพิ่ม (OOS N=${te.n})`;
    } else if (tr.ic >= 0.05 && te.ic >= 0.05) {
      verdict = "HIGH_SIGNAL";
      hint = "แปรผันตรงสม่ำเสมอทั้ง Train & OOS";
    } else if (tr.ic <= -0.05 && te.ic <= -0.05) {
      verdict = "INVERTED";
      hint = "แปรผกผันสม่ำเสมอ — ค่ายิ่งสูงยิ่งแย่ (ควรลดน้ำหนักหรือกลับทิศ)";
    } else if (Math.abs(tr.ic) >= 0.08 && tr.ic * te.ic <= 0) {
      verdict = "UNSTABLE";
      hint = "กลับทิศระหว่าง Train กับ OOS (เสี่ยง overfit)";
    }

    rows.push({
      factor: def.factor,
      label: def.label,
      trainN: tr.n,
      testN: te.n,
      fullIc: full.ic,
      trainIc: tr.ic,
      testIc: te.ic,
      verdict,
      hint,
    });
  }

  return rows.sort((a, b) => Math.abs(b.testIc) - Math.abs(a.testIc));
}

function computeTradeListMetrics(
  items: Array<{ pnlPct: number; weight?: number }>,
  maxSlots = 3,
): ContextGateMetrics {
  const capSlots = Math.max(1, Math.min(5, Math.round(maxSlots || 3)));
  let trades = 0;
  let wins = 0;
  let winSum = 0;
  let lossAbs = 0;
  let weightedSum = 0;
  let equity = 100;
  let peak = 100;
  let maxDd = 0;

  for (const item of items) {
    const w = item.weight ?? 1;
    if (w <= 0) continue;
    trades++;
    const ret = item.pnlPct * w;
    weightedSum += ret;
    if (ret > 0) {
      wins++;
      winSum += ret;
    } else {
      lossAbs += Math.abs(ret);
    }
    equity *= 1 + (ret / 100) / capSlots;
    if (equity > peak) peak = equity;
    const dd = peak > 0 ? ((peak - equity) / peak) * 100 : 0;
    if (dd > maxDd) maxDd = dd;
  }

  return {
    trades,
    winRatePct: trades > 0 ? roundTo((wins / trades) * 100, 1) : 0,
    avgPnlPct: trades > 0 ? roundTo(weightedSum / trades, 3) : 0,
    totalPnlPct: roundTo(weightedSum, 2),
    profitFactor: lossAbs > 0 ? roundTo(winSum / lossAbs, 2) : winSum > 0 ? 99 : 0,
    maxDrawdownPct: roundTo(maxDd, 2),
  };
}

/**
 * Evaluates how many SL trades gave back meaningful open profits (`>= +0.8%` or `>= +1.0%` MFE)
 * and runs a 70/30 chronological OOS sweep across Auto-BE triggers `[0.6, 0.8, 1.0, 1.2, 1.5]%`.
 */
export function evaluateMfeGivebackAndAutoBe(records: AdvancedQuantSetupRecord[]): MfeGivebackAutoBeSummary {
  const sorted = records.slice().sort((a, b) => a.entryTimeSec - b.entryTimeSec);
  const splitIdx = Math.floor(sorted.length * 0.7);
  const train = sorted.slice(0, splitIdx);
  const test = sorted.slice(splitIdx);

  const slRecords = sorted.filter((r) => r.hitSl);
  const slAt08 = slRecords.filter((r) => r.mfePct >= 0.8).length;
  const slAt10 = slRecords.filter((r) => r.mfePct >= 1.0).length;
  const avgMfeBeforeSl =
    slRecords.length > 0 ? roundTo(slRecords.reduce((s, r) => s + r.mfePct, 0) / slRecords.length, 2) : 0;

  const baseTestSlCount = test.filter((r) => r.hitSl).length;
  const baseTestAvg = test.length > 0 ? test.reduce((s, r) => s + r.pnlPct, 0) / test.length : 0;
  const baseTrainAvg = train.length > 0 ? train.reduce((s, r) => s + r.pnlPct, 0) / train.length : 0;

  const candidates: MfeAutoBeCandidate[] = AUTO_BE_TRIGGER_CANDIDATES.map((trig) => {
    const key = trig.toFixed(1);
    const trAvg =
      train.length > 0
        ? roundTo(train.reduce((s, r) => s + (r.autoBeReturns[key] ?? r.pnlPct), 0) / train.length, 3)
        : 0;
    const teItems = test.map((r) => ({ pnlPct: r.autoBeReturns[key] ?? r.pnlPct }));
    const teMetrics = computeTradeListMetrics(teItems);
    const teSl = test.filter((r) => r.autoBeHitSl[key] ?? r.hitSl).length;
    return {
      triggerPct: trig,
      label: `BE @ +${key}%`,
      trainAvgPnlPct: trAvg,
      testTrades: teMetrics.trades,
      testWinRatePct: teMetrics.winRatePct,
      testAvgPnlPct: teMetrics.avgPnlPct,
      testTotalPnlPct: teMetrics.totalPnlPct,
      testProfitFactor: teMetrics.profitFactor,
      testSlCount: teSl,
      testSavedSlCount: Math.max(0, baseTestSlCount - teSl),
    };
  });

  const winners = candidates
    .filter((c) => c.testTrades >= 10 && c.trainAvgPnlPct > baseTrainAvg + 0.02 && c.testAvgPnlPct > baseTestAvg + 0.02)
    .sort((a, b) => b.testAvgPnlPct - a.testAvgPnlPct || b.testProfitFactor - a.testProfitFactor);

  const best = winners[0] ?? null;
  const verdict =
    sorted.length < 20
      ? `รอตัวอย่างเพิ่ม (มี ${sorted.length} setups)`
      : best
        ? `Auto-BE @ +${best.triggerPct.toFixed(1)}% ผ่าน Train+OOS (OOS Avg ${best.testAvgPnlPct >= 0 ? "+" : ""}${best.testAvgPnlPct}% vs Base ${baseTestAvg >= 0 ? "+" : ""}${roundTo(baseTestAvg, 3)}%, เซฟ SL ${best.testSavedSlCount} ไม้)`
        : `ยังไม่แนะนำ Auto-BE (ไม่มีจุดล็อกที่ชนะ Base ทั้ง Train & OOS — มักโดนเขย่าหลุดก่อนถึง TP1)`;

  return {
    totalTrades: sorted.length,
    slTrades: slRecords.length,
    slGivebackAt08Count: slAt08,
    slGivebackAt10Count: slAt10,
    slGivebackAt08Pct: slRecords.length > 0 ? roundTo((slAt08 / slRecords.length) * 100, 1) : 0,
    slGivebackAt10Pct: slRecords.length > 0 ? roundTo((slAt10 / slRecords.length) * 100, 1) : 0,
    avgMfeBeforeSlPct: avgMfeBeforeSl,
    bestTriggerPct: best ? best.triggerPct : null,
    verdict,
    candidates,
  };
}

interface ContextGateRuleDef {
  id: string;
  label: string;
  market: "all" | "alpaca" | "crypto";
  matches: (r: AdvancedQuantSetupRecord) => boolean;
}

function getContextGateRuleDefs(): ContextGateRuleDef[] {
  return [
    {
      id: "USD_SURGE",
      label: "USD 5D Surge (>+0.5%)",
      market: "all",
      matches: (r) => r.usdRet5dPct !== null && r.usdRet5dPct > 0.5,
    },
    {
      id: "US_OVERLAP_14_21",
      label: "US Overlap Session (14-21 UTC)",
      market: "crypto",
      matches: (r) => r.sessionBucket === "US_OVERLAP_14_21",
    },
    {
      id: "US_OPEN_1H",
      label: "US Open 1st Hour",
      market: "alpaca",
      matches: (r) => r.sessionBucket === "US_OPEN_1H",
    },
    {
      id: "ISOLATED_CONCURRENCY",
      label: "Isolated Setup (1-2 concurrent)",
      market: "all",
      matches: (r) => r.concurrentSignals <= 2,
    },
    {
      id: "OI_FLUSH",
      label: "OI 4h Flush (<-1%)",
      market: "crypto",
      matches: (r) => r.oiDelta4hPct !== null && r.oiDelta4hPct < -1,
    },
    {
      id: "SELLER_DOM",
      label: "Seller Dominant Flow (<48.5%)",
      market: "crypto",
      matches: (r) => r.takerBuyPct12 !== null && r.takerBuyPct12 < 48.5,
    },
    {
      id: "BELOW_VWAP",
      label: "Below Intraday VWAP (<-0.5%)",
      market: "alpaca",
      matches: (r) => r.vwapDistPct !== null && r.vwapDistPct < -0.5,
    },
    {
      id: "MACRO_EVENT_DAY",
      label: "Macro Event Day",
      market: "all",
      matches: (r) => r.macroEventDay || r.macroEventInWindow,
    },
    {
      id: "NEGATIVE_ALPHA",
      label: "Negative Beta-Alpha (<0%)",
      market: "all",
      matches: (r) => r.betaAlpha24h !== null && r.betaAlpha24h < 0,
    },
    {
      id: "VOL_EXHAUSTION",
      label: "Vol Expansion (>1.25x ATR96)",
      market: "all",
      matches: (r) => r.atrCompressionRatio !== null && r.atrCompressionRatio > 1.25,
    },
    {
      id: "CROWDED_BASIS",
      label: "Crowded Perp Premium (>+5 bps)",
      market: "crypto",
      matches: (r) => r.perpBasisBps !== null && r.perpBasisBps > 5,
    },
    {
      id: "OVERNIGHT_GAP_UP",
      label: "Overnight Gap Up (>+0.5%)",
      market: "alpaca",
      matches: (r) => r.overnightGapPct !== null && r.overnightGapPct > 0.5,
    },
  ];
}

/**
 * Walk-forward 70/30 OOS pipeline testing each context rule under both Hard Gate (0x) and Half-Size (0.5x).
 */
export function evaluateContextGatesOos(
  records: AdvancedQuantSetupRecord[],
  exchange: string,
  maxSlots = 3,
): ContextGateOosSummary {
  const sorted = records.slice().sort((a, b) => a.entryTimeSec - b.entryTimeSec);
  const splitIdx = Math.floor(sorted.length * 0.7);
  const train = sorted.slice(0, splitIdx);
  const test = sorted.slice(splitIdx);
  const isAlpaca = exchange.toLowerCase() === "alpaca";

  const baseTrain = computeTradeListMetrics(train, maxSlots);
  const baseTest = computeTradeListMetrics(test, maxSlots);

  const rules: ContextGateRuleResult[] = [];
  for (const def of getContextGateRuleDefs()) {
    if (def.market === "alpaca" && !isAlpaca) continue;
    if (def.market === "crypto" && isAlpaca) continue;

    const affectedTrain = train.filter(def.matches).length;
    const affectedTest = test.filter(def.matches).length;
    if (affectedTrain === 0 && affectedTest === 0) continue;

    const hardTrain = computeTradeListMetrics(train.filter((r) => !def.matches(r)), maxSlots);
    const hardTest = computeTradeListMetrics(test.filter((r) => !def.matches(r)), maxSlots);
    const halfTrain = computeTradeListMetrics(
      train.map((r) => ({ pnlPct: r.pnlPct, weight: def.matches(r) ? 0.5 : 1 })),
      maxSlots,
    );
    const halfTest = computeTradeListMetrics(
      test.map((r) => ({ pnlPct: r.pnlPct, weight: def.matches(r) ? 0.5 : 1 })),
      maxSlots,
    );

    const hardTrainDelta = roundTo(hardTrain.avgPnlPct - baseTrain.avgPnlPct, 3);
    const hardTestDelta = roundTo(hardTest.avgPnlPct - baseTest.avgPnlPct, 3);
    const halfTrainDelta = roundTo(halfTrain.avgPnlPct - baseTrain.avgPnlPct, 3);
    const halfTestDelta = roundTo(halfTest.avgPnlPct - baseTest.avgPnlPct, 3);

    const hardOk =
      affectedTest >= 6 &&
      hardTest.trades >= 15 &&
      hardTrainDelta > 0 &&
      hardTestDelta >= 0.05 &&
      hardTest.profitFactor >= baseTest.profitFactor;
    const halfOk =
      affectedTest >= 6 &&
      halfTest.trades >= 15 &&
      halfTrainDelta > 0 &&
      halfTestDelta >= 0.03 &&
      halfTest.profitFactor >= baseTest.profitFactor;

    let recommendedAction: ContextGateRuleResult["recommendedAction"] = "NONE";
    let status: ContextGateRuleResult["status"] = "REJECT";
    let reason = "ไม่ชนะ Baseline ทั้ง Train & OOS";

    if (hardOk || halfOk) {
      status = "PROMOTE_READY";
      if (hardOk && (!halfOk || hardTestDelta >= halfTestDelta + 0.03)) {
        recommendedAction = "HARD_GATE";
        reason = `Hard Gate ชนะ Train (+${hardTrainDelta}pp) และ OOS (+${hardTestDelta}pp, PF ${hardTest.profitFactor})`;
      } else {
        recommendedAction = "HALF_SIZE";
        reason = `Half-Size 0.5x ชนะ Train (+${halfTrainDelta}pp) และ OOS (+${halfTestDelta}pp, PF ${halfTest.profitFactor})`;
      }
    } else if (affectedTest < 6 || test.length < 15) {
      status = "OBSERVE";
      reason = `รอตัวอย่าง OOS เพิ่ม (affected OOS=${affectedTest}/6)`;
    } else if (hardTrainDelta > 0 || halfTrainDelta > 0) {
      status = hardTestDelta > 0 || halfTestDelta > 0 ? "OBSERVE" : "REJECT";
      reason = `สัญญาณ OOS ยังอ่อน (Hard ${hardTestDelta >= 0 ? "+" : ""}${hardTestDelta}pp, Half ${halfTestDelta >= 0 ? "+" : ""}${halfTestDelta}pp)`;
    }

    rules.push({
      id: def.id,
      label: def.label,
      affectedTrain,
      affectedTest,
      hardGate: { trainDeltaPct: hardTrainDelta, testDeltaPct: hardTestDelta, test: hardTest },
      halfSize: { trainDeltaPct: halfTrainDelta, testDeltaPct: halfTestDelta, test: halfTest },
      recommendedAction,
      status,
      reason,
    });
  }

  rules.sort((a, b) => {
    const rank = (s: ContextGateRuleResult["status"]) => (s === "PROMOTE_READY" ? 2 : s === "OBSERVE" ? 1 : 0);
    return (
      rank(b.status) - rank(a.status) ||
      Math.max(b.hardGate.testDeltaPct, b.halfSize.testDeltaPct) -
        Math.max(a.hardGate.testDeltaPct, a.halfSize.testDeltaPct)
    );
  });

  const promotableCount = rules.filter((r) => r.status === "PROMOTE_READY").length;
  const verdict =
    promotableCount > 0
      ? `พบ ${promotableCount} Context Gate ผ่านเกณฑ์ Train+OOS พร้อมพิจารณาใช้งาน (${rules
          .filter((r) => r.status === "PROMOTE_READY")
          .slice(0, 2)
          .map((r) => `${r.id}:${r.recommendedAction}`)
          .join(", ")})`
      : test.length < 15
        ? `รอตัวอย่าง OOS เพิ่ม (OOS=${test.length} setups)`
        : "ยังไม่มี Context Gate ที่ผ่านเกณฑ์ Promotion ทั้ง Train & OOS";

  return {
    baselineTest: baseTest,
    promotableCount,
    verdict,
    rules,
  };
}

/**
 * Strictly Zero-Lookahead Confidence Sizing Audit:
 * Learns downweight (0.5x) and upweight (1.25x) tags ONLY on chronological Train 70%,
 * then evaluates Equal-Weight (1.0x) vs Confidence-Scaled on untouched OOS Test 30%.
 */
export function evaluateConfidenceSizingOos(
  records: AdvancedQuantSetupRecord[],
  exchange: string,
  maxSlots = 3,
): ConfidenceSizingAuditSummary {
  const sorted = records.slice().sort((a, b) => a.entryTimeSec - b.entryTimeSec);
  const splitIdx = Math.floor(sorted.length * 0.7);
  const train = sorted.slice(0, splitIdx);
  const test = sorted.slice(splitIdx);
  const isAlpaca = exchange.toLowerCase() === "alpaca";

  const tagDefs: Array<{ tag: string; market: "all" | "alpaca" | "crypto"; matches: (r: AdvancedQuantSetupRecord) => boolean }> = [
    { tag: "USD_SURGE", market: "all", matches: (r) => r.usdRet5dPct !== null && r.usdRet5dPct > 0.5 },
    { tag: "ISOLATED_CONC", market: "all", matches: (r) => r.concurrentSignals <= 2 },
    { tag: "SURGE_CONC", market: "all", matches: (r) => r.concurrentSignals >= 6 },
    { tag: "ATR_SQUEEZE", market: "all", matches: (r) => r.atrCompressionRatio !== null && r.atrCompressionRatio < 0.75 },
    { tag: "STRONG_ALPHA", market: "all", matches: (r) => r.betaAlpha24h !== null && r.betaAlpha24h >= 2.0 },
    { tag: "ABOVE_VWAP", market: "alpaca", matches: (r) => r.vwapDistPct !== null && r.vwapDistPct > 0.5 },
    { tag: "US_POWER_HOUR", market: "alpaca", matches: (r) => r.sessionBucket === "US_POWER_HOUR" },
    { tag: "US_OVERLAP", market: "crypto", matches: (r) => r.sessionBucket === "US_OVERLAP_14_21" },
    { tag: "OI_BUILD", market: "crypto", matches: (r) => r.oiDelta4hPct !== null && r.oiDelta4hPct > 1 },
    { tag: "OI_FLUSH", market: "crypto", matches: (r) => r.oiDelta4hPct !== null && r.oiDelta4hPct < -1 },
    { tag: "BUYER_DOM", market: "crypto", matches: (r) => r.takerBuyPct12 !== null && r.takerBuyPct12 >= 51.5 },
  ];

  const activeDownweightTags: string[] = [];
  const activeUpweightTags: string[] = [];

  for (const td of tagDefs) {
    if (td.market === "alpaca" && !isAlpaca) continue;
    if (td.market === "crypto" && isAlpaca) continue;
    const matched = train.filter(td.matches);
    if (matched.length < 8) continue;
    const avg = matched.reduce((s, r) => s + r.pnlPct, 0) / matched.length;
    if (avg <= -0.15) activeDownweightTags.push(td.tag);
    else if (avg >= 0.25) activeUpweightTags.push(td.tag);
  }

  const downSet = new Set(activeDownweightTags);
  const upSet = new Set(activeUpweightTags);

  const computeTestStats = (scaled: boolean) => {
    const weighted = test.map((r) => {
      if (!scaled) return { pnlPct: r.pnlPct, weight: 1.0 };
      let mult = 1.0;
      for (const td of tagDefs) {
        if (!td.matches(r)) continue;
        if (downSet.has(td.tag)) mult *= 0.5;
        else if (upSet.has(td.tag)) mult *= 1.25;
      }
      return { pnlPct: r.pnlPct, weight: clamp(mult, 0.5, 1.25) };
    });

    const m = computeTradeListMetrics(weighted, maxSlots);
    const rets = weighted.map((x) => x.pnlPct * x.weight);
    const mean = rets.length > 0 ? rets.reduce((s, v) => s + v, 0) / rets.length : 0;
    const variance =
      rets.length > 1 ? rets.reduce((s, v) => s + (v - mean) ** 2, 0) / (rets.length - 1) : 0;
    const std = Math.sqrt(Math.max(0, variance));
    const sharpeProxy = std > 1e-6 ? roundTo((mean / std) * Math.sqrt(Math.max(1, rets.length)), 2) : 0;
    const calmarProxy = m.maxDrawdownPct > 0.05 ? roundTo(m.totalPnlPct / m.maxDrawdownPct, 2) : m.totalPnlPct > 0 ? 99 : 0;
    return { ...m, sharpeProxy, calmarProxy };
  };

  const equalWeight = computeTestStats(false);
  const confidenceScaled = computeTestStats(true);
  const deltaPf = roundTo(confidenceScaled.profitFactor - equalWeight.profitFactor, 2);
  const deltaMaxDdPct = roundTo(confidenceScaled.maxDrawdownPct - equalWeight.maxDrawdownPct, 2);

  const verdict =
    test.length < 10
      ? `รอตัวอย่าง OOS เพิ่ม (OOS=${test.length})`
      : activeDownweightTags.length === 0 && activeUpweightTags.length === 0
        ? "Train ยังไม่พบ Tag ที่มีน้ำหนักพอจะปรับสเกล (ใช้ Equal-Weight 1.0x)"
        : deltaPf >= 0 && deltaMaxDdPct <= 0
          ? `Confidence Sizing ดีขึ้นใน OOS: PF ${equalWeight.profitFactor} -> ${confidenceScaled.profitFactor}, MaxDD ${equalWeight.maxDrawdownPct}% -> ${confidenceScaled.maxDrawdownPct}%`
          : `Confidence Sizing OOS: PF ${deltaPf >= 0 ? "+" : ""}${deltaPf}, MaxDD ${deltaMaxDdPct >= 0 ? "+" : ""}${deltaMaxDdPct}% (คง Equal-Weight ปลอดภัยกว่า)`;

  return {
    testTrades: test.length,
    equalWeight,
    confidenceScaled,
    deltaPf,
    deltaMaxDdPct,
    activeDownweightTags,
    activeUpweightTags,
    verdict,
  };
}

/**
 * Exponentially Decayed Regime Memory (default Half-Life = 14 days).
 * Surfaces whether recent regime expectancy is improving or deteriorating vs full-window baseline.
 */
export function computeDecayedRegimeMemory(
  records: AdvancedQuantSetupRecord[],
  halfLifeDays = 14,
): DecayedRegimeMemorySummary {
  if (records.length === 0) {
    return {
      halfLifeDays,
      fullWinRatePct: 0,
      fullAvgPnlPct: 0,
      fullProfitFactor: 0,
      decayedWinRatePct: 0,
      decayedAvgPnlPct: 0,
      decayedProfitFactor: 0,
      effectiveN: 0,
      drift: "STABLE",
    };
  }

  let latestSec = 0;
  for (const r of records) {
    if (r.entryTimeSec > latestSec) latestSec = r.entryTimeSec;
  }

  let fullWins = 0;
  let fullPnl = 0;
  let fullWinSum = 0;
  let fullLossAbs = 0;

  let wSum = 0;
  let wSqSum = 0;
  let wWins = 0;
  let wPnl = 0;
  let wWinSum = 0;
  let wLossAbs = 0;

  const hl = Math.max(1, halfLifeDays);
  for (const r of records) {
    if (r.pnlPct > 0) {
      fullWins++;
      fullWinSum += r.pnlPct;
    } else {
      fullLossAbs += Math.abs(r.pnlPct);
    }
    fullPnl += r.pnlPct;

    const ageDays = Math.max(0, (latestSec - r.entryTimeSec) / 86400);
    const w = Math.pow(0.5, ageDays / hl);
    wSum += w;
    wSqSum += w * w;
    wPnl += w * r.pnlPct;
    if (r.pnlPct > 0) {
      wWins += w;
      wWinSum += w * r.pnlPct;
    } else {
      wLossAbs += w * Math.abs(r.pnlPct);
    }
  }

  const n = records.length;
  const fullAvg = fullPnl / n;
  const decayedAvg = wSum > 0 ? wPnl / wSum : fullAvg;
  const effectiveN = wSqSum > 0 ? roundTo((wSum * wSum) / wSqSum, 1) : n;
  const delta = decayedAvg - fullAvg;
  const drift: DecayedRegimeMemorySummary["drift"] =
    delta >= 0.1 ? "IMPROVING" : delta <= -0.1 ? "DETERIORATING" : "STABLE";

  return {
    halfLifeDays: hl,
    fullWinRatePct: roundTo((fullWins / n) * 100, 1),
    fullAvgPnlPct: roundTo(fullAvg, 3),
    fullProfitFactor: fullLossAbs > 0 ? roundTo(fullWinSum / fullLossAbs, 2) : fullWinSum > 0 ? 99 : 0,
    decayedWinRatePct: wSum > 0 ? roundTo((wWins / wSum) * 100, 1) : 0,
    decayedAvgPnlPct: roundTo(decayedAvg, 3),
    decayedProfitFactor: wLossAbs > 0 ? roundTo(wWinSum / wLossAbs, 2) : wWinSum > 0 ? 99 : 0,
    effectiveN,
    drift,
  };
}
