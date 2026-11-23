/**
 * PostgreSQL-backed Survival-First Learning Engine (Levels 1, 2 & 3 + Shadow Walk-Forward)
 *
 * Level 1: Records full entry indicator snapshots, MAE/MFE trade outcomes, and
 *          expanded near-miss / shadow blocked signals into `trade_learning_journal`.
 * Level 2: Resolves 4h counterfactual outcomes against `market_candles` (with US RTH filtering
 *          for Alpaca) and runs rolling historical Shadow Walk-Forward learning across
 *          cached `market_candles` so low-frequency markets (e.g. US equities) learn immediately.
 * Level 3: Applies bounded self-tuning & 4-tier Adaptive Market-Survival Guard:
 *          - Survival Postures (NORMAL / CAUTIOUS / DEFENSIVE / CAPITAL_PRESERVATION)
 *          - Combined Real + Shadow Win-MAE P80 Disaster SL calibration ([2.5%, 5.0%], ±0.25%/wk)
 *          - Per-symbol expectancy scoring & pre-emptive whipsaw avoidance + 48h SL cooldowns.
 */

import type { Config } from "./config.js";
import type { BotDatabase } from "./db.js";
import { detectRecentShock24h, getMacroEventContext } from "./event-calendar.js";
import { isUsRegularTradingHoursBar } from "./exchange/alpaca/feed.js";
import type { Candle } from "./market-structure.js";
import type {
  LearningAdjustmentRecord,
  LearningJournalRecord,
  PgStore,
  SymbolLearningStateRecord,
} from "./pg-store.js";
import { calcMACD, calcRSI } from "./strategy/ma-fan/exhaustion.js";
import { calcEMA } from "./strategy/ma-fan/ribbon.js";
import type { PortfolioMacroState, PortfolioRadarCandidate } from "./strategy/ma-fan-portfolio/types.js";

const COOLDOWN_MS = 48 * 3_600_000;
const BLOCKED_EVAL_HORIZON_MS = 4 * 3_600_000;
const PARAM_NUDGE_INTERVAL_MS = 7 * 86_400_000;
const SHADOW_REFRESH_INTERVAL_MS = 6 * 3_600_000;
const POSTURE_LOG_COOLDOWN_MS = 2 * 3_600_000;
const MIN_CLOSED_TRADES_FOR_SL_NUDGE = 20;
const MIN_WIN_TRADES_FOR_SL_NUDGE = 8;

export interface ShadowSymbolProfile {
  symbol: string;
  trades: number;
  wins: number;
  losses: number;
  winRatePct: number;
  avgPnlPct: number;
  profitFactor: number;
  avgMaePct: number;
  avgMfePct: number;
  scoreAdj: number;
  avoidInDefensive: boolean;
  lastBarTimeSec: number;
}

export interface ShadowLearningSummary {
  updatedAt: number;
  exchange: string;
  symbolsAnalyzed: number;
  barsPerSymbolAvg: number;
  totalSetups: number;
  wins: number;
  losses: number;
  winRatePct: number;
  avgPnlPct: number;
  profitFactor: number;
  winMaeP80Pct: number | null;
  recommendedSlPct: number | null;
  bearMacroSetups: number;
  bearMacroWinRatePct: number;
  bearMacroAvgPnlPct: number;
  lowBreadthSetups: number;
  lowBreadthWinRatePct: number;
  lowBreadthAvgPnlPct: number;
  topProvenSymbols: ShadowSymbolProfile[];
  riskyWhipsawSymbols: ShadowSymbolProfile[];
  bySymbol: Record<string, ShadowSymbolProfile>;
  featureBucketEv?: Record<string, { setups: number; winRatePct: number; avgPnlPct: number }>;
  regimeFeatureBucketEv?: Record<string, { setups: number; winRatePct: number; avgPnlPct: number }>;
  sessionStats?: Record<string, { setups: number; winRatePct: number; avgPnlPct: number }>;
  openingSessionStats?: { setups: number; winRatePct: number; avgPnlPct: number };
  portfolioSim?: ShadowPortfolioReplaySummary | null;
  breadthSitOutAnalysis?: BreadthSitOutAnalysis | null;
  observeFactors?: ShadowObserveFactorsSummary | null;
  tuning?: TuningAnalysis | null;
}

export interface BucketMetric {
  setups: number;
  winRatePct: number;
  avgPnlPct: number;
}

export interface TimeDecayCheckpoint {
  bars: number;
  label: string;
  stillOpenCount: number;
  staleCount: number;
  staleEventuallyWinPct: number;
  staleAvgFinalPnlPct: number;
  healthyEventuallyWinPct: number;
  healthyAvgFinalPnlPct: number;
}

export interface LatestMacroRegimeSnapshot {
  benchmarkSymbol: string;
  asOfTimeSec: number | null;
  mayerMultiple: number | null;
  mayerZone: string | null;
  macroRiskIndex: number | null;
  macroRiskZone: string | null;
  drawdown180dPct: number | null;
  dailyAtr14Pct: number | null;
  usdProxySymbol: string;
  usdRet5dPct: number | null;
  usdZone: string | null;
}

export interface ShadowObserveFactorsSummary {
  scoreBuckets: Record<string, BucketMetric>;
  overextensionBuckets: Record<string, BucketMetric>;
  upperWickBuckets: Record<string, BucketMetric>;
  timeDecay: TimeDecayCheckpoint[];
  breadthVelocityBuckets: Record<string, BucketMetric>;
  benchSlope4hBuckets: Record<string, BucketMetric>;
  concurrencyBuckets: Record<string, BucketMetric>;
  eventBuckets: Record<string, BucketMetric>;
  mayerBuckets?: Record<string, BucketMetric>;
  macroRiskBuckets?: Record<string, BucketMetric>;
  usdStrengthBuckets?: Record<string, BucketMetric>;
  latestMacroRegime?: LatestMacroRegimeSnapshot | null;
}

export interface ShadowPortfolioReplaySummary {
  maxSlots: number;
  trades: number;
  wins: number;
  losses: number;
  winRatePct: number;
  avgPnlPct: number;
  totalPnlPct: number;
  profitFactor: number;
  maxDrawdownPct: number;
  skippedSlotFull: number;
}

export interface BreadthSitOutModeResult {
  mode: string;
  label: string;
  trainTrades: number;
  testTrades: number;
  testWinRatePct: number;
  testAvgPnlPct: number;
  testTotalPnlPct: number;
  testProfitFactor: number;
}

export interface BreadthSitOutAnalysis {
  currentMode: string;
  bestMode: string;
  bestLabel: string;
  testSetupsTotal: number;
  verdict: string;
  modes: BreadthSitOutModeResult[];
}

export interface PostExitAuditSummary {
  evaluated: number;
  stopHuntCount: number;
  stopHuntRatePct: number;
  earlyTpCount: number;
  earlyTpRatePct: number;
  avgPostSlReboundPct: number | null;
  avgPostTpExtraRunPct: number | null;
}

export type SurvivalPostureLevel = "NORMAL" | "CAUTIOUS" | "DEFENSIVE" | "CAPITAL_PRESERVATION";

export interface SurvivalGuardDecision {
  posture: SurvivalPostureLevel;
  enforced: boolean;
  configuredMaxSlots: number;
  effectiveMaxSlots: number;
  positionSizeScale: number;
  minRelStrength24h: number;
  minRsi1h: number;
  minBuyVolRatio12: number;
  blockRiskySymbols: boolean;
  reasons: string[];
  metrics: {
    breadthPct: number;
    macroPass4h: boolean;
    macroPass1d: boolean;
    recentConsecutiveLosses: number;
    rollingDrawdownPct: number;
    bearMacroAvgPnlPct: number | null;
    recentBlockedLoseRatePct: number | null;
    recentBlockedAvgPnlPct: number | null;
  };
}

export interface ShadowWalkForwardContext {
  tpMode?: string;
  pullbackMode?: "EMA8_HOLD" | "EMA4_TOUCH";
  minVolRatio?: number;
  minRelStrength?: number;
  minRsi1h?: number;
  maxSlots?: number;
  sitOutMode?: string;
  exchange: string;
  symbols: string[];
  benchmarkSymbol: string;
  slPct: number;
  fallbackSnapshots?: Map<string, { c15: Candle[]; c1h: Candle[] }>;
  benchmarkFallback15m?: Candle[];
  benchmarkFallback1d?: Candle[];
  usdProxySymbol?: string;
  usdProxyFallback1d?: Candle[];
  force?: boolean;
}

const POSTURE_NUMERIC: Record<SurvivalPostureLevel, number> = {
  NORMAL: 0,
  CAUTIOUS: 1,
  DEFENSIVE: 2,
  CAPITAL_PRESERVATION: 3,
};

const NUMERIC_TO_POSTURE: Record<number, SurvivalPostureLevel> = {
  0: "NORMAL",
  1: "CAUTIOUS",
  2: "DEFENSIVE",
  3: "CAPITAL_PRESERVATION",
};

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * p)));
  return sorted[idx]!;
}

function clamp(v: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, v));
}

function normalizeTsMs(ts: number): number {
  if (!Number.isFinite(ts) || ts <= 0) return 0;
  return ts < 1e12 ? Math.floor(ts * 1000) : Math.floor(ts);
}

/**
 * Detects whether `curBar` opened or jumped on an abnormal gap relative to `prevBar`
 * (US equities overnight/open gaps vs parabolic blow-off bars on 24/7 crypto).
 */
export function isGapInducedBar(prevBar: Candle | undefined, curBar: Candle, exchange = "alpaca"): boolean {
  if (!prevBar || prevBar.close <= 0) return false;
  const gapOpenPct = ((curBar.open - prevBar.close) / prevBar.close) * 100;
  const jumpClosePct = ((curBar.close - prevBar.close) / prevBar.close) * 100;
  const isOvernightSession = curBar.time - prevBar.time > 3600;
  if ((exchange || "alpaca").toLowerCase() !== "alpaca") {
    if (isOvernightSession && (gapOpenPct >= 0.8 || jumpClosePct >= 1.5)) return true;
    return gapOpenPct >= 0.8 || jumpClosePct >= 2.8;
  }
  if (isOvernightSession && (gapOpenPct >= 0.35 || jumpClosePct >= 0.65)) return true;
  return gapOpenPct >= 0.5 || jumpClosePct >= 0.9;
}

/**
 * Quant Q1: Rolling Pearson correlation of 1-bar returns across aligned timestamps
 * between candidate `c15A[idxA]` and held slot `c15B[idxB]`.
 */
export function calcRollingReturnCorrelation(
  c15A: Array<{ time: number; close: number }>,
  idxA: number,
  c15B: Array<{ time: number; close: number }>,
  idxB: number,
  windowBars = 48,
): number {
  if (!c15A || !c15B || idxA < windowBars || idxB < windowBars) return 0;
  const bTimeToIdx = new Map<number, number>();
  const startB = Math.max(1, idxB - windowBars * 2);
  for (let j = startB; j <= idxB && j < c15B.length; j++) {
    bTimeToIdx.set(c15B[j]!.time, j);
  }
  const xs: number[] = [];
  const ys: number[] = [];
  for (let k = 0; k < windowBars; k++) {
    const iA = idxA - k;
    if (iA <= 0 || iA >= c15A.length) continue;
    const curA = c15A[iA]!;
    const prevA = c15A[iA - 1]!;
    const jB = bTimeToIdx.get(curA.time);
    if (jB === undefined || jB <= 0 || prevA.close <= 0) continue;
    const curB = c15B[jB]!;
    const prevB = c15B[jB - 1]!;
    if (prevB.close <= 0) continue;
    xs.push((curA.close - prevA.close) / prevA.close);
    ys.push((curB.close - prevB.close) / prevB.close);
  }
  if (xs.length < 15) return 0;
  const n = xs.length;
  const meanX = xs.reduce((a, b) => a + b, 0) / n;
  const meanY = ys.reduce((a, b) => a + b, 0) / n;
  let cov = 0;
  let varX = 0;
  let varY = 0;
  for (let i = 0; i < n; i++) {
    const dx = xs[i]! - meanX;
    const dy = ys[i]! - meanY;
    cov += dx * dy;
    varX += dx * dx;
    varY += dy * dy;
  }
  if (varX <= 1e-12 || varY <= 1e-12) return 0;
  return clamp(cov / Math.sqrt(varX * varY), -1, 1);
}

/**
 * Quant Q4: 14-bar Wilder True Range percentage up to `upToIdx`.
 */
export function calcCandleAtrPct14(
  c15: Array<{ high: number; low: number; close: number }>,
  upToIdx?: number,
): number {
  if (!c15 || c15.length < 15) return 0.35;
  const end = Math.min(c15.length - 1, upToIdx ?? c15.length - 1);
  if (end < 14) return 0.35;
  const warmStart = Math.max(1, end - 70);
  if (end - warmStart + 1 < 14) return 0.35;

  let trSum = 0;
  let atrPct = 0.35;
  let count = 0;
  for (let i = warmStart; i <= end; i++) {
    const c = c15[i]!;
    const p = c15[i - 1]!;
    const tr = Math.max(c.high - c.low, Math.abs(c.high - p.close), Math.abs(c.low - p.close));
    const trPct = c.close > 0 ? (tr / c.close) * 100 : 0;
    count++;
    if (count <= 14) {
      trSum += trPct;
      if (count === 14) atrPct = trSum / 14;
    } else {
      atrPct = (atrPct * 13 + trPct) / 14;
    }
  }
  return atrPct > 0 ? atrPct : 0.35;
}

const nyOpenFmt = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  weekday: "short",
  hour: "numeric",
  minute: "numeric",
  hourCycle: "h23",
});
const nyOpenBucketCache = new Map<number, { isWeekday: boolean; baseMin: number }>();

/**
 * Quant Q3: Detects volatile session-opening windows (first 45m of US equities regular session,
 * or DST-aware 09:30-10:15 ET US cash open volatility spikes for 24/7 crypto).
 */
export function isOpeningSession45mBar(
  c15: Array<{ time: number; high?: number; low?: number; close?: number }>,
  idx: number,
  exchangeName = "alpaca",
  requireCryptoSpike = true,
): boolean {
  if (!c15 || idx <= 0 || idx >= c15.length) return false;
  if ((exchangeName || "").toLowerCase() === "alpaca") {
    for (let back = 0; back <= 2; back++) {
      const j = idx - back;
      if (j > 0 && c15[j]!.time - c15[j - 1]!.time > 3600) {
        return true;
      }
    }
    return false;
  }
  const bar = c15[idx]!;
  const hb = Math.floor(bar.time / 3600);
  let cached = nyOpenBucketCache.get(hb);
  if (!cached) {
    const parts = nyOpenFmt.formatToParts(new Date(hb * 3600 * 1000));
    let wd = "";
    let hr = 0;
    let mn = 0;
    for (const p of parts) {
      if (p.type === "weekday") wd = p.value;
      else if (p.type === "hour") hr = Number(p.value) % 24;
      else if (p.type === "minute") mn = Number(p.value) || 0;
    }
    cached = { isWeekday: wd !== "Sun" && wd !== "Sat", baseMin: hr * 60 + mn };
    if (nyOpenBucketCache.size > 10_000) nyOpenBucketCache.clear();
    nyOpenBucketCache.set(hb, cached);
  }
  if (!cached.isWeekday) return false;
  const mins = cached.baseMin + Math.floor((bar.time - hb * 3600) / 60);
  if (mins < 570 || mins >= 615) return false; // 09:30 - 10:15 ET (DST-aware EST & EDT)
  if (!requireCryptoSpike) return true;
  const rangePct =
    bar.close && bar.close > 0 && bar.high !== undefined && bar.low !== undefined
      ? ((bar.high - bar.low) / bar.close) * 100
      : 0;
  return rangePct >= 0.85;
}

/**
 * Classifies a timestamp into a market-session time bucket (DST-aware ET sessions for Alpaca,
 * UTC liquidity sessions for 24/7 Crypto).
 */
export function getSessionBucketLabel(barTimeSec: number, exchange = "alpaca"): string {
  if (!Number.isFinite(barTimeSec) || barTimeSec <= 0) return "UNKNOWN";
  if ((exchange || "").toLowerCase() === "alpaca") {
    const hb = Math.floor(barTimeSec / 3600);
    let cached = nyOpenBucketCache.get(hb);
    if (!cached) {
      const parts = nyOpenFmt.formatToParts(new Date(hb * 3600 * 1000));
      let wd = "";
      let hr = 0;
      let mn = 0;
      for (const p of parts) {
        if (p.type === "weekday") wd = p.value;
        else if (p.type === "hour") hr = Number(p.value) % 24;
        else if (p.type === "minute") mn = Number(p.value) || 0;
      }
      cached = { isWeekday: wd !== "Sun" && wd !== "Sat", baseMin: hr * 60 + mn };
      if (nyOpenBucketCache.size > 10_000) nyOpenBucketCache.clear();
      nyOpenBucketCache.set(hb, cached);
    }
    const mins = cached.baseMin + Math.floor((barTimeSec - hb * 3600) / 60);
    if (mins < 630) return "US_OPEN_1H";
    if (mins < 870) return "US_MIDDAY";
    return "US_POWER_HOUR";
  }
  const h = new Date(barTimeSec * 1000).getUTCHours();
  if (h < 8) return "ASIA_00_08";
  if (h < 14) return "EUROPE_08_14";
  if (h < 21) return "US_OVERLAP_14_21";
  return "LATE_US_21_00";
}

/**
 * Quant Q5: Discretize multi-factor setup features for Empirical Bayes expected-value shrinkage.
 */
export function computeFeatureBucketKey(
  rs24h: number,
  buyVolRatio: number,
  distanceToEma200Pct: number,
  rsi1h: number,
): string {
  const rsB = rs24h >= 1.2 ? "RS_HI" : rs24h >= 0 ? "RS_MID" : "RS_LO";
  const volB = buyVolRatio >= 2.0 ? "VOL_HI" : buyVolRatio >= 1.5 ? "VOL_MID" : "VOL_LO";
  const spB = distanceToEma200Pct >= 0.8 && distanceToEma200Pct <= 3.2 ? "SP_SWEET" : "SP_OUT";
  const rsiB = rsi1h >= 48 ? "H_STRONG" : "H_WEAK";
  return `${rsB}:${volB}:${spB}:${rsiB}`;
}

export function normalizeBlockReasonKey(rawReason: string | null | undefined): string {
  const first = (rawReason || "OTHER").split(",")[0]?.trim() || "OTHER";
  if (first === "MACRO_SIT_OUT" || first === "SHADOW_BEAR_MACRO" || first === "FILTER_FAIL") {
    return first;
  }
  if (first.startsWith("SURVIVAL_GUARD:")) {
    return first;
  }
  if (first.startsWith("FILTER:")) {
    const detail = first.slice("FILTER:".length).trim();
    if (detail.startsWith("RiskyShadow") || detail.includes("RISKY_SYMBOL")) return "SURVIVAL_GUARD:RISKY_SYMBOL";
    if (detail.startsWith("1hRSI")) return "FILTER:1hRSI";
    if (detail.startsWith("VolR")) return "FILTER:VolR";
    if (detail.startsWith("RS") && (detail.length === 2 || /^RS[\s<>=+-]/i.test(detail))) return "FILTER:RS";
    if (detail.includes("Cooldown")) return "FILTER:Cooldown";
    if (detail.includes("8x16_WeakMom")) return "FILTER:8x16_WeakMom";
    if (detail.startsWith("GAP_TRAP") || detail.startsWith("Gap Up Trap")) return "FILTER:GAP_TRAP";
    if (detail.startsWith("HIGH_CORR") || detail.startsWith("High Corr")) return "FILTER:HIGH_CORR";
    if (detail.startsWith("OPEN_SESSION_45M") || detail.startsWith("Opening 45m")) return "FILTER:OPEN_SESSION_45M";
    if (detail.startsWith("LOW_SNR_ATR") || detail.startsWith("Low SNR")) return "FILTER:LOW_SNR_ATR";
    if (detail.startsWith("NEG_EMPIRICAL_EV") || detail.startsWith("Negative Empirical EV")) return "FILTER:NEG_EMPIRICAL_EV";
    if (detail.includes("Survival")) return `FILTER:${detail}`;
  }
  return first;
}

function getBlockPriority(reason: string): number {
  if (reason.startsWith("SURVIVAL_GUARD:")) return 4;
  if (reason === "MACRO_SIT_OUT") return 3;
  if (reason.startsWith("FILTER")) return 2;
  if (reason === "SHADOW_BEAR_MACRO") return 1;
  return 0;
}

function getBreadthBucketLabel(breadthRaw: unknown): string {
  const val = Number(breadthRaw);
  if (!Number.isFinite(val)) return "UNKNOWN";
  if (val < 40) return "LOW (<40%)";
  if (val < 70) return "MID (40-70%)";
  return "HIGH (>=70%)";
}

function aggregateBlockedStats(
  records: LearningJournalRecord[],
  keyFn: (rec: LearningJournalRecord) => string,
) {
  const map = new Map<string, { total: number; wouldWin: number; wouldLose: number; avgRetPct: number }>();
  for (const b of records) {
    const key = keyFn(b);
    const cur = map.get(key) || { total: 0, wouldWin: 0, wouldLose: 0, avgRetPct: 0 };
    cur.total++;
    if (b.counterfactualVerdict === "WOULD_WIN") cur.wouldWin++;
    if (b.counterfactualVerdict === "WOULD_LOSE") cur.wouldLose++;
    cur.avgRetPct += Number(b.counterfactualPnlPct ?? 0);
    map.set(key, cur);
  }
  return Array.from(map.entries()).map(([key, st]) => ({
    key,
    evaluated: st.total,
    wouldWin: st.wouldWin,
    wouldLose: st.wouldLose,
    savedPct: st.total > 0 ? Number(((st.wouldLose / st.total) * 100).toFixed(1)) : 0,
    missedWinPct: st.total > 0 ? Number(((st.wouldWin / st.total) * 100).toFixed(1)) : 0,
    avgCounterfactualPnlPct: st.total > 0 ? Number((st.avgRetPct / st.total).toFixed(2)) : 0,
  }));
}

function estimateRoundTripCostPct(exchange: string, symbols: string[]): number {
  const ex = (exchange || "binance").toLowerCase();
  if (ex === "alpaca") return 0.06; // Zero commission; conservative synthetic spread/slippage allowance
  if (symbols.length > 0 && symbols.every((s) => s.toUpperCase().endsWith("FDUSD"))) {
    return 0.04; // Zero-maker FDUSD pairs with minor spread/slippage
  }
  return 0.15; // Binance USDT 0.075% x 2 round-trip fee + minor slippage
}

/**
 * Simulate MA-Fan TP1 (50% exit at target with +0.8% floor, lock BE +0.25% on remaining 50%)
 * vs hard Disaster SL over the forward 15m horizon bars, deducting round-trip trading costs.
 */
function simulateMaFanCounterfactual(
  entryPrice: number,
  distTargetPct: number,
  slTriggerPct: number,
  futureBars: Array<{ high: number; low: number; close: number }>,
  costPct = 0,
): {
  finalRet: number;
  maePct: number;
  mfePct: number;
  exitBarsHeld: number;
  checkpointRetPct: Record<number, number | null>;
  verdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL";
} {
  const tp1Pct = Number.isFinite(distTargetPct) && distTargetPct > 0
    ? Math.max(0.8, Math.min(distTargetPct, 8.0))
    : 2.0;
  const beLockPct = 0.25;

  let minLow = entryPrice;
  let maxHigh = entryPrice;
  let tp1Hit = false;
  let finalRet: number | null = null;
  let exitBarsHeld = futureBars.length;

  for (let i = 0; i < futureBars.length; i++) {
    const b = futureBars[i]!;
    if (b.low < minLow) minLow = b.low;
    if (b.high > maxHigh) maxHigh = b.high;
    const lowPct = ((b.low - entryPrice) / entryPrice) * 100;
    const highPct = ((b.high - entryPrice) / entryPrice) * 100;

    if (!tp1Hit) {
      if (lowPct <= slTriggerPct) {
        finalRet = slTriggerPct;
        exitBarsHeld = i + 1;
        break;
      }
      if (highPct >= tp1Pct) {
        tp1Hit = true;
      }
    } else if (lowPct <= beLockPct) {
      finalRet = 0.5 * tp1Pct + 0.5 * beLockPct;
      exitBarsHeld = i + 1;
      break;
    }
  }

  if (finalRet === null) {
    const lastClose = futureBars[futureBars.length - 1]!.close;
    const closeRetPct = ((lastClose - entryPrice) / entryPrice) * 100;
    finalRet = tp1Hit ? 0.5 * tp1Pct + 0.5 * closeRetPct : closeRetPct;
  }

  const netCost = Math.max(0, costPct);
  const checkpointRetPct: Record<number, number | null> = {};
  for (const cp of [4, 8, 16]) {
    if (futureBars.length >= cp && exitBarsHeld > cp) {
      const cpClose = futureBars[cp - 1]!.close;
      checkpointRetPct[cp] = ((cpClose - entryPrice) / entryPrice) * 100 - netCost;
    } else {
      checkpointRetPct[cp] = null;
    }
  }

  const netRet = finalRet - netCost;
  const maePct = ((minLow - entryPrice) / entryPrice) * 100;
  const mfePct = ((maxHigh - entryPrice) / entryPrice) * 100;
  const verdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL" =
    netRet >= 0.35 ? "WOULD_WIN" : netRet <= -0.35 ? "WOULD_LOSE" : "NEUTRAL";
  return { finalRet: netRet, maePct, mfePct, exitBarsHeld, checkpointRetPct, verdict };
}

export function classifyMayerZone(mayerMultiple: number | null): string | null {
  if (mayerMultiple === null || !Number.isFinite(mayerMultiple)) return null;
  if (mayerMultiple < 0.9) return "DEEP_BEAR (<0.9)";
  if (mayerMultiple < 1.0) return "BELOW_200D (0.9-1.0)";
  if (mayerMultiple < 1.25) return "HEALTHY_BULL (1.0-1.25)";
  return "OVERHEATED (>=1.25)";
}

export function classifyMacroRiskZone(macroRiskIndex: number | null): string | null {
  if (macroRiskIndex === null || !Number.isFinite(macroRiskIndex)) return null;
  if (macroRiskIndex < 35) return "CALM (<35)";
  if (macroRiskIndex < 65) return "ELEVATED (35-65)";
  return "HIGH (>=65)";
}

export function classifyUsdZone(usdRet5dPct: number | null): string | null {
  if (usdRet5dPct === null || !Number.isFinite(usdRet5dPct)) return null;
  if (usdRet5dPct < -0.5) return "USD_WEAK (<-0.5%)";
  if (usdRet5dPct > 0.5) return "USD_SURGE (>+0.5%)";
  return "USD_FLAT (-0.5..+0.5%)";
}

export interface DailyMacroRegimePoint {
  mayerMultiple: number | null;
  macroRiskIndex: number | null;
  drawdown180dPct: number | null;
  dailyAtr14Pct: number | null;
}

/**
 * Strictly causal 1D macro regime metrics up to daily bar `dIdx`:
 * - Mayer Multiple = Close / 200 DMA (null when < 200 daily bars)
 * - Macro Risk Index (0..100) = 40% 180D Drawdown + 30% 200 DMA Stretch + 30% 14D Daily ATR% Vol
 */
export function computeDailyMacroRegimeAt(
  c1d: Candle[],
  dIdx: number,
  exchange = "alpaca",
): DailyMacroRegimePoint {
  if (!c1d || dIdx < 0 || dIdx >= c1d.length) {
    return { mayerMultiple: null, macroRiskIndex: null, drawdown180dPct: null, dailyAtr14Pct: null };
  }
  const curClose = c1d[dIdx]!.close;
  if (!Number.isFinite(curClose) || curClose <= 0) {
    return { mayerMultiple: null, macroRiskIndex: null, drawdown180dPct: null, dailyAtr14Pct: null };
  }

  let mayerMultiple: number | null = null;
  if (dIdx >= 199) {
    let sum200 = 0;
    for (let k = dIdx - 199; k <= dIdx; k++) {
      sum200 += c1d[k]!.close;
    }
    const sma200 = sum200 / 200;
    if (Number.isFinite(sma200) && sma200 > 0) {
      mayerMultiple = Number((curClose / sma200).toFixed(4));
    }
  }

  let peak180 = curClose;
  const ddStart = Math.max(0, dIdx - 179);
  for (let k = ddStart; k <= dIdx; k++) {
    const hi = Math.max(c1d[k]!.high || 0, c1d[k]!.close || 0);
    if (hi > peak180) peak180 = hi;
  }
  const ddAbsPct = peak180 > 0 ? Math.max(0, ((peak180 - curClose) / peak180) * 100) : 0;
  const drawdown180dPct = Number((-ddAbsPct).toFixed(2));

  if (dIdx < 14) {
    return {
      mayerMultiple,
      macroRiskIndex: null,
      drawdown180dPct,
      dailyAtr14Pct: null,
    };
  }

  const dailyAtr14Pct = Number(calcCandleAtrPct14(c1d, dIdx).toFixed(3));
  const isAlpaca = (exchange || "alpaca").toLowerCase() === "alpaca";

  const ddMax = isAlpaca ? 20 : 40;
  const ddPts = clamp(ddAbsPct / ddMax, 0, 1) * 40;

  let stretchPts = 0;
  if (mayerMultiple !== null) {
    let stretchNorm = 0;
    if (mayerMultiple < 1.0) {
      stretchNorm = clamp((1.0 - mayerMultiple) / (isAlpaca ? 0.15 : 0.35), 0, 1);
    } else {
      const heatFloor = isAlpaca ? 1.15 : 1.5;
      const heatSpan = isAlpaca ? 0.2 : 0.8;
      if (mayerMultiple > heatFloor) {
        stretchNorm = clamp((mayerMultiple - heatFloor) / heatSpan, 0, 1);
      }
    }
    stretchPts = stretchNorm * 30;
  }

  const volFloor = isAlpaca ? 0.7 : 1.8;
  const volSpan = isAlpaca ? 2.0 : 5.0;
  const volPts = clamp((dailyAtr14Pct - volFloor) / volSpan, 0, 1) * 30;

  const rawRisk = mayerMultiple !== null ? ddPts + stretchPts + volPts : ((ddPts + volPts) * 100) / 70;
  const macroRiskIndex = Number(clamp(rawRisk, 0, 100).toFixed(1));

  return {
    mayerMultiple,
    macroRiskIndex,
    drawdown180dPct,
    dailyAtr14Pct,
  };
}

/**
 * Strictly causal 5-day USD strength proxy (% return).
 * - Alpaca `UUP` (`invert=false`): positive => stronger USD
 * - Binance `EURUSDT` (`invert=true`): inverted to USD/EUR so positive => stronger USD
 */
export function computeUsdStrength5dAt(
  usd1d: Candle[],
  uIdx: number,
  invert: boolean,
): number | null {
  if (!usd1d || uIdx < 5 || uIdx >= usd1d.length) return null;
  const cur = usd1d[uIdx]?.close ?? 0;
  const prev = usd1d[uIdx - 5]?.close ?? 0;
  if (!Number.isFinite(cur) || !Number.isFinite(prev) || cur <= 0 || prev <= 0) return null;
  const retPct = invert ? ((prev / cur) - 1) * 100 : ((cur - prev) / prev) * 100;
  return Number.isFinite(retPct) ? Number(retPct.toFixed(3)) : null;
}

export interface ObserveSetupInput {
  entryTimeSec: number;
  score: number;
  pnlPct: number;
  distFromBottomLowPct: number;
  upperWickRatioPct: number;
  breadthDelta4h: number;
  benchmarkRet4h: number;
  macroEventInWindow: boolean;
  macroEventDay: boolean;
  hasShock24h: boolean;
  mayerMultiple?: number | null;
  macroRiskIndex?: number | null;
  usdRet5dPct?: number | null;
  checkpointRetPct?: Record<number, number | null>;
}

/**
 * Aggregates Observe-Only learning analytics across Shadow/Replay setups (zero gating side effects).
 */
export function computeObserveFactorsFromSetups(setups: ObserveSetupInput[]): ShadowObserveFactorsSummary {
  const tally = () => new Map<string, { setups: number; wins: number; sumPnl: number }>();
  const bump = (map: Map<string, { setups: number; wins: number; sumPnl: number }>, key: string, pnlPct: number) => {
    const cur = map.get(key) || { setups: 0, wins: 0, sumPnl: 0 };
    cur.setups++;
    if (pnlPct > 0) cur.wins++;
    cur.sumPnl += pnlPct;
    map.set(key, cur);
  };
  const toRec = (map: Map<string, { setups: number; wins: number; sumPnl: number }>, order?: string[]) => {
    const out: Record<string, BucketMetric> = {};
    const keys = order ? [...order.filter((k) => map.has(k)), ...Array.from(map.keys()).filter((k) => !order.includes(k))] : Array.from(map.keys());
    for (const k of keys) {
      const st = map.get(k)!;
      out[k] = {
        setups: st.setups,
        winRatePct: st.setups > 0 ? Number(((st.wins / st.setups) * 100).toFixed(1)) : 0,
        avgPnlPct: st.setups > 0 ? Number((st.sumPnl / st.setups).toFixed(3)) : 0,
      };
    }
    return out;
  };

  const concurCounts = new Map<number, number>();
  for (const s of setups) {
    concurCounts.set(s.entryTimeSec, (concurCounts.get(s.entryTimeSec) || 0) + 1);
  }

  const scoreMap = tally();
  const overMap = tally();
  const wickMap = tally();
  const breadthMap = tally();
  const benchMap = tally();
  const concMap = tally();
  const eventMap = tally();
  const mayerMap = tally();
  const macroRiskMap = tally();
  const usdMap = tally();

  for (const s of setups) {
    const scoreKey = s.score < 30 ? "<30" : s.score < 50 ? "30-50" : s.score < 70 ? "50-70" : ">=70";
    bump(scoreMap, scoreKey, s.pnlPct);

    const overKey =
      s.distFromBottomLowPct < 1.5
        ? "FRESH (<1.5%)"
        : s.distFromBottomLowPct < 3.5
          ? "MID (1.5-3.5%)"
          : "EXTENDED (>=3.5%)";
    bump(overMap, overKey, s.pnlPct);

    const wickKey =
      s.upperWickRatioPct < 30
        ? "CLEAN (<30%)"
        : s.upperWickRatioPct < 50
          ? "MODERATE (30-50%)"
          : "HEAVY_WICK (>=50%)";
    bump(wickMap, wickKey, s.pnlPct);

    const brKey = s.breadthDelta4h > 5 ? "RISING (>+5pp)" : s.breadthDelta4h < -5 ? "FALLING (<-5pp)" : "FLAT (-5..+5pp)";
    bump(breadthMap, brKey, s.pnlPct);

    const b4Key =
      s.benchmarkRet4h > 0.5 ? "BENCH_UP (>+0.5%)" : s.benchmarkRet4h < -0.5 ? "BENCH_DUMP (<-0.5%)" : "BENCH_FLAT (-0.5..+0.5%)";
    bump(benchMap, b4Key, s.pnlPct);

    const cCnt = concurCounts.get(s.entryTimeSec) || 1;
    const cKey = cCnt <= 2 ? "ISOLATED (1-2)" : cCnt <= 5 ? "MODERATE (3-5)" : "SURGE (>=6)";
    bump(concMap, cKey, s.pnlPct);

    if (s.macroEventInWindow) bump(eventMap, "MACRO_EVENT_WINDOW", s.pnlPct);
    else if (s.macroEventDay) bump(eventMap, "MACRO_EVENT_DAY", s.pnlPct);
    if (s.hasShock24h) bump(eventMap, "NEWS_SHOCK_24H", s.pnlPct);
    if (!s.macroEventInWindow && !s.macroEventDay && !s.hasShock24h) {
      bump(eventMap, "NORMAL_DAY", s.pnlPct);
    }

    const mayerZone = classifyMayerZone(s.mayerMultiple ?? null);
    if (mayerZone) bump(mayerMap, mayerZone, s.pnlPct);

    const macroRiskZone = classifyMacroRiskZone(s.macroRiskIndex ?? null);
    if (macroRiskZone) bump(macroRiskMap, macroRiskZone, s.pnlPct);

    const usdZone = classifyUsdZone(s.usdRet5dPct ?? null);
    if (usdZone) bump(usdMap, usdZone, s.pnlPct);
  }

  const cpDefs = [
    { bars: 4, label: "1h (4 bars)" },
    { bars: 8, label: "2h (8 bars)" },
    { bars: 16, label: "4h (16 bars)" },
  ];
  const timeDecay: TimeDecayCheckpoint[] = cpDefs.map((cp) => {
    let stillOpenCount = 0;
    let staleCount = 0;
    let staleWins = 0;
    let staleSum = 0;
    let healthyCount = 0;
    let healthyWins = 0;
    let healthySum = 0;
    for (const s of setups) {
      const cpRet = s.checkpointRetPct?.[cp.bars];
      if (cpRet === null || cpRet === undefined || !Number.isFinite(cpRet)) continue;
      stillOpenCount++;
      if (cpRet < 0.3) {
        staleCount++;
        if (s.pnlPct > 0) staleWins++;
        staleSum += s.pnlPct;
      } else {
        healthyCount++;
        if (s.pnlPct > 0) healthyWins++;
        healthySum += s.pnlPct;
      }
    }
    return {
      bars: cp.bars,
      label: cp.label,
      stillOpenCount,
      staleCount,
      staleEventuallyWinPct: staleCount > 0 ? Number(((staleWins / staleCount) * 100).toFixed(1)) : 0,
      staleAvgFinalPnlPct: staleCount > 0 ? Number((staleSum / staleCount).toFixed(3)) : 0,
      healthyEventuallyWinPct: healthyCount > 0 ? Number(((healthyWins / healthyCount) * 100).toFixed(1)) : 0,
      healthyAvgFinalPnlPct: healthyCount > 0 ? Number((healthySum / healthyCount).toFixed(3)) : 0,
    };
  });

  return {
    scoreBuckets: toRec(scoreMap, ["<30", "30-50", "50-70", ">=70"]),
    overextensionBuckets: toRec(overMap, ["FRESH (<1.5%)", "MID (1.5-3.5%)", "EXTENDED (>=3.5%)"]),
    upperWickBuckets: toRec(wickMap, ["CLEAN (<30%)", "MODERATE (30-50%)", "HEAVY_WICK (>=50%)"]),
    timeDecay,
    breadthVelocityBuckets: toRec(breadthMap, ["RISING (>+5pp)", "FLAT (-5..+5pp)", "FALLING (<-5pp)"]),
    benchSlope4hBuckets: toRec(benchMap, ["BENCH_UP (>+0.5%)", "BENCH_FLAT (-0.5..+0.5%)", "BENCH_DUMP (<-0.5%)"]),
    concurrencyBuckets: toRec(concMap, ["ISOLATED (1-2)", "MODERATE (3-5)", "SURGE (>=6)"]),
    eventBuckets: toRec(eventMap, ["NORMAL_DAY", "MACRO_EVENT_DAY", "MACRO_EVENT_WINDOW", "NEWS_SHOCK_24H"]),
    mayerBuckets: toRec(mayerMap, ["DEEP_BEAR (<0.9)", "BELOW_200D (0.9-1.0)", "HEALTHY_BULL (1.0-1.25)", "OVERHEATED (>=1.25)"]),
    macroRiskBuckets: toRec(macroRiskMap, ["CALM (<35)", "ELEVATED (35-65)", "HIGH (>=65)"]),
    usdStrengthBuckets: toRec(usdMap, ["USD_WEAK (<-0.5%)", "USD_FLAT (-0.5..+0.5%)", "USD_SURGE (>+0.5%)"]),
  };
}

export interface ShadowReplaySetupInput {
  symbol: string;
  entryTimeSec: number;
  exitTimeSec: number;
  score: number;
  pnlPct: number;
  isBearMacro: boolean;
  breadthPct: number;
}

/**
 * Simulates causal Top-K concurrent slot allocation & equity curve across competing portfolio candidates.
 */
export function simulatePortfolioShadowReplay(
  setups: ShadowReplaySetupInput[],
  maxSlots = 3,
): ShadowPortfolioReplaySummary {
  const capSlots = Math.max(1, Math.min(5, Math.round(maxSlots || 3)));
  const ordered = setups
    .slice()
    .sort((a, b) => a.entryTimeSec - b.entryTimeSec || b.score - a.score);

  let activeSlots: Array<{ symbol: string; exitTimeSec: number }> = [];
  let trades = 0;
  let wins = 0;
  let winSum = 0;
  let lossAbs = 0;
  let pnlSum = 0;
  let skippedSlotFull = 0;
  let equity = 100;
  let peak = 100;
  let maxDd = 0;

  for (const s of ordered) {
    activeSlots = activeSlots.filter((sl) => sl.exitTimeSec > s.entryTimeSec);
    if (activeSlots.some((sl) => sl.symbol === s.symbol)) continue;
    if (activeSlots.length >= capSlots) {
      skippedSlotFull++;
      continue;
    }
    activeSlots.push({ symbol: s.symbol, exitTimeSec: Math.max(s.exitTimeSec, s.entryTimeSec + 900) });
    trades++;
    pnlSum += s.pnlPct;
    if (s.pnlPct > 0) {
      wins++;
      winSum += s.pnlPct;
    } else {
      lossAbs += Math.abs(s.pnlPct);
    }
    equity *= 1 + (s.pnlPct / 100) / capSlots;
    if (equity > peak) peak = equity;
    const dd = peak > 0 ? ((peak - equity) / peak) * 100 : 0;
    if (dd > maxDd) maxDd = dd;
  }

  return {
    maxSlots: capSlots,
    trades,
    wins,
    losses: trades - wins,
    winRatePct: trades > 0 ? Number(((wins / trades) * 100).toFixed(1)) : 0,
    avgPnlPct: trades > 0 ? Number((pnlSum / trades).toFixed(2)) : 0,
    totalPnlPct: Number(pnlSum.toFixed(2)),
    profitFactor: lossAbs > 0 ? Number((winSum / lossAbs).toFixed(2)) : winSum > 0 ? 99 : 0,
    maxDrawdownPct: Number(maxDd.toFixed(2)),
    skippedSlotFull,
  };
}

/**
 * Evaluates 5 chronological 70/30 Train/OOS-Test Breadth/Macro Sit-Out policies on shadow setups.
 */
export function evaluateBreadthSitOutOos(
  setups: ShadowReplaySetupInput[],
  currentSitOutMode = "dual",
): BreadthSitOutAnalysis {
  const sorted = setups.slice().sort((a, b) => a.entryTimeSec - b.entryTimeSec);
  const splitIdx = Math.floor(sorted.length * 0.7);
  const trainSetups = sorted.slice(0, splitIdx);
  const testSetups = sorted.slice(splitIdx);

  const defs: Array<{ mode: string; label: string; allow: (s: ShadowReplaySetupInput) => boolean }> = [
    { mode: "off", label: "Sit-Out OFF", allow: () => true },
    { mode: "breadth_20", label: "Bear & Breadth < 20%", allow: (s) => !(s.isBearMacro && s.breadthPct < 20) },
    { mode: "breadth_35", label: "Bear & Breadth < 35%", allow: (s) => !(s.isBearMacro && s.breadthPct < 35) },
    { mode: "breadth_50", label: "Bear & Breadth < 50%", allow: (s) => !(s.isBearMacro && s.breadthPct < 50) },
    { mode: "dual", label: "Dual 4H+1D Bear", allow: (s) => !s.isBearMacro },
  ];

  const modes: BreadthSitOutModeResult[] = defs.map((d) => {
    const tr = trainSetups.filter(d.allow);
    const te = testSetups.filter(d.allow);
    let wins = 0;
    let winSum = 0;
    let lossAbs = 0;
    let pnlSum = 0;
    for (const item of te) {
      pnlSum += item.pnlPct;
      if (item.pnlPct > 0) {
        wins++;
        winSum += item.pnlPct;
      } else {
        lossAbs += Math.abs(item.pnlPct);
      }
    }
    return {
      mode: d.mode,
      label: d.label,
      trainTrades: tr.length,
      testTrades: te.length,
      testWinRatePct: te.length > 0 ? Number(((wins / te.length) * 100).toFixed(1)) : 0,
      testAvgPnlPct: te.length > 0 ? Number((pnlSum / te.length).toFixed(2)) : 0,
      testTotalPnlPct: Number(pnlSum.toFixed(2)),
      testProfitFactor: lossAbs > 0 ? Number((winSum / lossAbs).toFixed(2)) : winSum > 0 ? 99 : 0,
    };
  });

  const eligible = modes.filter((m) => m.testTrades >= 5);
  const ranked = (eligible.length > 0 ? eligible : modes).slice().sort(
    (a, b) => b.testAvgPnlPct - a.testAvgPnlPct || b.testProfitFactor - a.testProfitFactor,
  );
  const best = ranked[0] || modes[0]!;
  const curNorm = currentSitOutMode === "off" || currentSitOutMode === "dual" ? currentSitOutMode : "dual";
  const curRes = modes.find((m) => m.mode === curNorm) || modes[modes.length - 1]!;
  const verdict =
    testSetups.length < 5
      ? `รอตัวอย่าง OOS เพิ่ม (มี ${testSetups.length} setups)`
      : best.mode === curNorm
        ? `โหมดปัจจุบัน (${curRes.label}) ให้ผลเฉลี่ย OOS ดีสุดแล้ว (${best.testAvgPnlPct >= 0 ? "+" : ""}${best.testAvgPnlPct}%/ไม้, PF ${best.testProfitFactor})`
        : `${best.label} ทำ OOS เฉลี่ย ${best.testAvgPnlPct >= 0 ? "+" : ""}${best.testAvgPnlPct}%/ไม้ (PF ${best.testProfitFactor}, ${best.testTrades} ไม้) vs ${curRes.label} (${curRes.testAvgPnlPct >= 0 ? "+" : ""}${curRes.testAvgPnlPct}%/ไม้)`;

  return {
    currentMode: currentSitOutMode,
    bestMode: best.mode,
    bestLabel: best.label,
    testSetupsTotal: testSetups.length,
    verdict,
    modes,
  };
}

type TpModeName = "EMA25" | "EMA75" | "EMA200";

function normalizeTpMode(m?: string): TpModeName {
  return m === "EMA25" || m === "EMA75" ? m : "EMA200";
}

export function encodeTpModeNumeric(mode: string | undefined): number {
  if (mode === "EMA25") return 25;
  if (mode === "EMA75") return 75;
  return 200;
}

export function decodeTpModeNumeric(num: number): TpModeName {
  const n = Math.round(Number(num));
  if (n === 25) return "EMA25";
  if (n === 75) return "EMA75";
  return "EMA200";
}

export type PullbackModeName = "EMA8_HOLD" | "EMA4_TOUCH";

function normalizePullbackMode(m?: string): PullbackModeName {
  return m === "EMA4_TOUCH" ? "EMA4_TOUCH" : "EMA8_HOLD";
}

export function encodePullbackModeNumeric(m: string | undefined): number {
  return m === "EMA4_TOUCH" ? 4 : 8;
}

export function decodePullbackModeNumeric(n: number): PullbackModeName {
  return Math.round(Number(n)) === 4 ? "EMA4_TOUCH" : "EMA8_HOLD";
}

function tuneCfgFromConfig(cfg: Config): TuneCfg {
  return {
    sl: Math.max(1.5, Math.min(5.0, Number(cfg.portfolioDisasterSlPct || 3.5))),
    minVolR: Number.isFinite(Number(cfg.portfolioMinBuyVolRatio)) ? Number(cfg.portfolioMinBuyVolRatio) : 1.3,
    minRs: Number.isFinite(Number(cfg.portfolioMinRelStrength24h)) ? Number(cfg.portfolioMinRelStrength24h) : -1.0,
    minRsi1h: Number.isFinite(Number(cfg.portfolioMinRsi1h)) ? Number(cfg.portfolioMinRsi1h) : 40,
    tpMode: normalizeTpMode(cfg.portfolioTp1TargetMode),
    pullbackMode: normalizePullbackMode(cfg.portfolioPullbackMode),
  };
}

function tuneCfgKey(t: TuneCfg): string {
  return `${t.sl}|${t.minVolR}|${t.minRs}|${t.minRsi1h}|${t.tpMode}|${t.pullbackMode}`;
}

interface TuningEvent {
  setupId: string;
  symbol: string;
  barIdx: number;
  timeSec: number;
  entryPrice: number;
  entryPriceEma8?: number;
  ema8HoldOk?: boolean;
  snrAtr?: number;
  minSnrAtr?: number;
  rsi1h: number;
  volR: number;
  rs24: number;
  dist25: number;
  dist75: number;
  dist200: number;
  dist25Ema8?: number;
  dist75Ema8?: number;
  dist200Ema8?: number;
  futureBars: Candle[];
}

export interface TuningMetrics {
  trades: number;
  wins: number;
  winRatePct: number;
  avgPnlPct: number;
  totalPnlPct: number;
  profitFactor: number;
}

export interface TuningRecommendation {
  param:
    | "portfolioDisasterSlPct"
    | "portfolioMinBuyVolRatio"
    | "portfolioMinRelStrength24h"
    | "portfolioMinRsi1h"
    | "portfolioTp1TargetMode"
    | "portfolioPullbackMode";
  label: string;
  current: number | string;
  suggested: number | string;
  trainAvgDeltaPct: number;
  testAvgDeltaPct: number;
  baselineTest: TuningMetrics;
  suggestedTest: TuningMetrics;
  confidence: "HIGH" | "MEDIUM" | "LOW";
  verdict: string;
  advisoryOnly: boolean;
  autoApplicable?: boolean;
}

export interface TuningAnalysis {
  generatedAt: number;
  roundTripCostPct: number;
  events: number;
  setups: number;
  trainSetups: number;
  testSetups: number;
  splitTimeSec: number;
  baselineTrain: TuningMetrics;
  baselineTest: TuningMetrics;
  recommendations: TuningRecommendation[];
  combined: {
    train: TuningMetrics;
    test: TuningMetrics;
    params: Record<string, number | string>;
    passedOos: boolean;
  } | null;
  note: string;
}

interface TuneCfg {
  sl: number;
  minVolR: number;
  minRs: number;
  minRsi1h: number;
  tpMode: TpModeName;
  pullbackMode: PullbackModeName;
}

interface MetricAcc {
  n: number;
  wins: number;
  sum: number;
  winSum: number;
  lossAbs: number;
}

function newAcc(): MetricAcc {
  return { n: 0, wins: 0, sum: 0, winSum: 0, lossAbs: 0 };
}

function finalizeAcc(a: MetricAcc): TuningMetrics {
  return {
    trades: a.n,
    wins: a.wins,
    winRatePct: a.n > 0 ? Number(((a.wins / a.n) * 100).toFixed(1)) : 0,
    avgPnlPct: a.n > 0 ? Number((a.sum / a.n).toFixed(3)) : 0,
    totalPnlPct: Number(a.sum.toFixed(2)),
    profitFactor: Number((a.lossAbs > 0 ? a.winSum / a.lossAbs : a.winSum > 0 ? 99 : 0).toFixed(2)),
  };
}

function quantScore(m: TuningMetrics): number {
  if (m.trades <= 0) return -1e9;
  return m.totalPnlPct + Math.max(-2, Math.min(3, m.profitFactor - 1)) * Math.max(1, Math.sqrt(m.trades));
}

function evalTuneConfig(groups: TuningEvent[][], cfg: TuneCfg, splitTime: number, costPct: number) {
  const train = newAcc();
  const test = newAcc();
  const lastBarBySym = new Map<string, number>();
  const useEma8 = cfg.pullbackMode === "EMA8_HOLD";

  for (const g of groups) {
    const e = g.find(
      (ev) =>
        ev.barIdx - (lastBarBySym.get(ev.symbol) ?? -999) >= 8 &&
        (!useEma8 || ev.ema8HoldOk !== false) &&
        (ev.snrAtr === undefined || ev.snrAtr >= (ev.minSnrAtr ?? 1.25)) &&
        ev.rsi1h >= cfg.minRsi1h &&
        ev.volR >= cfg.minVolR &&
        ev.rs24 >= cfg.minRs,
    );
    if (!e) continue;
    lastBarBySym.set(e.symbol, e.barIdx);

    const hasEma8 = useEma8 && e.entryPriceEma8 !== undefined && e.entryPriceEma8 > 0;
    const px = hasEma8 ? e.entryPriceEma8! : e.entryPrice;
    const dist = hasEma8
      ? cfg.tpMode === "EMA25"
        ? (e.dist25Ema8 ?? e.dist25)
        : cfg.tpMode === "EMA75"
          ? (e.dist75Ema8 ?? e.dist75)
          : (e.dist200Ema8 ?? e.dist200)
      : cfg.tpMode === "EMA25"
        ? e.dist25
        : cfg.tpMode === "EMA75"
          ? e.dist75
          : e.dist200;
    const sim = simulateMaFanCounterfactual(px, dist, -cfg.sl, e.futureBars, costPct);
    const acc = e.timeSec < splitTime ? train : test;
    acc.n++;
    acc.sum += sim.finalRet;
    if (sim.finalRet > 0) {
      acc.wins++;
      acc.winSum += sim.finalRet;
    } else {
      acc.lossAbs += Math.abs(sim.finalRet);
    }
  }
  return { train: finalizeAcc(train), test: finalizeAcc(test) };
}

/**
 * Advisory-only quant sensitivity analysis: optimizes strictly on Train (70%) using
 * fee-adjusted Total PnL + PF + retention, selects combined winners from Train alone
 * (zero Test data leakage), and validates once out-of-sample on Test (30%).
 */
function analyzeTuning(events: TuningEvent[], cur: TuneCfg, costPct = 0): TuningAnalysis | null {
  const groupsMap = new Map<string, TuningEvent[]>();
  for (const e of events) {
    const g = groupsMap.get(e.setupId) || [];
    g.push(e);
    groupsMap.set(e.setupId, g);
  }
  const groups = Array.from(groupsMap.values());
  for (const g of groups) g.sort((a, b) => a.timeSec - b.timeSec);
  groups.sort((a, b) => a[0]!.timeSec - b[0]!.timeSec);

  const emptyM: TuningMetrics = { trades: 0, wins: 0, winRatePct: 0, avgPnlPct: 0, totalPnlPct: 0, profitFactor: 0 };
  if (groups.length < 20) {
    return {
      generatedAt: Date.now(),
      roundTripCostPct: costPct,
      events: events.length,
      setups: groups.length,
      trainSetups: 0,
      testSetups: 0,
      splitTimeSec: 0,
      baselineTrain: emptyM,
      baselineTest: emptyM,
      recommendations: [],
      combined: null,
      note: `ข้อมูลยังน้อย (${groups.length} setups < 20) — ยังไม่ให้คำแนะนำ`,
    };
  }

  const splitTime = groups[Math.floor(groups.length * 0.7)]![0]!.timeSec;
  const base = evalTuneConfig(groups, cur, splitTime, costPct);
  const baseTrainScore = quantScore(base.train);
  const minTrainN = Math.max(12, Math.floor(base.train.trades * 0.55));

  const candidates: Array<{
    param: TuningRecommendation["param"];
    label: string;
    values: Array<number | string>;
    apply: (c: TuneCfg, v: any) => TuneCfg;
    curVal: number | string;
  }> = [
    { param: "portfolioDisasterSlPct", label: "Disaster SL %", values: [2.5, 3, 3.5, 4, 4.5, 5], apply: (c, v) => ({ ...c, sl: v }), curVal: cur.sl },
    { param: "portfolioMinBuyVolRatio", label: "Min Buy VolRatio (12 bars)", values: [1.0, 1.15, 1.3, 1.6, 2.0], apply: (c, v) => ({ ...c, minVolR: v }), curVal: cur.minVolR },
    { param: "portfolioMinRelStrength24h", label: "Min RS 24h vs Benchmark (%)", values: [-3, -2, -1, 0, 1], apply: (c, v) => ({ ...c, minRs: v }), curVal: cur.minRs },
    { param: "portfolioMinRsi1h", label: "Min 1h RSI", values: [35, 40, 45, 50], apply: (c, v) => ({ ...c, minRsi1h: v }), curVal: cur.minRsi1h },
    { param: "portfolioTp1TargetMode", label: "TP1 Target Mode", values: ["EMA25", "EMA75", "EMA200"], apply: (c, v) => ({ ...c, tpMode: v }), curVal: cur.tpMode },
    { param: "portfolioPullbackMode", label: "Pullback Entry Timing", values: ["EMA8_HOLD", "EMA4_TOUCH"], apply: (c, v) => ({ ...c, pullbackMode: v }), curVal: cur.pullbackMode },
  ];

  const recs: TuningRecommendation[] = [];
  const trainWinners: Array<{ cand: (typeof candidates)[number]; value: number | string }> = [];

  for (const cand of candidates) {
    let best: { v: number | string; res: ReturnType<typeof evalTuneConfig>; score: number } | null = null;
    for (const v of cand.values) {
      if (v === cand.curVal) continue;
      const res = evalTuneConfig(groups, cand.apply(cur, v), splitTime, costPct);
      if (res.train.trades < minTrainN) continue;
      if (res.train.avgPnlPct <= base.train.avgPnlPct + 0.03) continue;
      if (res.train.profitFactor < base.train.profitFactor) continue;
      const sc = quantScore(res.train);
      if (sc <= baseTrainScore) continue;
      if (!best || sc > best.score) best = { v, res, score: sc };
    }
    if (!best) continue;

    const trainDelta = best.res.train.avgPnlPct - base.train.avgPnlPct;
    // Strict Train-only selection for combined simulation (zero Test leakage)
    if (trainDelta >= 0.08 && best.res.train.profitFactor > base.train.profitFactor) {
      trainWinners.push({ cand, value: best.v });
    }

    const testDelta = best.res.test.avgPnlPct - base.test.avgPnlPct;
    const testN = best.res.test.trades;
    const pfHeld = best.res.test.profitFactor >= base.test.profitFactor;
    const confidence: TuningRecommendation["confidence"] =
      trainDelta >= 0.15 && testDelta >= 0.12 && testN >= 15 && pfHeld
        ? "HIGH"
        : testDelta > 0 && testN >= 8 && pfHeld
          ? "MEDIUM"
          : "LOW";

    const autoApplicable =
      (confidence === "HIGH" || confidence === "MEDIUM") &&
      testN >= 12 &&
      testDelta >= 0.03 &&
      best.res.test.profitFactor > base.test.profitFactor;

    const verdict =
      confidence === "LOW"
        ? "🛡️ บล็อกอัตโนมัติ (ไม่ผ่าน OOS Test — ป้องกัน Overfit)"
        : autoApplicable
          ? `🤖 พร้อมปรับอัตโนมัติ (OOS Test เฉลี่ย/ไม้ +${testDelta.toFixed(2)}pp, Total ${best.res.test.totalPnlPct >= 0 ? "+" : ""}${best.res.test.totalPnlPct}%, PF ${best.res.test.profitFactor}) • ความเชื่อมั่น ${confidence}`
          : `ช่วงทดสอบเฉลี่ย/ไม้ ${testDelta >= 0 ? "+" : ""}${testDelta.toFixed(2)}pp (Total ${best.res.test.totalPnlPct >= 0 ? "+" : ""}${best.res.test.totalPnlPct}%, PF ${best.res.test.profitFactor}) • ความเชื่อมั่น ${confidence}`;

    recs.push({
      param: cand.param,
      label: cand.label,
      current: cand.curVal,
      suggested: best.v,
      trainAvgDeltaPct: Number(trainDelta.toFixed(3)),
      testAvgDeltaPct: Number(testDelta.toFixed(3)),
      baselineTest: base.test,
      suggestedTest: best.res.test,
      confidence,
      verdict,
      advisoryOnly: !autoApplicable,
      autoApplicable,
    });
  }

  let combined: TuningAnalysis["combined"] = null;
  if (trainWinners.length > 0) {
    let cfg2: TuneCfg = { ...cur };
    const params: Record<string, number | string> = {};
    for (const w of trainWinners) {
      cfg2 = w.cand.apply(cfg2, w.value);
      params[w.cand.param] = w.value;
    }
    const res = evalTuneConfig(groups, cfg2, splitTime, costPct);
    const passedOos =
      res.test.trades >= 8 &&
      res.test.avgPnlPct > base.test.avgPnlPct &&
      res.test.profitFactor >= base.test.profitFactor;
    combined = { train: res.train, test: res.test, params, passedOos };
  }

  const trainSetups = groups.filter((g) => g[0]!.timeSec < splitTime).length;
  recs.sort((a, b) => b.testAvgDeltaPct - a.testAvgDeltaPct);
  return {
    generatedAt: Date.now(),
    roundTripCostPct: costPct,
    events: events.length,
    setups: groups.length,
    trainSetups,
    testSetups: groups.length - trainSetups,
    splitTimeSec: splitTime,
    baselineTrain: base.train,
    baselineTest: base.test,
    recommendations: recs,
    combined,
    note: `🤖 Auto-Pilot OOS (หักต้นทุนจำลอง ${costPct.toFixed(2)}%/รอบ) — คัดจาก Train 70% และปรับใช้เฉพาะค่าที่ผ่าน OOS Test 30% (HIGH/MEDIUM) อัตโนมัติ พร้อม Cooldown 7 วัน & Auto-Rollback`,
  };
}

export type LearningAutoTuneConfig = Pick<
  Config,
  "learningAutoTuneMode" | "portfolioSurvivalGuardEnforced" | "dryRun" | "exchange" | "alpacaBaseUrl"
>;

type AutoTunableParam = TuningRecommendation["param"];

const AUTO_TUNABLE_PARAMS: readonly AutoTunableParam[] = [
  "portfolioTp1TargetMode",
  "portfolioPullbackMode",
  "portfolioDisasterSlPct",
  "portfolioMinBuyVolRatio",
  "portfolioMinRelStrength24h",
  "portfolioMinRsi1h",
];

function matchesRecordedAdjustment(param: AutoTunableParam, cfg: Config, recordedNum: number): boolean {
  switch (param) {
    case "portfolioTp1TargetMode":
      return encodeTpModeNumeric(cfg.portfolioTp1TargetMode) === Math.round(recordedNum);
    case "portfolioPullbackMode":
      return encodePullbackModeNumeric(cfg.portfolioPullbackMode) === Math.round(recordedNum);
    case "portfolioDisasterSlPct":
      return Math.abs(Number(cfg.portfolioDisasterSlPct || 3.5) - recordedNum) < 0.05;
    case "portfolioMinBuyVolRatio": {
      const v = Number.isFinite(Number(cfg.portfolioMinBuyVolRatio)) ? Number(cfg.portfolioMinBuyVolRatio) : 1.3;
      return Math.abs(v - recordedNum) < 0.05;
    }
    case "portfolioMinRelStrength24h": {
      const v = Number.isFinite(Number(cfg.portfolioMinRelStrength24h)) ? Number(cfg.portfolioMinRelStrength24h) : -1.0;
      return Math.abs(v - recordedNum) < 0.05;
    }
    case "portfolioMinRsi1h": {
      const v = Number.isFinite(Number(cfg.portfolioMinRsi1h)) ? Number(cfg.portfolioMinRsi1h) : 40;
      return Math.abs(v - recordedNum) < 0.5;
    }
  }
}

function buildTunableCandidatePatch(
  param: AutoTunableParam,
  cfg: Config,
  suggested: number | string,
): { patchVal: number | TpModeName | PullbackModeName; oldNum: number; newNum: number; displayChange: string } | null {
  switch (param) {
    case "portfolioTp1TargetMode": {
      const curMode = cfg.portfolioTp1TargetMode || "EMA200";
      const nextMode = String(suggested) as TpModeName;
      if (!["EMA25", "EMA75", "EMA200"].includes(nextMode) || nextMode === curMode) return null;
      return {
        patchVal: nextMode,
        oldNum: encodeTpModeNumeric(curMode),
        newNum: encodeTpModeNumeric(nextMode),
        displayChange: `${curMode} -> ${nextMode}`,
      };
    }
    case "portfolioPullbackMode": {
      const curMode = normalizePullbackMode(cfg.portfolioPullbackMode);
      const nextMode = String(suggested) as PullbackModeName;
      if (!["EMA8_HOLD", "EMA4_TOUCH"].includes(nextMode) || nextMode === curMode) return null;
      return {
        patchVal: nextMode,
        oldNum: encodePullbackModeNumeric(curMode),
        newNum: encodePullbackModeNumeric(nextMode),
        displayChange: `${curMode} -> ${nextMode}`,
      };
    }
    case "portfolioMinBuyVolRatio": {
      const curVal = Number.isFinite(Number(cfg.portfolioMinBuyVolRatio)) ? Number(cfg.portfolioMinBuyVolRatio) : 1.3;
      const target = clamp(Number(suggested), 1.0, 2.0);
      const nextVal = Number(clamp(curVal + clamp(target - curVal, -0.30, 0.30), 1.0, 2.0).toFixed(2));
      if (Math.abs(nextVal - curVal) < 0.10) return null;
      return { patchVal: nextVal, oldNum: curVal, newNum: nextVal, displayChange: `${curVal} -> ${nextVal}` };
    }
    case "portfolioMinRelStrength24h": {
      const curVal = Number.isFinite(Number(cfg.portfolioMinRelStrength24h)) ? Number(cfg.portfolioMinRelStrength24h) : -1.0;
      const target = clamp(Number(suggested), -3.0, 1.0);
      const nextVal = Number(clamp(curVal + clamp(target - curVal, -1.0, 1.0), -3.0, 1.0).toFixed(2));
      if (Math.abs(nextVal - curVal) < 0.40) return null;
      return { patchVal: nextVal, oldNum: curVal, newNum: nextVal, displayChange: `${curVal}% -> ${nextVal}%` };
    }
    case "portfolioMinRsi1h": {
      const curVal = Number.isFinite(Number(cfg.portfolioMinRsi1h)) ? Number(cfg.portfolioMinRsi1h) : 40;
      const target = clamp(Number(suggested), 35, 50);
      const nextVal = Math.round(clamp(curVal + clamp(target - curVal, -5, 5), 35, 50));
      if (Math.abs(nextVal - curVal) < 3) return null;
      return { patchVal: nextVal, oldNum: curVal, newNum: nextVal, displayChange: `${curVal} -> ${nextVal}` };
    }
    case "portfolioDisasterSlPct": {
      const curVal = Number(cfg.portfolioDisasterSlPct || 3.5);
      const target = clamp(Number(suggested), 2.5, 5.0);
      const nextVal = Number(clamp(curVal + clamp(target - curVal, -0.25, 0.25), 2.5, 5.0).toFixed(2));
      if (Math.abs(nextVal - curVal) < 0.20) return null;
      return { patchVal: nextVal, oldNum: curVal, newNum: nextVal, displayChange: `${curVal}% -> ${nextVal}%` };
    }
  }
}

export class LearningEngine {
  private readonly db: BotDatabase;
  private readonly log: (msg: string) => void;
  private symbolStates = new Map<string, SymbolLearningStateRecord>();
  private recordedBlockedKeys = new Set<string>();
  private lastPeriodicRunTs = 0;
  private periodicInProgress = false;
  private autoTuneInProgress = false;
  private symbolStatesLoaded = false;

  // Shadow Walk-Forward & Survival Telemetry state
  private shadowSummary: ShadowLearningSummary | null = null;
  private shadowWinMaeAbs: number[] = [];
  private lastTuningEvents: TuningEvent[] = [];
  private lastTuningCostPct = 0;
  private lastTuningBaselineKey = "";
  private lastShadowRunTs = 0;
  private shadowInProgress = false;

  private recentBlockedStats: { count: number; loseRatePct: number; avgPnlPct: number } | null = null;
  private lastSurvivalDecision: SurvivalGuardDecision | null = null;
  private lastLoggedPosture: SurvivalPostureLevel | null = null;
  private lastPostureLogTs = 0;

  constructor(db: BotDatabase, log?: (msg: string) => void) {
    this.db = db;
    this.log = log ?? ((m) => console.log(`[learning] ${m}`));
  }

  private getPg(): PgStore | null {
    return this.db.getPgStore();
  }

  public isAutoTuneAllowed(cfg: LearningAutoTuneConfig): boolean {
    const mode = cfg.learningAutoTuneMode || "all";
    if (mode === "off") return false;
    if (mode === "all") return true;
    const isAlpacaPaper =
      String(cfg.exchange || "").toLowerCase() === "alpaca" &&
      String(cfg.alpacaBaseUrl || "").toLowerCase().includes("paper-api");
    return Boolean(cfg.dryRun || isAlpacaPaper);
  }

  public isSurvivalGuardEnforced(cfg: LearningAutoTuneConfig): boolean {
    return Boolean(cfg.portfolioSurvivalGuardEnforced) && this.isAutoTuneAllowed(cfg);
  }

  public async ensureSymbolStatesLoaded(): Promise<void> {
    if (this.symbolStatesLoaded) return;
    const pg = this.getPg();
    if (!pg) return;
    try {
      const [list, blockedEval, adjs] = await Promise.all([
        pg.loadSymbolLearningStates(),
        pg.loadLearningJournal("BLOCKED_SIGNAL", "EVALUATED", 150).catch(() => [] as LearningJournalRecord[]),
        pg.loadLearningAdjustments(30).catch(() => [] as LearningAdjustmentRecord[]),
      ]);
      for (const s of list) {
        this.symbolStates.set(s.symbol, s);
      }
      this.refreshBlockedTrapCache(blockedEval);
      const lastPostureAdj = adjs.find((a) => a.paramName === "survival_posture");
      if (lastPostureAdj) {
        this.lastLoggedPosture = NUMERIC_TO_POSTURE[Math.round(lastPostureAdj.newValue)] ?? "NORMAL";
        this.lastPostureLogTs = lastPostureAdj.createdAt;
      }
      this.symbolStatesLoaded = true;
    } catch {}
  }

  private refreshBlockedTrapCache(evaluatedBlocked: LearningJournalRecord[]): void {
    const valid = evaluatedBlocked
      .filter((b) => b.counterfactualVerdict !== null && b.counterfactualVerdict !== undefined)
      .slice(0, 100);
    if (valid.length < 10) {
      this.recentBlockedStats = null;
      return;
    }
    let wouldLose = 0;
    let sumPnl = 0;
    for (const b of valid) {
      if (b.counterfactualVerdict === "WOULD_LOSE") wouldLose++;
      sumPnl += Number(b.counterfactualPnlPct ?? 0);
    }
    this.recentBlockedStats = {
      count: valid.length,
      loseRatePct: Number(((wouldLose / valid.length) * 100).toFixed(1)),
      avgPnlPct: Number((sumPnl / valid.length).toFixed(2)),
    };
  }

  public isSymbolOnCooldown(symbol: string, cfg: LearningAutoTuneConfig): boolean {
    if (!this.isAutoTuneAllowed(cfg)) return false;
    const st = this.symbolStates.get(symbol.toUpperCase());
    return Boolean(st && st.cooldownUntilMs > Date.now());
  }

  public getSymbolScoreAdjustment(symbol: string, cfg: LearningAutoTuneConfig): number {
    if (!this.isAutoTuneAllowed(cfg)) return 0;
    const sym = symbol.toUpperCase();
    const st = this.symbolStates.get(sym);
    const cooldownPen = st && st.cooldownUntilMs > Date.now() ? (st.scorePenalty || -25) : 0;
    const shadowAdj = this.shadowSummary?.bySymbol[sym]?.scoreAdj ?? 0;
    return clamp(cooldownPen + shadowAdj, -35, 12);
  }

  public getSymbolScorePenalty(symbol: string, cfg: LearningAutoTuneConfig): number {
    return this.getSymbolScoreAdjustment(symbol, cfg);
  }

  public isSymbolShadowRisky(symbol: string, cfg: LearningAutoTuneConfig): boolean {
    if (!this.isAutoTuneAllowed(cfg)) return false;
    const prof = this.shadowSummary?.bySymbol[symbol.toUpperCase()];
    return Boolean(prof?.avoidInDefensive);
  }

  /**
   * Quant Q5: Compute Empirical Bayes Expected Value shrinkage combining symbol history
   * with the structural feature-bucket prior (RS24h, BuyVolRatio, DistanceToEMA200, 1hRSI).
   */
  public getEmpiricalBayesEvAdjustment(
    symbol: string,
    features: {
      rs24h: number;
      buyVolRatio12: number;
      distanceToEma200Pct: number;
      rsi1h: number;
      regime?: "BULL" | "BEAR";
    },
  ): { shrunkEvPct: number; scoreAdj: number; shouldBlockTrap: boolean } {
    const sym = String(symbol || "").toUpperCase();
    const symProf = this.shadowSummary?.bySymbol?.[sym];
    const n = symProf?.trades ?? 0;
    const symMean = symProf?.avgPnlPct ?? 0;
    const symWr = symProf?.winRatePct ?? 50;

    const bucketKey = computeFeatureBucketKey(
      features.rs24h,
      features.buyVolRatio12,
      features.distanceToEma200Pct,
      features.rsi1h,
    );
    const regimeKey = features.regime ? `${features.regime}:${bucketKey}` : null;
    const regimeBucket = regimeKey ? this.shadowSummary?.regimeFeatureBucketEv?.[regimeKey] : undefined;
    const bucket = this.shadowSummary?.featureBucketEv?.[bucketKey];
    const priorMean =
      regimeBucket && regimeBucket.setups >= 5
        ? regimeBucket.avgPnlPct
        : bucket && bucket.setups >= 4
          ? bucket.avgPnlPct
          : 0.05;

    const shrunkEvPct = (n * symMean + 5 * priorMean) / (n + 5);
    const shouldBlockTrap = n >= 3 && shrunkEvPct < -0.18 && symWr < 45;
    const scoreAdj = Math.round(clamp(shrunkEvPct * 30, -20, 25));

    return {
      shrunkEvPct: Number(shrunkEvPct.toFixed(3)),
      scoreAdj,
      shouldBlockTrap,
    };
  }

  public getShadowSummary(): ShadowLearningSummary | null {
    return this.shadowSummary;
  }

  public getLastSurvivalDecision(): SurvivalGuardDecision | null {
    return this.lastSurvivalDecision;
  }

  public async clearSymbolCooldown(symbol: string): Promise<{ success: boolean; symbol: string }> {
    const sym = String(symbol || "").toUpperCase().trim();
    if (!sym) return { success: false, symbol: "" };
    await this.ensureSymbolStatesLoaded();
    const prev = this.symbolStates.get(sym);
    if (prev) {
      const cleared: SymbolLearningStateRecord = {
        ...prev,
        consecutiveSl: 0,
        scorePenalty: 0,
        cooldownUntilMs: 0,
        lastReason: "MANUAL_COOLDOWN_CLEAR",
        updatedAt: Date.now(),
      };
      this.symbolStates.set(sym, cleared);
      const pg = this.getPg();
      if (pg) {
        await pg.upsertSymbolLearningState(cleared).catch(() => {});
        await pg
          .recordLearningAdjustment({
            paramName: `symbol_cooldown:${sym}`,
            oldValue: 48,
            newValue: 0,
            sampleSize: prev.wins + prev.losses,
            rationale: "Manual cooldown reset from Learning Dashboard",
            applied: true,
            createdAt: Date.now(),
          })
          .catch(() => {});
      }
      this.log(`🧠 [Learning] Manual cooldown cleared for ${sym}`);
    }
    return { success: true, symbol: sym };
  }

  // ── Level 1: Entry, Outcome (MFE/MAE), and Blocked Signal Recording ──────

  public async onPortfolioEntry(params: {
    exchange: string;
    symbol: string;
    slotIndex: number;
    entryTimeMs: number;
    entryPrice: number;
    signalPrice?: number;
    qty: number;
    allocatedQuote: number;
    snapshot?: PortfolioRadarCandidate;
    macro?: PortfolioMacroState;
    score: number;
    survivalObserve?: {
      posture: SurvivalPostureLevel;
      enforced: boolean;
      wouldBlockReason?: string | null;
    };
    observeContext?: {
      distFromBottomLowPct?: number;
      upperWickRatioPct?: number;
      atrPct?: number;
      breadthDelta4h?: number;
      benchmarkRet4h?: number;
      concurrentSignalsCount?: number;
      macroEventTag?: string | null;
      earningsTag?: string | null;
      hasShock24h?: boolean;
      mayerMultiple?: number | null;
      macroRiskIndex?: number | null;
      usdRet5dPct?: number | null;
    };
  }): Promise<string> {
    const sym = params.symbol.toUpperCase();
    const journalUid = `trade_${params.entryTimeMs}_${params.slotIndex}_${sym}`;
    const pg = this.getPg();
    if (!pg) return journalUid;

    const snap = params.snapshot;
    const entryBarSec = Math.floor(params.entryTimeMs / 1000);
    const sessionBucket = getSessionBucketLabel(entryBarSec, params.exchange);
    const marketRegime = (params.macro?.pass4h ?? true) || (params.macro?.pass1d ?? true) ? "BULL" : "BEAR";
    const oc = params.observeContext;
    const macroEventTag = oc?.macroEventTag !== undefined ? oc.macroEventTag : getMacroEventContext(entryBarSec).tag;
    const hasSignalPx = typeof params.signalPrice === "number" && params.signalPrice > 0 && params.entryPrice > 0;
    const entrySlippagePct = hasSignalPx
      ? Number((((params.entryPrice - params.signalPrice!) / params.signalPrice!) * 100).toFixed(4))
      : null;
    const rec: LearningJournalRecord = {
      journalUid,
      recordType: "TRADE",
      exchange: params.exchange,
      symbol: sym,
      status: "OPEN",
      entryTimeMs: params.entryTimeMs,
      entryPrice: params.entryPrice,
      qty: params.qty,
      allocatedQuote: params.allocatedQuote,
      features: {
        score: Number((snap?.score ?? params.score).toFixed(2)),
        rsi15: snap ? Number(snap.rsi15.toFixed(2)) : null,
        rsi1h: snap ? Number(snap.rsi1h.toFixed(2)) : null,
        buyVolRatio12: snap ? Number(snap.buyVolRatio12.toFixed(3)) : null,
        relStrength24h: snap ? Number(snap.relStrength24h.toFixed(2)) : null,
        upperSpreadPct: snap ? Number(snap.upperSpreadPct.toFixed(3)) : null,
        distEma4Pct: snap ? Number(snap.distanceToEma4Pct.toFixed(3)) : null,
        distEma200Pct: snap ? Number(snap.distanceToEma200Pct.toFixed(3)) : null,
        macroPass4h: params.macro?.pass4h ?? true,
        macroPass1d: params.macro?.pass1d ?? true,
        marketRegime,
        sessionBucket,
        macroEventTag: macroEventTag || null,
        earningsTag: oc?.earningsTag || null,
        breadthAboveEma25Pct: params.macro ? Number(params.macro.breadthAboveEma25Pct.toFixed(1)) : null,
        ...(oc
          ? {
              ...(Number.isFinite(Number(oc.distFromBottomLowPct))
                ? { distFromBottomLowPct: Number(Number(oc.distFromBottomLowPct).toFixed(3)) }
                : {}),
              ...(Number.isFinite(Number(oc.upperWickRatioPct))
                ? { upperWickRatioPct: Number(Number(oc.upperWickRatioPct).toFixed(2)) }
                : {}),
              ...(Number.isFinite(Number(oc.atrPct)) ? { atrPct: Number(Number(oc.atrPct).toFixed(3)) } : {}),
              ...(Number.isFinite(Number(oc.breadthDelta4h))
                ? { breadthDelta4h: Number(Number(oc.breadthDelta4h).toFixed(2)) }
                : {}),
              ...(Number.isFinite(Number(oc.benchmarkRet4h))
                ? { benchmarkRet4h: Number(Number(oc.benchmarkRet4h).toFixed(2)) }
                : {}),
              ...(Number.isFinite(Number(oc.concurrentSignalsCount))
                ? { concurrentSignalsCount: Math.round(Number(oc.concurrentSignalsCount)) }
                : {}),
              ...(typeof oc.hasShock24h === "boolean" ? { hasShock24h: oc.hasShock24h } : {}),
              ...(typeof oc.mayerMultiple === "number" && Number.isFinite(oc.mayerMultiple)
                ? { mayerMultiple: Number(oc.mayerMultiple.toFixed(4)) }
                : {}),
              ...(typeof oc.macroRiskIndex === "number" && Number.isFinite(oc.macroRiskIndex)
                ? { macroRiskIndex: Number(oc.macroRiskIndex.toFixed(1)) }
                : {}),
              ...(typeof oc.usdRet5dPct === "number" && Number.isFinite(oc.usdRet5dPct)
                ? { usdRet5dPct: Number(oc.usdRet5dPct.toFixed(3)) }
                : {}),
            }
          : {}),
        ...(hasSignalPx
          ? {
              signalPrice: Number(params.signalPrice!.toFixed(6)),
              entrySlippagePct,
            }
          : {}),
        ...(params.survivalObserve
          ? {
              survivalPosture: params.survivalObserve.posture,
              survivalEnforced: params.survivalObserve.enforced,
              survivalWouldBlock: params.survivalObserve.wouldBlockReason || null,
            }
          : {}),
      },
      updatedAt: Date.now(),
    };

    await pg.upsertLearningJournalBatch([rec]).catch((err) => {
      this.log(`⚠️ [Learning] Failed to log entry ${sym}: ${(err as Error).message}`);
    });
    return journalUid;
  }

  public async onPortfolioTradeClosed(params: {
    journalUid?: string;
    exchange: string;
    symbol: string;
    entryTimeMs: number;
    exitTimeMs: number;
    entryPrice: number;
    exitPrice: number;
    troughPrice: number;
    peakPrice: number;
    totalPnlQuote: number;
    allocatedQuote: number;
    exitReason: string;
    cfg: LearningAutoTuneConfig;
  }): Promise<void> {
    const pg = this.getPg();
    if (!pg) return;

    await this.ensureSymbolStatesLoaded();
    const sym = params.symbol.toUpperCase();
    const pnlPct = params.allocatedQuote > 0 ? (params.totalPnlQuote / params.allocatedQuote) * 100 : 0;
    const maePct = params.entryPrice > 0 ? Math.min(0, ((params.troughPrice - params.entryPrice) / params.entryPrice) * 100) : 0;
    const mfePct = params.entryPrice > 0 ? Math.max(0, ((params.peakPrice - params.entryPrice) / params.entryPrice) * 100) : 0;
    const holdMinutes = Math.max(0, (params.exitTimeMs - params.entryTimeMs) / 60_000);

    if (params.journalUid) {
      await pg
        .closeLearningTrade({
          journalUid: params.journalUid,
          exitTimeMs: params.exitTimeMs,
          exitPrice: params.exitPrice,
          pnlQuote: params.totalPnlQuote,
          pnlPct,
          maePct,
          mfePct,
          holdMinutes,
          exitReason: params.exitReason,
        })
        .catch(() => {});
    }

    const prev: SymbolLearningStateRecord = this.symbolStates.get(sym) ?? {
      symbol: sym,
      consecutiveSl: 0,
      wins: 0,
      losses: 0,
      totalPnlQuote: 0,
      scorePenalty: 0,
      cooldownUntilMs: 0,
      lastReason: "",
      updatedAt: Date.now(),
    };

    const isWin = params.totalPnlQuote > 0;
    const isSlHit = params.exitReason.startsWith("SL_") && params.exitReason !== "SL_BREAKEVEN" && !isWin;
    const nextConsecSl = isSlHit ? prev.consecutiveSl + 1 : (isWin || params.exitReason === "SL_BREAKEVEN") ? 0 : prev.consecutiveSl;
    let nextCooldownUntil = prev.cooldownUntilMs;
    let nextPenalty = isWin ? 0 : prev.scorePenalty;

    const autoTuneAllowed = this.isAutoTuneAllowed(params.cfg);
    if (autoTuneAllowed && nextConsecSl >= 2 && prev.totalPnlQuote + params.totalPnlQuote < 0) {
      nextCooldownUntil = Date.now() + COOLDOWN_MS;
      nextPenalty = -25;
      const rationale = `${nextConsecSl} consecutive SL hits (${params.exitReason}, PnL ${(prev.totalPnlQuote + params.totalPnlQuote).toFixed(2)}) -> 48h cooldown`;
      this.log(`🧠 [Learning] Cooldown applied to ${sym}: ${rationale}`);
      await pg
        .recordLearningAdjustment({
          paramName: `symbol_cooldown:${sym}`,
          oldValue: 0,
          newValue: 48,
          sampleSize: prev.wins + prev.losses + 1,
          rationale,
          applied: true,
          createdAt: Date.now(),
        })
        .catch(() => {});
    }

    const updated: SymbolLearningStateRecord = {
      symbol: sym,
      consecutiveSl: nextConsecSl,
      wins: prev.wins + (isWin ? 1 : 0),
      losses: prev.losses + (isWin ? 0 : 1),
      totalPnlQuote: prev.totalPnlQuote + params.totalPnlQuote,
      scorePenalty: nextPenalty,
      cooldownUntilMs: nextCooldownUntil,
      lastReason: params.exitReason,
      updatedAt: Date.now(),
    };

    this.symbolStates.set(sym, updated);
    await pg.upsertSymbolLearningState(updated).catch(() => {});
  }

  public async recordBlockedSignals(params: {
    exchange: string;
    barTimeSec: number;
    macro: PortfolioMacroState;
    candidates: PortfolioRadarCandidate[];
    extraBlocked?: Array<{ candidate: PortfolioRadarCandidate; blockReason: string; barTimeSec?: number }>;
    tp1TargetMode?: string;
  }): Promise<void> {
    const pg = this.getPg();
    if (!pg) return;

    const chosenByBarSym = new Map<string, { cand: PortfolioRadarCandidate; blockReason: string; barSec: number }>();

    const consider = (cand: PortfolioRadarCandidate, blockReason: string, explicitBarSec?: number) => {
      const barSec = explicitBarSec || cand.barTimeSec || params.barTimeSec;
      if (!barSec || barSec <= 0 || !cand.symbol || !(cand.price > 0)) return;
      const key = `${barSec}_${cand.symbol.toUpperCase()}`;
      const existing = chosenByBarSym.get(key);
      if (!existing || getBlockPriority(blockReason) > getBlockPriority(existing.blockReason)) {
        chosenByBarSym.set(key, { cand, blockReason, barSec });
      }
    };

    // Prefer structured keys from extraBlocked before display filterNotes at equal priority
    if (Array.isArray(params.extraBlocked)) {
      for (const ex of params.extraBlocked) {
        consider(ex.candidate, ex.blockReason, ex.barTimeSec);
      }
    }

    for (const c of params.candidates) {
      if (c.stage === "CROSS_8X16_FAIL_FILTERS") {
        consider(c, c.filterNote ? `FILTER:${c.filterNote}` : "FILTER_FAIL");
      } else if (c.stage === "PULLBACK_READY" && params.macro.isSitOut) {
        consider(c, "MACRO_SIT_OUT");
      }
    }

    if (chosenByBarSym.size === 0) return;

    const entries: LearningJournalRecord[] = [];
    const nowMs = Date.now();

    for (const [barSymKey, item] of chosenByBarSym.entries()) {
      if (this.recordedBlockedKeys.has(barSymKey)) continue;
      this.recordedBlockedKeys.add(barSymKey);

      const c = item.cand;
      const tpMode = normalizeTpMode(params.tp1TargetMode);
      const distTp1Pct =
        tpMode === "EMA25" && c.ema25 > 0
          ? ((c.ema25 - c.price) / c.price) * 100
          : tpMode === "EMA75" && c.ema75 > 0
            ? ((c.ema75 - c.price) / c.price) * 100
            : c.distanceToEma200Pct;
      const sessionBucket = getSessionBucketLabel(item.barSec, params.exchange);
      const marketRegime = params.macro.pass4h || params.macro.pass1d ? "BULL" : "BEAR";
      const macroEventTag = getMacroEventContext(item.barSec).tag;
      entries.push({
        journalUid: `blocked_${item.barSec}_${c.symbol.toUpperCase()}`,
        recordType: "BLOCKED_SIGNAL",
        exchange: params.exchange,
        symbol: c.symbol.toUpperCase(),
        status: "PENDING_EVAL",
        entryTimeMs: nowMs,
        entryPrice: c.price,
        blockReason: item.blockReason,
        features: {
          barTimeSec: item.barSec,
          score: Number(c.score.toFixed(2)),
          rsi15: Number(c.rsi15.toFixed(2)),
          rsi1h: Number(c.rsi1h.toFixed(2)),
          buyVolRatio12: Number(c.buyVolRatio12.toFixed(3)),
          relStrength24h: Number(c.relStrength24h.toFixed(2)),
          upperSpreadPct: Number(c.upperSpreadPct.toFixed(3)),
          distEma200Pct: Number(c.distanceToEma200Pct.toFixed(3)),
          tp1TargetMode: tpMode,
          distTp1Pct: Number(distTp1Pct.toFixed(3)),
          sessionBucket,
          marketRegime,
          macroEventTag,
          macroPass4h: params.macro.pass4h,
          macroPass1d: params.macro.pass1d,
          isSitOut: params.macro.isSitOut,
          breadthAboveEma25Pct: Number(params.macro.breadthAboveEma25Pct.toFixed(1)),
        },
        updatedAt: nowMs,
      });
    }

    if (this.recordedBlockedKeys.size > 3000) {
      const keep = Array.from(this.recordedBlockedKeys).slice(-1200);
      this.recordedBlockedKeys = new Set(keep);
    }

    if (entries.length > 0) {
      await pg.upsertLearningJournalBatch(entries).catch(() => {});
    }
  }

  // ── Level 2: Counterfactual Verification, Journal Sync & Shadow Learning ──

  public async evaluatePendingBlockedSignals(exchange: string, slPct = 3.5): Promise<number> {
    const pg = this.getPg();
    if (!pg) return 0;

    const cutoffMs = Date.now() - BLOCKED_EVAL_HORIZON_MS;
    const pending = await pg.loadPendingBlockedSignals(cutoffMs, 150).catch(() => [] as LearningJournalRecord[]);
    if (pending.length === 0) return 0;

    const syms = Array.from(new Set(pending.map((p) => p.symbol)));
    const candleMap = await pg.loadMarketCandlesBatch(exchange, syms, "15m", 240).catch(() => new Map());
    const isAlpaca = exchange.toLowerCase() === "alpaca";

    const updates: Array<{
      journalUid: string;
      counterfactualPnlPct: number;
      counterfactualVerdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL";
      maePct: number;
      mfePct: number;
    }> = [];

    const slTriggerPct = -Math.max(1.5, slPct || 3.5);
    const costPct = estimateRoundTripCostPct(exchange, syms);

    for (const p of pending) {
      let bars: Candle[] = candleMap.get(p.symbol) || [];
      if (isAlpaca) {
        bars = bars.filter((b) => isUsRegularTradingHoursBar(b.time, "15m"));
      }
      const barStartSec = Number(p.features?.barTimeSec || Math.floor(p.entryTimeMs / 1000));
      const futureBars = bars.filter((b) => b.time > barStartSec).slice(0, 16);
      if (futureBars.length < 8 || p.entryPrice <= 0) continue;

      const distTargetPct =
        p.features?.distTp1Pct !== undefined && Number.isFinite(Number(p.features.distTp1Pct))
          ? Number(p.features.distTp1Pct)
          : Number(p.features?.distEma200Pct);
      const sim = simulateMaFanCounterfactual(p.entryPrice, distTargetPct, slTriggerPct, futureBars, costPct);

      updates.push({
        journalUid: p.journalUid,
        counterfactualPnlPct: Number(sim.finalRet.toFixed(3)),
        counterfactualVerdict: sim.verdict,
        maePct: Number(sim.maePct.toFixed(3)),
        mfePct: Number(sim.mfePct.toFixed(3)),
      });
    }

    if (updates.length > 0) {
      await pg.updateBlockedEvaluations(updates).catch(() => {});
      const evaluated = await pg
        .loadLearningJournal("BLOCKED_SIGNAL", "EVALUATED", 150)
        .catch(() => [] as LearningJournalRecord[]);
      this.refreshBlockedTrapCache(evaluated);
    }
    return updates.length;
  }

  /**
   * Level 2 Upgrade: Track 4h forward market action after live closed trades exit
   * to measure Stop-Hunt rate (SL followed by rebound above entry) and Early-TP regret.
   */
  public async evaluatePostExitTrades(exchange: string): Promise<number> {
    const pg = this.getPg();
    if (!pg) return 0;

    const cutoffMs = Date.now() - BLOCKED_EVAL_HORIZON_MS;
    const closedTrades = await pg.loadLearningJournal("TRADE", "CLOSED", 100).catch(() => [] as LearningJournalRecord[]);
    const pending = closedTrades.filter(
      (t) =>
        Boolean(t.exitTimeMs && t.exitTimeMs <= cutoffMs) &&
        Boolean(t.exitPrice && t.exitPrice > 0) &&
        !t.features?.postExitEvaluated,
    );
    if (pending.length === 0) return 0;

    const syms = Array.from(new Set(pending.map((t) => t.symbol.toUpperCase())));
    const candleMap = await pg.loadMarketCandlesBatch(exchange, syms, "15m", 240).catch(() => new Map());
    const isAlpaca = exchange.toLowerCase() === "alpaca";
    let evaluatedCount = 0;

    for (const t of pending) {
      let bars: Candle[] = candleMap.get(t.symbol.toUpperCase()) || [];
      if (isAlpaca) {
        bars = bars.filter((b) => isUsRegularTradingHoursBar(b.time, "15m"));
      }
      const exitSec = Math.floor(Number(t.exitTimeMs) / 1000);
      const futureBars = bars.filter((b) => b.time > exitSec).slice(0, 16);
      if (futureBars.length < 6 || !t.exitPrice || t.exitPrice <= 0) continue;

      const maxHigh = Math.max(...futureBars.map((b) => b.high));
      const minLow = Math.min(...futureBars.map((b) => b.low));
      const lastClose = futureBars[futureBars.length - 1]!.close;
      const postExitMaxMovePct = Number((((maxHigh - t.exitPrice) / t.exitPrice) * 100).toFixed(3));
      const postExitMinMovePct = Number((((minLow - t.exitPrice) / t.exitPrice) * 100).toFixed(3));
      const postExitEndMovePct = Number((((lastClose - t.exitPrice) / t.exitPrice) * 100).toFixed(3));

      const pnl = Number(t.pnlQuote ?? 0);
      const isLoss = pnl < -0.05 || String(t.exitReason || "").includes("SL_");
      const isWin = pnl > 0.05;
      let postExitVerdict: "STOP_HUNT" | "EARLY_TP" | "GOOD_EXIT" = "GOOD_EXIT";
      if (isLoss && t.entryPrice > 0 && (maxHigh >= t.entryPrice * 1.005 || (postExitMaxMovePct >= 1.5 && maxHigh >= t.entryPrice))) {
        postExitVerdict = "STOP_HUNT";
      } else if (isWin && postExitMaxMovePct >= 1.5) {
        postExitVerdict = "EARLY_TP";
      }

      const nextFeatures = {
        ...(t.features || {}),
        postExitEvaluated: true,
        postExitBars: futureBars.length,
        postExitMaxMovePct,
        postExitMinMovePct,
        postExitEndMovePct,
        postExitVerdict,
      };
      await pg.updateLearningJournalFeatures(t.journalUid, nextFeatures).catch(() => {});
      evaluatedCount++;
    }
    return evaluatedCount;
  }

  /**
   * Synchronize `trade_learning_journal` closed TRADE rows & `symbol_learning_state`
   * against reconciled exchange trade rounds in `BotDatabase` so inaccurate pre-sync
   * fills (e.g. quote spike artifacts) do not poison SL tuning or symbol rankings.
   */
  public async reconcileJournalFromOrders(
    exchange: string,
  ): Promise<{ updatedTrades: number; updatedSymbols: number }> {
    const pg = this.getPg();
    if (!pg) return { updatedTrades: 0, updatedSymbols: 0 };

    await this.ensureSymbolStatesLoaded();
    const roundsRes = this.db.getTradeRounds("ALL");
    const closedRounds = roundsRes.rounds.filter((r) => r.status === "CLOSED");
    if (closedRounds.length === 0) return { updatedTrades: 0, updatedSymbols: 0 };

    // Load both OPEN and CLOSED trade rows so externally closed slots (e.g. Alpaca queued fills) also close cleanly
    const journalTrades = await pg.loadLearningJournal("TRADE", undefined, 500).catch(() => [] as LearningJournalRecord[]);
    const exLower = exchange.toLowerCase();
    const filteredJournal = journalTrades.filter((j) => j.exchange.toLowerCase() === exLower);
    if (filteredJournal.length === 0) return { updatedTrades: 0, updatedSymbols: 0 };

    const roundsBySym = new Map<string, typeof closedRounds>();
    for (const r of closedRounds) {
      const sym = String((r as any).symbol || "").toUpperCase().replace(/[\/\-_:]/g, "");
      if (!sym) continue;
      const list = roundsBySym.get(sym) || [];
      list.push(r);
      roundsBySym.set(sym, list);
    }

    let updatedTrades = 0;
    const touchedSymbols = new Set<string>();

    for (const j of filteredJournal) {
      const cleanSym = j.symbol.toUpperCase().replace(/[\/\-_:]/g, "");
      const symRounds = roundsBySym.get(cleanSym);
      if (!symRounds || symRounds.length === 0) continue;

      const jEntryMs = normalizeTsMs(j.entryTimeMs);
      let matched = symRounds.find((r) => {
        const sMs = normalizeTsMs(r.startTime);
        const eMs = normalizeTsMs(r.endTime || r.startTime);
        return jEntryMs >= sMs - 15 * 60_000 && jEntryMs <= eMs + 15 * 60_000;
      });

      if (!matched) {
        const sameDay = symRounds
          .map((r) => ({ r, diff: Math.abs(jEntryMs - normalizeTsMs(r.startTime)) }))
          .filter((x) => x.diff <= 24 * 3_600_000)
          .sort((a, b) => a.diff - b.diff);
        if (sameDay.length > 0 && (j.status !== "CLOSED" || Math.abs((j.pnlQuote ?? 0) - sameDay[0]!.r.netPnlQuote) > 0.5)) {
          matched = sameDay[0]!.r;
        }
      }

      if (!matched) continue;
      const realPnl = Number(matched.netPnlQuote ?? matched.grossPnlQuote ?? 0);
      const storedPnl = j.pnlQuote !== null && j.pnlQuote !== undefined ? Number(j.pnlQuote) : NaN;
      const wasOpen = j.status !== "CLOSED";
      if (!wasOpen && Number.isFinite(storedPnl) && Math.abs(realPnl - storedPnl) <= 0.25) continue;

      const alloc = Number(j.allocatedQuote || matched.buyCostQuote || 0);
      const nextPnlPct = alloc > 0 ? (realPnl / alloc) * 100 : Number(matched.grossPnlPct || 0);
      const nextHoldMin = matched.durationMs > 0 ? matched.durationMs / 60_000 : Number(j.holdMinutes ?? 0);
      const nextEntryPx = matched.avgBuyPrice > 0 ? matched.avgBuyPrice : j.entryPrice;
      const nextExitPx = matched.avgSellPrice > 0 ? matched.avgSellPrice : Number(j.exitPrice ?? j.entryPrice);

      if (wasOpen) {
        const exitMs = matched.endTime ? normalizeTsMs(matched.endTime) : Date.now();
        await pg
          .closeLearningTrade({
            journalUid: j.journalUid,
            exitTimeMs: exitMs,
            exitPrice: nextExitPx,
            pnlQuote: Number(realPnl.toFixed(4)),
            pnlPct: Number(nextPnlPct.toFixed(3)),
            maePct: Number(j.maePct ?? 0),
            mfePct: Number(j.mfePct ?? 0),
            holdMinutes: Number(nextHoldMin.toFixed(2)),
            exitReason: j.exitReason || "RECONCILED_ALPACA",
          })
          .catch(() => {});
        j.status = "CLOSED";
      } else {
        const nextExitReason =
          realPnl < -0.5 && j.exitReason === "SL_BREAKEVEN" ? "RECONCILED_SL" : (j.exitReason ?? null);
        await pg
          .updateLearningJournalTradeFill({
            journalUid: j.journalUid,
            entryPrice: nextEntryPx,
            exitPrice: nextExitPx,
            pnlQuote: Number(realPnl.toFixed(4)),
            pnlPct: Number(nextPnlPct.toFixed(3)),
            holdMinutes: Number(nextHoldMin.toFixed(2)),
            exitReason: nextExitReason,
          })
          .catch(() => {});
      }

      j.entryPrice = nextEntryPx;
      j.exitPrice = nextExitPx;
      j.pnlQuote = realPnl;
      j.pnlPct = nextPnlPct;
      updatedTrades++;
      touchedSymbols.add(j.symbol.toUpperCase());
    }

    // Recompute symbol_learning_state for touched symbols OR any symbol whose cached state diverges from closed TRADE rows
    const allJournalSyms = new Set(filteredJournal.filter((j) => j.status === "CLOSED").map((j) => j.symbol.toUpperCase()));
    let updatedSymbols = 0;
    for (const sym of allJournalSyms) {
      const symTrades = filteredJournal.filter((j) => j.status === "CLOSED" && j.symbol.toUpperCase() === sym);
      let wins = 0;
      let losses = 0;
      let totalPnlQuote = 0;
      for (const t of symTrades) {
        const p = Number(t.pnlQuote ?? 0);
        totalPnlQuote += p;
        if (p > 0) wins++;
        else losses++;
      }
      const prev = this.symbolStates.get(sym);
      const stateMatches =
        prev &&
        prev.wins === wins &&
        prev.losses === losses &&
        Math.abs(prev.totalPnlQuote - totalPnlQuote) <= 0.25;
      if (!touchedSymbols.has(sym) && stateMatches) continue;

      const nextState: SymbolLearningStateRecord = {
        symbol: sym,
        consecutiveSl: losses === 0 ? 0 : (prev?.consecutiveSl ?? 0),
        wins,
        losses,
        totalPnlQuote: Number(totalPnlQuote.toFixed(4)),
        scorePenalty: wins > losses || totalPnlQuote >= 0 ? 0 : (prev?.scorePenalty ?? 0),
        cooldownUntilMs: losses === 0 ? 0 : (prev?.cooldownUntilMs ?? 0),
        lastReason: prev?.lastReason || "JOURNAL_RECONCILED",
        updatedAt: Date.now(),
      };
      this.symbolStates.set(sym, nextState);
      await pg.upsertSymbolLearningState(nextState).catch(() => {});
      updatedSymbols++;
    }

    if (updatedTrades > 0 || updatedSymbols > 0) {
      this.log(`🧹 [Learning Reconcile] Updated ${updatedTrades} journal trade(s) & ${updatedSymbols} symbol state(s) from reconciled fills`);
    }
    return { updatedTrades, updatedSymbols };
  }

  /**
   * Rolling Historical Shadow Walk-Forward Learner:
   * Scans recent `market_candles` (or fallback RAM snapshots) across the active portfolio universe
   * using causal bar-by-bar MA-Fan signals to calibrate per-symbol expectancy, bear-regime risk,
   * low-breadth risk, and Win-MAE P80 without waiting months for 20 live closed trades.
   */
  public async runShadowWalkForward(opts: ShadowWalkForwardContext): Promise<ShadowLearningSummary | null> {
    if (this.shadowInProgress) return this.shadowSummary;
    const now = Date.now();
    if (!opts.force && this.shadowSummary && now - this.lastShadowRunTs < SHADOW_REFRESH_INTERVAL_MS) {
      return this.shadowSummary;
    }

    const cleanSymbols = Array.from(new Set(opts.symbols.map((s) => s.toUpperCase().trim()).filter(Boolean)));
    if (cleanSymbols.length === 0) return this.shadowSummary;

    this.shadowInProgress = true;
    try {
      const ex = (opts.exchange || "binance").toLowerCase();
      const isAlpaca = ex === "alpaca";
      const pg = this.getPg();
      const benchSym = (opts.benchmarkSymbol || (isAlpaca ? "SPY" : "BTCUSDT")).toUpperCase();
      const usdSym = (opts.usdProxySymbol || (isAlpaca ? "UUP" : "EURUSDT")).toUpperCase();
      const allNeed15m = Array.from(new Set([benchSym, ...cleanSymbols]));

      const pg15m = pg
        ? await pg.loadMarketCandlesBatch(ex, allNeed15m, "15m", 2400).catch(() => new Map<string, Candle[]>())
        : new Map<string, Candle[]>();
      const pg1h = pg
        ? await pg.loadMarketCandlesBatch(ex, cleanSymbols, "1h", 700).catch(() => new Map<string, Candle[]>())
        : new Map<string, Candle[]>();
      const pg1d = pg
        ? await pg.loadMarketCandlesBatch(ex, Array.from(new Set([benchSym, usdSym])), "1d", 320).catch(() => new Map<string, Candle[]>())
        : new Map<string, Candle[]>();

      const pickClosedDaily = (dbBars: Candle[] | undefined, fbBars: Candle[] | undefined): Candle[] => {
        const dbClosed = (dbBars || []).filter((c) => c.isClosed !== false);
        const fbClosed = (fbBars || []).filter((c) => c.isClosed !== false);
        const chosen = dbClosed.length >= fbClosed.length ? dbClosed : fbClosed;
        return chosen.slice().sort((a, b) => a.time - b.time);
      };
      const bench1d = pickClosedDaily(pg1d.get(benchSym), opts.benchmarkFallback1d);
      const usd1d = pickClosedDaily(pg1d.get(usdSym), opts.usdProxyFallback1d);
      const bench1dRegimes = bench1d.map((_, dIdx) => computeDailyMacroRegimeAt(bench1d, dIdx, ex));
      const usd1dRets = usd1d.map((_, uIdx) => computeUsdStrength5dAt(usd1d, uIdx, !isAlpaca));

      const getBars = (sym: string, tf: "15m" | "1h"): Candle[] => {
        const dbBars = (tf === "15m" ? pg15m.get(sym) : pg1h.get(sym)) || [];
        const fb = opts.fallbackSnapshots?.get(sym);
        const fbBars = tf === "15m"
          ? (sym === benchSym && opts.benchmarkFallback15m?.length ? opts.benchmarkFallback15m : fb?.c15)
          : fb?.c1h;
        let chosen = dbBars.length >= (tf === "15m" ? 220 : 20) ? dbBars : (fbBars && fbBars.length > dbBars.length ? fbBars : dbBars);
        chosen = chosen.filter((c) => c.isClosed !== false);
        if (isAlpaca) {
          chosen = chosen.filter((c) => isUsRegularTradingHoursBar(c.time, tf));
        }
        return chosen;
      };

      // Build benchmark regime series & time-indexed universe breadth
      const bench15 = getBars(benchSym, "15m");
      const benchStateByTime = new Map<number, { ret24h: number; ret4h: number; isBear: boolean }>();
      if (bench15.length >= 200) {
        const benchCloses = bench15.map((c) => c.close);
        const benchM200 = calcEMA(benchCloses, 200);
        for (let i = 200; i < bench15.length; i++) {
          const ref24 = bench15[Math.max(0, i - 96)]!.open;
          const ref4 = bench15[Math.max(0, i - 16)]!.open;
          const ret24h = ref24 > 0 ? ((bench15[i]!.close - ref24) / ref24) * 100 : 0;
          const ret4h = ref4 > 0 ? ((bench15[i]!.close - ref4) / ref4) * 100 : 0;
          benchStateByTime.set(bench15[i]!.time, {
            ret24h,
            ret4h,
            isBear: bench15[i]!.close < benchM200[i]!,
          });
        }
      }

      type PrecalcCoin = {
        symbol: string;
        c15: Candle[];
        c1h: Candle[];
        m4: number[];
        m8: number[];
        m16: number[];
        m25: number[];
        m75: number[];
        m200: number[];
        rsi15: number[];
        macdHist15: number[];
        rsi1h: number[];
      };

      const precalc: PrecalcCoin[] = [];
      const breadthByTime = new Map<number, { above25: number; total: number }>();
      let barSum = 0;

      for (const sym of cleanSymbols) {
        const c15 = getBars(sym, "15m");
        if (c15.length < 220) continue;
        const c1h = getBars(sym, "1h");
        const closes15 = c15.map((c) => c.close);
        const m4 = calcEMA(closes15, 4);
        const m8 = calcEMA(closes15, 8);
        const m16 = calcEMA(closes15, 16);
        const m25 = calcEMA(closes15, 25);
        const m75 = calcEMA(closes15, 75);
        const m200 = calcEMA(closes15, 200);
        const rsi15 = calcRSI(c15, 14);
        const macdHist15 = calcMACD(c15).hist;
        const rsi1h = c1h.length >= 20 ? calcRSI(c1h, 14) : [];

        if (sym !== benchSym) {
          for (let i = 200; i < c15.length; i++) {
            const t = c15[i]!.time;
            const cur = breadthByTime.get(t) || { above25: 0, total: 0 };
            cur.total++;
            if (c15[i]!.close > m25[i]!) cur.above25++;
            breadthByTime.set(t, cur);
          }
        }

        barSum += c15.length;
        precalc.push({ symbol: sym, c15, c1h, m4, m8, m16, m25, m75, m200, rsi15, macdHist15, rsi1h });
      }

      if (precalc.length === 0) return this.shadowSummary;

      const slTriggerPct = -Math.max(1.5, Math.min(5.0, opts.slPct || 3.5));
      const costPct = estimateRoundTripCostPct(ex, cleanSymbols);
      const baseMinVolR = Number.isFinite(Number(opts.minVolRatio)) ? Number(opts.minVolRatio) : 1.3;
      const baseMinRs = Number.isFinite(Number(opts.minRelStrength)) ? Number(opts.minRelStrength) : -1.0;
      const baseMinRsi1h = Number.isFinite(Number(opts.minRsi1h)) ? Number(opts.minRsi1h) : 40;
      const baseTpMode = normalizeTpMode(opts.tpMode);
      const basePullbackMode = normalizePullbackMode(opts.pullbackMode);

      const bySymbol: Record<string, ShadowSymbolProfile> = {};
      const tuningEvents: TuningEvent[] = [];
      const winMaeAbs: number[] = [];
      const featureBucketAcc = new Map<string, { setups: number; wins: number; sumPnl: number }>();
      const regimeBucketAcc = new Map<string, { setups: number; wins: number; sumPnl: number }>();
      const sessionAcc = new Map<string, { setups: number; wins: number; sumPnl: number }>();
      const primaryReplaySetups: ShadowReplaySetupInput[] = [];
      const observeSetupRecords: ObserveSetupInput[] = [];
      const openSessionAcc = { setups: 0, wins: 0, sumPnl: 0 };

      let totalSetups = 0;
      let totalWins = 0;
      let totalPnlSum = 0;
      let sumWinPct = 0;
      let sumLossAbsPct = 0;

      let bearSetups = 0;
      let bearWins = 0;
      let bearPnlSum = 0;

      let lowBreadthSetups = 0;
      let lowBreadthWins = 0;
      let lowBreadthPnlSum = 0;

      for (const coin of precalc) {
        const n = coin.c15.length;
        // Primary shadow state machine (kept identical to live strategy + backtest)
        let hadBottomFan = false;
        let fanStartIdx = -1;
        let waitingPullback = false;
        let waitFromIdx = -1;
        let crossWasGap = false;
        let bottomLow = Infinity;
        let lastEntryIdx = -999;

        // Independent raw setup state machine strictly for filter what-if analysis
        let rawHadBottomFan = false;
        let rawFanStartIdx = -1;
        let rawWaitingPullback = false;
        let rawWaitFromIdx = -1;
        let rawCrossWasGap = false;

        let p1 = -1;
        let p1d = -1;
        let pUsd = -1;

        let sTrades = 0;
        let sWins = 0;
        let sWinSum = 0;
        let sLossAbs = 0;
        let sPnlSum = 0;
        let sMaeSum = 0;
        let sMfeSum = 0;

        for (let idx = 200; idx < n - 9; idx++) {
          const c = coin.c15[idx]!;
          while (p1 + 1 < coin.c1h.length && coin.c1h[p1 + 1]!.time + 3600 <= c.time) {
            p1++;
          }
          while (p1d + 1 < bench1d.length && bench1d[p1d + 1]!.time + 86400 <= c.time) {
            p1d++;
          }
          while (pUsd + 1 < usd1d.length && usd1d[pUsd + 1]!.time + 86400 <= c.time) {
            pUsd++;
          }

          if (waitingPullback && (idx - waitFromIdx >= 12 || coin.m8[idx]! < coin.m16[idx]!)) {
            waitingPullback = false;
            crossWasGap = false;
          }
          if (rawWaitingPullback && (idx - rawWaitFromIdx >= 12 || coin.m8[idx]! < coin.m16[idx]!)) {
            rawWaitingPullback = false;
            rawCrossWasGap = false;
          }

          const prev = idx - 1;
          const prevBar = coin.c15[prev]!;
          const limitP = coin.m4[prev]!;
          const limitP8 = coin.m8[prev]!;
          const touchedEma4 = limitP > 0 && coin.m8[prev]! >= coin.m16[prev]! && c.low <= limitP * 1.001;
          const curBarGap = isGapInducedBar(prevBar, c, ex);
          const ema8HoldOk =
            limitP8 > 0 &&
            coin.m8[prev]! >= coin.m16[prev]! &&
            !rawCrossWasGap &&
            !curBarGap &&
            c.low <= limitP8 * 1.0015 &&
            c.close >= coin.m8[idx]! * 0.999;
          const primaryTriggered =
            basePullbackMode === "EMA8_HOLD"
              ? limitP8 > 0 &&
                coin.m8[prev]! >= coin.m16[prev]! &&
                !crossWasGap &&
                !curBarGap &&
                c.low <= limitP8 * 1.0015 &&
                c.close >= coin.m8[idx]! * 0.999
              : touchedEma4;
          const activePrimary = waitingPullback && idx > waitFromIdx && idx - lastEntryIdx >= 8 && primaryTriggered;
          const activeRaw = rawWaitingPullback && idx > rawWaitFromIdx && (touchedEma4 || ema8HoldOk);

          if (activePrimary || activeRaw) {
            const rsi1hVal = coin.rsi1h.length >= 20 && p1 >= 14 ? (coin.rsi1h[p1] ?? 50) : 50;
            let upV = 0;
            let dnV = 0;
            for (let k = Math.max(0, prev - 11); k <= prev; k++) {
              const b = coin.c15[k]!;
              const qv = (b.volume || 1) * b.close;
              if (b.close >= b.open) upV += qv;
              else dnV += qv;
            }
            const volR = dnV > 0 ? upV / dnV : 2.0;

            const ref24 = coin.c15[Math.max(0, prev - 96)]!;
            const coinRet24 = ref24.open > 0 ? ((prevBar.close - ref24.open) / ref24.open) * 100 : 0;
            const benchInfo = benchStateByTime.get(prevBar.time);
            const rs24 = coinRet24 - (benchInfo?.ret24h ?? 0);
            const entryPrice = limitP > 0 ? Math.min(c.open, limitP) : c.close;
            const entryPriceEma8 = limitP8 > 0 ? Math.min(c.open, limitP8 * 1.001) : entryPrice;
            const futureBars = coin.c15.slice(idx + 1, Math.min(n, idx + 33));

            if (entryPrice > 0 && futureBars.length >= 8) {
              const dist25 = ((coin.m25[prev]! - entryPrice) / entryPrice) * 100;
              const dist75 = ((coin.m75[prev]! - entryPrice) / entryPrice) * 100;
              const dist200 = ((coin.m200[prev]! - entryPrice) / entryPrice) * 100;
              const dist25Ema8 = ((coin.m25[prev]! - entryPriceEma8) / entryPriceEma8) * 100;
              const dist75Ema8 = ((coin.m75[prev]! - entryPriceEma8) / entryPriceEma8) * 100;
              const dist200Ema8 = ((coin.m200[prev]! - entryPriceEma8) / entryPriceEma8) * 100;

              const curAtrPct = Math.max(0.12, calcCandleAtrPct14(coin.c15, prev));
              const isAlpacaEx = ex === "alpaca";
              const effDist200 = basePullbackMode === "EMA8_HOLD" ? dist200Ema8 : dist200;
              const snrAtr = (isAlpacaEx ? effDist200 : Math.max(0.8, effDist200)) / curAtrPct;
              const minSnrAtr = isAlpacaEx ? 1.25 : 0.65;

              if (activeRaw) {
                tuningEvents.push({
                  setupId: `${coin.symbol}|${rawWaitFromIdx}`,
                  symbol: coin.symbol,
                  barIdx: idx,
                  timeSec: c.time,
                  entryPrice,
                  entryPriceEma8,
                  ema8HoldOk,
                  snrAtr,
                  minSnrAtr,
                  rsi1h: rsi1hVal,
                  volR,
                  rs24,
                  dist25,
                  dist75,
                  dist200,
                  dist25Ema8,
                  dist75Ema8,
                  dist200Ema8,
                  futureBars,
                });
              }

              if (activePrimary && rsi1hVal >= baseMinRsi1h && volR >= baseMinVolR && rs24 >= baseMinRs) {
                const useEma8Primary = basePullbackMode === "EMA8_HOLD" && entryPriceEma8 > 0;
                const simEntryPrice = useEma8Primary ? entryPriceEma8 : entryPrice;
                const d25 = useEma8Primary ? dist25Ema8 : dist25;
                const d75 = useEma8Primary ? dist75Ema8 : dist75;
                const d200 = useEma8Primary ? dist200Ema8 : dist200;
                const distTarget = baseTpMode === "EMA25" ? d25 : baseTpMode === "EMA75" ? d75 : d200;
                const sim = simulateMaFanCounterfactual(simEntryPrice, distTarget, slTriggerPct, futureBars, costPct);

                lastEntryIdx = idx;
                waitingPullback = false;
                crossWasGap = false;
                hadBottomFan = false;

                totalSetups++;
                totalPnlSum += sim.finalRet;
                sTrades++;
                sPnlSum += sim.finalRet;
                sMaeSum += sim.maePct;
                sMfeSum += sim.mfePct;

                const isWin = sim.finalRet > 0;
                if (isWin) {
                  totalWins++;
                  sumWinPct += sim.finalRet;
                  sWins++;
                  sWinSum += sim.finalRet;
                  winMaeAbs.push(Math.abs(sim.maePct));
                } else {
                  sumLossAbsPct += Math.abs(sim.finalRet);
                  sLossAbs += Math.abs(sim.finalRet);
                }

                if (benchInfo?.isBear) {
                  bearSetups++;
                  if (isWin) bearWins++;
                  bearPnlSum += sim.finalRet;
                }

                const bStats = breadthByTime.get(prevBar.time);
                const barBreadthPct = bStats && bStats.total >= 5 ? (bStats.above25 / bStats.total) * 100 : 50;
                const prevBStats = breadthByTime.get(coin.c15[Math.max(0, prev - 16)]!.time);
                const prevBreadthPct = prevBStats && prevBStats.total >= 5 ? (prevBStats.above25 / prevBStats.total) * 100 : barBreadthPct;
                const breadthDelta4h = barBreadthPct - prevBreadthPct;
                const benchmarkRet4h = benchInfo?.ret4h ?? 0;
                const distFromBottomLowPct = Number.isFinite(bottomLow) && bottomLow > 0 ? ((simEntryPrice - bottomLow) / bottomLow) * 100 : 0;
                const upperWickRatioPct = c.high > c.low ? ((c.high - Math.max(c.open, c.close)) / (c.high - c.low)) * 100 : 0;
                const macroEv = getMacroEventContext(c.time);
                const hasShock24h = detectRecentShock24h(coin.c15, prev, curAtrPct);
                bottomLow = Infinity;
                if (bStats && bStats.total >= 5 && barBreadthPct < 30) {
                  lowBreadthSetups++;
                  if (isWin) lowBreadthWins++;
                  lowBreadthPnlSum += sim.finalRet;
                }

                const bKey = computeFeatureBucketKey(rs24, volR, d200, rsi1hVal);
                const fb = featureBucketAcc.get(bKey) || { setups: 0, wins: 0, sumPnl: 0 };
                fb.setups++;
                if (isWin) fb.wins++;
                fb.sumPnl += sim.finalRet;
                featureBucketAcc.set(bKey, fb);

                const regKey = `${benchInfo?.isBear ? "BEAR" : "BULL"}:${bKey}`;
                const rfb = regimeBucketAcc.get(regKey) || { setups: 0, wins: 0, sumPnl: 0 };
                rfb.setups++;
                if (isWin) rfb.wins++;
                rfb.sumPnl += sim.finalRet;
                regimeBucketAcc.set(regKey, rfb);

                const sessKey = getSessionBucketLabel(c.time, ex);
                const ss = sessionAcc.get(sessKey) || { setups: 0, wins: 0, sumPnl: 0 };
                ss.setups++;
                if (isWin) ss.wins++;
                ss.sumPnl += sim.finalRet;
                sessionAcc.set(sessKey, ss);

                const upSpread = coin.m200[prev]! > 0 ? ((coin.m200[prev]! - coin.m75[prev]!) / coin.m200[prev]!) * 100 : 0;
                const candScore = Math.min(3, volR) * 20 + Math.max(-5, Math.min(10, rs24)) * 3 - upSpread * 10;
                const exitBar = futureBars[Math.min(futureBars.length - 1, Math.max(0, sim.exitBarsHeld - 1))];
                primaryReplaySetups.push({
                  symbol: coin.symbol,
                  entryTimeSec: c.time,
                  exitTimeSec: exitBar ? exitBar.time : c.time + 900,
                  score: candScore,
                  pnlPct: sim.finalRet,
                  isBearMacro: Boolean(benchInfo?.isBear),
                  breadthPct: barBreadthPct,
                });
                const dMacro = p1d >= 0 ? bench1dRegimes[p1d] : undefined;
                const dUsdRet = pUsd >= 0 ? usd1dRets[pUsd] : null;
                observeSetupRecords.push({
                  entryTimeSec: c.time,
                  score: candScore,
                  pnlPct: sim.finalRet,
                  distFromBottomLowPct,
                  upperWickRatioPct,
                  breadthDelta4h,
                  benchmarkRet4h,
                  macroEventInWindow: macroEv.inWindow,
                  macroEventDay: macroEv.isEventDay,
                  hasShock24h,
                  mayerMultiple: dMacro?.mayerMultiple ?? null,
                  macroRiskIndex: dMacro?.macroRiskIndex ?? null,
                  usdRet5dPct: dUsdRet ?? null,
                  checkpointRetPct: sim.checkpointRetPct,
                });

                if (isOpeningSession45mBar(coin.c15, idx, ex, false)) {
                  openSessionAcc.setups++;
                  if (isWin) openSessionAcc.wins++;
                  openSessionAcc.sumPnl += sim.finalRet;
                }
              }
            }
          }

          const isBottomFan =
            coin.m4[idx]! < coin.m8[idx]! &&
            coin.m8[idx]! < coin.m16[idx]! &&
            coin.m16[idx]! < coin.m25[idx]! &&
            coin.m25[idx]! < coin.m75[idx]! &&
            coin.m75[idx]! < coin.m200[idx]!;
          if (isBottomFan) {
            hadBottomFan = true;
            fanStartIdx = idx;
            bottomLow = Math.min(bottomLow, c.low);
            rawHadBottomFan = true;
            rawFanStartIdx = idx;
          }
          if (hadBottomFan && idx - fanStartIdx > 20 && !waitingPullback) {
            hadBottomFan = false;
            bottomLow = Infinity;
          }
          if (rawHadBottomFan && idx - rawFanStartIdx > 20 && !rawWaitingPullback) {
            rawHadBottomFan = false;
          }

          const is8Cross16Up = coin.m8[idx - 1]! <= coin.m16[idx - 1]! && coin.m8[idx]! > coin.m16[idx]!;
          if (is8Cross16Up) {
            const rsiOk = (coin.rsi15[idx] ?? 0) >= 35 && (coin.rsi15[idx] ?? 0) > (coin.rsi15[idx - 1] ?? 0);
            const macdOk = (coin.macdHist15[idx] ?? 0) > (coin.macdHist15[idx - 1] ?? 0);
            if (rsiOk && macdOk) {
              const prevPrevBar = idx >= 2 ? coin.c15[idx - 2] : undefined;
              const postOpenBarGap = Boolean(
                prevBar && prevPrevBar && prevBar.time - prevPrevBar.time > 3600 && isGapInducedBar(prevPrevBar, prevBar, ex),
              );
              const crossGap = isGapInducedBar(prevBar, c, ex) || postOpenBarGap;
              if (!waitingPullback && hadBottomFan) {
                waitingPullback = true;
                waitFromIdx = idx;
                crossWasGap = crossGap;
              }
              if (!rawWaitingPullback && rawHadBottomFan) {
                rawWaitingPullback = true;
                rawWaitFromIdx = idx;
                rawCrossWasGap = crossGap;
              }
            }
          }
        }

        if (sTrades > 0) {
          const wr = (sWins / sTrades) * 100;
          const avgPnl = sPnlSum / sTrades;
          const pf = sLossAbs > 0 ? sWinSum / sLossAbs : sWinSum > 0 ? 99 : 0;
          let scoreAdj = 0;
          let avoidInDefensive = false;
          if (sTrades >= 4 && wr < 35 && avgPnl < -0.5) {
            scoreAdj = -20;
            avoidInDefensive = true;
          } else if (sTrades >= 4 && wr < 45 && avgPnl < -0.25) {
            scoreAdj = -10;
            avoidInDefensive = true;
          } else if (sTrades >= 4 && wr >= 60 && avgPnl > 0.4) {
            scoreAdj = 10;
          } else if (sTrades >= 3 && wr >= 55 && avgPnl > 0.2) {
            scoreAdj = 5;
          }

          bySymbol[coin.symbol] = {
            symbol: coin.symbol,
            trades: sTrades,
            wins: sWins,
            losses: sTrades - sWins,
            winRatePct: Number(wr.toFixed(1)),
            avgPnlPct: Number(avgPnl.toFixed(2)),
            profitFactor: Number(pf.toFixed(2)),
            avgMaePct: Number((sMaeSum / sTrades).toFixed(2)),
            avgMfePct: Number((sMfeSum / sTrades).toFixed(2)),
            scoreAdj,
            avoidInDefensive,
            lastBarTimeSec: coin.c15[n - 1]!.time,
          };
        }
      }

      const profiles = Object.values(bySymbol);
      const topProvenSymbols = profiles
        .filter((p) => p.trades >= 3 && p.avgPnlPct > 0)
        .sort((a, b) => (b.winRatePct * 0.5 + b.avgPnlPct * 15) - (a.winRatePct * 0.5 + a.avgPnlPct * 15))
        .slice(0, 8);
      const riskyWhipsawSymbols = profiles
        .filter((p) => (p.trades >= 3 && p.avgPnlPct < -0.2) || p.avoidInDefensive)
        .sort((a, b) => a.avgPnlPct - b.avgPnlPct)
        .slice(0, 8);

      const maeP80Abs = winMaeAbs.length >= 5 ? percentile(winMaeAbs, 0.8) : null;
      const recSlPct = maeP80Abs !== null ? Math.round(clamp(maeP80Abs + 0.8, 2.5, 5.0) * 4) / 4 : null;
      const overallPf = sumLossAbsPct > 0 ? sumWinPct / sumLossAbsPct : sumWinPct > 0 ? 99 : 0;

      const initTuneCfg = tuneCfgFromConfig({
        portfolioDisasterSlPct: opts.slPct || 3.5,
        portfolioMinBuyVolRatio: baseMinVolR,
        portfolioMinRelStrength24h: baseMinRs,
        portfolioMinRsi1h: baseMinRsi1h,
        portfolioTp1TargetMode: baseTpMode,
        portfolioPullbackMode: basePullbackMode,
      } as Config);
      const toBucketStatsRecord = (acc: Map<string, { setups: number; wins: number; sumPnl: number }>) => {
        const out: Record<string, { setups: number; winRatePct: number; avgPnlPct: number }> = {};
        for (const [k, st] of acc.entries()) {
          out[k] = {
            setups: st.setups,
            winRatePct: st.setups > 0 ? Number(((st.wins / st.setups) * 100).toFixed(1)) : 0,
            avgPnlPct: st.setups > 0 ? Number((st.sumPnl / st.setups).toFixed(3)) : 0,
          };
        }
        return out;
      };
      const featureBucketEv = toBucketStatsRecord(featureBucketAcc);
      const regimeFeatureBucketEv = toBucketStatsRecord(regimeBucketAcc);
      const sessionStats = toBucketStatsRecord(sessionAcc);
      const portfolioSim = simulatePortfolioShadowReplay(
        primaryReplaySetups,
        Math.max(1, Math.min(5, Number(opts.maxSlots) || 3)),
      );
      const breadthSitOutAnalysis = evaluateBreadthSitOutOos(primaryReplaySetups, opts.sitOutMode || "dual");
      const observeFactors = computeObserveFactorsFromSetups(observeSetupRecords);
      const latestBench = bench1d.length > 0 ? bench1dRegimes[bench1d.length - 1] : undefined;
      const latestUsd = usd1d.length > 0 ? usd1dRets[usd1d.length - 1] : null;
      observeFactors.latestMacroRegime = {
        benchmarkSymbol: benchSym,
        asOfTimeSec: bench1d.length > 0 ? bench1d[bench1d.length - 1]!.time : null,
        mayerMultiple: latestBench?.mayerMultiple ?? null,
        mayerZone: classifyMayerZone(latestBench?.mayerMultiple ?? null),
        macroRiskIndex: latestBench?.macroRiskIndex ?? null,
        macroRiskZone: classifyMacroRiskZone(latestBench?.macroRiskIndex ?? null),
        drawdown180dPct: latestBench?.drawdown180dPct ?? null,
        dailyAtr14Pct: latestBench?.dailyAtr14Pct ?? null,
        usdProxySymbol: usdSym,
        usdRet5dPct: latestUsd ?? null,
        usdZone: classifyUsdZone(latestUsd ?? null),
      };
      const openingSessionStats = {
        setups: openSessionAcc.setups,
        winRatePct: openSessionAcc.setups > 0 ? Number(((openSessionAcc.wins / openSessionAcc.setups) * 100).toFixed(1)) : 0,
        avgPnlPct: openSessionAcc.setups > 0 ? Number((openSessionAcc.sumPnl / openSessionAcc.setups).toFixed(3)) : 0,
      };

      const summary: ShadowLearningSummary = {
        updatedAt: now,
        exchange: ex,
        symbolsAnalyzed: precalc.length,
        barsPerSymbolAvg: Math.round(barSum / precalc.length),
        totalSetups,
        wins: totalWins,
        losses: totalSetups - totalWins,
        winRatePct: totalSetups > 0 ? Number(((totalWins / totalSetups) * 100).toFixed(1)) : 0,
        avgPnlPct: totalSetups > 0 ? Number((totalPnlSum / totalSetups).toFixed(2)) : 0,
        profitFactor: Number(overallPf.toFixed(2)),
        winMaeP80Pct: maeP80Abs !== null ? Number((-maeP80Abs).toFixed(2)) : null,
        recommendedSlPct: recSlPct,
        bearMacroSetups: bearSetups,
        bearMacroWinRatePct: bearSetups > 0 ? Number(((bearWins / bearSetups) * 100).toFixed(1)) : 0,
        bearMacroAvgPnlPct: bearSetups > 0 ? Number((bearPnlSum / bearSetups).toFixed(2)) : 0,
        lowBreadthSetups: lowBreadthSetups,
        lowBreadthWinRatePct: lowBreadthSetups > 0 ? Number(((lowBreadthWins / lowBreadthSetups) * 100).toFixed(1)) : 0,
        lowBreadthAvgPnlPct: lowBreadthSetups > 0 ? Number((lowBreadthPnlSum / lowBreadthSetups).toFixed(2)) : 0,
        topProvenSymbols,
        riskyWhipsawSymbols,
        bySymbol,
        featureBucketEv,
        regimeFeatureBucketEv,
        sessionStats,
        openingSessionStats,
        portfolioSim,
        breadthSitOutAnalysis,
        observeFactors,
        tuning: analyzeTuning(tuningEvents, initTuneCfg, costPct),
      };

      this.shadowSummary = summary;
      this.shadowWinMaeAbs = winMaeAbs;
      this.lastTuningEvents = tuningEvents;
      this.lastTuningCostPct = costPct;
      this.lastTuningBaselineKey = tuneCfgKey(initTuneCfg);
      this.lastShadowRunTs = now;
      this.log(
        `🔬 [Shadow Learning] Scanned ${summary.symbolsAnalyzed} tickers (~${summary.barsPerSymbolAvg} bars): ${summary.totalSetups} setups, WR ${summary.winRatePct}%, PF ${summary.profitFactor}, BearAvg ${summary.bearMacroAvgPnlPct >= 0 ? "+" : ""}${summary.bearMacroAvgPnlPct}% (${summary.bearMacroSetups}t)`,
      );
      return summary;
    } finally {
      this.shadowInProgress = false;
    }
  }

  // ── Level 3: Survival Guard & Bounded Auto-Tuning ───────────────────────

  public evaluateSurvivalPosture(params: {
    cfg: Config;
    macro: PortfolioMacroState;
    configuredMaxSlots: number;
    currentEquity?: number;
  }): SurvivalGuardDecision {
    const now = Date.now();
    const configuredMaxSlots = Math.max(1, Math.min(5, Number(params.configuredMaxSlots) || 3));
    const enforced = this.isSurvivalGuardEnforced(params.cfg);

    const closedRounds = this.db
      .getTradeRounds("ALL")
      .rounds.filter((r) => r.status === "CLOSED")
      .sort((a, b) => normalizeTsMs(b.endTime || b.startTime) - normalizeTsMs(a.endTime || a.startTime));

    let recentConsecutiveLosses = 0;
    let latestLossMs = 0;
    for (const r of closedRounds) {
      const net = Number(r.netPnlQuote ?? r.grossPnlQuote ?? 0);
      if (net < -0.05) {
        if (recentConsecutiveLosses === 0) latestLossMs = normalizeTsMs(r.endTime || r.startTime);
        recentConsecutiveLosses++;
      } else {
        break;
      }
    }

    const thirtyDaysAgo = now - 30 * 86_400_000;
    const recent30dChrono = closedRounds
      .filter((r) => normalizeTsMs(r.endTime || r.startTime) >= thirtyDaysAgo)
      .slice()
      .reverse();

    let rollingDrawdownPct = 0;
    if (recent30dChrono.length > 0) {
      const sum30d = recent30dChrono.reduce((s, r) => s + Number(r.netPnlQuote ?? r.grossPnlQuote ?? 0), 0);
      const curEq = Number(params.currentEquity || 0);
      const defaultCap = String(params.cfg.exchange || "").toLowerCase() === "alpaca" ? 10_000 : 200;
      const startEq = curEq > 0 ? Math.max(100, curEq * 0.5, curEq - sum30d) : defaultCap;
      let eq = startEq;
      let peak = startEq;
      for (const r of recent30dChrono) {
        eq += Number(r.netPnlQuote ?? r.grossPnlQuote ?? 0);
        if (eq > peak) peak = eq;
      }
      const latestEq = curEq > 0 ? Math.min(eq, curEq) : eq;
      if (peak > 0 && latestEq < peak) {
        rollingDrawdownPct = Number((((peak - latestEq) / peak) * 100).toFixed(2));
      }
    }

    const breadth = Number(params.macro?.breadthAboveEma25Pct ?? 50);
    const pass4h = params.macro?.pass4h ?? true;
    const pass1d = params.macro?.pass1d ?? true;
    const bothMacroFail = !pass4h && !pass1d;

    const shadow = this.shadowSummary;
    const bearMacroAvgPnl = shadow && shadow.bearMacroSetups >= 5 ? shadow.bearMacroAvgPnlPct : null;
    const shadowBearNegative = bearMacroAvgPnl !== null && bearMacroAvgPnl < -0.15;
    const blockedTrapHigh = Boolean(
      this.recentBlockedStats &&
      this.recentBlockedStats.count >= 10 &&
      this.recentBlockedStats.loseRatePct >= 55 &&
      this.recentBlockedStats.avgPnlPct <= -0.25,
    );

    const lastLossWithin24h = latestLossMs > 0 && now - latestLossMs < 24 * 3_600_000;
    const recoveryOk = pass4h || pass1d || breadth >= 45;

    const baseMinRs = Number.isFinite(Number(params.cfg.portfolioMinRelStrength24h))
      ? Number(params.cfg.portfolioMinRelStrength24h)
      : -1.0;
    const baseMinRsi1h = Number.isFinite(Number(params.cfg.portfolioMinRsi1h))
      ? Number(params.cfg.portfolioMinRsi1h)
      : 40;
    const baseMinVolR = Number.isFinite(Number(params.cfg.portfolioMinBuyVolRatio))
      ? Number(params.cfg.portfolioMinBuyVolRatio)
      : 1.3;

    let posture: SurvivalPostureLevel = "NORMAL";
    let effectiveMaxSlots = configuredMaxSlots;
    let positionSizeScale = 1.0;
    let minRelStrength24h = baseMinRs;
    let minRsi1h = baseMinRsi1h;
    let minBuyVolRatio12 = baseMinVolR;
    let blockRiskySymbols = false;
    const reasons: string[] = [];

    const capPresStreakTrigger = recentConsecutiveLosses >= 4 && (lastLossWithin24h || !recoveryOk);
    if (rollingDrawdownPct >= 7.0 || capPresStreakTrigger) {
      posture = "CAPITAL_PRESERVATION";
      effectiveMaxSlots = 0;
      positionSizeScale = 0;
      minRelStrength24h = Math.max(baseMinRs, 1.5);
      minRsi1h = Math.max(baseMinRsi1h, 45);
      minBuyVolRatio12 = Math.max(baseMinVolR, 1.6);
      blockRiskySymbols = true;
      if (rollingDrawdownPct >= 7.0) reasons.push(`Rolling DD ${rollingDrawdownPct.toFixed(1)}% >= 7%`);
      if (capPresStreakTrigger) reasons.push(`${recentConsecutiveLosses} consecutive losses`);
    } else if (
      rollingDrawdownPct >= 4.0 ||
      recentConsecutiveLosses >= 3 ||
      (bothMacroFail && breadth < 25 && (shadowBearNegative || blockedTrapHigh)) ||
      (bothMacroFail && breadth < 20)
    ) {
      posture = "DEFENSIVE";
      effectiveMaxSlots = Math.min(configuredMaxSlots, 1);
      positionSizeScale = 0.5;
      minRelStrength24h = Math.max(baseMinRs, 1.0);
      minRsi1h = Math.max(baseMinRsi1h, 42);
      minBuyVolRatio12 = Math.max(baseMinVolR, 1.5);
      blockRiskySymbols = true;
      if (rollingDrawdownPct >= 4.0) reasons.push(`Rolling DD ${rollingDrawdownPct.toFixed(1)}% >= 4%`);
      if (recentConsecutiveLosses >= 3) reasons.push(`${recentConsecutiveLosses} consecutive losses`);
      if (bothMacroFail) reasons.push(`${params.macro.benchmarkSymbol} 4H+1D bear & Breadth ${breadth.toFixed(0)}%`);
      if (shadowBearNegative) reasons.push(`Shadow BearAvg ${bearMacroAvgPnl!.toFixed(2)}%`);
      if (blockedTrapHigh) reasons.push(`Trap rate ${this.recentBlockedStats!.loseRatePct.toFixed(0)}%`);
    } else if (
      recentConsecutiveLosses >= 2 ||
      rollingDrawdownPct >= 2.5 ||
      (breadth < 35 && (!pass4h || !pass1d)) ||
      (bothMacroFail && blockedTrapHigh)
    ) {
      posture = "CAUTIOUS";
      effectiveMaxSlots = Math.max(1, Math.min(configuredMaxSlots, configuredMaxSlots - 1));
      positionSizeScale = 0.75;
      minRelStrength24h = Math.max(baseMinRs, 0.0);
      minRsi1h = Math.max(baseMinRsi1h, 42);
      minBuyVolRatio12 = Math.max(baseMinVolR, 1.3);
      blockRiskySymbols = true;
      if (recentConsecutiveLosses >= 2) reasons.push(`${recentConsecutiveLosses} consecutive losses`);
      if (rollingDrawdownPct >= 2.5) reasons.push(`Rolling DD ${rollingDrawdownPct.toFixed(1)}% >= 2.5%`);
      if (breadth < 35 && (!pass4h || !pass1d)) {
        reasons.push(`Weak breadth ${breadth.toFixed(0)}% (<35%)`);
      }
      if (blockedTrapHigh) reasons.push(`Blocked signal lose rate ${this.recentBlockedStats!.loseRatePct.toFixed(0)}%`);
    } else {
      reasons.push("Regime & portfolio metrics healthy");
    }

    const decision: SurvivalGuardDecision = {
      posture,
      enforced,
      configuredMaxSlots,
      effectiveMaxSlots,
      positionSizeScale,
      minRelStrength24h,
      minRsi1h,
      minBuyVolRatio12,
      blockRiskySymbols,
      reasons,
      metrics: {
        breadthPct: Number(breadth.toFixed(1)),
        macroPass4h: pass4h,
        macroPass1d: pass1d,
        recentConsecutiveLosses,
        rollingDrawdownPct,
        bearMacroAvgPnlPct: bearMacroAvgPnl,
        recentBlockedLoseRatePct: this.recentBlockedStats?.loseRatePct ?? null,
        recentBlockedAvgPnlPct: this.recentBlockedStats?.avgPnlPct ?? null,
      },
    };

    const prevPosture = this.lastSurvivalDecision?.posture;
    this.lastSurvivalDecision = decision;

    const pg = this.getPg();
    if (
      pg &&
      this.symbolStatesLoaded &&
      posture !== (this.lastLoggedPosture ?? prevPosture ?? "NORMAL") &&
      now - this.lastPostureLogTs >= POSTURE_LOG_COOLDOWN_MS
    ) {
      const oldPosture = this.lastLoggedPosture ?? prevPosture ?? "NORMAL";
      this.lastLoggedPosture = posture;
      this.lastPostureLogTs = now;
      const modeTag = enforced ? "" : " [Observe-Only]";
      this.log(`🛡️ [Survival Guard]${modeTag} Posture ${oldPosture} -> ${posture} | MaxSlots ${effectiveMaxSlots}/${configuredMaxSlots}, Size ${(positionSizeScale * 100).toFixed(0)}% (${reasons.join("; ")})`);
      void pg
        .recordLearningAdjustment({
          paramName: "survival_posture",
          oldValue: POSTURE_NUMERIC[oldPosture],
          newValue: POSTURE_NUMERIC[posture],
          sampleSize: Math.max(closedRounds.length, shadow?.totalSetups ?? 0),
          rationale: `${oldPosture} -> ${posture}${modeTag}: ${reasons.join("; ")}`,
          applied: enforced,
          createdAt: now,
        })
        .catch(() => {});
    }

    return decision;
  }

  private refreshShadowTuningFromConfig(cfg: Config): void {
    if (!this.shadowSummary || this.lastTuningEvents.length === 0) return;
    const nextTuneCfg = tuneCfgFromConfig(cfg);
    const nextKey = tuneCfgKey(nextTuneCfg);
    if (nextKey === this.lastTuningBaselineKey) return;
    this.shadowSummary = {
      ...this.shadowSummary,
      tuning: analyzeTuning(this.lastTuningEvents, nextTuneCfg, this.lastTuningCostPct),
    };
    this.lastTuningBaselineKey = nextKey;
  }

  public async applyAutonomousTuning(
    cfg: Config,
    onApplySetting: (patch: Record<string, any>) => void,
  ): Promise<void> {
    let effectiveCfg: Config = { ...cfg };
    this.refreshShadowTuningFromConfig(effectiveCfg);

    const pg = this.getPg();
    if (!pg || !this.isAutoTuneAllowed(effectiveCfg) || this.autoTuneInProgress) return;

    this.autoTuneInProgress = true;
    try {
      const now = Date.now();
      const [adjustmentsRaw, closed] = await Promise.all([
        pg.loadLearningAdjustments(50).catch(() => [] as LearningAdjustmentRecord[]),
        pg.loadLearningJournal("TRADE", "CLOSED", 200).catch(() => [] as LearningJournalRecord[]),
      ]);
      const adjustments = [...adjustmentsRaw];
      const sampleBase = closed.length + (this.shadowSummary?.totalSetups ?? 0);

      // Step A: Self-heal legacy un-gated WinMAE P80 shadow SL nudges (e.g. v0.3.0.95)
      let restoredSlThisCycle = false;
      const lastSlAdj = adjustments.find((a) => a.paramName === "portfolioDisasterSlPct");
      const isLegacyShadowWinMaeSl = Boolean(
        lastSlAdj &&
          lastSlAdj.rationale.includes("Shadow(") &&
          lastSlAdj.rationale.includes("WinMAE_P80") &&
          !lastSlAdj.rationale.includes("OOS-Confirmed") &&
          !lastSlAdj.rationale.includes("OOS Auto-Pilot") &&
          !lastSlAdj.rationale.includes("Auto-Restore") &&
          !lastSlAdj.rationale.includes("Auto-Rollback"),
      );
      if (isLegacyShadowWinMaeSl && lastSlAdj) {
        const curSl = Number(effectiveCfg.portfolioDisasterSlPct || 3.5);
        const slRec = this.shadowSummary?.tuning?.recommendations.find((r) => r.param === "portfolioDisasterSlPct");
        const slDownConfirmed = Boolean(
          slRec && (slRec.confidence === "HIGH" || slRec.confidence === "MEDIUM") && Number(slRec.suggested) <= curSl,
        );
        if (!slDownConfirmed && Math.abs(curSl - lastSlAdj.newValue) < 0.05 && Math.abs(lastSlAdj.oldValue - curSl) >= 0.2) {
          const restoreSl = Number(clamp(lastSlAdj.oldValue, 2.5, 5.0).toFixed(2));
          const rationale = `Auto-Restore: คืนค่า SL จาก WinMAE Shadow เดิมที่ยังไม่ผ่าน OOS Test -> ${restoreSl.toFixed(2)}%`;
          onApplySetting({ portfolioDisasterSlPct: restoreSl });
          effectiveCfg = { ...effectiveCfg, portfolioDisasterSlPct: restoreSl };
          this.log(
            `🔄 [Learning Auto-Restore] portfolioDisasterSlPct: ${curSl}% -> ${restoreSl}% (คืนค่าจาก WinMAE Shadow เดิมที่ยังไม่ยืนยัน OOS)`,
          );
          const recRow: LearningAdjustmentRecord = {
            paramName: "portfolioDisasterSlPct",
            oldValue: curSl,
            newValue: restoreSl,
            sampleSize: sampleBase,
            rationale,
            applied: true,
            createdAt: now,
          };
          await pg.recordLearningAdjustment(recRow).catch(() => {});
          adjustments.unshift(recRow);
          this.refreshShadowTuningFromConfig(effectiveCfg);
          restoredSlThisCycle = true;
        }
      }

      // Step B: Auto-Rollback Guard if previously tuned setting reverses in OOS or degrades live
      for (const param of AUTO_TUNABLE_PARAMS) {
        const lastAdj = adjustments.find((a) => a.paramName === param);
        if (!lastAdj) continue;
        if (lastAdj.rationale.startsWith("Auto-Restore") || lastAdj.rationale.startsWith("Auto-Rollback")) continue;
        if (now - lastAdj.createdAt < 6 * 3_600_000) continue;
        if (!matchesRecordedAdjustment(param, effectiveCfg, lastAdj.newValue)) continue;

        const rec = this.shadowSummary?.tuning?.recommendations.find((r) => r.param === param);
        let oosReversal = false;
        if (rec && (rec.confidence === "HIGH" || rec.confidence === "MEDIUM")) {
          oosReversal =
            param === "portfolioTp1TargetMode"
              ? encodeTpModeNumeric(String(rec.suggested)) === Math.round(lastAdj.oldValue)
              : param === "portfolioPullbackMode"
                ? encodePullbackModeNumeric(String(rec.suggested)) === Math.round(lastAdj.oldValue)
                : (Number(rec.suggested) - lastAdj.newValue) * (lastAdj.oldValue - lastAdj.newValue) > 0;
        }

        const postTrades = closed.filter((t) => Number(t.entryTimeMs || 0) >= lastAdj.createdAt);
        const postWins = postTrades.filter((t) => Number(t.pnlQuote ?? 0) > 0).length;
        const postPnl = postTrades.reduce((s, t) => s + Number(t.pnlQuote ?? 0), 0);
        const liveDegraded = postTrades.length >= 4 && postWins / postTrades.length < 0.25 && postPnl < 0;

        if (oosReversal || liveDegraded) {
          const rollbackVal =
            param === "portfolioTp1TargetMode"
              ? decodeTpModeNumeric(lastAdj.oldValue)
              : param === "portfolioPullbackMode"
                ? decodePullbackModeNumeric(lastAdj.oldValue)
                : lastAdj.oldValue;
          const reasonTag = oosReversal && liveDegraded ? "OOS+Live" : oosReversal ? "OOS" : "Live";
          const rationale = `Auto-Rollback: ถอยกลับค่าเดิมเนื่องจากผลทดสอบ OOS/ผลเทรดจริงหลังจูนอ่อนตัวลง (${reasonTag})`;
          onApplySetting({ [param]: rollbackVal });
          effectiveCfg = { ...effectiveCfg, [param]: rollbackVal };
          this.log(`↩️ [Learning Auto-Rollback] ${param}: ${lastAdj.newValue} -> ${rollbackVal} (${reasonTag})`);
          await pg
            .recordLearningAdjustment({
              paramName: param,
              oldValue: lastAdj.newValue,
              newValue: lastAdj.oldValue,
              sampleSize: sampleBase,
              rationale,
              applied: true,
              createdAt: now,
            })
            .catch(() => {});
          this.refreshShadowTuningFromConfig(effectiveCfg);
          return;
        }
      }

      // Step C: OOS-Confirmed Autonomous Tuning (at most 1 parameter change per cycle, 7d per-param cooldown)
      const sortedRecs = (this.shadowSummary?.tuning?.recommendations ?? [])
        .slice()
        .sort((a, b) => b.testAvgDeltaPct - a.testAvgDeltaPct);

      for (const rec of sortedRecs) {
        if (!rec.autoApplicable) continue;
        const lastParamAdj = adjustments.find(
          (a) => a.paramName === rec.param && !a.rationale.startsWith("Auto-Restore"),
        );
        if (lastParamAdj && now - lastParamAdj.createdAt < PARAM_NUDGE_INTERVAL_MS) continue;

        const candidate = buildTunableCandidatePatch(rec.param, effectiveCfg, rec.suggested);
        if (!candidate) continue;

        const rationale = `OOS Auto-Pilot (${rec.confidence}): ${rec.label} ${candidate.displayChange} • Test +${rec.testAvgDeltaPct.toFixed(2)}pp/ไม้ (Total ${rec.suggestedTest.totalPnlPct >= 0 ? "+" : ""}${rec.suggestedTest.totalPnlPct}%, PF ${rec.suggestedTest.profitFactor}, ${rec.suggestedTest.trades} ไม้)`;
        onApplySetting({ [rec.param]: candidate.patchVal });
        effectiveCfg = { ...effectiveCfg, [rec.param]: candidate.patchVal };
        this.log(`🧠 [Learning Auto-Pilot] ${rec.param}: ${candidate.displayChange} (${rationale})`);
        await pg
          .recordLearningAdjustment({
            paramName: rec.param,
            oldValue: candidate.oldNum,
            newValue: candidate.newNum,
            sampleSize: sampleBase,
            rationale,
            applied: true,
            createdAt: now,
          })
          .catch(() => {});
        this.refreshShadowTuningFromConfig(effectiveCfg);
        return;
      }

      // Fallback Real-Trade SL Calibration when >= 20 real trades exist and no OOS change fired
      if (restoredSlThisCycle) return;
      const lastSlNonRestore = adjustments.find(
        (a) => a.paramName === "portfolioDisasterSlPct" && !a.rationale.startsWith("Auto-Restore"),
      );
      if (lastSlNonRestore && now - lastSlNonRestore.createdAt < PARAM_NUDGE_INTERVAL_MS) return;

      const wins = closed.filter((t) => (t.pnlQuote ?? 0) > 0 && typeof t.maePct === "number");
      const realEnough = closed.length >= MIN_CLOSED_TRADES_FOR_SL_NUDGE && wins.length >= MIN_WIN_TRADES_FOR_SL_NUDGE;
      if (!realEnough) return;

      const slRec = this.shadowSummary?.tuning?.recommendations.find((r) => r.param === "portfolioDisasterSlPct");
      const weightedWinMaeAbs: number[] = [];
      for (const w of wins) {
        const val = Math.abs(Number(w.maePct || 0));
        weightedWinMaeAbs.push(val, val, val); // Real wins carry 3x weight
      }
      for (const sm of this.shadowWinMaeAbs) {
        weightedWinMaeAbs.push(sm);
      }
      if (weightedWinMaeAbs.length === 0) return;

      const curSl = Number(effectiveCfg.portfolioDisasterSlPct || 3.5);
      const maeP80 = percentile(weightedWinMaeAbs, 0.8);
      const maeTargetSl = Math.round(clamp(maeP80 + 0.8, 2.5, 5.0) * 4) / 4; // 0.25% step
      const realBlockedByOos = Boolean(
        slRec?.confidence === "LOW" && (maeTargetSl - curSl) * (Number(slRec.suggested) - curSl) > 0,
      );
      if (realBlockedByOos) return;

      const boundedStep = clamp(maeTargetSl - curSl, -0.25, 0.25);
      const nextSl = Number(clamp(curSl + boundedStep, 2.5, 5.0).toFixed(2));
      if (Math.abs(nextSl - curSl) >= 0.2) {
        const rationale = `Real(${closed.length}t/${wins.length}w) WinMAE_P80=-${maeP80.toFixed(2)}% -> target=${maeTargetSl.toFixed(2)}%, bounded step ${curSl.toFixed(2)}% -> ${nextSl.toFixed(2)}%`;
        onApplySetting({ portfolioDisasterSlPct: nextSl });
        effectiveCfg = { ...effectiveCfg, portfolioDisasterSlPct: nextSl };
        this.log(`🧠 [Learning Auto-Tune] portfolioDisasterSlPct: ${curSl}% -> ${nextSl}% (${rationale})`);
        await pg
          .recordLearningAdjustment({
            paramName: "portfolioDisasterSlPct",
            oldValue: curSl,
            newValue: nextSl,
            sampleSize: sampleBase,
            rationale,
            applied: true,
            createdAt: now,
          })
          .catch(() => {});
        this.refreshShadowTuningFromConfig(effectiveCfg);
      }
    } finally {
      this.autoTuneInProgress = false;
    }
  }

  public async runPeriodicCycle(
    cfg: Config,
    onApplySetting: (patch: Record<string, any>) => void,
    shadowContext?: ShadowWalkForwardContext,
  ): Promise<void> {
    const pg = this.getPg();
    if (this.periodicInProgress) return;

    const now = Date.now();
    if (now - this.lastPeriodicRunTs < 30 * 60_000) return;
    this.lastPeriodicRunTs = now;
    this.periodicInProgress = true;

    try {
      await this.ensureSymbolStatesLoaded();
      if (pg) {
        await this.evaluatePendingBlockedSignals(cfg.exchange || "binance", cfg.portfolioDisasterSlPct || 3.5);
        await this.evaluatePostExitTrades(cfg.exchange || "binance").catch(() => {});
      }

      if (shadowContext && now - this.lastShadowRunTs >= SHADOW_REFRESH_INTERVAL_MS) {
        await this.runShadowWalkForward(shadowContext).catch(() => {});
      }

      await this.applyAutonomousTuning(cfg, onApplySetting);
    } finally {
      this.periodicInProgress = false;
    }
  }

  public async buildReport(cfg: Config) {
    this.refreshShadowTuningFromConfig(cfg);
    const activeTechnicalParams = {
      portfolioCapitalUsagePct: Number(cfg.portfolioCapitalUsagePct || 50),
      portfolioPullbackMode: normalizePullbackMode(cfg.portfolioPullbackMode),
      portfolioTp1TargetMode: cfg.portfolioTp1TargetMode || "EMA200",
      portfolioDisasterSlPct: Number(cfg.portfolioDisasterSlPct || 3.5),
      portfolioMinBuyVolRatio: Number.isFinite(Number(cfg.portfolioMinBuyVolRatio))
        ? Number(cfg.portfolioMinBuyVolRatio)
        : 1.3,
      portfolioMinRelStrength24h: Number.isFinite(Number(cfg.portfolioMinRelStrength24h))
        ? Number(cfg.portfolioMinRelStrength24h)
        : -1.0,
      portfolioMinRsi1h: Number.isFinite(Number(cfg.portfolioMinRsi1h)) ? Number(cfg.portfolioMinRsi1h) : 40,
      portfolioSitOutMode: cfg.portfolioSitOutMode || "dual",
      portfolioQuantCorrelationGuard: cfg.portfolioQuantCorrelationGuard !== false,
      portfolioQuantRegimeConditional: cfg.portfolioQuantRegimeConditional !== false,
      portfolioQuantSessionGuard: cfg.portfolioQuantSessionGuard !== false,
      portfolioQuantVolTargetedRisk: cfg.portfolioQuantVolTargetedRisk !== false,
      portfolioQuantEmpiricalEv: cfg.portfolioQuantEmpiricalEv !== false,
      portfolioSurvivalGuardEnforced: Boolean(cfg.portfolioSurvivalGuardEnforced),
    };

    const pg = this.getPg();
    if (!pg) {
      return {
        postgresActive: false,
        autoTuneMode: cfg.learningAutoTuneMode || "all",
        autoTuneEffective: false,
        summary: { activeTechnicalParams },
        survival: this.lastSurvivalDecision,
        shadowLearning: this.shadowSummary,
        message: "PostgreSQL not connected; learning journal requires DATABASE_URL.",
      };
    }

    await this.ensureSymbolStatesLoaded();
    const [closedTrades, openTrades, evaluatedBlocked, pendingBlocked, symbolStates, adjustments] = await Promise.all([
      pg.loadLearningJournal("TRADE", "CLOSED", 500).catch(() => [] as LearningJournalRecord[]),
      pg.loadLearningJournal("TRADE", "OPEN", 50).catch(() => [] as LearningJournalRecord[]),
      pg.loadLearningJournal("BLOCKED_SIGNAL", "EVALUATED", 500).catch(() => [] as LearningJournalRecord[]),
      pg.loadLearningJournal("BLOCKED_SIGNAL", "PENDING_EVAL", 100).catch(() => [] as LearningJournalRecord[]),
      pg.loadSymbolLearningStates().catch(() => [] as SymbolLearningStateRecord[]),
      pg.loadLearningAdjustments(30).catch(() => [] as LearningAdjustmentRecord[]),
    ]);

    this.refreshBlockedTrapCache(evaluatedBlocked);

    const wins = closedTrades.filter((t) => (t.pnlQuote ?? 0) > 0 && typeof t.maePct === "number");
    const realWinCount = closedTrades.filter((t) => (t.pnlQuote ?? 0) > 0).length;
    const totalPnlQuote = closedTrades.reduce((s, t) => s + Number(t.pnlQuote ?? 0), 0);
    const avgPnlPct =
      closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + Number(t.pnlPct ?? 0), 0) / closedTrades.length : 0;
    const avgMaePct =
      closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + Number(t.maePct ?? 0), 0) / closedTrades.length : 0;
    const avgMfePct =
      closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + Number(t.mfePct ?? 0), 0) / closedTrades.length : 0;

    const winMaeP80Pct =
      wins.length >= 3 ? -percentile(wins.map((w) => Math.abs(Number(w.maePct ?? 0))), 0.8) : null;

    const combinedMaeAbs: number[] = [];
    for (const w of wins) {
      const v = Math.abs(Number(w.maePct ?? 0));
      combinedMaeAbs.push(v, v, v);
    }
    for (const sm of this.shadowWinMaeAbs) {
      combinedMaeAbs.push(sm);
    }
    const combinedWinMaeP80Pct = combinedMaeAbs.length >= 5 ? -percentile(combinedMaeAbs, 0.8) : winMaeP80Pct;

    const realEnough = closedTrades.length >= MIN_CLOSED_TRADES_FOR_SL_NUDGE && realWinCount >= MIN_WIN_TRADES_FOR_SL_NUDGE;
    const slRec = this.shadowSummary?.tuning?.recommendations.find((r) => r.param === "portfolioDisasterSlPct");
    const slRecConfirmed = Boolean(slRec && (slRec.confidence === "HIGH" || slRec.confidence === "MEDIUM"));
    const curSlReport = Number(cfg.portfolioDisasterSlPct || 3.5);
    const maeTargetSl =
      combinedWinMaeP80Pct !== null ? Math.round(clamp(Math.abs(combinedWinMaeP80Pct) + 0.8, 2.5, 5.0) * 4) / 4 : null;
    const realBlockedByOos = Boolean(
      slRec?.confidence === "LOW" &&
      maeTargetSl !== null &&
      (maeTargetSl - curSlReport) * (Number(slRec.suggested) - curSlReport) > 0,
    );
    const recommendedSlPct = slRecConfirmed
      ? clamp(Number(slRec!.suggested), 2.5, 5.0)
      : realEnough && maeTargetSl !== null && !realBlockedByOos
        ? maeTargetSl
        : null;
    const slTuneReady = Boolean(slRecConfirmed || (realEnough && !realBlockedByOos));

    const byReasonMap = new Map<string, { count: number; wins: number; pnlQuote: number }>();
    for (const t of closedTrades) {
      const key = t.exitReason || "UNKNOWN";
      const cur = byReasonMap.get(key) || { count: 0, wins: 0, pnlQuote: 0 };
      cur.count++;
      if ((t.pnlQuote ?? 0) > 0) cur.wins++;
      cur.pnlQuote += Number(t.pnlQuote ?? 0);
      byReasonMap.set(key, cur);
    }

    const blockedSummary = aggregateBlockedStats(evaluatedBlocked, (b) => normalizeBlockReasonKey(b.blockReason)).map(
      ({ key, ...rest }) => ({ reason: key, ...rest }),
    );
    const macroSitOutRecords = evaluatedBlocked.filter(
      (b) => b.blockReason === "MACRO_SIT_OUT" || b.blockReason === "SHADOW_BEAR_MACRO",
    );
    const macroSitOutByBreadth = aggregateBlockedStats(macroSitOutRecords, (b) =>
      getBreadthBucketLabel(b.features?.breadthAboveEma25Pct),
    ).map(({ key, ...rest }) => ({ bucket: key, ...rest }));

    const nowMs = Date.now();
    const activeCooldowns = symbolStates
      .filter((s) => s.cooldownUntilMs > nowMs)
      .map((s) => ({
        symbol: s.symbol,
        consecutiveSl: s.consecutiveSl,
        hoursRemaining: Number(((s.cooldownUntilMs - nowMs) / 3_600_000).toFixed(1)),
        totalPnlQuote: Number(s.totalPnlQuote.toFixed(2)),
      }));

    const observedWarnTrades = closedTrades.filter((t) => Boolean(t.features?.survivalWouldBlock));
    const observedWarnWins = observedWarnTrades.filter((t) => (t.pnlQuote ?? 0) > 0).length;
    const observedWarnPnl = observedWarnTrades.reduce((sum, t) => sum + Number(t.pnlQuote ?? 0), 0);
    const survivalValidation = {
      observedBlockedTrades: observedWarnTrades.length,
      wins: observedWarnWins,
      losses: observedWarnTrades.length - observedWarnWins,
      winRatePct:
        observedWarnTrades.length > 0 ? Number(((observedWarnWins / observedWarnTrades.length) * 100).toFixed(1)) : 0,
      totalPnlQuote: Number(observedWarnPnl.toFixed(2)),
    };

    const slipSamples = [...closedTrades, ...openTrades]
      .filter((t) => t.features?.entrySlippagePct !== undefined && t.features?.entrySlippagePct !== null)
      .map((t) => Number(t.features!.entrySlippagePct))
      .filter((v) => Number.isFinite(v));
    const executionAudit = {
      samples: slipSamples.length,
      avgEntrySlippagePct:
        slipSamples.length > 0 ? Number((slipSamples.reduce((a, b) => a + b, 0) / slipSamples.length).toFixed(4)) : null,
      maxAdverseSlippagePct: slipSamples.length > 0 ? Number(Math.max(...slipSamples).toFixed(4)) : null,
    };

    const postEvalTrades = closedTrades.filter((t) => Boolean(t.features?.postExitEvaluated));
    const postLossTrades = postEvalTrades.filter(
      (t) => Number(t.pnlQuote ?? 0) < -0.05 || String(t.exitReason || "").includes("SL_"),
    );
    const postWinTrades = postEvalTrades.filter((t) => Number(t.pnlQuote ?? 0) > 0.05);
    const stopHuntTrades = postLossTrades.filter((t) => t.features?.postExitVerdict === "STOP_HUNT");
    const earlyTpTrades = postWinTrades.filter((t) => t.features?.postExitVerdict === "EARLY_TP");
    const avgPostField = (arr: LearningJournalRecord[], key: string) => {
      const vals = arr.map((t) => Number(t.features?.[key])).filter((v) => Number.isFinite(v));
      return vals.length > 0 ? Number((vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(2)) : null;
    };
    const postExitAudit: PostExitAuditSummary = {
      evaluated: postEvalTrades.length,
      stopHuntCount: stopHuntTrades.length,
      stopHuntRatePct:
        postLossTrades.length > 0 ? Number(((stopHuntTrades.length / postLossTrades.length) * 100).toFixed(1)) : 0,
      earlyTpCount: earlyTpTrades.length,
      earlyTpRatePct:
        postWinTrades.length > 0 ? Number(((earlyTpTrades.length / postWinTrades.length) * 100).toFixed(1)) : 0,
      avgPostSlReboundPct: avgPostField(postLossTrades, "postExitMaxMovePct"),
      avgPostTpExtraRunPct: avgPostField(postWinTrades, "postExitMaxMovePct"),
    };

    return {
      postgresActive: true,
      instanceKey: pg.instanceKey,
      autoTuneMode: cfg.learningAutoTuneMode || "all",
      autoTuneEffective: this.isAutoTuneAllowed(cfg),
      survivalEnforced: this.isSurvivalGuardEnforced(cfg),
      summary: {
        closedTrades: closedTrades.length,
        openTrades: openTrades.length,
        wins: realWinCount,
        losses: closedTrades.length - realWinCount,
        winRatePct: closedTrades.length > 0 ? Number(((realWinCount / closedTrades.length) * 100).toFixed(1)) : 0,
        totalPnlQuote: Number(totalPnlQuote.toFixed(2)),
        avgPnlPct: Number(avgPnlPct.toFixed(2)),
        avgMaePct: Number(avgMaePct.toFixed(2)),
        avgMfePct: Number(avgMfePct.toFixed(2)),
        winMaeP80Pct: winMaeP80Pct !== null ? Number(winMaeP80Pct.toFixed(2)) : null,
        combinedWinMaeP80Pct: combinedWinMaeP80Pct !== null ? Number(combinedWinMaeP80Pct.toFixed(2)) : null,
        currentDisasterSlPct: Number(cfg.portfolioDisasterSlPct || 3.5),
        recommendedDisasterSlPct: recommendedSlPct,
        samplesNeededForSlTune: slTuneReady ? 0 : Math.max(0, MIN_CLOSED_TRADES_FOR_SL_NUDGE - closedTrades.length),
        activeTechnicalParams,
      },
      survival: this.lastSurvivalDecision,
      survivalValidation,
      executionAudit,
      postExitAudit,
      shadowLearning: this.shadowSummary,
      exitReasons: Array.from(byReasonMap.entries()).map(([reason, r]) => ({
        reason,
        count: r.count,
        winRatePct: r.count > 0 ? Number(((r.wins / r.count) * 100).toFixed(1)) : 0,
        pnlQuote: Number(r.pnlQuote.toFixed(2)),
      })),
      blockedSignals: {
        pendingEvaluation: pendingBlocked.length,
        evaluatedTotal: evaluatedBlocked.length,
        breakdown: blockedSummary,
        macroSitOutByBreadth,
      },
      activeCooldowns,
      symbolPerformance: symbolStates
        .slice()
        .sort((a, b) => b.totalPnlQuote - a.totalPnlQuote)
        .slice(0, 30),
      recentAdjustments: adjustments,
    };
  }
}
