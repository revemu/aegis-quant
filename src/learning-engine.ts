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
  exchange: string;
  symbols: string[];
  benchmarkSymbol: string;
  slPct: number;
  fallbackSnapshots?: Map<string, { c15: Candle[]; c1h: Candle[] }>;
  benchmarkFallback15m?: Candle[];
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

function normalizeBlockReasonKey(rawReason: string | null | undefined): string {
  const first = (rawReason || "OTHER").split(",")[0]?.trim() || "OTHER";
  if (first === "MACRO_SIT_OUT" || first === "SHADOW_BEAR_MACRO" || first === "FILTER_FAIL") {
    return first;
  }
  if (first.startsWith("SURVIVAL_GUARD:")) {
    return first;
  }
  if (first.startsWith("FILTER:")) {
    const detail = first.slice("FILTER:".length).trim();
    if (detail.startsWith("1hRSI")) return "FILTER:1hRSI<40";
    if (detail.startsWith("VolR")) return "FILTER:VolR<1.3";
    if (detail.startsWith("RS")) return "FILTER:RS<-1%";
    if (detail.includes("Cooldown")) return "FILTER:Cooldown";
    if (detail.includes("8x16_WeakMom")) return "FILTER:8x16_WeakMom";
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

/**
 * Simulate MA-Fan TP1 (50% exit at EMA200 with +0.8% floor, lock BE +0.25% on remaining 50%)
 * vs hard Disaster SL over the forward 15m horizon bars.
 */
function simulateMaFanCounterfactual(
  entryPrice: number,
  distEma200Pct: number,
  slTriggerPct: number,
  futureBars: Array<{ high: number; low: number; close: number }>,
): { finalRet: number; maePct: number; mfePct: number; verdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL" } {
  const tp1Pct = Number.isFinite(distEma200Pct) && distEma200Pct > 0
    ? Math.max(0.8, Math.min(distEma200Pct, 8.0))
    : 2.0;
  const beLockPct = 0.25;

  let minLow = entryPrice;
  let maxHigh = entryPrice;
  let tp1Hit = false;
  let finalRet: number | null = null;

  for (const b of futureBars) {
    if (b.low < minLow) minLow = b.low;
    if (b.high > maxHigh) maxHigh = b.high;
    const lowPct = ((b.low - entryPrice) / entryPrice) * 100;
    const highPct = ((b.high - entryPrice) / entryPrice) * 100;

    if (!tp1Hit) {
      if (lowPct <= slTriggerPct) {
        finalRet = slTriggerPct;
        break;
      }
      if (highPct >= tp1Pct) {
        tp1Hit = true;
      }
    } else if (lowPct <= beLockPct) {
      finalRet = 0.5 * tp1Pct + 0.5 * beLockPct;
      break;
    }
  }

  if (finalRet === null) {
    const lastClose = futureBars[futureBars.length - 1]!.close;
    const closeRetPct = ((lastClose - entryPrice) / entryPrice) * 100;
    finalRet = tp1Hit ? 0.5 * tp1Pct + 0.5 * closeRetPct : closeRetPct;
  }

  const maePct = ((minLow - entryPrice) / entryPrice) * 100;
  const mfePct = ((maxHigh - entryPrice) / entryPrice) * 100;
  const verdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL" =
    finalRet >= 0.35 ? "WOULD_WIN" : finalRet <= -0.35 ? "WOULD_LOSE" : "NEUTRAL";
  return { finalRet, maePct, mfePct, verdict };
}

export type LearningAutoTuneConfig = Pick<Config, "learningAutoTuneMode" | "dryRun" | "exchange" | "alpacaBaseUrl">;

export class LearningEngine {
  private readonly db: BotDatabase;
  private readonly log: (msg: string) => void;
  private symbolStates = new Map<string, SymbolLearningStateRecord>();
  private recordedBlockedKeys = new Set<string>();
  private lastPeriodicRunTs = 0;
  private periodicInProgress = false;
  private symbolStatesLoaded = false;

  // Shadow Walk-Forward & Survival Telemetry state
  private shadowSummary: ShadowLearningSummary | null = null;
  private shadowWinMaeAbs: number[] = [];
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
    const mode = cfg.learningAutoTuneMode || "dry_run_only";
    if (mode === "off") return false;
    if (mode === "all") return true;
    const isAlpacaPaper =
      String(cfg.exchange || "").toLowerCase() === "alpaca" &&
      String(cfg.alpacaBaseUrl || "").toLowerCase().includes("paper-api");
    return Boolean(cfg.dryRun || isAlpacaPaper);
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
    qty: number;
    allocatedQuote: number;
    snapshot?: PortfolioRadarCandidate;
    macro?: PortfolioMacroState;
    score: number;
  }): Promise<string> {
    const sym = params.symbol.toUpperCase();
    const journalUid = `trade_${params.entryTimeMs}_${params.slotIndex}_${sym}`;
    const pg = this.getPg();
    if (!pg) return journalUid;

    const snap = params.snapshot;
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
        breadthAboveEma25Pct: params.macro ? Number(params.macro.breadthAboveEma25Pct.toFixed(1)) : null,
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

    for (const c of params.candidates) {
      if (c.stage === "CROSS_8X16_FAIL_FILTERS") {
        consider(c, c.filterNote ? `FILTER:${c.filterNote}` : "FILTER_FAIL");
      } else if (c.stage === "PULLBACK_READY" && params.macro.isSitOut) {
        consider(c, "MACRO_SIT_OUT");
      }
    }

    if (Array.isArray(params.extraBlocked)) {
      for (const ex of params.extraBlocked) {
        consider(ex.candidate, ex.blockReason, ex.barTimeSec);
      }
    }

    if (chosenByBarSym.size === 0) return;

    const entries: LearningJournalRecord[] = [];
    const nowMs = Date.now();

    for (const [barSymKey, item] of chosenByBarSym.entries()) {
      if (this.recordedBlockedKeys.has(barSymKey)) continue;
      this.recordedBlockedKeys.add(barSymKey);

      const c = item.cand;
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

    for (const p of pending) {
      let bars: Candle[] = candleMap.get(p.symbol) || [];
      if (isAlpaca) {
        bars = bars.filter((b) => isUsRegularTradingHoursBar(b.time, "15m"));
      }
      const barStartSec = Number(p.features?.barTimeSec || Math.floor(p.entryTimeMs / 1000));
      const futureBars = bars.filter((b) => b.time > barStartSec).slice(0, 16);
      if (futureBars.length < 8 || p.entryPrice <= 0) continue;

      const distEma200Pct = Number(p.features?.distEma200Pct);
      const sim = simulateMaFanCounterfactual(p.entryPrice, distEma200Pct, slTriggerPct, futureBars);

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

    const journalTrades = await pg.loadLearningJournal("TRADE", "CLOSED", 500).catch(() => [] as LearningJournalRecord[]);
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
        if (sameDay.length > 0 && Math.abs((j.pnlQuote ?? 0) - sameDay[0]!.r.netPnlQuote) > 0.5) {
          matched = sameDay[0]!.r;
        }
      }

      if (!matched) continue;
      const realPnl = Number(matched.netPnlQuote ?? matched.grossPnlQuote ?? 0);
      const storedPnl = Number(j.pnlQuote ?? 0);
      if (Math.abs(realPnl - storedPnl) <= 0.25) continue;

      const alloc = Number(j.allocatedQuote || matched.buyCostQuote || 0);
      const nextPnlPct = alloc > 0 ? (realPnl / alloc) * 100 : Number(matched.grossPnlPct || 0);
      const nextHoldMin = matched.durationMs > 0 ? matched.durationMs / 60_000 : Number(j.holdMinutes ?? 0);
      const nextExitReason =
        realPnl < -0.5 && j.exitReason === "SL_BREAKEVEN" ? "RECONCILED_SL" : (j.exitReason ?? null);

      await pg
        .updateLearningJournalTradeFill({
          journalUid: j.journalUid,
          entryPrice: matched.avgBuyPrice > 0 ? matched.avgBuyPrice : j.entryPrice,
          exitPrice: matched.avgSellPrice > 0 ? matched.avgSellPrice : Number(j.exitPrice ?? j.entryPrice),
          pnlQuote: Number(realPnl.toFixed(4)),
          pnlPct: Number(nextPnlPct.toFixed(3)),
          holdMinutes: Number(nextHoldMin.toFixed(2)),
          exitReason: nextExitReason,
        })
        .catch(() => {});

      j.pnlQuote = realPnl;
      j.pnlPct = nextPnlPct;
      updatedTrades++;
      touchedSymbols.add(j.symbol.toUpperCase());
    }

    let updatedSymbols = 0;
    for (const sym of touchedSymbols) {
      const symTrades = filteredJournal.filter((j) => j.symbol.toUpperCase() === sym);
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
      const nextState: SymbolLearningStateRecord = {
        symbol: sym,
        consecutiveSl: losses === 0 ? 0 : (prev?.consecutiveSl ?? 0),
        wins,
        losses,
        totalPnlQuote: Number(totalPnlQuote.toFixed(4)),
        scorePenalty: wins > losses && totalPnlQuote >= 0 ? 0 : (prev?.scorePenalty ?? 0),
        cooldownUntilMs: losses === 0 ? 0 : (prev?.cooldownUntilMs ?? 0),
        lastReason: prev?.lastReason || "JOURNAL_RECONCILED",
        updatedAt: Date.now(),
      };
      this.symbolStates.set(sym, nextState);
      await pg.upsertSymbolLearningState(nextState).catch(() => {});
      updatedSymbols++;
    }

    if (updatedTrades > 0) {
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
      const allNeed15m = Array.from(new Set([benchSym, ...cleanSymbols]));

      const pg15m = pg
        ? await pg.loadMarketCandlesBatch(ex, allNeed15m, "15m", 800).catch(() => new Map<string, Candle[]>())
        : new Map<string, Candle[]>();
      const pg1h = pg
        ? await pg.loadMarketCandlesBatch(ex, cleanSymbols, "1h", 250).catch(() => new Map<string, Candle[]>())
        : new Map<string, Candle[]>();

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
      const benchStateByTime = new Map<number, { ret24h: number; isBear: boolean }>();
      if (bench15.length >= 200) {
        const benchCloses = bench15.map((c) => c.close);
        const benchM200 = calcEMA(benchCloses, 200);
        for (let i = 200; i < bench15.length; i++) {
          const ref24 = bench15[Math.max(0, i - 96)]!.open;
          const ret24h = ref24 > 0 ? ((bench15[i]!.close - ref24) / ref24) * 100 : 0;
          benchStateByTime.set(bench15[i]!.time, {
            ret24h,
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
      const bySymbol: Record<string, ShadowSymbolProfile> = {};
      const winMaeAbs: number[] = [];

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
        let hadBottomFan = false;
        let fanStartIdx = -1;
        let waitingPullback = false;
        let waitFromIdx = -1;
        let lastEntryIdx = -999;
        let p1 = -1;

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

          if (waitingPullback && (idx - waitFromIdx >= 12 || coin.m8[idx]! < coin.m16[idx]!)) {
            waitingPullback = false;
          }

          const prev = idx - 1;
          if (waitingPullback && idx > waitFromIdx && idx - lastEntryIdx >= 8 && coin.m8[prev]! >= coin.m16[prev]!) {
            const limitP = coin.m4[prev]!;
            if (limitP > 0 && c.low <= limitP * 1.001) {
              const rsi1hOk = !(coin.rsi1h.length >= 20 && p1 >= 14 && (coin.rsi1h[p1] ?? 50) < 40);
              let upV = 0;
              let dnV = 0;
              for (let k = Math.max(0, prev - 11); k <= prev; k++) {
                const b = coin.c15[k]!;
                const qv = (b.volume || 1) * b.close;
                if (b.close >= b.open) upV += qv;
                else dnV += qv;
              }
              const volR = dnV > 0 ? upV / dnV : 2.0;

              const prevBar = coin.c15[prev]!;
              const ref24 = coin.c15[Math.max(0, prev - 96)]!;
              const coinRet24 = ref24.open > 0 ? ((prevBar.close - ref24.open) / ref24.open) * 100 : 0;
              const benchInfo = benchStateByTime.get(prevBar.time);
              const rs24 = coinRet24 - (benchInfo?.ret24h ?? 0);

              if (rsi1hOk && volR >= 1.3 && rs24 >= -1.0) {
                const entryPrice = Math.min(c.open, limitP);
                const futureBars = coin.c15.slice(idx + 1, Math.min(n, idx + 33));
                if (entryPrice > 0 && futureBars.length >= 8) {
                  const dist200 = ((coin.m200[prev]! - entryPrice) / entryPrice) * 100;
                  const sim = simulateMaFanCounterfactual(entryPrice, dist200, slTriggerPct, futureBars);

                  lastEntryIdx = idx;
                  waitingPullback = false;
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
                  if (bStats && bStats.total >= 5 && (bStats.above25 / bStats.total) * 100 < 30) {
                    lowBreadthSetups++;
                    if (isWin) lowBreadthWins++;
                    lowBreadthPnlSum += sim.finalRet;
                  }
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
          }
          if (hadBottomFan && idx - fanStartIdx > 20 && !waitingPullback) {
            hadBottomFan = false;
          }

          const is8Cross16Up = coin.m8[idx - 1]! <= coin.m16[idx - 1]! && coin.m8[idx]! > coin.m16[idx]!;
          if (!waitingPullback && hadBottomFan && is8Cross16Up) {
            const rsiOk = (coin.rsi15[idx] ?? 0) >= 35 && (coin.rsi15[idx] ?? 0) > (coin.rsi15[idx - 1] ?? 0);
            const macdOk = (coin.macdHist15[idx] ?? 0) > (coin.macdHist15[idx - 1] ?? 0);
            if (rsiOk && macdOk) {
              waitingPullback = true;
              waitFromIdx = idx;
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
      };

      this.shadowSummary = summary;
      this.shadowWinMaeAbs = winMaeAbs;
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
    const enforced = this.isAutoTuneAllowed(params.cfg);

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

    let posture: SurvivalPostureLevel = "NORMAL";
    let effectiveMaxSlots = configuredMaxSlots;
    let positionSizeScale = 1.0;
    let minRelStrength24h = -1.0;
    let minRsi1h = 40;
    let minBuyVolRatio12 = 1.3;
    let blockRiskySymbols = false;
    const reasons: string[] = [];

    const capPresStreakTrigger = recentConsecutiveLosses >= 4 && (lastLossWithin24h || !recoveryOk);
    if (rollingDrawdownPct >= 7.0 || capPresStreakTrigger) {
      posture = "CAPITAL_PRESERVATION";
      effectiveMaxSlots = 0;
      positionSizeScale = 0;
      minRelStrength24h = 1.5;
      minRsi1h = 45;
      minBuyVolRatio12 = 1.6;
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
      minRelStrength24h = 1.0;
      minRsi1h = 42;
      minBuyVolRatio12 = 1.5;
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
      minRelStrength24h = 0.0;
      minRsi1h = 42;
      minBuyVolRatio12 = 1.3;
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
      enforced &&
      this.symbolStatesLoaded &&
      posture !== (this.lastLoggedPosture ?? prevPosture ?? "NORMAL") &&
      now - this.lastPostureLogTs >= POSTURE_LOG_COOLDOWN_MS
    ) {
      const oldPosture = this.lastLoggedPosture ?? prevPosture ?? "NORMAL";
      this.lastLoggedPosture = posture;
      this.lastPostureLogTs = now;
      this.log(`🛡️ [Survival Guard] Posture ${oldPosture} -> ${posture} | MaxSlots ${effectiveMaxSlots}/${configuredMaxSlots}, Size ${(positionSizeScale * 100).toFixed(0)}% (${reasons.join("; ")})`);
      void pg
        .recordLearningAdjustment({
          paramName: "survival_posture",
          oldValue: POSTURE_NUMERIC[oldPosture],
          newValue: POSTURE_NUMERIC[posture],
          sampleSize: Math.max(closedRounds.length, shadow?.totalSetups ?? 0),
          rationale: `${oldPosture} -> ${posture}: ${reasons.join("; ")}`,
          applied: true,
          createdAt: now,
        })
        .catch(() => {});
    }

    return decision;
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
      }

      if (shadowContext && now - this.lastShadowRunTs >= SHADOW_REFRESH_INTERVAL_MS) {
        await this.runShadowWalkForward(shadowContext).catch(() => {});
      }

      if (!pg || !this.isAutoTuneAllowed(cfg)) return;

      const adjustments = await pg.loadLearningAdjustments(30).catch(() => [] as LearningAdjustmentRecord[]);
      const lastSlAdj = adjustments.find((a) => a.paramName === "portfolioDisasterSlPct");
      if (lastSlAdj && now - lastSlAdj.createdAt < PARAM_NUDGE_INTERVAL_MS) {
        return;
      }

      const closed = await pg.loadLearningJournal("TRADE", "CLOSED", 200).catch(() => [] as LearningJournalRecord[]);
      const wins = closed.filter((t) => (t.pnlQuote ?? 0) > 0 && typeof t.maePct === "number");
      const realEnough = closed.length >= MIN_CLOSED_TRADES_FOR_SL_NUDGE && wins.length >= MIN_WIN_TRADES_FOR_SL_NUDGE;
      const shadowEnough =
        Boolean(this.shadowSummary && this.shadowSummary.totalSetups >= 30 && this.shadowSummary.wins >= 15 && this.shadowWinMaeAbs.length >= 10);

      if (!realEnough && !shadowEnough) return;

      const weightedWinMaeAbs: number[] = [];
      for (const w of wins) {
        const val = Math.abs(Number(w.maePct || 0));
        weightedWinMaeAbs.push(val, val, val); // Real wins carry 3x weight
      }
      for (const sm of this.shadowWinMaeAbs) {
        weightedWinMaeAbs.push(sm);
      }
      if (weightedWinMaeAbs.length === 0) return;

      const maeP80 = percentile(weightedWinMaeAbs, 0.8);
      const targetSl = Math.round(clamp(maeP80 + 0.8, 2.5, 5.0) * 4) / 4; // 0.25% step
      const curSl = Number(cfg.portfolioDisasterSlPct || 3.5);
      const boundedStep = clamp(targetSl - curSl, -0.25, 0.25);
      const nextSl = Number(clamp(curSl + boundedStep, 2.5, 5.0).toFixed(2));

      if (Math.abs(nextSl - curSl) >= 0.2) {
        const rationale = `Real(${closed.length}t/${wins.length}w) + Shadow(${this.shadowSummary?.totalSetups ?? 0}t/${this.shadowWinMaeAbs.length}w), Combined WinMAE_P80=-${maeP80.toFixed(2)}% -> target=${targetSl.toFixed(2)}%, bounded step ${curSl.toFixed(2)}% -> ${nextSl.toFixed(2)}%`;
        onApplySetting({ portfolioDisasterSlPct: nextSl });
        this.log(`🧠 [Learning Auto-Tune] portfolioDisasterSlPct: ${curSl}% -> ${nextSl}% (${rationale})`);
        await pg
          .recordLearningAdjustment({
            paramName: "portfolioDisasterSlPct",
            oldValue: curSl,
            newValue: nextSl,
            sampleSize: closed.length + (this.shadowSummary?.totalSetups ?? 0),
            rationale,
            applied: true,
            createdAt: now,
          })
          .catch(() => {});
      }
    } finally {
      this.periodicInProgress = false;
    }
  }

  public async buildReport(cfg: Config) {
    const pg = this.getPg();
    if (!pg) {
      return {
        postgresActive: false,
        autoTuneMode: cfg.learningAutoTuneMode || "dry_run_only",
        autoTuneEffective: false,
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
    const shadowEnough = Boolean(
      this.shadowSummary && this.shadowSummary.totalSetups >= 30 && this.shadowSummary.wins >= 15 && this.shadowWinMaeAbs.length >= 10,
    );
    const recommendedSlPct =
      (realEnough || shadowEnough) && combinedWinMaeP80Pct !== null
        ? Math.round(clamp(Math.abs(combinedWinMaeP80Pct) + 0.8, 2.5, 5.0) * 4) / 4
        : null;

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

    return {
      postgresActive: true,
      instanceKey: pg.instanceKey,
      autoTuneMode: cfg.learningAutoTuneMode || "dry_run_only",
      autoTuneEffective: this.isAutoTuneAllowed(cfg),
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
        samplesNeededForSlTune: shadowEnough ? 0 : Math.max(0, MIN_CLOSED_TRADES_FOR_SL_NUDGE - closedTrades.length),
      },
      survival: this.lastSurvivalDecision,
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
