/**
 * PostgreSQL-backed Learning Engine (Levels 1, 2 & 3)
 *
 * Level 1: Records full entry indicator snapshots, MAE/MFE trade outcomes, and
 *          near-miss blocked signals into `trade_learning_journal`.
 * Level 2: Resolves 4h counterfactual outcomes against `market_candles` and builds
 *          structured learning analytics (`/api/learning/report`).
 * Level 3: Applies bounded self-tuning (48h symbol cooldown after >= 2 consecutive SLs,
 *          and weekly +/-0.25% SL nudges within [2.5%, 5.0%]) with strict sample/time guardrails.
 */

import type { Config } from "./config.js";
import type { BotDatabase } from "./db.js";
import type {
  LearningAdjustmentRecord,
  LearningJournalRecord,
  PgStore,
  SymbolLearningStateRecord,
} from "./pg-store.js";
import type { PortfolioMacroState, PortfolioRadarCandidate } from "./strategy/ma-fan-portfolio/types.js";

const COOLDOWN_MS = 48 * 3_600_000;
const BLOCKED_EVAL_HORIZON_MS = 4 * 3_600_000;
const PARAM_NUDGE_INTERVAL_MS = 7 * 86_400_000;
const MIN_CLOSED_TRADES_FOR_SL_NUDGE = 20;
const MIN_WIN_TRADES_FOR_SL_NUDGE = 8;

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * p)));
  return sorted[idx]!;
}

function clamp(v: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, v));
}

function normalizeBlockReasonKey(rawReason: string | null | undefined): string {
  const first = (rawReason || "OTHER").split(",")[0]?.trim() || "OTHER";
  if (first === "MACRO_SIT_OUT" || first === "FILTER_FAIL") return first;
  if (first.startsWith("FILTER:")) {
    const detail = first.slice("FILTER:".length).trim();
    if (detail.startsWith("1hRSI")) return "FILTER:1hRSI<40";
    if (detail.startsWith("VolR")) return "FILTER:VolR<1.3";
    if (detail.startsWith("RS")) return "FILTER:RS<-1%";
    if (detail.includes("Cooldown")) return "FILTER:Cooldown";
  }
  return first;
}

export class LearningEngine {
  private readonly db: BotDatabase;
  private readonly log: (msg: string) => void;
  private symbolStates = new Map<string, SymbolLearningStateRecord>();
  private recordedBlockedKeys = new Set<string>();
  private lastPeriodicRunTs = 0;
  private periodicInProgress = false;
  private symbolStatesLoaded = false;

  constructor(db: BotDatabase, log?: (msg: string) => void) {
    this.db = db;
    this.log = log ?? ((m) => console.log(`[learning] ${m}`));
  }

  private getPg(): PgStore | null {
    return this.db.getPgStore();
  }

  public isAutoTuneAllowed(cfg: Pick<Config, "learningAutoTuneMode" | "dryRun">): boolean {
    const mode = cfg.learningAutoTuneMode || "dry_run_only";
    if (mode === "off") return false;
    if (mode === "all") return true;
    return Boolean(cfg.dryRun);
  }

  public async ensureSymbolStatesLoaded(): Promise<void> {
    if (this.symbolStatesLoaded) return;
    const pg = this.getPg();
    if (!pg) return;
    try {
      const list = await pg.loadSymbolLearningStates();
      for (const s of list) {
        this.symbolStates.set(s.symbol, s);
      }
      this.symbolStatesLoaded = true;
    } catch {}
  }

  public isSymbolOnCooldown(symbol: string, cfg: Pick<Config, "learningAutoTuneMode" | "dryRun">): boolean {
    if (!this.isAutoTuneAllowed(cfg)) return false;
    const st = this.symbolStates.get(symbol.toUpperCase());
    return Boolean(st && st.cooldownUntilMs > Date.now());
  }

  public getSymbolScorePenalty(symbol: string, cfg: Pick<Config, "learningAutoTuneMode" | "dryRun">): number {
    if (!this.isAutoTuneAllowed(cfg)) return 0;
    const st = this.symbolStates.get(symbol.toUpperCase());
    if (!st) return 0;
    if (st.cooldownUntilMs > Date.now()) return st.scorePenalty || -25;
    return 0;
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
    cfg: Pick<Config, "learningAutoTuneMode" | "dryRun">;
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

    const isSlHit = params.exitReason.startsWith("SL_");
    const isWin = params.totalPnlQuote > 0;
    const nextConsecSl = isSlHit ? prev.consecutiveSl + 1 : isWin ? 0 : prev.consecutiveSl;
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
  }): Promise<void> {
    const pg = this.getPg();
    if (!pg || params.barTimeSec <= 0 || params.candidates.length === 0) return;

    const entries: LearningJournalRecord[] = [];
    const nowMs = Date.now();

    for (const c of params.candidates) {
      let blockReason = "";
      if (c.stage === "CROSS_8X16_FAIL_FILTERS") {
        blockReason = c.filterNote ? `FILTER:${c.filterNote}` : "FILTER_FAIL";
      } else if (c.stage === "PULLBACK_READY" && params.macro.isSitOut) {
        blockReason = "MACRO_SIT_OUT";
      } else {
        continue;
      }

      const key = `${params.barTimeSec}_${c.symbol}_${blockReason}`;
      if (this.recordedBlockedKeys.has(key)) continue;
      this.recordedBlockedKeys.add(key);

      entries.push({
        journalUid: `blocked_${params.barTimeSec}_${c.symbol}`,
        recordType: "BLOCKED_SIGNAL",
        exchange: params.exchange,
        symbol: c.symbol,
        status: "PENDING_EVAL",
        entryTimeMs: nowMs,
        entryPrice: c.price,
        blockReason,
        features: {
          barTimeSec: params.barTimeSec,
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
        },
        updatedAt: nowMs,
      });
    }

    if (this.recordedBlockedKeys.size > 1500) {
      const keep = Array.from(this.recordedBlockedKeys).slice(-600);
      this.recordedBlockedKeys = new Set(keep);
    }

    if (entries.length > 0) {
      await pg.upsertLearningJournalBatch(entries).catch(() => {});
    }
  }

  // ── Level 2 & 3: Counterfactual Verification, Report & Bounded Tuning ────

  public async evaluatePendingBlockedSignals(exchange: string, slPct = 3.5): Promise<number> {
    const pg = this.getPg();
    if (!pg) return 0;

    const cutoffMs = Date.now() - BLOCKED_EVAL_HORIZON_MS;
    const pending = await pg.loadPendingBlockedSignals(cutoffMs, 100).catch(() => [] as LearningJournalRecord[]);
    if (pending.length === 0) return 0;

    const syms = Array.from(new Set(pending.map((p) => p.symbol)));
    const candleMap = await pg.loadMarketCandlesBatch(exchange, syms, "15m", 120).catch(() => new Map());

    const updates: Array<{
      journalUid: string;
      counterfactualPnlPct: number;
      counterfactualVerdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL";
      maePct: number;
      mfePct: number;
    }> = [];

    const tpTargetPct = 2.0;
    const slTriggerPct = -Math.max(1.5, slPct || 3.5);

    for (const p of pending) {
      const bars = candleMap.get(p.symbol) || [];
      const barStartSec = Number(p.features?.barTimeSec || Math.floor(p.entryTimeMs / 1000));
      const futureBars = bars.filter((b: any) => b.time > barStartSec).slice(0, 16);
      if (futureBars.length < 8 || p.entryPrice <= 0) continue;

      let minLow = p.entryPrice;
      let maxHigh = p.entryPrice;
      let barrierRet: number | null = null;

      for (const b of futureBars) {
        if (b.low < minLow) minLow = b.low;
        if (b.high > maxHigh) maxHigh = b.high;
        const lowPct = ((b.low - p.entryPrice) / p.entryPrice) * 100;
        const highPct = ((b.high - p.entryPrice) / p.entryPrice) * 100;
        if (lowPct <= slTriggerPct) {
          barrierRet = slTriggerPct;
          break;
        }
        if (highPct >= tpTargetPct) {
          barrierRet = tpTargetPct;
          break;
        }
      }

      const lastClose = futureBars[futureBars.length - 1]!.close;
      const finalRet = barrierRet ?? ((lastClose - p.entryPrice) / p.entryPrice) * 100;
      const maePct = ((minLow - p.entryPrice) / p.entryPrice) * 100;
      const mfePct = ((maxHigh - p.entryPrice) / p.entryPrice) * 100;
      const verdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL" =
        finalRet >= 0.35 ? "WOULD_WIN" : finalRet <= -0.35 ? "WOULD_LOSE" : "NEUTRAL";

      updates.push({
        journalUid: p.journalUid,
        counterfactualPnlPct: Number(finalRet.toFixed(3)),
        counterfactualVerdict: verdict,
        maePct: Number(maePct.toFixed(3)),
        mfePct: Number(mfePct.toFixed(3)),
      });
    }

    if (updates.length > 0) {
      await pg.updateBlockedEvaluations(updates).catch(() => {});
    }
    return updates.length;
  }

  public async runPeriodicCycle(
    cfg: Config,
    onApplySetting: (patch: Record<string, any>) => void,
  ): Promise<void> {
    const pg = this.getPg();
    if (!pg || this.periodicInProgress) return;

    const now = Date.now();
    if (now - this.lastPeriodicRunTs < 30 * 60_000) return;
    this.lastPeriodicRunTs = now;
    this.periodicInProgress = true;

    try {
      await this.ensureSymbolStatesLoaded();
      await this.evaluatePendingBlockedSignals(cfg.exchange || "binance", cfg.portfolioDisasterSlPct || 3.5);

      if (!this.isAutoTuneAllowed(cfg)) return;

      const adjustments = await pg.loadLearningAdjustments(20).catch(() => [] as LearningAdjustmentRecord[]);
      const lastSlAdj = adjustments.find((a) => a.paramName === "portfolioDisasterSlPct");
      if (lastSlAdj && now - lastSlAdj.createdAt < PARAM_NUDGE_INTERVAL_MS) {
        return;
      }

      const closed = await pg.loadLearningJournal("TRADE", "CLOSED", 200).catch(() => [] as LearningJournalRecord[]);
      const wins = closed.filter((t) => (t.pnlQuote ?? 0) > 0 && typeof t.maePct === "number");
      if (closed.length < MIN_CLOSED_TRADES_FOR_SL_NUDGE || wins.length < MIN_WIN_TRADES_FOR_SL_NUDGE) {
        return;
      }

      const winMaeAbs = wins.map((w) => Math.abs(Number(w.maePct || 0)));
      const maeP80 = percentile(winMaeAbs, 0.8);
      const targetSl = Math.round(clamp(maeP80 + 0.8, 2.5, 5.0) * 4) / 4; // 0.25% step
      const curSl = Number(cfg.portfolioDisasterSlPct || 3.5);
      const boundedStep = clamp(targetSl - curSl, -0.25, 0.25);
      const nextSl = Number(clamp(curSl + boundedStep, 2.5, 5.0).toFixed(2));

      if (Math.abs(nextSl - curSl) >= 0.2) {
        const rationale = `Closed=${closed.length}, WinMAE_P80=-${maeP80.toFixed(2)}% -> target=${targetSl.toFixed(2)}%, bounded step ${curSl.toFixed(2)}% -> ${nextSl.toFixed(2)}%`;
        onApplySetting({ portfolioDisasterSlPct: nextSl });
        this.log(`🧠 [Learning Auto-Tune] portfolioDisasterSlPct: ${curSl}% -> ${nextSl}% (${rationale})`);
        await pg
          .recordLearningAdjustment({
            paramName: "portfolioDisasterSlPct",
            oldValue: curSl,
            newValue: nextSl,
            sampleSize: closed.length,
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

    const wins = closedTrades.filter((t) => (t.pnlQuote ?? 0) > 0);
    const totalPnlQuote = closedTrades.reduce((s, t) => s + Number(t.pnlQuote ?? 0), 0);
    const avgPnlPct =
      closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + Number(t.pnlPct ?? 0), 0) / closedTrades.length : 0;
    const avgMaePct =
      closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + Number(t.maePct ?? 0), 0) / closedTrades.length : 0;
    const avgMfePct =
      closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + Number(t.mfePct ?? 0), 0) / closedTrades.length : 0;

    const winMaeP80Pct =
      wins.length >= 3 ? -percentile(wins.map((w) => Math.abs(Number(w.maePct ?? 0))), 0.8) : null;
    const recommendedSlPct =
      closedTrades.length >= MIN_CLOSED_TRADES_FOR_SL_NUDGE && winMaeP80Pct !== null
        ? Math.round(clamp(Math.abs(winMaeP80Pct) + 0.8, 2.5, 5.0) * 4) / 4
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

    const blockedStatsMap = new Map<string, { total: number; wouldWin: number; wouldLose: number; avgRetPct: number }>();
    for (const b of evaluatedBlocked) {
      const key = normalizeBlockReasonKey(b.blockReason);
      const cur = blockedStatsMap.get(key) || { total: 0, wouldWin: 0, wouldLose: 0, avgRetPct: 0 };
      cur.total++;
      if (b.counterfactualVerdict === "WOULD_WIN") cur.wouldWin++;
      if (b.counterfactualVerdict === "WOULD_LOSE") cur.wouldLose++;
      cur.avgRetPct += Number(b.counterfactualPnlPct ?? 0);
      blockedStatsMap.set(key, cur);
    }

    const blockedSummary = Array.from(blockedStatsMap.entries()).map(([reason, st]) => ({
      reason,
      evaluated: st.total,
      wouldWin: st.wouldWin,
      wouldLose: st.wouldLose,
      savedPct: st.total > 0 ? Number(((st.wouldLose / st.total) * 100).toFixed(1)) : 0,
      missedWinPct: st.total > 0 ? Number(((st.wouldWin / st.total) * 100).toFixed(1)) : 0,
      avgCounterfactualPnlPct: st.total > 0 ? Number((st.avgRetPct / st.total).toFixed(2)) : 0,
    }));

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
        wins: wins.length,
        losses: closedTrades.length - wins.length,
        winRatePct: closedTrades.length > 0 ? Number(((wins.length / closedTrades.length) * 100).toFixed(1)) : 0,
        totalPnlQuote: Number(totalPnlQuote.toFixed(2)),
        avgPnlPct: Number(avgPnlPct.toFixed(2)),
        avgMaePct: Number(avgMaePct.toFixed(2)),
        avgMfePct: Number(avgMfePct.toFixed(2)),
        winMaeP80Pct: winMaeP80Pct !== null ? Number(winMaeP80Pct.toFixed(2)) : null,
        currentDisasterSlPct: Number(cfg.portfolioDisasterSlPct || 3.5),
        recommendedDisasterSlPct: recommendedSlPct,
        samplesNeededForSlTune: Math.max(0, MIN_CLOSED_TRADES_FOR_SL_NUDGE - closedTrades.length),
      },
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
