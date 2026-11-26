/**
 * Shared Core Portfolio & Risk Manager
 * ------------------------------------
 * Exchange-agnostic portfolio accounting, capital budgeting, position/cost-basis ledger,
 * and live exchange position reconciliation layer shared across portfolio strategies.
 */

import type { PortfolioSlotRecord } from "../db.js";
import type { ExchangeLiveHolding, IExchangeClient } from "../exchange/types.js";
import type { Lot } from "../strategy/types.js";

export interface SlotEntryBudgetInput {
  standardSlotUnit: number;
  availCash: number;
  sizeScale?: number;
  volTargetedEnabled?: boolean;
  atrPct?: number;
  medianAtrPct?: number;
}

export interface SlotEntryBudgetResult {
  allocQuote: number;
  targetCapital: number;
  hardSlotCap: number;
  volScale: number;
}

/**
 * Pure entry sizing helper combining standard slot unit, survival scale,
 * optional inverse-volatility scaling, and available cash clamping.
 */
export function computeSlotEntryBudget(input: SlotEntryBudgetInput): SlotEntryBudgetResult {
  const sizeScale = Math.max(0, Math.min(1, input.sizeScale ?? 1.0));
  let volScale = 1.0;
  const atrPct = Number(input.atrPct ?? 0);
  const medianAtrPct = Number(input.medianAtrPct ?? 0);
  if (input.volTargetedEnabled && atrPct > 0 && medianAtrPct > 0 && atrPct > medianAtrPct * 1.35) {
    volScale = Math.max(0.7, Math.min(1.0, (medianAtrPct * 1.25) / atrPct));
  }
  const hardSlotCap = Math.max(0, Number(input.standardSlotUnit || 0));
  const rawTarget = hardSlotCap * sizeScale * volScale;
  const availCash = Math.max(0, Number(input.availCash || 0));
  const allocQuote = Math.max(0, Math.min(rawTarget, availCash, hardSlotCap));
  const targetCapital = Math.min(allocQuote, hardSlotCap);
  return { allocQuote, targetCapital, hardSlotCap, volScale };
}

export interface CorePortfolioManagerOptions {
  exchange: IExchangeClient;
  isDryRun: () => boolean;
  getMaxSlots: () => number;
  getCapitalUsagePct: () => number;
  getMaxBudgetUsd: () => number;
  getDisasterSlPct: () => number;
  getDefaultCapital?: () => number;
  getFeeRate?: (symbol: string) => number;
  getLivePrice?: (symbol: string) => number | undefined;
  log?: (msg: string) => void;
  nowFn?: () => number;
}

export interface PortfolioPersistedStateSnapshot {
  portfolioSlots: PortfolioSlotRecord[];
  portfolioCash: number;
  portfolioInitialCapital: number;
  realizedPnl: number;
  tradeRealizedPnl: number;
}

export interface BuyFillLedgerResult {
  slot: PortfolioSlotRecord;
  isTopUp: boolean;
  actualCost: number;
  entryFee: number;
}

export interface SellFillLedgerResult {
  boundedQty: number;
  proceeds: number;
  exitFee: number;
  legPnl: number;
  remainingQty: number;
}

export interface PositionSyncResult {
  supported: boolean;
  retainedSlots: PortfolioSlotRecord[];
  clearedSlots: PortfolioSlotRecord[];
  adoptedSlots: PortfolioSlotRecord[];
  shrunkSymbols: Set<string>;
}

export class CorePortfolioManager {
  public slots: PortfolioSlotRecord[] = [];
  public cashBalance = 0;
  public initialCapital = 0;
  public realizedPnl = 0;
  public tradeRealizedPnl = 0;
  public walletQuoteBalance = 0;
  public walletQuoteTotal = 0;
  public lastBalanceSyncTs = 0;

  private exchange: IExchangeClient;
  private isDryRun: () => boolean;
  private getMaxSlotsFn: () => number;
  private getCapitalUsagePctFn: () => number;
  private getMaxBudgetUsdFn: () => number;
  private getDisasterSlPctFn: () => number;
  private getDefaultCapitalFn: () => number;
  private getFeeRateFn: (symbol: string) => number;
  private getLivePriceFn: (symbol: string) => number | undefined;
  private log: (msg: string) => void;
  private nowFn: () => number;

  constructor(opts: CorePortfolioManagerOptions) {
    this.exchange = opts.exchange;
    this.isDryRun = opts.isDryRun;
    this.getMaxSlotsFn = opts.getMaxSlots;
    this.getCapitalUsagePctFn = opts.getCapitalUsagePct;
    this.getMaxBudgetUsdFn = opts.getMaxBudgetUsd;
    this.getDisasterSlPctFn = opts.getDisasterSlPct;
    this.getDefaultCapitalFn = opts.getDefaultCapital ?? (() => 10_000);
    this.getFeeRateFn = opts.getFeeRate ?? (() => 0.00075);
    this.getLivePriceFn = opts.getLivePrice ?? (() => undefined);
    this.log = opts.log ?? (() => {});
    this.nowFn = opts.nowFn ?? (() => Date.now());
  }

  public setExchange(exchange: IExchangeClient): void {
    this.exchange = exchange;
  }

  // ── State Persistence & Reset ──────────────────────────────────────────────

  public loadState(saved?: {
    portfolioSlots?: PortfolioSlotRecord[];
    portfolioCash?: number;
    portfolioInitialCapital?: number;
    realizedPnl?: number;
    tradeRealizedPnl?: number;
  }): void {
    if (saved) {
      this.slots = Array.isArray(saved.portfolioSlots) ? saved.portfolioSlots.map((s) => ({ ...s })) : [];
      this.realizedPnl = Number(saved.realizedPnl || 0);
      this.tradeRealizedPnl = Number(saved.tradeRealizedPnl ?? saved.realizedPnl ?? 0);
      this.cashBalance = Number(saved.portfolioCash || 0);
      this.initialCapital = Number(saved.portfolioInitialCapital || 0);
    } else {
      this.slots = [];
      this.realizedPnl = 0;
      this.tradeRealizedPnl = 0;
    }
  }

  public exportState(): PortfolioPersistedStateSnapshot {
    return {
      portfolioSlots: this.slots.map((s) => ({ ...s })),
      portfolioCash: this.cashBalance,
      portfolioInitialCapital: this.initialCapital,
      realizedPnl: this.realizedPnl,
      tradeRealizedPnl: this.tradeRealizedPnl,
    };
  }

  public resetPositions(): number {
    const cleared = this.slots.length;
    this.slots = [];
    return cleared;
  }

  public resetState(opts?: { resetPnl?: boolean; fullReset?: boolean }): void {
    this.slots = [];
    if (opts?.resetPnl || opts?.fullReset) {
      this.realizedPnl = 0;
      this.tradeRealizedPnl = 0;
      this.cashBalance = this.initialCapital;
    }
  }

  public syncRealizedPnl(netPnl: number, grossPnl?: number): void {
    this.realizedPnl = netPnl;
    this.tradeRealizedPnl = grossPnl ?? netPnl;
  }

  public removeSlot(slotIndex: number): void {
    this.slots = this.slots.filter((s) => s.slotIndex !== slotIndex);
  }

  public getBaseHeldTotal(): number {
    return this.slots.reduce((s, x) => s + x.qty, 0);
  }

  public toLots(): Lot[] {
    return this.slots.map((s, idx) => ({
      price: s.entryPrice,
      qty: s.qty,
      time: s.entryTime,
      zoneIndex: idx,
      stopLossPrice: s.stopLossPrice,
      beLocked: s.beLocked,
      profitLocked: s.tp1Done,
    }));
  }

  // ── Capital Budgeting & MM Sizing ──────────────────────────────────────────

  public getMaxSlots(): number {
    return Math.max(1, Math.min(5, Number(this.getMaxSlotsFn()) || 3));
  }

  public getCapitalUsageRatio(): number {
    const pct = Number(this.getCapitalUsagePctFn());
    return Math.min(100, Math.max(10, Number.isFinite(pct) && pct > 0 ? pct : 50)) / 100;
  }

  public getTotalDeployedCost(): number {
    return this.slots.reduce(
      (s, slot) => s + (slot.entryPrice > 0 ? slot.qty * slot.entryPrice : slot.allocatedCapital),
      0,
    );
  }

  public getTotalHeldValue(): number {
    return this.slots.reduce(
      (s, slot) =>
        s + slot.qty * (this.getLivePriceFn(slot.symbol) || slot.lastKnownPrice || slot.entryPrice),
      0,
    );
  }

  public getEffectiveBudgetCap(): number {
    const cap = Number(this.getMaxBudgetUsdFn()) || 0;
    if (cap > 0) return cap;
    const deployed = this.getTotalDeployedCost();
    const heldVal = this.getTotalHeldValue();
    const rawFree =
      !this.isDryRun() && this.exchange.hasCredentials() && this.walletQuoteBalance > 0
        ? this.walletQuoteBalance
        : this.cashBalance;
    const liveEquity = this.walletQuoteTotal > 0 ? this.walletQuoteTotal : rawFree + (heldVal || deployed);
    // Anchor to initialCapital (up to 1.25x liveEquity) so normal drawdowns absorbed by reserve buffer do not shrink slot size
    if (this.initialCapital > 0 && liveEquity > 0) {
      return Math.max(liveEquity, Math.min(this.initialCapital, liveEquity * 1.25));
    }
    if (this.initialCapital > 0) return this.initialCapital;
    if (liveEquity > 0) return liveEquity;
    return this.getDefaultCapitalFn();
  }

  public getMaxActiveBudget(): number {
    return this.getEffectiveBudgetCap() * this.getCapitalUsageRatio();
  }

  public getStandardSlotUnit(): number {
    return this.getMaxActiveBudget() / this.getMaxSlots();
  }

  public getAvailableBuyCash(): number {
    const deployed = this.getTotalDeployedCost();
    const mmRemaining = Math.max(0, this.getMaxActiveBudget() - deployed);
    if (!this.isDryRun() && this.exchange.hasCredentials() && (this.lastBalanceSyncTs > 0 || this.walletQuoteBalance > 0)) {
      return Math.min(Math.max(0, this.walletQuoteBalance), mmRemaining);
    }
    if (this.cashBalance > 0) {
      return Math.min(this.cashBalance, mmRemaining);
    }
    return mmRemaining;
  }

  public computeCurrentEquity(): number {
    const maxBudget = Number(this.getMaxBudgetUsdFn()) || 0;
    if (maxBudget > 0) {
      return Math.max(10, maxBudget + this.realizedPnl);
    }
    if (this.walletQuoteTotal > 0) {
      return this.walletQuoteTotal;
    }
    return (this.cashBalance > 0 ? this.cashBalance : this.getEffectiveBudgetCap()) + this.getTotalHeldValue();
  }

  // ── Position & Cost-Basis Ledger Mutations ─────────────────────────────────

  public applyBuyFill(opts: {
    symbol: string;
    qty: number;
    execPrice: number;
    targetCapital: number;
    slotIndexHint?: number;
    entryReason?: string;
    createdAtMs?: number;
  }): BuyFillLedgerResult {
    const { symbol, qty: execQty, execPrice } = opts;
    const now = this.nowFn();
    const actualCost = execQty * execPrice;
    const feeRate = Math.max(0, Number(this.getFeeRateFn(symbol) || 0));
    const entryFee = actualCost * feeRate;
    const slPct = Math.max(0, Number(this.getDisasterSlPctFn() ?? 3.5));
    const maxSlots = this.getMaxSlots();
    const existingSlot = this.slots.find((s) => s.symbol === symbol);

    let activeSlot: PortfolioSlotRecord;
    let isTopUp = false;

    if (existingSlot) {
      isTopUp = true;
      const prevCost = existingSlot.qty * existingSlot.entryPrice;
      const newQty = existingSlot.qty + execQty;
      const newAvgEntry = newQty > 0 ? (prevCost + actualCost) / newQty : execPrice;
      existingSlot.qty = newQty;
      existingSlot.initialQty = (existingSlot.initialQty || 0) + execQty;
      existingSlot.entryPrice = newAvgEntry;
      existingSlot.allocatedCapital = (existingSlot.allocatedCapital || prevCost) + actualCost;
      existingSlot.entryFeeQuote = (existingSlot.entryFeeQuote || 0) + entryFee;
      existingSlot.lastKnownPrice = execPrice;
      existingSlot.peakPrice = Math.max(existingSlot.peakPrice || execPrice, execPrice);
      existingSlot.troughPrice = existingSlot.troughPrice > 0 ? Math.min(existingSlot.troughPrice, execPrice) : execPrice;
      if (!existingSlot.beLocked && !existingSlot.tp1Done && slPct > 0) {
        existingSlot.stopLossPrice = newAvgEntry * (1 - slPct / 100);
      }
      if (opts.targetCapital > 0) {
        existingSlot.targetCapital = existingSlot.targetCapital
          ? Math.min(existingSlot.targetCapital, opts.targetCapital)
          : opts.targetCapital;
      }
      existingSlot.lastBuyAttemptMs = now;
      activeSlot = existingSlot;
    } else {
      const usedIdx = new Set(this.slots.map((s) => s.slotIndex));
      let slotIndex =
        typeof opts.slotIndexHint === "number" &&
        opts.slotIndexHint >= 0 &&
        opts.slotIndexHint < maxSlots &&
        !usedIdx.has(opts.slotIndexHint)
          ? opts.slotIndexHint
          : 0;
      while (usedIdx.has(slotIndex)) slotIndex++;

      const slPrice = slPct > 0 ? execPrice * (1 - slPct / 100) : 0;
      const baseCreatedAt = opts.createdAtMs || now;

      activeSlot = {
        slotIndex,
        symbol,
        entryTime: baseCreatedAt,
        entryPrice: execPrice,
        allocatedCapital: actualCost,
        targetCapital: opts.targetCapital > 0 ? opts.targetCapital : actualCost,
        topUpAttempts: 0,
        topUpDeadlineMs: baseCreatedAt + 15 * 60_000,
        lastBuyAttemptMs: now,
        stopLossPrice: slPrice,
        qty: execQty,
        initialQty: execQty,
        troughPrice: execPrice,
        peakPrice: execPrice,
        lastKnownPrice: execPrice,
        beLocked: false,
        tp1Done: false,
        realizedProceeds: 0,
        entryFeeQuote: entryFee,
        realizedExitFees: 0,
        activeBarsHeld: 0,
        entryReason: opts.entryReason,
      };
      this.slots.push(activeSlot);
    }

    if (this.isDryRun() || this.cashBalance > 0) {
      this.cashBalance = Math.max(0, this.cashBalance - actualCost);
    }
    if (this.walletQuoteBalance > 0) {
      this.walletQuoteBalance = Math.max(0, this.walletQuoteBalance - actualCost);
    }

    return { slot: activeSlot, isTopUp, actualCost, entryFee };
  }

  public applySellFill(
    slot: PortfolioSlotRecord,
    opts: { sellQty: number; execPrice: number },
  ): SellFillLedgerResult {
    const boundedQty = Math.max(0, Math.min(slot.qty, opts.sellQty));
    const execPrice = opts.execPrice;
    const proceeds = boundedQty * execPrice;
    const feeRate = Math.max(0, Number(this.getFeeRateFn(slot.symbol) || 0));
    const exitFee = proceeds * feeRate;
    const origQty = slot.initialQty > 0 ? slot.initialQty : slot.qty;
    const soldShareOfInitial = origQty > 0 ? Math.min(1, boundedQty / origQty) : 1;
    const costBasisSlice = slot.entryPrice > 0 ? boundedQty * slot.entryPrice : slot.allocatedCapital * soldShareOfInitial;
    const entryFeeSlice = slot.entryFeeQuote * soldShareOfInitial;
    const legPnl = proceeds - exitFee - costBasisSlice - entryFeeSlice;

    slot.realizedProceeds += proceeds - exitFee;
    slot.realizedExitFees += exitFee;
    slot.qty = Math.max(0, slot.qty - boundedQty);

    this.realizedPnl += legPnl;
    this.tradeRealizedPnl += legPnl;
    if (this.isDryRun() || this.cashBalance >= 0) {
      this.cashBalance += proceeds - exitFee;
    }
    if (this.walletQuoteBalance >= 0) {
      this.walletQuoteBalance += proceeds - exitFee;
    }

    return { boundedQty, proceeds, exitFee, legPnl, remainingQty: slot.qty };
  }

  // ── Live Exchange Wallet & Position Sync ───────────────────────────────────

  public async refreshWalletBalances(targetSymbol: string, firstSlotQty = 0): Promise<any> {
    try {
      if (!this.isDryRun() && this.exchange.hasCredentials()) {
        const bal = await this.exchange.getAccountBalances(targetSymbol);
        this.walletQuoteBalance = bal.quoteFree || 0;
        this.walletQuoteTotal = bal.quoteTotal || this.walletQuoteBalance;
        if (this.initialCapital <= 0 && this.walletQuoteTotal > 0) {
          this.initialCapital = this.walletQuoteTotal;
        }
        if (this.cashBalance <= 0 && this.walletQuoteBalance > 0) {
          this.cashBalance = this.walletQuoteBalance;
        }
        this.lastBalanceSyncTs = this.nowFn();
        return {
          base: bal.baseFree || 0,
          quote: this.walletQuoteBalance,
          somi: 0,
          usdso: this.walletQuoteBalance,
          address: this.exchange.exchangeName.toUpperCase(),
        };
      }
    } catch (err) {
      this.log(`⚠️ Portfolio wallet refresh error: ${(err as Error).message}`);
    }

    if (this.initialCapital <= 0) {
      const cap = Number(this.getMaxBudgetUsdFn()) || 0;
      this.initialCapital = cap > 0 ? cap : this.getDefaultCapitalFn();
    }
    const deployed = this.getTotalDeployedCost();
    const estFree = Math.max(0, this.initialCapital + this.realizedPnl - deployed);
    this.cashBalance = estFree;
    this.walletQuoteBalance = estFree;
    const heldMarketVal = this.slots.reduce((s, slot) => s + slot.qty * (slot.lastKnownPrice || slot.entryPrice), 0);
    this.walletQuoteTotal = estFree + heldMarketVal;
    return {
      base: firstSlotQty,
      quote: estFree,
      somi: 0,
      usdso: estFree,
      address: this.isDryRun() ? "SIMULATED" : this.exchange.exchangeName.toUpperCase(),
    };
  }

  public async syncPositionsWithExchange(opts?: {
    getRecentEntryTarget?: (sym: string) => { targetCapital: number; entryTime: number; deadlineMs: number } | undefined;
    adoptedReason?: string;
  }): Promise<PositionSyncResult> {
    const emptyResult: PositionSyncResult = {
      supported: false,
      retainedSlots: this.slots,
      clearedSlots: [],
      adoptedSlots: [],
      shrunkSymbols: new Set(),
    };
    if (this.isDryRun() || !this.exchange.hasCredentials() || typeof this.exchange.getAllPositions !== "function") {
      return emptyResult;
    }

    const positions = await this.exchange.getAllPositions();
    const heldMap = new Map<string, ExchangeLiveHolding>(positions.map((p) => [p.symbol, p]));
    const retainedSlots: PortfolioSlotRecord[] = [];
    const clearedSlots: PortfolioSlotRecord[] = [];
    const adoptedSlots: PortfolioSlotRecord[] = [];
    const shrunkSymbols = new Set<string>();

    for (const slot of this.slots) {
      const exPos = heldMap.get(slot.symbol);
      if (exPos && exPos.qty > 0) {
        if (exPos.qty < slot.qty - 1e-6) {
          shrunkSymbols.add(slot.symbol);
        }
        slot.qty = exPos.qty;
        if (exPos.currentPrice && exPos.currentPrice > 0) {
          slot.lastKnownPrice = exPos.currentPrice;
        }
        if (slot.entryPrice <= 0 && exPos.avgEntryPrice > 0) {
          slot.entryPrice = exPos.avgEntryPrice;
        }
        retainedSlots.push(slot);
        heldMap.delete(slot.symbol);
      } else {
        clearedSlots.push(slot);
        this.log(
          `🔄 [Portfolio Sync] Slot ${slot.slotIndex + 1} (${slot.symbol}) position is 0 on exchange — clearing closed slot & reconciling fills`,
        );
      }
    }
    this.slots = retainedSlots;

    const maxSlots = this.getMaxSlots();
    const standardCap = this.getStandardSlotUnit();
    const slPct = Math.max(0, Number(this.getDisasterSlPctFn() ?? 3.5));

    for (const [sym, p] of heldMap.entries()) {
      if (this.slots.length >= maxSlots) break;
      const usedIdx = new Set(this.slots.map((s) => s.slotIndex));
      let slotIndex = 0;
      while (usedIdx.has(slotIndex)) slotIndex++;

      const entryPrice = p.avgEntryPrice || p.currentPrice || 0;
      const allocatedCapital = p.marketValue && p.marketValue > 0 ? p.marketValue : entryPrice * p.qty;
      const knownTarget = opts?.getRecentEntryTarget?.(sym);
      const adoptedTargetCap = knownTarget ? Math.min(knownTarget.targetCapital, standardCap) : undefined;
      const adoptedSlot: PortfolioSlotRecord = {
        slotIndex,
        symbol: sym,
        entryTime: knownTarget?.entryTime || this.nowFn(),
        entryPrice,
        allocatedCapital,
        ...(adoptedTargetCap
          ? { targetCapital: adoptedTargetCap, topUpDeadlineMs: knownTarget!.deadlineMs, topUpAttempts: 0 }
          : {}),
        stopLossPrice: slPct > 0 ? entryPrice * (1 - slPct / 100) : 0,
        qty: p.qty,
        initialQty: p.qty,
        troughPrice: p.currentPrice || entryPrice,
        peakPrice: p.currentPrice || entryPrice,
        lastKnownPrice: p.currentPrice || entryPrice,
        beLocked: false,
        tp1Done: false,
        realizedProceeds: 0,
        entryFeeQuote: 0,
        realizedExitFees: 0,
        activeBarsHeld: 0,
        entryReason: opts?.adoptedReason || "Adopted Exchange Position",
      };
      this.slots.push(adoptedSlot);
      adoptedSlots.push(adoptedSlot);
      this.log(
        `🔄 [Portfolio Sync] Adopted live position into Slot ${slotIndex + 1}: ${sym} (${p.qty} @ $${entryPrice.toFixed(2)})`,
      );
    }

    return {
      supported: true,
      retainedSlots,
      clearedSlots,
      adoptedSlots,
      shrunkSymbols,
    };
  }
}
