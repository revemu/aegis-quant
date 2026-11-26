/**
 * Shared Core Order Management System (OMS)
 * -----------------------------------------
 * Exchange-agnostic layer sitting between Strategy engines and IExchangeClient adapters.
 * Provides:
 * 1. Write-Ahead Pending Order Persistence (recorded BEFORE API submission)
 * 2. Fail-Closed Pre-Flight Locks (pendingOrders + openOrders + liveHolding checks)
 * 3. Delta Position Sizing (Delta = min(targetCapital, hardSlotCap) - LiveHeld)
 * 4. 5-State Order Reconciliation with strict delta fill accounting (no double-count, no fail-open)
 * 5. Per-symbol in-flight mutex & error/retry cooldowns
 */

import type { CorePendingOrderRecord } from "../db.js";
import type {
  ExchangeLiveHolding,
  ExchangeOpenOrder,
  ExchangeOrderResult,
  IExchangeClient,
} from "../exchange/types.js";

export type { CorePendingOrderRecord };

export interface DeltaBuyPlanInput {
  targetCapital: number;
  hardSlotCap: number;
  liveHeldQty: number;
  livePrice: number;
  avgEntryPrice?: number;
  availCash: number;
  minNotional?: number;
  minShortfallRatio?: number;
  minShortfallUsd?: number;
  stepSize: number;
}

export interface DeltaBuyPlanResult {
  shouldBuy: boolean;
  reason: "OK" | "ALREADY_AT_TARGET" | "BELOW_MIN_NOTIONAL" | "INVALID_PRICE";
  effectiveCap: number;
  liveHeldNotional: number;
  rawShortfall: number;
  deltaQuote: number;
  deltaQty: number;
}

export interface TargetBuyRequest {
  symbol: string;
  intent: "ENTRY" | "TOP_UP";
  targetCapital: number;
  hardSlotCap: number;
  availCash: number;
  refPrice: number;
  capBuffer?: number;
  stepSize: number;
  pricePrecision?: number;
  minNotional?: number;
  minShortfallRatio?: number;
  minShortfallUsd?: number;
  barTimeSec?: number;
  attempt?: number;
  slotIndex?: number;
  reason?: string;
  knownHeldQty?: number;
  knownAvgEntryPrice?: number;
}

export interface TargetBuyResponse {
  outcome:
    | "FILLED"
    | "PARTIAL_TERMINAL"
    | "PARTIAL_WORKING"
    | "OPEN_WORKING"
    | "UNFILLED_TERMINAL"
    | "ALREADY_SATISFIED"
    | "SKIPPED"
    | "BLOCKED"
    | "ABORTED_FAIL_CLOSED"
    | "ERROR_PENDING_RECONCILE";
  reason?: string;
  error?: string;
  clientOrderId?: string;
  orderId?: string;
  executedQty: number;
  avgPrice: number;
  isTerminal: boolean;
  targetCapital: number;
  plan?: DeltaBuyPlanResult;
  liveHoldingBefore?: ExchangeLiveHolding;
}

export interface ManagedSellRequest {
  symbol: string;
  intent: "EXIT_TP1" | "EXIT_FULL";
  orderType: "IOC" | "MARKET";
  qty: number;
  price?: number;
  refPrice: number;
  pricePrecision?: number;
  slotIndex?: number;
  reason?: string;
  cancelConflictingOpenSells?: boolean;
}

export interface ManagedSellResponse {
  outcome:
    | "FILLED"
    | "PARTIAL_TERMINAL"
    | "PARTIAL_WORKING"
    | "OPEN_WORKING"
    | "UNFILLED_TERMINAL"
    | "BLOCKED"
    | "ABORTED_FAIL_CLOSED"
    | "ERROR_PENDING_RECONCILE";
  reason?: string;
  error?: string;
  clientOrderId?: string;
  orderId?: string;
  executedQty: number;
  avgPrice: number;
  isTerminal: boolean;
}

export interface ReconcileCallbacks {
  onFillDelta?: (
    rec: CorePendingOrderRecord,
    deltaQty: number,
    avgPrice: number,
    isTerminal: boolean,
  ) => Promise<void> | void;
  onTerminal?: (
    rec: CorePendingOrderRecord,
    finalStatus: string,
    totalExecutedQty: number,
    avgPrice: number,
  ) => Promise<void> | void;
  onDisappeared?: (rec: CorePendingOrderRecord) => Promise<void> | void;
}

export interface ReconcileSummary {
  checked: number;
  filledDeltas: number;
  resolvedTerminal: number;
  disappeared: number;
  stillOpen: number;
  apiErrors: number;
}

export interface CoreOrderManagerOptions {
  exchange: IExchangeClient;
  isDryRun: () => boolean;
  log?: (msg: string) => void;
  persist: (pending: CorePendingOrderRecord[]) => void;
  staleBuyCancelMs?: number;
  notFoundGraceMs?: number;
  nowFn?: () => number;
}

const TERMINAL_STATUSES = new Set([
  "FILLED",
  "CANCELED",
  "CANCELLED",
  "REJECTED",
  "EXPIRED",
  "DONE_FOR_DAY",
]);

/**
 * Computes exact remaining BUY quantity using Delta = min(targetCapital, hardSlotCap) - LiveHeld.
 * Exported as a pure function for deterministic verification and unit testing.
 */
export function computeDeltaBuyPlan(input: DeltaBuyPlanInput): DeltaBuyPlanResult {
  const effectiveCap = Math.max(0, Math.min(input.targetCapital, input.hardSlotCap));
  const livePrice = Number(input.livePrice || 0);
  if (!Number.isFinite(livePrice) || livePrice <= 0 || effectiveCap <= 0) {
    return {
      shouldBuy: false,
      reason: "INVALID_PRICE",
      effectiveCap,
      liveHeldNotional: 0,
      rawShortfall: 0,
      deltaQuote: 0,
      deltaQty: 0,
    };
  }

  const heldQty = Math.max(0, Number(input.liveHeldQty || 0));
  const avgEntry = Math.max(0, Number(input.avgEntryPrice || 0));
  // Use max of live market value and cost basis so price dips never cause over-buying above cost cap
  const marketNotional = heldQty * livePrice;
  const costNotional = avgEntry > 0 ? heldQty * avgEntry : 0;
  const liveHeldNotional = Math.max(marketNotional, costNotional);

  const rawShortfall = Math.max(0, effectiveCap - liveHeldNotional);
  const minRatio = input.minShortfallRatio ?? 0.1;
  const minNotional = Math.max(1, input.minNotional ?? 1);
  const minGapUsd = Math.max(minNotional, input.minShortfallUsd ?? 50);

  if (liveHeldNotional >= effectiveCap * (1 - minRatio) || rawShortfall < minGapUsd) {
    return {
      shouldBuy: false,
      reason: "ALREADY_AT_TARGET",
      effectiveCap,
      liveHeldNotional,
      rawShortfall,
      deltaQuote: 0,
      deltaQty: 0,
    };
  }

  const clampedQuote = Math.min(rawShortfall, Math.max(0, input.availCash));
  const stepSize = input.stepSize > 0 ? input.stepSize : 0.0001;
  const rawQty = clampedQuote / livePrice;
  const deltaQty = Number((Math.floor(rawQty / stepSize + 1e-12) * stepSize).toFixed(8));
  const deltaQuote = deltaQty * livePrice;

  if (deltaQty <= 0 || deltaQuote < minNotional) {
    return {
      shouldBuy: false,
      reason: "BELOW_MIN_NOTIONAL",
      effectiveCap,
      liveHeldNotional,
      rawShortfall,
      deltaQuote,
      deltaQty: 0,
    };
  }

  return {
    shouldBuy: true,
    reason: "OK",
    effectiveCap,
    liveHeldNotional,
    rawShortfall,
    deltaQuote,
    deltaQty,
  };
}

export function isOrderNotFoundError(err: unknown): boolean {
  const msg = String((err as Error)?.message || err || "");
  return (
    /\b404\b/.test(msg) ||
    /ORDER_NOT_FOUND/i.test(msg) ||
    msg.includes("-2013") ||
    /order does not exist/i.test(msg) ||
    /unknown order/i.test(msg)
  );
}

export function isTerminalExchangeOrder(status?: string, isWorking?: boolean): boolean {
  const upper = String(status || "").toUpperCase();
  if (!TERMINAL_STATUSES.has(upper)) return false;
  return isWorking !== true;
}

export class CoreOrderManager {
  private exchange: IExchangeClient;
  private isDryRun: () => boolean;
  private log: (msg: string) => void;
  private persist: (pending: CorePendingOrderRecord[]) => void;
  private staleBuyCancelMs: number;
  private notFoundGraceMs: number;
  private nowFn: () => number;

  private pendingOrders: CorePendingOrderRecord[] = [];
  private inFlightSymbols = new Set<string>();
  private symbolCooldownUntil = new Map<string, number>();
  private recentEntryTargets = new Map<
    string,
    { targetCapital: number; entryTime: number; deadlineMs: number }
  >();

  constructor(opts: CoreOrderManagerOptions) {
    this.exchange = opts.exchange;
    this.isDryRun = opts.isDryRun;
    this.log = opts.log ?? (() => {});
    this.persist = opts.persist;
    this.staleBuyCancelMs = opts.staleBuyCancelMs ?? 30_000;
    this.notFoundGraceMs = opts.notFoundGraceMs ?? 15_000;
    this.nowFn = opts.nowFn ?? (() => Date.now());
  }

  public setExchange(exchange: IExchangeClient): void {
    this.exchange = exchange;
  }

  public loadPendingOrders(records: CorePendingOrderRecord[]): void {
    this.pendingOrders = Array.isArray(records) ? records.map((r) => ({ ...r })) : [];
  }

  public getPendingOrders(): CorePendingOrderRecord[] {
    return this.pendingOrders.map((r) => ({ ...r }));
  }

  public hasBlockingPendingOrder(symbol: string, side?: "BUY" | "SELL"): boolean {
    if (this.inFlightSymbols.has(symbol)) return true;
    return this.pendingOrders.some(
      (r) => r.symbol === symbol && (!side || r.side === side),
    );
  }

  public isOnCooldown(symbol: string, side: "BUY" | "SELL"): boolean {
    const until = this.symbolCooldownUntil.get(`${symbol}:${side}`) ?? 0;
    return this.nowFn() < until;
  }

  public getRecentEntryTarget(
    symbol: string,
  ): { targetCapital: number; entryTime: number; deadlineMs: number } | undefined {
    const entry = this.recentEntryTargets.get(symbol);
    if (!entry) return undefined;
    if (this.nowFn() > entry.deadlineMs) {
      this.recentEntryTargets.delete(symbol);
      return undefined;
    }
    return { ...entry };
  }

  private setCooldown(symbol: string, side: "BUY" | "SELL", ms: number): void {
    this.symbolCooldownUntil.set(`${symbol}:${side}`, this.nowFn() + ms);
  }

  private buildClientOrderId(
    side: "BUY" | "SELL",
    symbol: string,
    barTimeSec?: number,
    attempt = 0,
  ): string {
    const cleanSym = symbol.replace(/[^A-Za-z0-9]/g, "").slice(0, 8) || "SYM";
    const sTag = side === "BUY" ? "b" : "s";
    const barTag = String(barTimeSec || Math.floor(this.nowFn() / 1000)).slice(-7);
    const rand = Math.random().toString(36).slice(2, 6);
    // Keep strictly <= 36 chars so both Binance Spot and Alpaca accept it
    return `ag_${sTag}_${cleanSym}_${barTag}_${attempt}_${rand}`.slice(0, 36);
  }

  private async fetchLiveHoldingSafe(symbol: string): Promise<ExchangeLiveHolding> {
    if (typeof this.exchange.getLiveHolding === "function") {
      return await this.exchange.getLiveHolding(symbol);
    }
    const bal = await this.exchange.getAccountBalances(symbol);
    return {
      symbol,
      qty: Number(bal.baseTotal || 0),
      qtyAvailable: Number(bal.baseFree || 0),
      avgEntryPrice: 0,
    };
  }

  private extractAvgPrice(
    order: Partial<ExchangeOpenOrder | ExchangeOrderResult>,
    fallbackPrice: number,
  ): number {
    const execQty = Number(order.executedQty || 0);
    const cumQuote = Number(order.cummulativeQuoteQty || 0);
    if (execQty > 0 && cumQuote > 0) return cumQuote / execQty;
    if (order.price && Number(order.price) > 0) return Number(order.price);
    return fallbackPrice;
  }

  private async lookupPendingOrderOnExchange(
    rec: CorePendingOrderRecord,
  ): Promise<ExchangeOpenOrder | ExchangeOrderResult | null> {
    if (rec.orderId) {
      try {
        return await this.exchange.getOrder(rec.symbol, rec.orderId);
      } catch (err) {
        if (!isOrderNotFoundError(err)) {
          throw err;
        }
        // Fallback to clientOrderId if broker rotated internal ID, else confirm missing
        if (typeof this.exchange.getOrderByClientId === "function") {
          return await this.exchange.getOrderByClientId(rec.symbol, rec.clientOrderId);
        }
        return null;
      }
    }

    if (typeof this.exchange.getOrderByClientId === "function") {
      return await this.exchange.getOrderByClientId(rec.symbol, rec.clientOrderId);
    }

    // Fallback for adapters without getOrderByClientId (throws on API error -> fail-closed)
    const openOrders = await this.exchange.getOpenOrders(rec.symbol);
    const foundOpen = openOrders.find(
      (o) => o.clientOrderId === rec.clientOrderId || (rec.orderId && String(o.orderId) === rec.orderId),
    );
    if (foundOpen) return foundOpen;

    if (typeof this.exchange.getRecentOrders === "function") {
      const recents = await this.exchange.getRecentOrders(rec.symbol, 50);
      const foundRecent = recents.find(
        (o) => o.clientOrderId === rec.clientOrderId || (rec.orderId && String(o.orderId) === rec.orderId),
      );
      if (foundRecent) return foundRecent;
    }

    return null;
  }

  private removePendingOrder(clientOrderId: string): void {
    const before = this.pendingOrders.length;
    this.pendingOrders = this.pendingOrders.filter((r) => r.clientOrderId !== clientOrderId);
    if (this.pendingOrders.length !== before) {
      this.persist(this.getPendingOrders());
    }
  }

  /**
   * 5-State Order Reconciler:
   * 1) OPEN / working -> stays in pendingOrders (blocking duplicates)
   * 2) PARTIALLY_FILLED -> applies ONLY incremental deltaFill (fixes double-counting bug)
   * 3) TERMINAL (FILLED / CANCELED / REJECTED / EXPIRED) -> applies final deltaFill, removes record
   * 4) DISAPPEARED (404 + empty getOpenOrders past grace) -> clears safely after callback
   * 5) API_ERROR -> FAIL-CLOSED (keeps pending record intact & blocks new orders)
   */
  public async reconcilePendingOrders(callbacks: ReconcileCallbacks = {}): Promise<ReconcileSummary> {
    const summary: ReconcileSummary = {
      checked: 0,
      filledDeltas: 0,
      resolvedTerminal: 0,
      disappeared: 0,
      stillOpen: 0,
      apiErrors: 0,
    };

    if (this.isDryRun() || !this.exchange.hasCredentials() || this.pendingOrders.length === 0) {
      return summary;
    }

    const snapshot = [...this.pendingOrders];
    const now = this.nowFn();

    for (const liveRef of snapshot) {
      const rec = this.pendingOrders.find((r) => r.clientOrderId === liveRef.clientOrderId);
      if (!rec) continue;
      summary.checked++;

      let exOrder: ExchangeOpenOrder | ExchangeOrderResult | null = null;
      try {
        exOrder = await this.lookupPendingOrderOnExchange(rec);
      } catch (err) {
        // Case 5: Fail-Closed on API/Network error — NEVER delete pending record blindly!
        summary.apiErrors++;
        this.log(
          `⚠️ [OMS Reconcile Fail-Closed] ${rec.symbol} ${rec.side} (${rec.clientOrderId}) lookup error — keeping lock intact: ${(err as Error).message}`,
        );
        continue;
      }

      if (!exOrder) {
        // Case 4: Exchange explicitly returned 404 / -2013
        rec.notFoundChecks = (rec.notFoundChecks || 0) + 1;
        const ageMs = now - rec.createdAt;
        if (ageMs < this.notFoundGraceMs || rec.notFoundChecks < 2) {
          this.persist(this.getPendingOrders());
          summary.stillOpen++;
          continue;
        }

        // Cross-check getOpenOrders before declaring disappeared (fail-closed if throws)
        try {
          const openList = await this.exchange.getOpenOrders(rec.symbol);
          const stillWorking = openList.find(
            (o) => o.clientOrderId === rec.clientOrderId || (rec.orderId && String(o.orderId) === rec.orderId),
          );
          if (stillWorking) {
            rec.orderId = String(stillWorking.orderId || rec.orderId || "");
            rec.status = stillWorking.executedQty > 0 ? "PARTIALLY_FILLED" : "OPEN";
            rec.notFoundChecks = 0;
            this.persist(this.getPendingOrders());
            summary.stillOpen++;
            continue;
          }
        } catch (err) {
          summary.apiErrors++;
          this.log(
            `⚠️ [OMS Reconcile Fail-Closed] Open-orders verification failed for ${rec.symbol} (${rec.clientOrderId}): ${(err as Error).message}`,
          );
          continue;
        }

        this.log(
          `🧹 [OMS Reconcile] ${rec.symbol} ${rec.side} order (${rec.clientOrderId}) confirmed ORDER_DISAPPEARED after ${(ageMs / 1000).toFixed(1)}s — clearing lock & reconciling holdings`,
        );
        this.removePendingOrder(rec.clientOrderId);
        summary.disappeared++;
        await callbacks.onDisappeared?.(rec);
        continue;
      }

      // Order found on exchange -> sync ID, incremental fills, and terminal state
      if (exOrder.orderId) rec.orderId = String(exOrder.orderId);
      const cumFilled = Math.max(0, Number(exOrder.executedQty || 0));
      const deltaFill = Math.max(0, cumFilled - Number(rec.filledQtyAccounted || 0));
      const avgPrice = this.extractAvgPrice(exOrder, rec.lastFillPrice || rec.refPrice);
      const upperStatus = String(exOrder.status || "").toUpperCase();
      const isTerminal = isTerminalExchangeOrder(upperStatus, (exOrder as ExchangeOpenOrder).isWorking);

      if (deltaFill > 1e-8) {
        rec.filledQtyAccounted = cumFilled;
        rec.lastFillPrice = avgPrice;
        rec.updatedAt = now;
        this.persist(this.getPendingOrders());
        summary.filledDeltas++;
        await callbacks.onFillDelta?.(rec, deltaFill, avgPrice, isTerminal);
      }

      if (isTerminal) {
        this.removePendingOrder(rec.clientOrderId);
        summary.resolvedTerminal++;
        await callbacks.onTerminal?.(rec, upperStatus, cumFilled, avgPrice);
      } else {
        rec.status = cumFilled > 0 ? "PARTIALLY_FILLED" : rec.status === "CANCELING" ? "CANCELING" : "OPEN";
        rec.notFoundChecks = 0;
        rec.updatedAt = now;

        // Cancel stale working BUY order, but keep CANCELING lock until exchange confirms terminal
        if (
          rec.side === "BUY" &&
          rec.status !== "CANCELING" &&
          now - rec.createdAt >= this.staleBuyCancelMs &&
          rec.orderId
        ) {
          try {
            await this.exchange.cancelOrder(rec.symbol, rec.orderId);
            rec.status = "CANCELING";
            this.log(
              `🗑️ [OMS Reconcile] Cancelled stale ${rec.symbol} BUY #${rec.orderId} (${cumFilled}/${rec.requestedQty} filled) — holding lock until terminal confirmation`,
            );
          } catch (err) {
            this.log(`⚠️ [OMS Reconcile] Cancel attempt failed for ${rec.symbol} #${rec.orderId}: ${(err as Error).message}`);
          }
        }

        this.persist(this.getPendingOrders());
        summary.stillOpen++;
      }
    }

    return summary;
  }

  /**
   * Executes a BUY up to `min(targetCapital, hardSlotCap)` minus live held position on exchange.
   * Enforces:
   * - In-flight mutex + Pending order lock + Cooldown
   * - Fail-closed exchange open-order check & live holding check
   * - Write-Ahead persistence of clientOrderId BEFORE hitting placeOrder()
   */
  public async submitTargetBuy(req: TargetBuyRequest): Promise<TargetBuyResponse> {
    const symbol = req.symbol;
    const hardCap = Math.min(req.targetCapital, req.hardSlotCap);

    if (this.inFlightSymbols.has(symbol)) {
      return {
        outcome: "BLOCKED",
        reason: "IN_FLIGHT",
        executedQty: 0,
        avgPrice: req.refPrice,
        isTerminal: false,
        targetCapital: hardCap,
      };
    }
    if (this.hasBlockingPendingOrder(symbol, "BUY")) {
      return {
        outcome: "BLOCKED",
        reason: "PENDING_ORDER_EXISTS",
        executedQty: 0,
        avgPrice: req.refPrice,
        isTerminal: false,
        targetCapital: hardCap,
      };
    }
    if (this.isOnCooldown(symbol, "BUY")) {
      return {
        outcome: "BLOCKED",
        reason: "SYMBOL_BUY_COOLDOWN",
        executedQty: 0,
        avgPrice: req.refPrice,
        isTerminal: false,
        targetCapital: hardCap,
      };
    }

    this.inFlightSymbols.add(symbol);
    try {
      const isLive = !this.isDryRun() && this.exchange.hasCredentials();
      let liveHolding: ExchangeLiveHolding = {
        symbol,
        qty: Math.max(0, Number(req.knownHeldQty || 0)),
        qtyAvailable: Math.max(0, Number(req.knownHeldQty || 0)),
        avgEntryPrice: Math.max(0, Number(req.knownAvgEntryPrice || 0)),
      };

      if (isLive) {
        // Lock 1: Fail-Closed Open BUY Order Check
        let openOrders: ExchangeOpenOrder[];
        try {
          openOrders = await this.exchange.getOpenOrders(symbol);
        } catch (err) {
          this.setCooldown(symbol, "BUY", 15_000);
          this.log(`🛑 [OMS Fail-Closed] ${symbol} aborted: getOpenOrders failed (${(err as Error).message})`);
          return {
            outcome: "ABORTED_FAIL_CLOSED",
            reason: "OPEN_ORDERS_CHECK_FAILED",
            error: (err as Error).message,
            executedQty: 0,
            avgPrice: req.refPrice,
            isTerminal: false,
            targetCapital: hardCap,
          };
        }

        const existingOpenBuy = openOrders.find((o) => o.side === "BUY");
        if (existingOpenBuy) {
          this.setCooldown(symbol, "BUY", 15_000);
          // Track untracked open buy in pendingOrders so reconciler manages it
          const cid = existingOpenBuy.clientOrderId || `ext_${existingOpenBuy.orderId}`;
          if (!this.pendingOrders.some((r) => r.clientOrderId === cid || r.orderId === String(existingOpenBuy.orderId))) {
            this.pendingOrders.push({
              clientOrderId: cid,
              orderId: String(existingOpenBuy.orderId),
              symbol,
              side: "BUY",
              intent: req.intent,
              targetCapital: hardCap,
              requestedQty: Number(existingOpenBuy.origQty || 0),
              filledQtyAccounted: Math.max(0, Number(req.knownHeldQty || 0)),
              refPrice: req.refPrice,
              createdAt: (existingOpenBuy.time || Math.floor(this.nowFn() / 1000)) * 1000,
              updatedAt: this.nowFn(),
              status: existingOpenBuy.executedQty > 0 ? "PARTIALLY_FILLED" : "OPEN",
              slotIndex: req.slotIndex,
              reason: req.reason,
            });
            this.persist(this.getPendingOrders());
          }
          this.log(`🛑 [OMS Lock 1] Blocked ${symbol} BUY — exchange already has open BUY #${existingOpenBuy.orderId}`);
          return {
            outcome: "BLOCKED",
            reason: "EXCHANGE_OPEN_BUY_EXISTS",
            orderId: String(existingOpenBuy.orderId),
            clientOrderId: existingOpenBuy.clientOrderId,
            executedQty: 0,
            avgPrice: req.refPrice,
            isTerminal: false,
            targetCapital: hardCap,
          };
        }

        // Lock 2: Fail-Closed Live Position Check
        try {
          liveHolding = await this.fetchLiveHoldingSafe(symbol);
        } catch (err) {
          this.setCooldown(symbol, "BUY", 15_000);
          this.log(`🛑 [OMS Fail-Closed] ${symbol} aborted: getLiveHolding failed (${(err as Error).message})`);
          return {
            outcome: "ABORTED_FAIL_CLOSED",
            reason: "HOLDING_CHECK_FAILED",
            error: (err as Error).message,
            executedQty: 0,
            avgPrice: req.refPrice,
            isTerminal: false,
            targetCapital: hardCap,
          };
        }
      }

      const effectiveAvgEntry =
        liveHolding.avgEntryPrice > 0 ? liveHolding.avgEntryPrice : Math.max(0, Number(req.knownAvgEntryPrice || 0));

      // Lock 3: Delta Calculation against Live Held Position
      const plan = computeDeltaBuyPlan({
        targetCapital: req.targetCapital,
        hardSlotCap: req.hardSlotCap,
        liveHeldQty: liveHolding.qty,
        livePrice: req.refPrice,
        avgEntryPrice: effectiveAvgEntry,
        availCash: req.availCash,
        minNotional: req.minNotional,
        minShortfallRatio: req.minShortfallRatio,
        minShortfallUsd: req.minShortfallUsd,
        stepSize: req.stepSize,
      });

      if (!plan.shouldBuy) {
        if (plan.reason === "ALREADY_AT_TARGET") {
          return {
            outcome: "ALREADY_SATISFIED",
            reason: plan.reason,
            executedQty: 0,
            avgPrice: req.refPrice,
            isTerminal: true,
            targetCapital: plan.effectiveCap,
            plan,
            liveHoldingBefore: liveHolding,
          };
        }
        return {
          outcome: "SKIPPED",
          reason: plan.reason,
          executedQty: 0,
          avgPrice: req.refPrice,
          isTerminal: true,
          targetCapital: plan.effectiveCap,
          plan,
          liveHoldingBefore: liveHolding,
        };
      }

      // Preserve intended targetCapital for adopt recovery if process/order gets interrupted
      this.recentEntryTargets.set(symbol, {
        targetCapital: plan.effectiveCap,
        entryTime: this.nowFn(),
        deadlineMs: this.nowFn() + 15 * 60_000,
      });

      const clientOrderId = this.buildClientOrderId("BUY", symbol, req.barTimeSec, req.attempt ?? 0);

      if (!isLive) {
        return {
          outcome: "FILLED",
          clientOrderId,
          orderId: `sim_${clientOrderId}`,
          executedQty: plan.deltaQty,
          avgPrice: req.refPrice,
          isTerminal: true,
          targetCapital: plan.effectiveCap,
          plan,
          liveHoldingBefore: liveHolding,
        };
      }

      // Lock 4: Write-Ahead Persistence BEFORE sending API order
      const pendingRec: CorePendingOrderRecord = {
        clientOrderId,
        symbol,
        side: "BUY",
        intent: req.intent,
        targetCapital: plan.effectiveCap,
        requestedQty: plan.deltaQty,
        filledQtyAccounted: 0,
        refPrice: req.refPrice,
        createdAt: this.nowFn(),
        updatedAt: this.nowFn(),
        status: "SUBMITTING",
        slotIndex: req.slotIndex,
        reason: req.reason,
      };
      this.pendingOrders.push(pendingRec);
      this.persist(this.getPendingOrders());

      const limitPx = Number((req.refPrice * (req.capBuffer ?? 1.003)).toFixed(req.pricePrecision || 2));
      try {
        const res = await this.exchange.placeOrder({
          symbol,
          side: "BUY",
          type: "IOC",
          qty: plan.deltaQty,
          price: limitPx,
          clientOrderId,
        });

        pendingRec.orderId = res.orderId ? String(res.orderId) : pendingRec.orderId;
        const execQty = Math.max(0, Number(res.executedQty || 0));
        const avgPrice = this.extractAvgPrice(res, req.refPrice);
        const isTerminal = isTerminalExchangeOrder(res.status, (res as any).isWorking);

        if (isTerminal) {
          pendingRec.filledQtyAccounted = execQty;
          this.removePendingOrder(clientOrderId);
          if (execQty > 0) {
            if (execQty < plan.deltaQty * 0.99) {
              this.setCooldown(symbol, "BUY", 15_000);
            }
            return {
              outcome: execQty >= plan.deltaQty * 0.99 ? "FILLED" : "PARTIAL_TERMINAL",
              clientOrderId,
              orderId: pendingRec.orderId,
              executedQty: execQty,
              avgPrice,
              isTerminal: true,
              targetCapital: plan.effectiveCap,
              plan,
              liveHoldingBefore: liveHolding,
            };
          }
          this.setCooldown(symbol, "BUY", 15_000);
          return {
            outcome: "UNFILLED_TERMINAL",
            clientOrderId,
            orderId: pendingRec.orderId,
            executedQty: 0,
            avgPrice,
            isTerminal: true,
            targetCapital: plan.effectiveCap,
            plan,
            liveHoldingBefore: liveHolding,
          };
        }

        // Non-terminal (still OPEN / PARTIALLY_FILLED / PENDING_CANCEL on exchange) -> KEEP LOCK!
        pendingRec.status = execQty > 0 ? "PARTIALLY_FILLED" : "OPEN";
        pendingRec.filledQtyAccounted = execQty;
        pendingRec.lastFillPrice = avgPrice;
        pendingRec.updatedAt = this.nowFn();
        this.persist(this.getPendingOrders());
        this.setCooldown(symbol, "BUY", 15_000);

        return {
          outcome: execQty > 0 ? "PARTIAL_WORKING" : "OPEN_WORKING",
          clientOrderId,
          orderId: pendingRec.orderId,
          executedQty: execQty,
          avgPrice,
          isTerminal: false,
          targetCapital: plan.effectiveCap,
          plan,
          liveHoldingBefore: liveHolding,
        };
      } catch (err) {
        const errAny = err as any;
        if (errAny?.orderId) pendingRec.orderId = String(errAny.orderId);
        pendingRec.status = "OPEN";
        pendingRec.updatedAt = this.nowFn();
        this.persist(this.getPendingOrders());
        this.setCooldown(symbol, "BUY", 60_000);

        return {
          outcome: "ERROR_PENDING_RECONCILE",
          error: (err as Error).message,
          clientOrderId,
          orderId: pendingRec.orderId,
          executedQty: 0,
          avgPrice: req.refPrice,
          isTerminal: false,
          targetCapital: plan.effectiveCap,
          plan,
          liveHoldingBefore: liveHolding,
        };
      }
    } finally {
      this.inFlightSymbols.delete(symbol);
    }
  }

  /**
   * Executes a managed SELL order with Write-Ahead persistence and duplicate-sell protection.
   */
  public async submitManagedSell(req: ManagedSellRequest): Promise<ManagedSellResponse> {
    const symbol = req.symbol;
    if (this.inFlightSymbols.has(symbol)) {
      return {
        outcome: "BLOCKED",
        reason: "IN_FLIGHT",
        executedQty: 0,
        avgPrice: req.refPrice,
        isTerminal: false,
      };
    }
    if (this.hasBlockingPendingOrder(symbol, "SELL")) {
      return {
        outcome: "BLOCKED",
        reason: "PENDING_SELL_EXISTS",
        executedQty: 0,
        avgPrice: req.refPrice,
        isTerminal: false,
      };
    }

    this.inFlightSymbols.add(symbol);
    try {
      const isLive = !this.isDryRun() && this.exchange.hasCredentials();
      const clientOrderId = this.buildClientOrderId("SELL", symbol, undefined, 0);

      if (!isLive) {
        return {
          outcome: "FILLED",
          clientOrderId,
          orderId: `sim_${clientOrderId}`,
          executedQty: req.qty,
          avgPrice: req.refPrice,
          isTerminal: true,
        };
      }

      // Check existing open sells on exchange (fail-closed)
      let openOrders: ExchangeOpenOrder[];
      try {
        openOrders = await this.exchange.getOpenOrders(symbol);
      } catch (err) {
        this.log(`🛑 [OMS Fail-Closed] ${symbol} SELL aborted: getOpenOrders failed (${(err as Error).message})`);
        return {
          outcome: "ABORTED_FAIL_CLOSED",
          reason: "OPEN_ORDERS_CHECK_FAILED",
          error: (err as Error).message,
          executedQty: 0,
          avgPrice: req.refPrice,
          isTerminal: false,
        };
      }

      const openSells = openOrders.filter((o) => o.side === "SELL");
      if (openSells.length > 0) {
        const hasPendingMarket = openSells.some((o) => String(o.type || "").toUpperCase() === "MARKET");
        if (hasPendingMarket || !req.cancelConflictingOpenSells) {
          const existing = openSells[0]!;
          const cid = existing.clientOrderId || `ext_sell_${existing.orderId}`;
          if (!this.pendingOrders.some((r) => r.clientOrderId === cid || r.orderId === String(existing.orderId))) {
            this.pendingOrders.push({
              clientOrderId: cid,
              orderId: String(existing.orderId),
              symbol,
              side: "SELL",
              intent: req.intent,
              targetCapital: 0,
              requestedQty: Number(existing.origQty || req.qty),
              filledQtyAccounted: 0,
              refPrice: req.refPrice,
              createdAt: (existing.time || Math.floor(this.nowFn() / 1000)) * 1000,
              updatedAt: this.nowFn(),
              status: existing.executedQty > 0 ? "PARTIALLY_FILLED" : "OPEN",
              slotIndex: req.slotIndex,
              reason: req.reason,
            });
            this.persist(this.getPendingOrders());
          }
          return {
            outcome: "OPEN_WORKING",
            reason: "EXCHANGE_OPEN_SELL_EXISTS",
            orderId: String(existing.orderId),
            clientOrderId: existing.clientOrderId,
            executedQty: 0,
            avgPrice: req.refPrice,
            isTerminal: false,
          };
        }

        // Urgent exit overrides stale non-market open sells cleanly before placing MARKET exit
        for (const os of openSells) {
          await this.exchange.cancelOrder(symbol, os.orderId).catch(() => {});
        }
        await new Promise((r) => setTimeout(r, 800));
      }

      const pendingRec: CorePendingOrderRecord = {
        clientOrderId,
        symbol,
        side: "SELL",
        intent: req.intent,
        targetCapital: 0,
        requestedQty: req.qty,
        filledQtyAccounted: 0,
        refPrice: req.refPrice,
        createdAt: this.nowFn(),
        updatedAt: this.nowFn(),
        status: "SUBMITTING",
        slotIndex: req.slotIndex,
        reason: req.reason,
      };
      this.pendingOrders.push(pendingRec);
      this.persist(this.getPendingOrders());

      try {
        const res = await this.exchange.placeOrder({
          symbol,
          side: "SELL",
          type: req.orderType,
          qty: req.qty,
          price: req.price !== undefined ? Number(req.price.toFixed(req.pricePrecision || 2)) : undefined,
          clientOrderId,
        });

        pendingRec.orderId = res.orderId ? String(res.orderId) : pendingRec.orderId;
        const execQty = Math.max(0, Number(res.executedQty || 0));
        const avgPrice = this.extractAvgPrice(res, req.refPrice);
        const isTerminal = isTerminalExchangeOrder(res.status, (res as any).isWorking);

        if (isTerminal) {
          pendingRec.filledQtyAccounted = execQty;
          this.removePendingOrder(clientOrderId);
          if (execQty > 0) {
            return {
              outcome: execQty >= req.qty * 0.99 ? "FILLED" : "PARTIAL_TERMINAL",
              clientOrderId,
              orderId: pendingRec.orderId,
              executedQty: execQty,
              avgPrice,
              isTerminal: true,
            };
          }
          return {
            outcome: "UNFILLED_TERMINAL",
            clientOrderId,
            orderId: pendingRec.orderId,
            executedQty: 0,
            avgPrice,
            isTerminal: true,
          };
        }

        pendingRec.status = execQty > 0 ? "PARTIALLY_FILLED" : "OPEN";
        pendingRec.filledQtyAccounted = execQty;
        pendingRec.lastFillPrice = avgPrice;
        pendingRec.updatedAt = this.nowFn();
        this.persist(this.getPendingOrders());
        return {
          outcome: execQty > 0 ? "PARTIAL_WORKING" : "OPEN_WORKING",
          clientOrderId,
          orderId: pendingRec.orderId,
          executedQty: execQty,
          avgPrice,
          isTerminal: false,
        };
      } catch (err) {
        const errAny = err as any;
        if (errAny?.orderId) pendingRec.orderId = String(errAny.orderId);
        pendingRec.status = "OPEN";
        pendingRec.updatedAt = this.nowFn();
        this.persist(this.getPendingOrders());
        return {
          outcome: "ERROR_PENDING_RECONCILE",
          error: (err as Error).message,
          clientOrderId,
          orderId: pendingRec.orderId,
          executedQty: 0,
          avgPrice: req.refPrice,
          isTerminal: false,
        };
      }
    } finally {
      this.inFlightSymbols.delete(symbol);
    }
  }
}
