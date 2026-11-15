/**
 * MA-Fan Multi-Slot Portfolio Strategy ("ma_fan_portfolio")
 * Executes the walk-forward verified MA-Fan Portfolio strategy (maxSlots = 3, 15m EMA4 Pullback -> EMA200 / Full Bullish Fan)
 * across a multi-ticker universe (US Stocks on Alpaca or Crypto on Binance) with real-time slot inspection.
 */

import type { Config } from "../../config.js";
import type { IExchangeClient, ExchangeSymbolInfo } from "../../exchange/types.js";
import { fetchAlpacaMultiSnapshots, normalizeStockSymbol } from "../../exchange/alpaca/feed.js";
import { AlpacaPortfolioWsClient } from "../../exchange/alpaca/ws.js";
import type { Candle } from "../../market-structure.js";
import type { AtrSource } from "../../types.js";
import { BotDatabase, getDatabase, type PortfolioSlotRecord, type UnifiedOrderRecord } from "../../db.js";
import { LearningEngine } from "../../learning-engine.js";
import type { IStrategy, Lot, MarketClockInfo, StrategyContext, StrategyEvent, StrategyType } from "../types.js";
import type {
  CoinSetupState,
  PortfolioMacroState,
  PortfolioRadarCandidate,
  PortfolioSlotTelemetry,
  SymbolIndicatorSnapshot,
} from "./types.js";
import {
  buildSymbolIndicators,
  evaluateBenchmarkSitOut,
  fetchPortfolioUniverseAndBenchmark,
} from "./universe.js";

const PORTFOLIO_DB_KEY = "__PORTFOLIO__";

interface ActivePortfolioSlot extends PortfolioSlotRecord {
  tp1TargetPrice?: number;
}

export class MaFanPortfolioStrategy implements IStrategy {
  public readonly name = "MA-Fan Portfolio (3-Slot Rotation)";
  public readonly strategyType: StrategyType = "ma_fan_portfolio";

  public atrSource: AtrSource;
  private exchange: IExchangeClient;
  private symbolInfo: ExchangeSymbolInfo;
  private cfg: Config;
  private log: (msg: string) => void;
  private db: BotDatabase;
  private learning: LearningEngine;

  private eventListeners: ((event: StrategyEvent) => void)[] = [];
  private isPaused: boolean = true;
  private isMarketClosed: boolean = false;
  private marketClock?: MarketClockInfo;
  private isSwitchingSymbol: boolean = false;
  private tickInProgress: boolean = false;

  // Portfolio Capital & State
  private slots: ActivePortfolioSlot[] = [];
  private cashBalance: number = 0;
  private initialCapital: number = 0;
  private realizedPnl: number = 0;
  private tradeRealizedPnl: number = 0;
  private walletQuoteBalance: number = 0;
  private walletQuoteTotal: number = 0;

  // Universe Scanner & Indicators
  private snapshots = new Map<string, SymbolIndicatorSnapshot>();
  private setupStates = new Map<string, CoinSetupState>();
  private radarList: PortfolioRadarCandidate[] = [];
  private benchmark4h: Candle[] = [];
  private benchmark1d: Candle[] = [];
  private benchmark15m: Candle[] = [];
  private benchmarkSymbol: string = "SPY";
  private macroState: PortfolioMacroState = {
    benchmarkSymbol: "SPY",
    benchmarkPrice: 0,
    benchmarkRet24h: 0,
    isSitOut: false,
    sitOutMode: "dual",
    reason: "Initializing portfolio scanner...",
    breadthAboveEma25Pct: 50,
    universeCount: 0,
    pass4h: true,
    pass1d: true,
    lastScanTime: 0,
  };

  private inspectedSymbol: string = "";
  private lastUniverseFetchTs: number = 0;
  private lastSnapshotPollTs: number = 0;
  private lastBalanceSyncTs: number = 0;
  private lastTelemetryEmitTs: number = 0;
  private universeFetchPromise: Promise<void> | null = null;

  // Real-time Alpaca WebSocket hybrid stream (<= 30 symbols) & live chart sync
  private alpacaWs?: AlpacaPortfolioWsClient;
  private wsConnected: boolean = false;
  private lastWsCredsKey: string = "";
  private lastChartEmitTs: number = 0;
  private lastSyncedClosed15mBySymbol = new Map<string, number>();
  private lastSyncedClosed1hBySymbol = new Map<string, number>();

  constructor(context: StrategyContext) {
    this.exchange = context.exchange;
    this.symbolInfo = context.symbolInfo;
    this.cfg = { ...context.config };
    this.atrSource = context.atrSource;
    this.log = context.log ?? ((msg) => console.log(`[ma-fan-portfolio] ${msg}`));
    this.db = getDatabase({ dbPath: this.cfg.dbPath });
    this.learning = new LearningEngine(this.db, this.log);

    this.inspectedSymbol = this.resolveInitialInspectedSymbol();
    this.reloadState();
    void this.learning.ensureSymbolStatesLoaded();
  }

  private resolveInitialInspectedSymbol(): string {
    const isAlpaca = this.exchange.exchangeName === "alpaca";
    const candidate = this.cfg.portfolioInspectedSymbol || this.cfg.symbol || this.symbolInfo.symbol;
    return isAlpaca ? (normalizeStockSymbol(candidate) || "AAPL") : String(candidate || "BTCUSDT").toUpperCase();
  }

  private resolveAlpacaCreds() {
    return {
      apiKey: this.getRealAlpacaApiKey() || "",
      apiSecret: this.getRealAlpacaApiSecret() || "",
      dataUrl: this.cfg.alpacaDataUrl || "https://data.alpaca.markets",
    };
  }

  private getMaxSlots(): number {
    return Math.max(1, Math.min(5, Number(this.cfg.portfolioMaxSlots) || 3));
  }

  private getEffectiveBudgetCap(): number {
    const cap = Number(this.cfg.portfolioMaxBudgetUsd) || 0;
    if (cap > 0) return cap;
    if (this.initialCapital > 0) return this.initialCapital;
    if (this.walletQuoteTotal > 0) return this.walletQuoteTotal;
    return this.exchange.exchangeName === "alpaca" ? 10_000 : 200;
  }

  private getAvailableBuyCash(): number {
    const cap = Number(this.cfg.portfolioMaxBudgetUsd) || 0;
    const deployed = this.slots.reduce((s, slot) => s + slot.allocatedCapital, 0);
    if (cap > 0) {
      const budgetRemaining = Math.max(0, cap + this.realizedPnl - deployed);
      if (!this.cfg.dryRun && this.walletQuoteBalance > 0) {
        return Math.min(this.walletQuoteBalance, budgetRemaining);
      }
      return budgetRemaining;
    }
    if (!this.cfg.dryRun && this.exchange.hasCredentials() && this.walletQuoteBalance > 0) {
      return this.walletQuoteBalance;
    }
    if (this.cashBalance > 0) return this.cashBalance;
    const baseCap = this.exchange.exchangeName === "alpaca" ? 10_000 : 200;
    return Math.max(0, baseCap + this.realizedPnl - deployed);
  }

  // ── Lifecycle & Interface Methods ──────────────────────────────────────────

  public getIntervalMs(): number {
    return Math.max(500, this.cfg.intervalMs || 1000);
  }

  public stop(): void {
    if (this.alpacaWs) {
      this.alpacaWs.stop();
      this.alpacaWs = undefined;
    }
    this.wsConnected = false;
    this.lastWsCredsKey = "";
  }

  private ensureAlpacaWs(): void {
    if (this.exchange.exchangeName !== "alpaca") {
      if (this.alpacaWs) this.stop();
      return;
    }
    const creds = this.resolveAlpacaCreds();
    if (!creds.apiKey || !creds.apiSecret) return;

    const credsKey = `${creds.apiKey}:${creds.dataUrl}`;
    if (this.alpacaWs && this.lastWsCredsKey === credsKey) {
      return;
    }

    this.alpacaWs?.stop();
    this.lastWsCredsKey = credsKey;
    this.alpacaWs = new AlpacaPortfolioWsClient({
      apiKey: creds.apiKey,
      apiSecret: creds.apiSecret,
      dataUrl: creds.dataUrl,
      onPriceUpdate: (sym, price, tsMs) => this.handleWsPriceUpdate(sym, price, tsMs),
      onStatusChange: (connected) => {
        this.wsConnected = connected;
      },
      log: (msg) => this.log(msg),
    });
    this.alpacaWs.start();
    this.syncAlpacaWsSubscriptions();
  }

  private syncAlpacaWsSubscriptions(): void {
    if (!this.alpacaWs) return;
    // Strict priority order within Alpaca Basic 30-symbol limit:
    // 1. Macro benchmark (SPY)
    // 2. Active held slots (3 slots)
    // 3. Currently inspected chart ticker
    // 4. Radar candidates sorted by stage (PULLBACK_READY > WAITING_PULLBACK > BOTTOM_FAN > rest)
    const ordered = [
      this.benchmarkSymbol,
      ...this.slots.map((s) => s.symbol),
      this.inspectedSymbol,
      ...this.radarList.map((r) => r.symbol),
    ].filter(Boolean);
    this.alpacaWs.setSymbols(ordered);
  }

  private updateCandleBarWithLivePrice(candles: Candle[], periodSec: number, price: number, tsMs: number): boolean {
    if (!candles || candles.length === 0 || !Number.isFinite(price) || price <= 0) return false;
    const sec = Math.floor(tsMs / 1000);
    const bucketTime = Math.floor(sec / periodSec) * periodSec;
    const last = candles[candles.length - 1];

    if (last && last.time === bucketTime) {
      const changed = price !== last.close || price > last.high || price < last.low;
      last.high = Math.max(last.high, price);
      last.low = Math.min(last.low, price);
      last.close = price;
      last.isClosed = false;
      return changed;
    }

    if (!last || bucketTime > last.time) {
      if (last && last.isClosed === false) {
        last.isClosed = true;
      }
      candles.push({
        time: bucketTime,
        open: price,
        high: price,
        low: price,
        close: price,
        volume: 0,
        isClosed: false,
      });
      if (candles.length > 400) candles.shift();
      return true;
    }
    return false;
  }

  private emitInspectedChartCandles(opts: { force?: boolean; includeNewlyClosed?: boolean } = {}): void {
    const now = Date.now();
    if (!opts.force && now - this.lastChartEmitTs < 250) return;

    const sym = this.inspectedSymbol || this.slots[0]?.symbol || this.radarList[0]?.symbol || "";
    const ind = sym ? this.snapshots.get(sym) : undefined;
    if (!ind) return;

    this.lastChartEmitTs = now;

    if (opts.includeNewlyClosed) {
      if (ind.c15.length >= 2) {
        const closed15 = ind.c15[ind.c15.length - 2]!;
        if (closed15.isClosed !== false && closed15.time > (this.lastSyncedClosed15mBySymbol.get(sym) || 0)) {
          this.lastSyncedClosed15mBySymbol.set(sym, closed15.time);
          this.emit({ type: "candle", data: { candle: { ...closed15 }, isClosed: true } });
        }
      }
      if (ind.c1h.length >= 2) {
        const closed1h = ind.c1h[ind.c1h.length - 2]!;
        if (closed1h.isClosed !== false && closed1h.time > (this.lastSyncedClosed1hBySymbol.get(sym) || 0)) {
          this.lastSyncedClosed1hBySymbol.set(sym, closed1h.time);
          this.emit({ type: "htf_candle", data: { candle: { ...closed1h }, isClosed: true } });
        }
      }
    }

    const latest15 = ind.c15[ind.c15.length - 1];
    if (latest15) {
      this.emit({ type: "candle", data: { candle: { ...latest15 }, isClosed: latest15.isClosed !== false } });
    }
    const latest1h = ind.c1h[ind.c1h.length - 1];
    if (latest1h) {
      this.emit({ type: "htf_candle", data: { candle: { ...latest1h }, isClosed: latest1h.isClosed !== false } });
    }
  }

  private handleWsPriceUpdate(sym: string, price: number, tsMs: number): void {
    const allowCandleMutation = this.exchange.exchangeName !== "alpaca" || !this.isMarketClosed;
    const ind = this.snapshots.get(sym);
    if (ind) {
      ind.livePrice = price;
      if (allowCandleMutation) {
        this.updateCandleBarWithLivePrice(ind.c15, 900, price, tsMs);
        this.updateCandleBarWithLivePrice(ind.c1h, 3600, price, tsMs);
      }
    }
    if (allowCandleMutation && sym === this.benchmarkSymbol && this.benchmark15m.length > 0) {
      this.updateCandleBarWithLivePrice(this.benchmark15m, 900, price, tsMs);
    }

    const isInspected = sym === this.inspectedSymbol;
    const isHeldSlot = this.slots.some((s) => s.symbol === sym);
    if (isInspected && allowCandleMutation) {
      this.emitInspectedChartCandles({ force: false });
    }
    if ((isInspected || isHeldSlot) && Date.now() - this.lastTelemetryEmitTs >= 400) {
      this.emitTelemetryTick();
    }
  }

  public onEvent(cb: (event: StrategyEvent) => void): void {
    this.eventListeners.push(cb);
  }

  private emit(event: StrategyEvent): void {
    for (const cb of this.eventListeners) {
      try {
        cb(event);
      } catch (err) {
        this.log(`event listener error: ${(err as Error).message}`);
      }
    }
  }

  public setPaused(paused: boolean): boolean {
    this.isPaused = paused;
    this.db.saveState({ isPaused: paused }, true, PORTFOLIO_DB_KEY);
    this.emitTelemetryTick();
    return this.isPaused;
  }

  public getIsPaused(): boolean {
    return this.isPaused;
  }

  public setMarketClosed(closed: boolean, marketClock?: MarketClockInfo): void {
    this.isMarketClosed = closed;
    if (marketClock) this.marketClock = marketClock;
  }

  public onCandleClosed(_candle: Candle): void {
    // Universe scans happen cleanly on 15m bar intervals / lastClosedTime check in tick()
  }

  public getDatabase(): BotDatabase {
    return this.db;
  }

  public getRuntimeConfig(): any {
    const out: Record<string, any> = { ...this.cfg };
    if (out.binanceApiKey) out.binanceApiKey = "******";
    if (out.binanceApiSecret) out.binanceApiSecret = "******";
    if (out.dreamdexPrivateKey) out.dreamdexPrivateKey = "******";
    if (out.alpacaApiKey) out.alpacaApiKey = "******";
    if (out.alpacaApiSecret) out.alpacaApiSecret = "******";
    out.symbol = this.inspectedSymbol || this.cfg.symbol;
    return out;
  }

  public getRealDreamdexPrivateKey(): string | undefined { return this.cfg.dreamdexPrivateKey || undefined; }
  public getRealBinanceApiKey(): string | undefined { return this.cfg.binanceApiKey || undefined; }
  public getRealBinanceApiSecret(): string | undefined { return this.cfg.binanceApiSecret || undefined; }
  public getRealAlpacaApiKey(): string | undefined { return this.cfg.alpacaApiKey || undefined; }
  public getRealAlpacaApiSecret(): string | undefined { return this.cfg.alpacaApiSecret || undefined; }

  public updateRuntimeSettings(s: any): any {
    if (!s || typeof s !== "object") return this.getRuntimeConfig();
    const prevPreset = this.cfg.portfolioUniversePreset;
    const prevWhitelist = JSON.stringify(this.cfg.portfolioCustomWhitelist || []);
    const prevBlacklist = JSON.stringify(this.cfg.portfolioBlacklist || []);
    const prevMaxPairs = this.cfg.portfolioMaxUniversePairs;

    const clean: Record<string, any> = { ...s };
    for (const secretKey of ["binanceApiKey", "binanceApiSecret", "dreamdexPrivateKey", "alpacaApiKey", "alpacaApiSecret"]) {
      if (clean[secretKey] === "******") delete clean[secretKey];
    }

    Object.assign(this.cfg, clean);
    this.cfg.portfolioMaxSlots = Math.max(1, Math.min(5, Number(this.cfg.portfolioMaxSlots) || 3));
    if (Array.isArray(clean.portfolioCustomWhitelist)) {
      this.cfg.portfolioCustomWhitelist = clean.portfolioCustomWhitelist.map((x: any) => String(x).toUpperCase().trim()).filter(Boolean);
    } else if (typeof clean.portfolioCustomWhitelist === "string") {
      this.cfg.portfolioCustomWhitelist = clean.portfolioCustomWhitelist
        .split(/[\s,;]+/)
        .map((x: string) => x.toUpperCase().trim())
        .filter(Boolean);
    }
    if (Array.isArray(clean.portfolioBlacklist)) {
      this.cfg.portfolioBlacklist = clean.portfolioBlacklist.map((x: any) => String(x).toUpperCase().trim()).filter(Boolean);
    } else if (typeof clean.portfolioBlacklist === "string") {
      this.cfg.portfolioBlacklist = clean.portfolioBlacklist
        .split(/[\s,;]+/)
        .map((x: string) => x.toUpperCase().trim())
        .filter(Boolean);
    }

    // Persist settings to DB
    for (const [k, v] of Object.entries(clean)) {
      if (k !== "symbol" || typeof v === "string") {
        this.db.setSetting(k, v);
      }
    }
    this.db.setSetting("portfolioMaxSlots", this.cfg.portfolioMaxSlots);
    this.db.setSetting("portfolioCustomWhitelist", this.cfg.portfolioCustomWhitelist);
    this.db.setSetting("portfolioBlacklist", this.cfg.portfolioBlacklist);

    const universeChanged =
      prevPreset !== this.cfg.portfolioUniversePreset ||
      prevWhitelist !== JSON.stringify(this.cfg.portfolioCustomWhitelist || []) ||
      prevBlacklist !== JSON.stringify(this.cfg.portfolioBlacklist || []) ||
      prevMaxPairs !== this.cfg.portfolioMaxUniversePairs;

    if (universeChanged) {
      this.lastUniverseFetchTs = 0;
    }

    this.emitTelemetryTick();
    return this.getRuntimeConfig();
  }

  public reloadState(): void {
    const saved = this.db.getState(PORTFOLIO_DB_KEY);
    if (saved) {
      this.slots = Array.isArray(saved.portfolioSlots) ? saved.portfolioSlots.map((s) => ({ ...s })) : [];
      this.realizedPnl = Number(saved.realizedPnl || 0);
      this.tradeRealizedPnl = Number(saved.tradeRealizedPnl ?? saved.realizedPnl ?? 0);
      this.cashBalance = Number(saved.portfolioCash || 0);
      this.initialCapital = Number(saved.portfolioInitialCapital || 0);
      if (typeof saved.isPaused === "boolean") {
        // Keep safe boot respect if called from boot; update on explicit reload
      }
    } else {
      this.slots = [];
      this.realizedPnl = 0;
      this.tradeRealizedPnl = 0;
    }
  }

  public saveState(immediate = false): void {
    this.db.saveState(
      {
        portfolioSlots: this.slots.map((s) => ({ ...s })),
        portfolioCash: this.cashBalance,
        portfolioInitialCapital: this.initialCapital,
        realizedPnl: this.realizedPnl,
        tradeRealizedPnl: this.tradeRealizedPnl,
        isPaused: this.isPaused,
        lastUpdated: Date.now(),
      },
      immediate,
      PORTFOLIO_DB_KEY,
    );
  }

  public getRecentOrders(): any[] {
    return this.db.getOrderActivity(250, "ALL");
  }

  public baseHeld(): number {
    const active = this.slots.find((s) => s.symbol === this.inspectedSymbol) || this.slots[0];
    return active ? active.qty : 0;
  }

  public getAvgEntryPrice(): number {
    const active = this.slots.find((s) => s.symbol === this.inspectedSymbol) || this.slots[0];
    return active ? active.entryPrice : 0;
  }

  public getLots(): Lot[] {
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

  public setSwitchingSymbol(val: boolean): void {
    this.isSwitchingSymbol = val;
  }

  public async prepareForSymbolSwitch(_targetSymbol: string): Promise<void> {
    this.saveState(true);
  }

  public updateExchangeClient(newExchange: IExchangeClient, nextInfo: ExchangeSymbolInfo, atrSource: AtrSource): void {
    this.stop();
    this.exchange = newExchange;
    this.symbolInfo = nextInfo;
    this.atrSource = atrSource;
    this.snapshots.clear();
    this.setupStates.clear();
    this.radarList = [];
    this.lastUniverseFetchTs = 0;
    this.inspectedSymbol = this.resolveInitialInspectedSymbol();
    this.ensureAlpacaWs();
    this.emitTelemetryTick();
  }

  public updateSymbolInfo(nextInfo: ExchangeSymbolInfo, atrSource: AtrSource): void {
    this.symbolInfo = nextInfo;
    this.atrSource = atrSource;
    const clean = this.exchange.exchangeName === "alpaca" ? normalizeStockSymbol(nextInfo.symbol) : nextInfo.symbol;
    if (clean) {
      void this.inspectSymbol(clean);
    }
  }

  // ── Chart & Inspection Helpers ─────────────────────────────────────────────

  public async inspectSymbol(symbol: string): Promise<any> {
    const clean = this.exchange.exchangeName === "alpaca"
      ? normalizeStockSymbol(symbol)
      : String(symbol || "").toUpperCase().replace(/[\/\-_:]/g, "");
    if (!clean) return this.getInspectPayload();

    this.inspectedSymbol = clean;
    this.cfg.portfolioInspectedSymbol = clean;
    this.db.setSetting("portfolioInspectedSymbol", clean);

    if (!this.snapshots.has(clean)) {
      // Load this specific symbol on demand so chart renders immediately
      try {
        if (this.exchange.exchangeName === "alpaca") {
          const { fetchAlpacaBars } = await import("../../exchange/alpaca/feed.js");
          const creds = this.resolveAlpacaCreds();
          const [c15, c1h] = await Promise.all([
            fetchAlpacaBars({ symbol: clean, interval: "15m", limit: 260, ...creds }),
            fetchAlpacaBars({ symbol: clean, interval: "1h", limit: 120, ...creds }),
          ]);
          const snap = buildSymbolIndicators(clean, c15, c1h);
          if (snap) this.snapshots.set(clean, snap);
        } else {
          const baseUrl = this.cfg.binanceBaseUrl || "https://api.binance.com";
          const [r15, r1h] = await Promise.all([
            fetch(`${baseUrl}/api/v3/klines?symbol=${clean}&interval=15m&limit=260`).then((r) => r.json()).catch(() => []),
            fetch(`${baseUrl}/api/v3/klines?symbol=${clean}&interval=1h&limit=120`).then((r) => r.json()).catch(() => []),
          ]);
          const toCandles = (arr: any[]): Candle[] =>
            Array.isArray(arr)
              ? arr.map((k) => ({
                  time: Math.floor(Number(k[0]) / 1000),
                  open: Number(k[1]),
                  high: Number(k[2]),
                  low: Number(k[3]),
                  close: Number(k[4]),
                  volume: Number(k[5] ?? 0),
                  isClosed: Number(k[6]) < Date.now(),
                }))
              : [];
          const snap = buildSymbolIndicators(clean, toCandles(r15), toCandles(r1h));
          if (snap) this.snapshots.set(clean, snap);
        }
      } catch (err) {
        this.log(`⚠️ Failed to fetch on-demand inspect candles for ${clean}: ${(err as Error).message}`);
      }
    }

    this.syncAlpacaWsSubscriptions();
    this.emitTelemetryTick();
    return this.getInspectPayload();
  }

  public getChartCandles(): Candle[] {
    const snap = this.snapshots.get(this.inspectedSymbol);
    if (snap) return snap.c15;
    const firstSlot = this.slots[0];
    if (firstSlot && this.snapshots.get(firstSlot.symbol)) {
      return this.snapshots.get(firstSlot.symbol)!.c15;
    }
    return this.snapshots.values().next().value?.c15 ?? [];
  }

  public getHtfChartCandles(): Candle[] {
    const snap = this.snapshots.get(this.inspectedSymbol);
    if (snap) return snap.c1h;
    return this.snapshots.values().next().value?.c1h ?? [];
  }

  public getInspectPayload() {
    const sym = this.inspectedSymbol || this.slots[0]?.symbol || this.radarList[0]?.symbol || this.symbolInfo.symbol;
    const snap = this.snapshots.get(sym);
    const slot = this.slots.find((s) => s.symbol === sym);
    const radar = this.radarList.find((r) => r.symbol === sym);
    if (!snap) {
      return { symbol: sym, candles: [], htfCandles: [], emas: null, slot: slot || null, radar: radar || null };
    }
    const buildLine = (vals: number[]) =>
      snap.c15.map((c, idx) => ({ time: c.time, value: Number((vals[idx] ?? c.close).toFixed(6)) }));
    return {
      symbol: sym,
      candles: snap.c15,
      htfCandles: snap.c1h,
      emas: {
        ema4: buildLine(snap.m4),
        ema8: buildLine(snap.m8),
        ema16: buildLine(snap.m16),
        ema25: buildLine(snap.m25),
        ema75: buildLine(snap.m75),
        ema200: buildLine(snap.m200),
      },
      slot: slot || null,
      radar: radar || null,
    };
  }

  public getPortfolioRadarSnapshot() {
    return {
      macro: this.macroState,
      slots: this.buildSlotTelemetryList(),
      radar: this.radarList.slice(0, 20),
      inspectedSymbol: this.inspectedSymbol,
    };
  }

  public async getLearningReport() {
    return this.learning.buildReport(this.cfg);
  }

  // ── Wallet & Position Sync ─────────────────────────────────────────────────

  public async refreshWalletBalances(): Promise<any> {
    try {
      const targetSym = this.inspectedSymbol || this.symbolInfo.symbol;
      if (!this.cfg.dryRun && this.exchange.hasCredentials()) {
        const bal = await this.exchange.getAccountBalances(targetSym);
        this.walletQuoteBalance = bal.quoteFree || 0;
        this.walletQuoteTotal = bal.quoteTotal || this.walletQuoteBalance;
        if (this.initialCapital <= 0 && this.walletQuoteTotal > 0) {
          this.initialCapital = this.walletQuoteTotal;
        }
        if (this.cashBalance <= 0 && this.walletQuoteBalance > 0) {
          this.cashBalance = this.walletQuoteBalance;
        }
        this.lastBalanceSyncTs = Date.now();
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

    const defaultCap = this.exchange.exchangeName === "alpaca" ? 10_000 : 200;
    if (this.initialCapital <= 0) {
      this.initialCapital = this.cfg.portfolioMaxBudgetUsd > 0 ? this.cfg.portfolioMaxBudgetUsd : defaultCap;
    }
    const deployed = this.slots.reduce((s, slot) => s + slot.allocatedCapital, 0);
    const estFree = Math.max(0, this.initialCapital + this.realizedPnl - deployed);
    this.cashBalance = estFree;
    this.walletQuoteBalance = estFree;
    const heldMarketVal = this.slots.reduce((s, slot) => s + slot.qty * (slot.lastKnownPrice || slot.entryPrice), 0);
    this.walletQuoteTotal = estFree + heldMarketVal;
    return {
      base: this.baseHeld(),
      quote: estFree,
      somi: 0,
      usdso: estFree,
      address: this.cfg.dryRun ? "SIMULATED" : this.exchange.exchangeName.toUpperCase(),
    };
  }

  public async syncOnChainOrders(): Promise<void> {
    if (this.cfg.dryRun || !this.exchange.hasCredentials()) return;
    if (this.exchange.exchangeName === "alpaca" && typeof (this.exchange as any).getAllPositions === "function") {
      try {
        const positions = await (this.exchange as any).getAllPositions();
        const heldMap = new Map<string, any>(positions.map((p: any) => [p.symbol, p]));
        // Update live qty / price on matching persisted slots, and adopt orphaned Alpaca positions if free slots exist
        for (const slot of this.slots) {
          const exPos = heldMap.get(slot.symbol);
          if (exPos && exPos.qty > 0) {
            slot.qty = exPos.qty;
            slot.lastKnownPrice = exPos.currentPrice || slot.lastKnownPrice;
            if (slot.entryPrice <= 0 && exPos.avgEntryPrice > 0) slot.entryPrice = exPos.avgEntryPrice;
            heldMap.delete(slot.symbol);
          }
        }
        const maxSlots = this.getMaxSlots();
        for (const [sym, p] of heldMap.entries()) {
          if (this.slots.length >= maxSlots) break;
          const usedIdx = new Set(this.slots.map((s) => s.slotIndex));
          let slotIndex = 0;
          while (usedIdx.has(slotIndex)) slotIndex++;
          const slPct = Number(this.cfg.portfolioDisasterSlPct ?? 3.5);
          this.slots.push({
            slotIndex,
            symbol: sym,
            entryTime: Date.now(),
            entryPrice: p.avgEntryPrice || p.currentPrice,
            allocatedCapital: p.marketValue || p.avgEntryPrice * p.qty,
            stopLossPrice: slPct > 0 ? (p.avgEntryPrice || p.currentPrice) * (1 - slPct / 100) : 0,
            qty: p.qty,
            initialQty: p.qty,
            troughPrice: p.currentPrice || p.avgEntryPrice,
            peakPrice: p.currentPrice || p.avgEntryPrice,
            lastKnownPrice: p.currentPrice || p.avgEntryPrice,
            beLocked: false,
            tp1Done: false,
            realizedProceeds: 0,
            entryFeeQuote: 0,
            realizedExitFees: 0,
            activeBarsHeld: 0,
            entryReason: "Adopted Alpaca Position",
          });
          this.log(`🔄 [Portfolio Sync] Adopted live Alpaca position into Slot ${slotIndex + 1}: ${sym} (${p.qty} @ $${p.avgEntryPrice})`);
        }
        this.saveState();
      } catch (err) {
        this.log(`⚠️ Failed to sync Alpaca positions: ${(err as Error).message}`);
      }
    }
  }

  public async reconcileInventory(_forceAdopt?: boolean): Promise<void> {
    await this.syncOnChainOrders();
    await this.refreshWalletBalances();
  }

  // ── User Controls (Cancel, Close Slot, Sell All, Reset) ───────────────────

  public async cancelAllOrdersUser(): Promise<{ cancelledCount: number }> {
    let count = 0;
    if (!this.cfg.dryRun && this.exchange.hasCredentials()) {
      try {
        const res = await this.exchange.cancelAllOpenOrders(this.inspectedSymbol || this.symbolInfo.symbol);
        count = res?.canceledCount || 0;
      } catch (err) {
        this.log(`⚠️ Cancel all error: ${(err as Error).message}`);
      }
    }
    return { cancelledCount: count };
  }

  public async closeSlot(slotIndex: number, reason = "MANUAL_SLOT_EXIT"): Promise<{ success: boolean; symbol?: string; pnl?: number; error?: string }> {
    const idx = this.slots.findIndex((s) => s.slotIndex === slotIndex);
    if (idx < 0) return { success: false, error: `Slot ${slotIndex + 1} is empty` };
    const slot = this.slots[idx]!;
    const exitPrice = this.snapshots.get(slot.symbol)?.livePrice || slot.lastKnownPrice || slot.entryPrice;
    const ok = await this.executeExitSlot(slot, slot.qty, exitPrice, reason, true);
    this.emitTelemetryTick();
    if (!ok) return { success: false, symbol: slot.symbol, error: `Failed to execute exit order for ${slot.symbol}` };
    return { success: true, symbol: slot.symbol };
  }

  public async sellAllUser(): Promise<{ success: boolean; soldQty: number; price: number; error?: string }> {
    if (this.slots.length === 0) {
      return { success: true, soldQty: 0, price: 0 };
    }
    let totalSold = 0;
    let lastPrice = 0;
    const copy = [...this.slots];
    for (const slot of copy) {
      const px = this.snapshots.get(slot.symbol)?.livePrice || slot.lastKnownPrice || slot.entryPrice;
      totalSold += slot.qty;
      lastPrice = px;
      await this.executeExitSlot(slot, slot.qty, px, "EMERGENCY_CLOSE_ALL", true);
    }
    this.emitTelemetryTick();
    return { success: true, soldQty: totalSold, price: lastPrice };
  }

  public async resetPosition(): Promise<{ success: boolean; clearedLots: number }> {
    const count = this.slots.length;
    this.slots = [];
    this.saveState(true);
    this.emitTelemetryTick();
    return { success: true, clearedLots: count };
  }

  public async resetBotState(opts?: any): Promise<{ success: boolean }> {
    this.slots = [];
    this.setupStates.clear();
    if (opts?.resetPnl || opts?.fullReset) {
      this.realizedPnl = 0;
      this.tradeRealizedPnl = 0;
      this.cashBalance = this.initialCapital;
    }
    this.saveState(true);
    this.emitTelemetryTick();
    return { success: true };
  }

  // ── Core Main Tick & Universe Evaluation ───────────────────────────────────

  public async tick(): Promise<void> {
    if (this.tickInProgress || this.isSwitchingSymbol) return;
    this.tickInProgress = true;
    try {
      const now = Date.now();
      const creds = this.resolveAlpacaCreds();
      this.ensureAlpacaWs();

      // 1. Periodically refresh full Universe 15m + 1h bars (every 60s when open, 5m when market closed, or immediately on boot)
      const universeRefreshMs = this.isMarketClosed ? 300_000 : 60_000;
      if (this.snapshots.size === 0 || now - this.lastUniverseFetchTs >= universeRefreshMs) {
        if (!this.universeFetchPromise) {
          this.universeFetchPromise = this.refreshUniverseBars().finally(() => {
            this.universeFetchPromise = null;
          });
        }
        await this.universeFetchPromise;
      }

      // 2. Periodically refresh live prices for held slots + top radar (every 5s when open)
      const snapPollMs = this.isMarketClosed ? 30_000 : 5_000;
      if (now - this.lastSnapshotPollTs >= snapPollMs && this.snapshots.size > 0) {
        this.lastSnapshotPollTs = now;
        await this.refreshLiveQuotes(creds);
      }

      // 3. Refresh wallet balance periodically
      if (now - this.lastBalanceSyncTs >= 20_000) {
        await this.refreshWalletBalances();
      }

      // 4. Evaluate active slots exits & 15m scanner/entries
      await this.evaluatePortfolioCycle();

      // 5. Emit dashboard telemetry
      if (now - this.lastTelemetryEmitTs >= 900) {
        this.emitTelemetryTick();
      }
    } catch (err) {
      this.log(`⚠️ Portfolio tick error: ${(err as Error).message}`);
    } finally {
      this.tickInProgress = false;
    }
  }

  private async refreshUniverseBars(): Promise<void> {
    const existingPrices = new Map<string, number>();
    const existingCandles = new Map<string, { c15: Candle[]; c1h: Candle[] }>();
    for (const [sym, snap] of this.snapshots.entries()) {
      existingPrices.set(sym, snap.livePrice);
      existingCandles.set(sym, { c15: snap.c15, c1h: snap.c1h });
    }

    const res = await fetchPortfolioUniverseAndBenchmark({
      exchangeName: this.exchange.exchangeName,
      cfg: this.cfg,
      alpacaCreds: this.resolveAlpacaCreds(),
      existingLivePrices: existingPrices,
      existingCandles: existingCandles.size > 0 ? existingCandles : undefined,
      existingBenchmarkCandles: {
        c4h: this.benchmark4h,
        c1d: this.benchmark1d,
        c15: this.benchmark15m,
      },
      pgStore: this.db.getPgStore(),
      log: (m) => this.log(m),
    });

    this.benchmarkSymbol = res.benchmarkSymbol;
    this.benchmark4h = res.benchmark4h;
    this.benchmark1d = res.benchmark1d;
    this.benchmark15m = res.benchmark15m;

    for (const [sym, ind] of res.snapshots.entries()) {
      const wsTickAt = this.alpacaWs?.getSymbolLastTickAt(sym) || 0;
      const prevSnap = this.snapshots.get(sym);
      if (wsTickAt > Date.now() - 15_000 && prevSnap && prevSnap.livePrice > 0) {
        ind.livePrice = prevSnap.livePrice;
        this.updateCandleBarWithLivePrice(ind.c15, 900, prevSnap.livePrice, wsTickAt);
        this.updateCandleBarWithLivePrice(ind.c1h, 3600, prevSnap.livePrice, wsTickAt);
      }
      this.snapshots.set(sym, ind);
    }

    // Ensure inspectedSymbol points to something loaded
    if (!this.snapshots.has(this.inspectedSymbol)) {
      this.inspectedSymbol = this.slots[0]?.symbol || res.symbols[0] || this.inspectedSymbol;
    }

    this.lastUniverseFetchTs = Date.now();
    this.syncAlpacaWsSubscriptions();
    this.emitInspectedChartCandles({ force: true, includeNewlyClosed: true });
  }

  private async refreshLiveQuotes(creds: { apiKey: string; apiSecret: string; dataUrl: string }): Promise<void> {
    const nowMs = Date.now();
    if (this.exchange.exchangeName === "alpaca") {
      const watchSyms = Array.from(
        new Set([
          this.benchmarkSymbol,
          this.inspectedSymbol,
          ...this.slots.map((s) => s.symbol),
          ...this.radarList.slice(0, 20).map((r) => r.symbol),
        ].filter(Boolean)),
      );
      const wsHealthy = Boolean(this.alpacaWs?.isConnected());
      const targetsToPoll = wsHealthy
        ? watchSyms.filter((s) => nowMs - (this.alpacaWs?.getSymbolLastTickAt(s) || 0) >= 12_000)
        : watchSyms;
      if (targetsToPoll.length === 0) return;

      const snaps = await fetchAlpacaMultiSnapshots({ symbols: targetsToPoll, ...creds }).catch(() => new Map());
      let inspectedUpdated = false;
      const allowCandleMutation = !this.isMarketClosed && this.marketClock?.isOpen !== false;
      for (const [sym, book] of snaps.entries()) {
        const ind = this.snapshots.get(sym);
        if (ind && book.mid > 0) {
          ind.livePrice = book.mid;
          // Use the snapshot's actual quote/trade timestamp and never synthesize flat weekend/off-hours candles
          const snapTsMs = book.time > 0 ? book.time * 1000 : 0;
          if (allowCandleMutation && snapTsMs > 0) {
            const changed15 = this.updateCandleBarWithLivePrice(ind.c15, 900, book.mid, snapTsMs);
            const changed1h = this.updateCandleBarWithLivePrice(ind.c1h, 3600, book.mid, snapTsMs);
            if (sym === this.inspectedSymbol && (changed15 || changed1h)) inspectedUpdated = true;
          }
        }
      }
      if (inspectedUpdated) {
        this.emitInspectedChartCandles({ force: true });
      }
      return;
    }

    // Binance bulk ticker
    try {
      const baseUrl = this.cfg.binanceBaseUrl || "https://api.binance.com";
      const res = await fetch(`${baseUrl}/api/v3/ticker/price`);
      if (!res.ok) return;
      const list = (await res.json()) as Array<{ symbol: string; price: string }>;
      const priceMap = new Map(list.map((p) => [p.symbol, parseFloat(p.price)]));
      let inspectedUpdated = false;
      for (const [sym, ind] of this.snapshots.entries()) {
        const p = priceMap.get(sym);
        if (p && p > 0) {
          ind.livePrice = p;
          this.updateCandleBarWithLivePrice(ind.c15, 900, p, nowMs);
          this.updateCandleBarWithLivePrice(ind.c1h, 3600, p, nowMs);
          if (sym === this.inspectedSymbol) inspectedUpdated = true;
        }
      }
      const benchPx = this.benchmarkSymbol ? priceMap.get(this.benchmarkSymbol) : undefined;
      if (benchPx && benchPx > 0 && this.benchmark15m.length > 0) {
        this.updateCandleBarWithLivePrice(this.benchmark15m, 900, benchPx, nowMs);
      }
      if (inspectedUpdated) {
        this.emitInspectedChartCandles({ force: true });
      }
    } catch {}
  }

  private async evaluatePortfolioCycle(): Promise<void> {
    const canExecute = !this.isPaused && !this.isMarketClosed;
    const sitOutEval = evaluateBenchmarkSitOut({
      benchmarkSymbol: this.benchmarkSymbol,
      benchmark4h: this.benchmark4h,
      benchmark1d: this.benchmark1d,
      benchmark15m: this.benchmark15m,
      sitOutMode: this.cfg.portfolioSitOutMode || "dual",
    });

    // Breadth: % of universe above EMA25 on latest closed 15m candle
    let above25 = 0;
    let totalCount = 0;
    for (const ind of this.snapshots.values()) {
      if (ind.symbol === this.benchmarkSymbol) continue;
      totalCount++;
      const idx = ind.lastClosedIdx;
      if (ind.c15[idx] && ind.m25[idx] && ind.c15[idx]!.close > ind.m25[idx]!) {
        above25++;
      }
    }

    this.macroState = {
      benchmarkSymbol: this.benchmarkSymbol,
      benchmarkPrice: sitOutEval.benchmarkPrice,
      benchmarkRet24h: sitOutEval.benchmarkRet24h,
      isSitOut: sitOutEval.isSitOut,
      sitOutMode: this.cfg.portfolioSitOutMode || "dual",
      reason: sitOutEval.reason,
      breadthAboveEma25Pct: totalCount > 0 ? (above25 / totalCount) * 100 : 50,
      universeCount: this.snapshots.size,
      pass4h: sitOutEval.pass4h,
      pass1d: sitOutEval.pass1d,
      lastScanTime: Date.now(),
    };

    // 1. Evaluate Active Slot Exits (always update telemetry prices, execute only when active)
    const tp1Mode = this.cfg.portfolioTp1TargetMode || "EMA200";
    const disasterSlPct = Number(this.cfg.portfolioDisasterSlPct ?? 3.5);
    const maxHoldHours = Number(this.cfg.portfolioMaxHoldHours || 0);

    for (const slot of [...this.slots]) {
      const ind = this.snapshots.get(slot.symbol);
      const px = ind?.livePrice || slot.lastKnownPrice || slot.entryPrice;
      slot.lastKnownPrice = px;
      slot.troughPrice = Math.min(slot.troughPrice, px);
      slot.peakPrice = Math.max(slot.peakPrice, px);

      let tp1TargetPrice = slot.entryPrice * 1.008;
      let isBullishFan = false;
      if (ind) {
        const i = ind.lastClosedIdx;
        if (tp1Mode !== "none") {
          const emaVal = tp1Mode === "EMA25" ? ind.m25[i]! : tp1Mode === "EMA75" ? ind.m75[i]! : ind.m200[i]!;
          tp1TargetPrice = Math.max(emaVal, slot.entryPrice * 1.008);
        }
        isBullishFan =
          ind.m4[i]! > ind.m8[i]! &&
          ind.m8[i]! > ind.m16[i]! &&
          ind.m16[i]! > ind.m25[i]! &&
          ind.m25[i]! > ind.m75[i]! &&
          ind.m75[i]! > ind.m200[i]!;
        if (ind.lastClosedTime > slot.entryTime) {
          slot.activeBarsHeld = Math.max(slot.activeBarsHeld, Math.floor((ind.lastClosedTime - slot.entryTime) / 900));
        }
      }
      slot.tp1TargetPrice = tp1TargetPrice;

      if (!canExecute) continue;

      // Hard Disaster SL
      if (disasterSlPct > 0 && px <= slot.entryPrice * (1 - disasterSlPct / 100)) {
        await this.executeExitSlot(slot, slot.qty, px, `SL_CALIBRATED_${disasterSlPct}%`, true);
        continue;
      }

      // Locked Breakeven Stop
      if (slot.beLocked && px <= slot.entryPrice * 1.0025) {
        await this.executeExitSlot(slot, slot.qty, px, "SL_BREAKEVEN", true);
        continue;
      }

      // TP1 Target (EMA200 / EMA75 / EMA25 with +0.8% floor)
      if (tp1Mode !== "none" && px >= tp1TargetPrice) {
        let changed = false;
        if (this.cfg.portfolioLockBreakevenAtEma200 !== false && !slot.beLocked) {
          slot.beLocked = true;
          slot.stopLossPrice = Math.max(slot.stopLossPrice, slot.entryPrice * 1.0025);
          changed = true;
          this.log(`🛡️ [Slot ${slot.slotIndex + 1} ${slot.symbol}] Reached ${tp1Mode} ($${tp1TargetPrice.toFixed(2)}) -> Locked Breakeven!`);
        }
        if (this.cfg.portfolioPartialTpAtEma200 !== false && !slot.tp1Done) {
          const halfQty = slot.qty * 0.5;
          const sold = await this.executeExitSlot(slot, halfQty, px, `TP1_${tp1Mode}`, false);
          if (sold) {
            slot.tp1Done = true;
            changed = true;
          }
        }
        if (changed) this.saveState(true);
      }

      // TP2 Full Bullish Fan
      if (isBullishFan) {
        await this.executeExitSlot(slot, slot.qty, px, "FULL_BULLISH_FAN", true);
        continue;
      }

      // Optional Max Hold Hours
      const heldHours = (Date.now() - slot.entryTime) / 3_600_000;
      if (maxHoldHours > 0 && heldHours >= maxHoldHours) {
        await this.executeExitSlot(slot, slot.qty, px, "MAX_HOLD_HOURS", true);
        continue;
      }
    }

    // 2. Scan Universe & Update Setup States + Live Radar
    const newRadar: PortfolioRadarCandidate[] = [];
    const entryCandidates: Array<{ sym: string; price: number; score: number; cand: PortfolioRadarCandidate }> = [];

    for (const [sym, ind] of this.snapshots.entries()) {
      const idx = ind.lastClosedIdx;
      if (idx < 200) continue;

      let st = this.setupStates.get(sym);
      if (!st) {
        // Seed recent setup state from trailing closed bars so bot sees setups already in progress on startup!
        st = this.bootstrapSetupState(ind);
        this.setupStates.set(sym, st);
      }

      const barTime = ind.lastClosedTime;
      const c = ind.c15[idx]!;
      const isHeld = this.slots.some((s) => s.symbol === sym);

      const m4 = ind.m4[idx]!;
      const m8 = ind.m8[idx]!;
      const m16 = ind.m16[idx]!;
      const m25 = ind.m25[idx]!;
      const m75 = ind.m75[idx]!;
      const m200 = ind.m200[idx]!;
      const prevM8 = ind.m8[idx - 1]!;
      const prevM16 = ind.m16[idx - 1]!;

      const isBottomFan = m4 < m8 && m8 < m16 && m16 < m25 && m25 < m75 && m75 < m200;
      if (isBottomFan) {
        st.hadBottomFan = true;
        st.fanExpiryBarTime = barTime + 20 * 900;
        st.bottomLow = Math.min(st.bottomLow, c.low);
      }
      if (barTime > st.fanExpiryBarTime && !st.waitingPullback && !isHeld) {
        st.hadBottomFan = false;
        st.bottomLow = Infinity;
      }

      if (st.waitingPullback && (barTime >= st.pullbackExpiryBarTime || m8 < m16)) {
        st.waitingPullback = false;
      }

      const is8Cross16Up = prevM8 <= prevM16 && m8 > m16;
      if (!isHeld && !st.waitingPullback && st.hadBottomFan && is8Cross16Up) {
        const rsiOk = (ind.rsi15[idx] ?? 0) >= 35 && (ind.rsi15[idx] ?? 0) > (ind.rsi15[idx - 1] ?? 0);
        const macdOk = (ind.macdHist15[idx] ?? 0) > (ind.macdHist15[idx - 1] ?? 0);
        if (rsiOk && macdOk) {
          st.waitingPullback = true;
          st.waitFromBarTime = barTime;
          st.pullbackExpiryBarTime = barTime + 12 * 900;
        }
      }

      // Compute Confirmation Metrics
      const rsi15 = ind.rsi15[idx] ?? 50;
      const rsi1h = ind.rsi1h.length > 0 ? (ind.rsi1h[ind.rsi1h.length - 1] ?? 50) : 50;

      let upV = 0;
      let dnV = 0;
      for (let k = Math.max(0, idx - 11); k <= idx; k++) {
        const b = ind.c15[k]!;
        const qv = (b.volume || 1) * b.close;
        if (b.close >= b.open) upV += qv;
        else dnV += qv;
      }
      const buyVolRatio12 = dnV > 0 ? upV / dnV : 2.0;

      const ref24Idx = Math.max(0, idx - 96);
      const coinRet24h = ind.c15[ref24Idx]?.open ? ((c.close - ind.c15[ref24Idx]!.open) / ind.c15[ref24Idx]!.open) * 100 : 0;
      const relStrength24h = coinRet24h - sitOutEval.benchmarkRet24h;
      const upperSpreadPct = m200 > 0 ? ((m200 - m75) / m200) * 100 : 0;

      const baseScore =
        this.cfg.portfolioRankBy === "RSI15"
          ? rsi15
          : Math.min(3, buyVolRatio12) * 20 + Math.max(-5, Math.min(10, relStrength24h)) * 3 - upperSpreadPct * 10;
      const score = baseScore + this.learning.getSymbolScorePenalty(sym, this.cfg);
      const onLearningCooldown = this.learning.isSymbolOnCooldown(sym, this.cfg);

      const livePx = ind.livePrice || c.close;
      const distanceToEma4Pct = m4 > 0 ? ((livePx - m4) / m4) * 100 : 0;
      const distanceToEma200Pct = livePx > 0 ? ((m200 - livePx) / livePx) * 100 : 0;

      let stage: PortfolioRadarCandidate["stage"] = "SCANNING";
      let filterNote = "";
      const passesFilters = rsi1h >= 40 && buyVolRatio12 >= 1.3 && relStrength24h >= -1.0;

      if (isHeld) {
        stage = "HOLDING";
      } else if (st.waitingPullback && m8 >= m16) {
        if (livePx <= m4 * 1.001) {
          if (onLearningCooldown) {
            stage = "CROSS_8X16_FAIL_FILTERS";
            filterNote = "Learning Cooldown";
          } else if (passesFilters) {
            stage = "PULLBACK_READY";
          } else {
            stage = "CROSS_8X16_FAIL_FILTERS";
            const fails: string[] = [];
            if (rsi1h < 40) fails.push(`1hRSI ${rsi1h.toFixed(0)}<40`);
            if (buyVolRatio12 < 1.3) fails.push(`VolR ${buyVolRatio12.toFixed(2)}<1.3`);
            if (relStrength24h < -1.0) fails.push(`RS ${relStrength24h.toFixed(1)}%`);
            filterNote = fails.join(", ");
          }
        } else {
          stage = "WAITING_PULLBACK";
        }
      } else if (st.hadBottomFan || isBottomFan) {
        stage = "BOTTOM_FAN";
      }

      const cand: PortfolioRadarCandidate = {
        symbol: sym,
        price: livePx,
        stage,
        score,
        rsi15,
        rsi1h,
        buyVolRatio12,
        relStrength24h,
        upperSpreadPct,
        ema4: m4,
        ema8: m8,
        ema16: m16,
        ema25: m25,
        ema75: m75,
        ema200: m200,
        distanceToEma4Pct,
        distanceToEma200Pct,
        filterNote,
      };
      newRadar.push(cand);

      if (!isHeld && stage === "PULLBACK_READY" && !onLearningCooldown) {
        entryCandidates.push({ sym, price: livePx, score, cand });
      }
    }

    // Sort Radar cleanly by actionable stage priority, then score descending
    const stageWeight: Record<PortfolioRadarCandidate["stage"], number> = {
      PULLBACK_READY: 500,
      WAITING_PULLBACK: 400,
      BOTTOM_FAN: 300,
      CROSS_8X16_FAIL_FILTERS: 200,
      HOLDING: 150,
      SCANNING: 0,
    };
    newRadar.sort((a, b) => (stageWeight[b.stage] + b.score) - (stageWeight[a.stage] + a.score));
    this.radarList = newRadar;
    this.syncAlpacaWsSubscriptions();

    // Learning Level 1 & 3: Record near-miss blocked signals only when active & market open, and run periodic bounded learning cycle
    const latestClosedBarSec = this.snapshots.get(this.inspectedSymbol)?.lastClosedTime || 0;
    if (canExecute && latestClosedBarSec > 0) {
      void this.learning.recordBlockedSignals({
        exchange: this.exchange.exchangeName,
        barTimeSec: latestClosedBarSec,
        macro: this.macroState,
        candidates: newRadar,
      });
    }
    void this.learning.runPeriodicCycle(this.cfg, (patch) => this.updateRuntimeSettings(patch));

    // 3. Allocate Free Slots if Entries Ready & Allowed
    const maxSlots = this.getMaxSlots();
    const freeSlots = maxSlots - this.slots.length;
    if (canExecute && freeSlots > 0 && !sitOutEval.isSitOut && entryCandidates.length > 0) {
      entryCandidates.sort((a, b) => b.score - a.score);
      for (const ch of entryCandidates.slice(0, freeSlots)) {
        const currentFree = maxSlots - this.slots.length;
        if (currentFree <= 0) break;
        const availCash = this.getAvailableBuyCash();
        const alloc = availCash / currentFree;
        if (alloc < 5) break;
        await this.executeEntrySlot(ch.sym, ch.price, alloc, ch.score, ch.cand);
      }
    }
  }

  private bootstrapSetupState(ind: SymbolIndicatorSnapshot): CoinSetupState {
    const end = ind.lastClosedIdx;
    const start = Math.max(200, end - 30);
    const st: CoinSetupState = {
      hadBottomFan: false,
      fanExpiryBarTime: 0,
      waitingPullback: false,
      pullbackExpiryBarTime: 0,
      waitFromBarTime: 0,
      bottomLow: Infinity,
    };
    for (let idx = start; idx <= end; idx++) {
      const barTime = ind.c15[idx]!.time;
      const m4 = ind.m4[idx]!;
      const m8 = ind.m8[idx]!;
      const m16 = ind.m16[idx]!;
      const m25 = ind.m25[idx]!;
      const m75 = ind.m75[idx]!;
      const m200 = ind.m200[idx]!;
      const isBottomFan = m4 < m8 && m8 < m16 && m16 < m25 && m25 < m75 && m75 < m200;
      if (isBottomFan) {
        st.hadBottomFan = true;
        st.fanExpiryBarTime = barTime + 20 * 900;
        st.bottomLow = Math.min(st.bottomLow, ind.c15[idx]!.low);
      }
      if (barTime > st.fanExpiryBarTime && !st.waitingPullback) {
        st.hadBottomFan = false;
        st.bottomLow = Infinity;
      }
      if (st.waitingPullback && (barTime >= st.pullbackExpiryBarTime || m8 < m16)) {
        st.waitingPullback = false;
      }
      if (idx > 0 && !st.waitingPullback && st.hadBottomFan && ind.m8[idx - 1]! <= ind.m16[idx - 1]! && m8 > m16) {
        const rsiOk = (ind.rsi15[idx] ?? 0) >= 35 && (ind.rsi15[idx] ?? 0) > (ind.rsi15[idx - 1] ?? 0);
        const macdOk = (ind.macdHist15[idx] ?? 0) > (ind.macdHist15[idx - 1] ?? 0);
        if (rsiOk && macdOk) {
          st.waitingPullback = true;
          st.waitFromBarTime = barTime;
          st.pullbackExpiryBarTime = barTime + 12 * 900;
        }
      }
    }
    return st;
  }

  private async executeEntrySlot(
    symbol: string,
    refPrice: number,
    allocQuote: number,
    score: number,
    entryCand?: PortfolioRadarCandidate,
  ): Promise<void> {
    const usedIdx = new Set(this.slots.map((s) => s.slotIndex));
    let slotIndex = 0;
    while (usedIdx.has(slotIndex)) slotIndex++;

    let execPrice = refPrice;
    let execQty = allocQuote / execPrice;
    let orderId = `sim_buy_${Date.now()}_${slotIndex}`;

    try {
      const symInfo = await this.exchange.getExchangeInfo(symbol).catch(() => this.symbolInfo);
      const stepSize = symInfo.stepSize || 0.0001;
      execQty = Math.floor(execQty / stepSize) * stepSize;
      if (execQty <= 0 || execQty * execPrice < (symInfo.minNotional || 1)) return;

      if (!this.cfg.dryRun && this.exchange.hasCredentials()) {
        const orderRes = await this.exchange.placeOrder({
          symbol,
          side: "BUY",
          type: this.exchange.exchangeName === "alpaca" ? "MARKET" : "IOC",
          qty: execQty,
          price: Number((execPrice * 1.002).toFixed(symInfo.pricePrecision || 2)),
        });
        orderId = String(orderRes.orderId || orderId);
        if (orderRes.executedQty && orderRes.executedQty > 0) {
          execQty = orderRes.executedQty;
          if (orderRes.cummulativeQuoteQty && orderRes.cummulativeQuoteQty > 0) {
            execPrice = orderRes.cummulativeQuoteQty / execQty;
          }
        }
      }
    } catch (err) {
      this.log(`❌ [Slot ${slotIndex + 1} ${symbol}] Entry order failed: ${(err as Error).message}`);
      return;
    }

    const actualCost = execQty * execPrice;
    const feePct = this.exchange.exchangeName === "alpaca" || symbol.endsWith("FDUSD") ? 0 : 0.00075;
    const entryFee = actualCost * feePct;
    const slPct = Number(this.cfg.portfolioDisasterSlPct ?? 3.5);
    const slPrice = slPct > 0 ? execPrice * (1 - slPct / 100) : 0;

    const slot: ActivePortfolioSlot = {
      slotIndex,
      symbol,
      entryTime: Date.now(),
      entryPrice: execPrice,
      allocatedCapital: actualCost,
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
      entryReason: `MA-Fan Pullback EMA4 (Score ${score.toFixed(1)})`,
    };
    this.slots.push(slot);
    if (this.cfg.dryRun || this.cashBalance > 0) {
      this.cashBalance = Math.max(0, this.cashBalance - actualCost);
    }

    const st = this.setupStates.get(symbol);
    if (st) {
      st.waitingPullback = false;
      st.hadBottomFan = false;
    }

    const orderRec: UnifiedOrderRecord = {
      id: `mafan_buy_${Date.now()}_${slotIndex}`,
      orderId,
      symbol,
      baseAsset: this.exchange.exchangeName === "alpaca" ? symbol : symbol.replace(/(FDUSD|USDT|USDC)$/i, ""),
      quoteAsset: this.exchange.exchangeName === "alpaca" ? "USD" : (symbol.endsWith("FDUSD") ? "FDUSD" : "USDT"),
      side: "BUY",
      price: execPrice,
      fillPrice: execPrice,
      qty: execQty,
      executedQty: execQty,
      notionalQuote: actualCost,
      notional: actualCost,
      status: "FILLED",
      action: "BUY_FILL",
      levelDesc: `Slot ${slotIndex + 1} • EMA4 Pullback Entry`,
      reason: slot.entryReason,
      placedTime: slot.entryTime,
      fillTime: slot.entryTime,
      time: slot.entryTime,
      fee: entryFee,
      dryRun: this.cfg.dryRun,
    };
    this.db.recordEvent(orderRec);
    this.emit({ type: "order", data: orderRec });
    slot.learningJournalUid = await this.learning.onPortfolioEntry({
      exchange: this.exchange.exchangeName,
      symbol,
      slotIndex,
      entryTimeMs: slot.entryTime,
      entryPrice: execPrice,
      qty: execQty,
      allocatedQuote: actualCost,
      snapshot: entryCand ?? this.radarList.find((r) => r.symbol === symbol),
      macro: this.macroState,
      score,
    });
    this.saveState(true);
    this.log(`🚀 [Slot ${slotIndex + 1}/${this.getMaxSlots()} ENTER] Bought ${execQty} ${symbol} @ $${execPrice.toFixed(2)} ($${actualCost.toFixed(2)}) | SL $${slPrice.toFixed(2)}`);
  }

  private async executeExitSlot(
    slot: ActivePortfolioSlot,
    rawSellQty: number,
    refPrice: number,
    reason: string,
    closeFullSlot: boolean,
  ): Promise<boolean> {
    let sellQty = Math.min(slot.qty, rawSellQty);
    let execPrice = refPrice;
    let orderId = `sim_sell_${Date.now()}_${slot.slotIndex}`;

    try {
      const symInfo = await this.exchange.getExchangeInfo(slot.symbol).catch(() => this.symbolInfo);
      const stepSize = symInfo.stepSize || 0.0001;
      sellQty = closeFullSlot ? slot.qty : Math.floor(sellQty / stepSize) * stepSize;
      if (sellQty <= 0) return false;

      if (!this.cfg.dryRun && this.exchange.hasCredentials()) {
        const orderRes = await this.exchange.placeOrder({
          symbol: slot.symbol,
          side: "SELL",
          type: this.exchange.exchangeName === "alpaca" ? "MARKET" : "IOC",
          qty: sellQty,
          price: Number((execPrice * 0.998).toFixed(symInfo.pricePrecision || 2)),
        });
        orderId = String(orderRes.orderId || orderId);
        if (orderRes.executedQty && orderRes.executedQty > 0) {
          sellQty = Math.min(slot.qty, orderRes.executedQty);
          if (orderRes.cummulativeQuoteQty && orderRes.cummulativeQuoteQty > 0) {
            execPrice = orderRes.cummulativeQuoteQty / sellQty;
          }
        }
      }
    } catch (err) {
      this.log(`❌ [Slot ${slot.slotIndex + 1} ${slot.symbol}] Exit (${reason}) failed: ${(err as Error).message}`);
      return false;
    }

    const proceeds = sellQty * execPrice;
    const feePct = this.exchange.exchangeName === "alpaca" || slot.symbol.endsWith("FDUSD") ? 0 : 0.00075;
    const exitFee = proceeds * feePct;
    const portionRatio = slot.qty > 0 ? Math.min(1, sellQty / slot.qty) : 1;
    const costBasisSlice = (slot.allocatedCapital - slot.realizedProceeds) * portionRatio;
    const legPnl = proceeds - exitFee - costBasisSlice - (slot.entryFeeQuote * portionRatio);

    slot.realizedProceeds += proceeds - exitFee;
    slot.realizedExitFees += exitFee;
    slot.qty = Math.max(0, slot.qty - sellQty);

    this.realizedPnl += legPnl;
    this.tradeRealizedPnl += legPnl;
    if (this.cfg.dryRun || this.cashBalance >= 0) {
      this.cashBalance += proceeds - exitFee;
    }

    const isSl = reason.startsWith("SL_");
    const orderRec: UnifiedOrderRecord = {
      id: `mafan_sell_${Date.now()}_${slot.slotIndex}`,
      orderId,
      symbol: slot.symbol,
      baseAsset: this.exchange.exchangeName === "alpaca" ? slot.symbol : slot.symbol.replace(/(FDUSD|USDT|USDC)$/i, ""),
      quoteAsset: this.exchange.exchangeName === "alpaca" ? "USD" : (slot.symbol.endsWith("FDUSD") ? "FDUSD" : "USDT"),
      side: "SELL",
      price: execPrice,
      fillPrice: execPrice,
      qty: sellQty,
      executedQty: sellQty,
      notionalQuote: proceeds,
      notional: proceeds,
      status: "FILLED",
      action: isSl ? "CUT" : "TAKE_PROFIT",
      levelDesc: `Slot ${slot.slotIndex + 1} • ${reason}`,
      reason: `Slot ${slot.slotIndex + 1} Exit (${reason})`,
      placedTime: Date.now(),
      fillTime: Date.now(),
      time: Date.now(),
      pnl: legPnl,
      pnlQuote: legPnl,
      fee: exitFee,
      dryRun: this.cfg.dryRun,
    };
    this.db.recordEvent(orderRec);
    this.emit({ type: "order", data: orderRec });

    if (closeFullSlot || slot.qty <= 1e-6) {
      const totalSlotPnl = slot.realizedProceeds - slot.allocatedCapital - slot.entryFeeQuote;
      void this.learning.onPortfolioTradeClosed({
        journalUid: slot.learningJournalUid,
        exchange: this.exchange.exchangeName,
        symbol: slot.symbol,
        entryTimeMs: slot.entryTime,
        exitTimeMs: Date.now(),
        entryPrice: slot.entryPrice,
        exitPrice: execPrice,
        troughPrice: slot.troughPrice,
        peakPrice: slot.peakPrice,
        totalPnlQuote: totalSlotPnl,
        allocatedQuote: slot.allocatedCapital,
        exitReason: reason,
        cfg: this.cfg,
      });
      this.slots = this.slots.filter((s) => s.slotIndex !== slot.slotIndex);
      const st = this.setupStates.get(slot.symbol);
      if (st) {
        st.hadBottomFan = false;
        st.waitingPullback = false;
        st.bottomLow = Infinity;
      }
      this.emit({ type: "position_flat", data: { symbol: slot.symbol, pnl: legPnl, reason } });
    }

    this.saveState(true);
    this.log(`💰 [Slot ${slot.slotIndex + 1} EXIT ${reason}] Sold ${sellQty} ${slot.symbol} @ $${execPrice.toFixed(2)} | PnL: ${legPnl >= 0 ? "+" : ""}$${legPnl.toFixed(2)}`);
    return true;
  }

  // ── Telemetry Construction ─────────────────────────────────────────────────

  private buildSlotTelemetryList(): PortfolioSlotTelemetry[] {
    const maxSlots = this.getMaxSlots();
    const list: PortfolioSlotTelemetry[] = [];
    const freeCash = this.getAvailableBuyCash();
    const readyCandidates = this.radarList.filter((r) => r.stage === "PULLBACK_READY" || r.stage === "WAITING_PULLBACK" || r.stage === "BOTTOM_FAN");
    let waitCursor = 0;

    for (let i = 0; i < maxSlots; i++) {
      const slot = this.slots.find((s) => s.slotIndex === i);
      if (slot) {
        const ind = this.snapshots.get(slot.symbol);
        const curPx = ind?.livePrice || slot.lastKnownPrice || slot.entryPrice;
        const mktVal = slot.qty * curPx;
        const remainingCost = Math.max(0.01, slot.allocatedCapital - slot.realizedProceeds);
        const unreal = mktVal - remainingCost;
        const unrealPct = remainingCost > 0 ? (unreal / remainingCost) * 100 : 0;
        const tp1 = slot.tp1TargetPrice || slot.entryPrice * 1.008;
        const sl = slot.stopLossPrice || slot.entryPrice * 0.965;
        const range = Math.max(0.0001, tp1 - sl);
        const progressPct = Math.max(0, Math.min(100, ((curPx - sl) / range) * 100));

        let emas: PortfolioSlotTelemetry["emas"];
        let isBullishFan = false;
        if (ind) {
          const idx = ind.lastClosedIdx;
          emas = {
            ema4: ind.m4[idx]!,
            ema8: ind.m8[idx]!,
            ema16: ind.m16[idx]!,
            ema25: ind.m25[idx]!,
            ema75: ind.m75[idx]!,
            ema200: ind.m200[idx]!,
          };
          isBullishFan =
            emas.ema4 > emas.ema8 &&
            emas.ema8 > emas.ema16 &&
            emas.ema16 > emas.ema25 &&
            emas.ema25 > emas.ema75 &&
            emas.ema75 > emas.ema200;
        }

        let phaseText = "ถือรอถึง EMA200";
        if (slot.tp1Done && slot.beLocked) phaseText = "TP1 แล้ว • ล็อก BE รอ Full Fan";
        else if (slot.beLocked) phaseText = "ล็อก BE แล้ว • รันกำไร";
        else if (isBullishFan) phaseText = "Full Bullish Fan!";

        list.push({
          slotIndex: i,
          status: "HOLDING",
          symbol: slot.symbol,
          entryPrice: slot.entryPrice,
          currentPrice: curPx,
          qty: slot.qty,
          initialQty: slot.initialQty,
          allocatedCapital: slot.allocatedCapital,
          marketValue: mktVal,
          unrealizedPnl: unreal,
          unrealizedPnlPct: unrealPct,
          stopLossPrice: sl,
          tp1TargetPrice: tp1,
          tp1Done: slot.tp1Done,
          beLocked: slot.beLocked,
          isBullishFan,
          holdingBars: slot.activeBarsHeld,
          entryTime: slot.entryTime,
          phaseText,
          progressPct,
          emas,
        });
      } else {
        const freeSlotCount = Math.max(1, maxSlots - this.slots.length);
        const nextCand = readyCandidates[waitCursor++];
        list.push({
          slotIndex: i,
          status: "EMPTY",
          nextCandidate: nextCand,
          readyBudget: freeCash / freeSlotCount,
        });
      }
    }
    return list;
  }

  public emitTelemetryTick(): void {
    this.lastTelemetryEmitTs = Date.now();
    const slotTelemetries = this.buildSlotTelemetryList();
    const totalUnrealizedPnl = slotTelemetries.reduce((sum, s) => sum + (s.unrealizedPnl || 0), 0);
    const totalDeployed = slotTelemetries.reduce((sum, s) => sum + (s.allocatedCapital || 0), 0);
    const totalHeldValue = slotTelemetries.reduce((sum, s) => sum + (s.marketValue || 0), 0);

    const inspectedSym = this.inspectedSymbol || this.slots[0]?.symbol || this.radarList[0]?.symbol || this.symbolInfo.symbol;
    const inspectedInd = this.snapshots.get(inspectedSym);
    const inspectedSlot = slotTelemetries.find((s) => s.symbol === inspectedSym);
    const inspectedLive = inspectedInd?.livePrice || inspectedSlot?.currentPrice || 0;

    const roundsData = this.db.getTradeRounds("ALL");
    const closedRounds = roundsData.rounds.filter((r) => r.status === "CLOSED");
    const sumWin = closedRounds.filter((r) => (r.netPnlQuote ?? r.grossPnlQuote ?? 0) > 0).reduce((s, r) => s + (r.netPnlQuote ?? r.grossPnlQuote ?? 0), 0);
    const sumLoss = Math.abs(closedRounds.filter((r) => (r.netPnlQuote ?? r.grossPnlQuote ?? 0) <= 0).reduce((s, r) => s + (r.netPnlQuote ?? r.grossPnlQuote ?? 0), 0));
    const profitFactor = sumLoss > 0 ? sumWin / sumLoss : (sumWin > 0 ? 99 : 0);

    // Prevent stale single-symbol realizedPnl (e.g. from another exchange) from polluting Portfolio KPI
    const effectiveRealizedPnl =
      roundsData.summary.totalRounds > 0
        ? roundsData.summary.totalNetPnlQuote
        : (this.slots.length > 0 ? this.realizedPnl : 0);
    const effectiveTradeRealizedPnl =
      roundsData.summary.totalRounds > 0
        ? roundsData.summary.totalGrossPnlQuote
        : (this.slots.length > 0 ? this.tradeRealizedPnl : 0);

    const roundsSummary = {
      winRate: roundsData.summary.winRatePct,
      wins: roundsData.summary.winRounds,
      losses: roundsData.summary.lossRounds,
      totalCycles: roundsData.summary.closedRounds,
      profitFactor,
    };
    const freeCash = this.getAvailableBuyCash();
    const isAlpaca = this.exchange.exchangeName === "alpaca";
    const quoteAsset = isAlpaca ? "USD" : (this.symbolInfo.quoteAsset || "USDT");

    const maxSlots = this.getMaxSlots();
    let botActionCode = "SCANNING_PORTFOLIO";
    let botActionTitle = `🔭 สแกนหาจังหวะ MA-Fan (${this.slots.length}/${maxSlots} Slots)`;
    let botActionReason = this.macroState.reason;

    if (this.isPaused) {
      botActionCode = "PAUSED";
      botActionTitle = "⏸️ PAUSED (Monitoring Portfolio)";
      botActionReason = `หยุดซื้อ-ขายชั่วคราว | ถืออยู่ ${this.slots.length}/${maxSlots} ช่อง | ${this.macroState.reason}`;
    } else if (this.isMarketClosed) {
      botActionCode = "MARKET_CLOSED";
      botActionTitle = "🇺🇸 ตลาด US ปิดทำการ (Monitoring)";
      botActionReason = `เฝ้าระวังพอร์ต ${this.slots.length}/${maxSlots} ช่อง | ${this.macroState.reason}`;
    } else if (this.macroState.isSitOut) {
      botActionCode = "MACRO_SIT_OUT";
      botActionTitle = `🛡️ Macro Sit-Out (${this.macroState.benchmarkSymbol})`;
      botActionReason = `งดเข้า Slot ใหม่ชั่วคราว (${this.macroState.reason}) — เฝ้าจุดขาย Slot ที่ถืออยู่ปกติ`;
    } else if (this.slots.length >= maxSlots) {
      botActionCode = "FULL_ALLOCATION";
      botActionTitle = `✅ เต็มพอร์ต ${maxSlots}/${maxSlots} Slots`;
      botActionReason = "ถือครบ 3 ช่องแล้ว กำลังติดตาม TP1 EMA200 / Lock BE / Full Bullish Fan แบบเรียลไทม์";
    } else {
      const readyCount = this.radarList.filter((r) => r.stage === "PULLBACK_READY").length;
      const waitCount = this.radarList.filter((r) => r.stage === "WAITING_PULLBACK").length;
      botActionReason = `พร้อมเข้า ${readyCount} ตัว • รอย่อ EMA4 ${waitCount} ตัว • Universe ${this.macroState.universeCount} ตัว • ${this.macroState.reason}`;
    }

    const tickData = {
      time: Date.now(),
      strategyType: "ma_fan_portfolio",
      strategyMode: "ma_fan_portfolio",
      strategyName: this.name,
      executionTimeframe: "15m",
      supervisorTimeframe: "1h",
      exchange: this.exchange.exchangeName,
      dryRun: this.cfg.dryRun,
      ...(isAlpaca ? { alpacaBaseUrl: this.cfg.alpacaBaseUrl } : {}),
      symbol: inspectedSym,
      baseAsset: isAlpaca ? inspectedSym : inspectedSym.replace(/(FDUSD|USDT|USDC)$/i, ""),
      quoteAsset,
      pricePrecision: isAlpaca ? 2 : 4,
      qtyPrecision: isAlpaca ? 4 : 4,
      mid: inspectedLive,
      bestBid: inspectedLive,
      bestAsk: inspectedLive,
      isPaused: this.isPaused,
      isMarketClosed: this.isMarketClosed,
      marketClock: this.marketClock,
      unrealizedPnl: totalUnrealizedPnl,
      realizedPnl: effectiveRealizedPnl,
      tradeRealizedPnl: effectiveTradeRealizedPnl,
      netPnlQuote: effectiveRealizedPnl,
      netPnlUsdso: effectiveRealizedPnl,
      avgEntry: inspectedSlot?.entryPrice || 0,
      baseHeld: this.slots.reduce((s, x) => s + x.qty, 0),
      inventoryQuote: totalHeldValue,
      inventoryUsdso: totalHeldValue,
      maxInventoryQuote: this.getEffectiveBudgetCap(),
      maxInventoryUsdso: this.getEffectiveBudgetCap(),
      walletQuoteBalance: freeCash,
      walletUsdsoBalance: freeCash,
      walletBaseBalance: inspectedSlot?.qty || 0,
      walletTotalValueQuote: freeCash + totalHeldValue,
      walletTotalValueUsdso: freeCash + totalHeldValue,
      lots: this.getLots(),
      openOrders: [],
      metrics: roundsSummary,
      botAction: {
        code: botActionCode,
        title: botActionTitle,
        reason: botActionReason,
        trigger: `${this.slots.length}/${maxSlots} Slots • Cash $${freeCash.toFixed(0)}`,
        statusType: this.isPaused || this.isMarketClosed ? "warning" : (this.macroState.isSitOut ? "warning" : "success"),
      },
      portfolio: {
        maxSlots,
        usedSlots: this.slots.length,
        freeSlots: Math.max(0, maxSlots - this.slots.length),
        totalDeployed,
        totalHeldValue,
        freeCash,
        budgetCap: this.getEffectiveBudgetCap(),
        slots: slotTelemetries,
        radar: this.radarList.slice(0, 15),
        macro: this.macroState,
        inspectedSymbol: inspectedSym,
        inspectedEmas: inspectedSlot?.emas || this.radarList.find((r) => r.symbol === inspectedSym) || null,
        tp1Mode: this.cfg.portfolioTp1TargetMode || "EMA200",
        disasterSlPct: Number(this.cfg.portfolioDisasterSlPct ?? 3.5),
        wsConnected: isAlpaca
          ? this.wsConnected
          : (this.snapshots.size > 0 && Date.now() - this.lastSnapshotPollTs < 30_000),
        wsSubscribedCount: isAlpaca ? (this.alpacaWs?.getSubscribedSymbols().length || 0) : this.snapshots.size,
      },
    };

    this.emit({ type: "tick", data: tickData });
  }
}
