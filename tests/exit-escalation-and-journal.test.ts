import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LearningEngine } from "../src/learning-engine.js";
import {
  MaFanPortfolioStrategy,
  resolveAlpacaExitLimitPrice,
} from "../src/strategy/ma-fan-portfolio/index.js";
import type { Config } from "../src/config.js";
import type { LearningJournalRecord, SymbolLearningStateRecord } from "../src/pg-store.js";
import type {
  ExchangeAccountBalances,
  ExchangeLiveHolding,
  ExchangeOpenOrder,
  ExchangeOrderResult,
  ExchangeSymbolInfo,
  IExchangeClient,
  PlaceOrderParams,
} from "../src/exchange/types.js";

function createMockPg(initialJournal: LearningJournalRecord[] = []) {
  const journal = new Map<string, LearningJournalRecord>(
    initialJournal.map((r) => [r.journalUid, { ...r }]),
  );
  const symbolStates = new Map<string, SymbolLearningStateRecord>();
  const adjustments: any[] = [];

  return {
    journal,
    symbolStates,
    adjustments,
    pg: {
      loadSymbolLearningStates: async () => Array.from(symbolStates.values()),
      upsertSymbolLearningState: async (s: SymbolLearningStateRecord) => {
        symbolStates.set(s.symbol, { ...s });
      },
      loadLearningJournal: async (recordType: "TRADE" | "BLOCKED_SIGNAL", status?: string) => {
        return Array.from(journal.values()).filter(
          (r) => r.recordType === recordType && (!status || r.status === status),
        );
      },
      upsertLearningJournalBatch: async (entries: LearningJournalRecord[]) => {
        for (const e of entries) {
          journal.set(e.journalUid, { ...e });
        }
      },
      closeLearningTrade: async (params: {
        journalUid: string;
        exitTimeMs: number;
        exitPrice: number;
        pnlQuote: number;
        pnlPct: number;
        maePct: number;
        mfePct: number;
        holdMinutes: number;
        exitReason: string;
      }) => {
        const cur = journal.get(params.journalUid);
        if (cur) {
          cur.status = "CLOSED";
          cur.exitTimeMs = params.exitTimeMs;
          cur.exitPrice = params.exitPrice;
          cur.pnlQuote = params.pnlQuote;
          cur.pnlPct = params.pnlPct;
          cur.maePct = params.maePct;
          cur.mfePct = params.mfePct;
          cur.holdMinutes = params.holdMinutes;
          cur.exitReason = params.exitReason;
        }
      },
      updateLearningJournalTradeFill: async (params: {
        journalUid: string;
        entryPrice: number;
        exitPrice: number;
        pnlQuote: number;
        pnlPct: number;
        holdMinutes: number;
        exitReason: string | null;
      }) => {
        const cur = journal.get(params.journalUid);
        if (cur) {
          cur.entryPrice = params.entryPrice;
          cur.exitPrice = params.exitPrice;
          cur.pnlQuote = params.pnlQuote;
          cur.pnlPct = params.pnlPct;
          cur.holdMinutes = params.holdMinutes;
          cur.exitReason = params.exitReason;
        }
      },
      recordLearningAdjustment: async (adj: any) => {
        adjustments.push(adj);
      },
      loadLearningBlockedStats: async () => [],
      loadLearningSummaryCounts: async () => ({ openTrades: 0, closedTrades: 0, pendingBlocked: 0 }),
    },
  };
}

function createMockDb(pg: any, closedRounds: any[] = []) {
  return {
    getPgStore: () => pg,
    getTradeRounds: () => ({
      rounds: closedRounds,
      summary: {
        closedRounds: closedRounds.length,
        totalNetPnlQuote: closedRounds.reduce((s, r) => s + Number(r.netPnlQuote || 0), 0),
      },
    }),
    syncRecentTradesFromExchange: async () => {},
  } as any;
}

function createMockExchange(overrides: Partial<IExchangeClient> = {}): IExchangeClient {
  const symInfo: ExchangeSymbolInfo = {
    symbol: "NVDA",
    status: "TRADING",
    baseAsset: "NVDA",
    quoteAsset: "USD",
    baseAssetPrecision: 4,
    quotePrecision: 2,
    minQty: 0.0001,
    maxQty: 1_000_000,
    stepSize: 0.0001,
    qtyPrecision: 4,
    minPrice: 0.01,
    maxPrice: 1_000_000,
    tickSize: 0.01,
    pricePrecision: 2,
    minNotional: 1,
  };
  const bal: ExchangeAccountBalances = {
    baseAsset: "NVDA",
    quoteAsset: "USD",
    baseFree: 0,
    baseLocked: 0,
    baseTotal: 0,
    quoteFree: 40_000,
    quoteLocked: 0,
    quoteTotal: 50_000,
    allBalances: { USD: { free: 40_000, locked: 0 } },
  };
  return {
    exchangeName: "alpaca",
    isDex: false,
    hasGasFees: false,
    nativeGasAsset: "USD",
    hasCredentials: () => true,
    syncTime: async () => Date.now(),
    getExchangeInfo: async () => symInfo,
    getAccountBalances: async () => bal,
    getOpenOrders: async () => [],
    getOrder: async () => {
      throw new Error("HTTP 404");
    },
    getOrderByClientId: async () => null,
    getLiveHolding: async (symbol: string): Promise<ExchangeLiveHolding> => ({
      symbol,
      qty: 0,
      qtyAvailable: 0,
      avgEntryPrice: 0,
      marketValue: 0,
    }),
    getTopOfBook: async () => undefined,
    placeOrder: async (params: PlaceOrderParams): Promise<ExchangeOrderResult> => ({
      symbol: params.symbol,
      orderId: "ord_1",
      clientOrderId: params.clientOrderId || "cid_1",
      transactTime: Math.floor(Date.now() / 1000),
      price: params.price || 149.28,
      origQty: params.qty,
      executedQty: params.qty,
      cummulativeQuoteQty: (params.price || 149.28) * params.qty,
      status: "FILLED",
      timeInForce: "DAY",
      type: params.type,
      side: params.side,
    }),
    cancelOrder: async () => ({ success: true }),
    cancelAllOpenOrders: async () => ({ success: true }),
    updateCredentials: () => {},
    ...overrides,
  };
}

test("1. resolveAlpacaExitLimitPrice anchors to bestBid and widens protective buffer to -0.5%", () => {
  // Anchors to lower bestBid when within 3% of refPrice
  assert.equal(resolveAlpacaExitLimitPrice(149.5, 149.2, 0.995, 2), Number((149.2 * 0.995).toFixed(2)));
  // Falls back to refPrice when bestBid is missing or stale (>3% away)
  assert.equal(resolveAlpacaExitLimitPrice(149.5, undefined, 0.995, 2), Number((149.5 * 0.995).toFixed(2)));
  assert.equal(resolveAlpacaExitLimitPrice(149.5, 130.0, 0.995, 2), Number((149.5 * 0.995).toFixed(2)));
});

test("2. reconcileJournalFromOrders protects active live slots and skips rounds ended before entry", async () => {
  const earlierCloseMs = Date.UTC(2026, 10, 20, 14, 35, 0);
  const newEntryMs = Date.UTC(2026, 10, 20, 18, 0, 0); // within 24h same-day window of earlierCloseMs
  const { pg, journal } = createMockPg([
    {
      journalUid: "trade_voo_live",
      recordType: "TRADE",
      exchange: "alpaca",
      symbol: "VOO",
      status: "OPEN",
      entryTimeMs: newEntryMs,
      entryPrice: 657.44,
      qty: 6,
      allocatedQuote: 3944.64,
      features: {} as any,
      updatedAt: newEntryMs,
    },
    {
      journalUid: "trade_xom_open_orphan",
      recordType: "TRADE",
      exchange: "alpaca",
      symbol: "XOM",
      status: "OPEN",
      entryTimeMs: newEntryMs,
      entryPrice: 159.21,
      qty: 100,
      allocatedQuote: 15921,
      features: {} as any,
      updatedAt: newEntryMs,
    },
  ]);

  const closedRounds = [
    {
      symbol: "VOO",
      status: "CLOSED",
      startTime: earlierCloseMs - 3_600_000,
      endTime: earlierCloseMs,
      avgBuyPrice: 675.8,
      avgSellPrice: 654.7,
      buyCostQuote: 10_000,
      netPnlQuote: -2030.54,
      durationMs: 3_600_000,
    },
    {
      symbol: "XOM",
      status: "CLOSED",
      startTime: earlierCloseMs - 3_600_000,
      endTime: earlierCloseMs, // ended BEFORE newEntryMs - 15m
      avgBuyPrice: 155,
      avgSellPrice: 156,
      buyCostQuote: 5000,
      netPnlQuote: 32,
      durationMs: 3_600_000,
    },
  ];

  const engine = new LearningEngine(createMockDb(pg, closedRounds), () => {});
  const res = await engine.reconcileJournalFromOrders("alpaca", [
    { symbol: "VOO", learningJournalUid: "trade_voo_live", entryTime: newEntryMs },
  ]);

  assert.equal(res.updatedTrades, 0);
  assert.equal(journal.get("trade_voo_live")?.status, "OPEN");
  assert.equal(journal.get("trade_xom_open_orphan")?.status, "OPEN");
});

test("3. ensureActiveSlotsInJournal binds existing open row and backfills missing reconciled slot without duplication", async () => {
  const entryXomMs = Date.UTC(2026, 10, 25, 18, 15, 36);
  const entryVooMs = Date.UTC(2026, 10, 25, 20, 45, 30);

  const { pg, journal } = createMockPg([
    {
      journalUid: "trade_existing_xom",
      recordType: "TRADE",
      exchange: "alpaca",
      symbol: "XOM",
      status: "OPEN",
      entryTimeMs: entryXomMs,
      entryPrice: 159.21,
      qty: 104.6835,
      allocatedQuote: 16666.66,
      features: {} as any,
      updatedAt: entryXomMs,
    },
  ]);

  const engine = new LearningEngine(createMockDb(pg), () => {});
  const slots: any[] = [
    {
      slotIndex: 0,
      symbol: "XOM",
      entryTime: entryXomMs + 10_000, // slightly offset timestamp, missing learningJournalUid
      entryPrice: 159.21,
      qty: 52.3418,
      initialQty: 104.6835,
      allocatedCapital: 16666.66,
    },
    {
      slotIndex: 2,
      symbol: "VOO",
      entryTime: entryVooMs,
      entryPrice: 657.44,
      qty: 3,
      initialQty: 6,
      allocatedCapital: 3947.94,
    },
  ];

  const changed1 = await engine.ensureActiveSlotsInJournal({ exchange: "alpaca", slots });
  assert.equal(changed1, 2);
  assert.equal(slots[0].learningJournalUid, "trade_existing_xom");
  assert.ok(slots[1].learningJournalUid?.includes("VOO"));
  assert.equal(journal.size, 2);

  // Second run is idempotent
  const changed2 = await engine.ensureActiveSlotsInJournal({ exchange: "alpaca", slots });
  assert.equal(changed2, 0);
  assert.equal(journal.size, 2);
});

test("4. evaluateSurvivalPosture suppresses misleading 4H+1D bear & high-breadth reason when triggered by DD", () => {
  const now = Date.now();
  const closedRounds = [
    {
      symbol: "META",
      status: "CLOSED",
      startTime: now - 5 * 86_400_000,
      endTime: now - 4 * 86_400_000,
      netPnlQuote: -4400,
    },
  ];
  const engine = new LearningEngine(createMockDb(null, closedRounds), () => {});
  const decision = engine.evaluateSurvivalPosture({
    cfg: {
      exchange: "alpaca",
      portfolioMinRelStrength24h: -1,
      portfolioMinRsi1h: 40,
      portfolioMinBuyVolRatio: 1.6,
      portfolioSurvivalGuardEnforced: false,
    } as Config,
    macro: {
      benchmarkSymbol: "SPY",
      benchmarkPrice: 728.35,
      benchmarkRet24h: 4.83,
      isSitOut: false,
      sitOutMode: "breadth_35",
      reason: "SPY Bear — Breadth 85% >= 35%",
      breadthAboveEma25Pct: 85.4,
      universeCount: 49,
      pass4h: false,
      pass1d: false,
      lastScanTime: now,
    },
    configuredMaxSlots: 3,
    currentEquity: 95_600,
  });

  assert.equal(decision.posture, "DEFENSIVE");
  assert.ok(decision.reasons.some((r) => r.includes("Rolling DD")));
  assert.equal(
    decision.reasons.some((r) => r.includes("4H+1D bear")),
    false,
  );

  // When breadth actually drops < 20% with 4H+1D bear, the macro bear-breadth reason IS emitted
  const cleanEngine = new LearningEngine(createMockDb(null, []), () => {});
  const bearLowBreadth = cleanEngine.evaluateSurvivalPosture({
    cfg: { exchange: "binance", portfolioSurvivalGuardEnforced: false } as Config,
    macro: {
      benchmarkSymbol: "BTCUSDT",
      benchmarkPrice: 59800,
      benchmarkRet24h: -3.2,
      isSitOut: true,
      sitOutMode: "breadth_35",
      reason: "Bear",
      breadthAboveEma25Pct: 17,
      universeCount: 25,
      pass4h: false,
      pass1d: false,
      lastScanTime: now,
    },
    configuredMaxSlots: 2,
    currentEquity: 200,
  });
  assert.equal(bearLowBreadth.posture, "DEFENSIVE");
  assert.ok(bearLowBreadth.reasons.some((r) => r.includes("BTCUSDT 4H+1D bear & Breadth 17%")));
});

test("5. executeExitSlot escalates Alpaca protective limit timeout (0 fill) immediately to MARKET, and guards cancel-race fills", async (t) => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "aegis-exit-test-"));
  t.after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  // Case A: Protective limit times out with 0 fills -> reconcile confirms CANCELED -> immediate MARKET escalation fills TP1
  const placedOrdersA: PlaceOrderParams[] = [];
  const exA = createMockExchange({
    getTopOfBook: async () => ({ bestBid: 149.2, bestAsk: 149.3, mid: 149.25, bidQty: 100, askQty: 100, time: Math.floor(Date.now() / 1000) }),
    getAllPositions: async () => [{ symbol: "NVDA", qty: 35, qtyAvailable: 35, avgEntryPrice: 147.66, currentPrice: 149.25, marketValue: 5223.75 }],
    getOrder: async (_sym, id) => ({
      symbol: "NVDA",
      orderId: id,
      clientOrderId: "cid_ioc",
      price: 148.45,
      origQty: 17.5,
      executedQty: 0,
      cummulativeQuoteQty: 0,
      status: "CANCELED",
      timeInForce: "DAY",
      type: "LIMIT",
      side: "SELL",
      time: Math.floor(Date.now() / 1000),
      updateTime: Math.floor(Date.now() / 1000),
      isWorking: false,
    }),
    getOrderByClientId: async () => null,
    getOpenOrders: async () => [],
    placeOrder: async (params: PlaceOrderParams) => {
      placedOrdersA.push(params);
      if (params.type === "IOC") {
        const err = new Error(`Alpaca protective limit ${params.symbol} @ $${params.price} unfilled within timeout (canceled)`) as any;
        err.orderId = "ord_ioc_timeout";
        err.orderStatus = "canceled";
        err.isTerminal = true;
        throw err;
      }
      return {
        symbol: params.symbol,
        orderId: "ord_mkt_fallback",
        clientOrderId: params.clientOrderId || "cid_mkt",
        transactTime: Math.floor(Date.now() / 1000),
        price: 149.22,
        origQty: params.qty,
        executedQty: params.qty,
        cummulativeQuoteQty: 149.22 * params.qty,
        status: "FILLED",
        timeInForce: "DAY",
        type: "MARKET",
        side: "SELL",
      };
    },
  });

  const cfgA = {
    exchange: "alpaca",
    strategyType: "ma_fan_portfolio",
    dryRun: false,
    dbPath: path.join(tmpDir, "dbA.json"),
    symbol: "NVDA",
    portfolioMaxSlots: 3,
    portfolioCapitalUsagePct: 50,
    portfolioDisasterSlPct: 2,
  } as Config;

  const stratA = new MaFanPortfolioStrategy({
    exchange: exA,
    symbolInfo: await exA.getExchangeInfo("NVDA"),
    config: cfgA,
    atrSource: { getAtr: () => 0 } as any,
    log: () => {},
  });
  (stratA as any).isMarketClosed = false;
  (stratA as any).portfolio.slots = [
    {
      slotIndex: 0,
      symbol: "NVDA",
      entryTime: Date.now() - 3_600_000,
      entryPrice: 147.66,
      allocatedCapital: 5168.1,
      stopLossPrice: 148.03,
      qty: 35,
      initialQty: 35,
      troughPrice: 147.5,
      peakPrice: 149.4,
      lastKnownPrice: 149.25,
      beLocked: true,
      tp1Done: false,
      realizedProceeds: 0,
      entryFeeQuote: 0,
      realizedExitFees: 0,
      activeBarsHeld: 4,
    },
  ];

  const soldA = await (stratA as any).executeExitSlot(
    (stratA as any).slots[0],
    17.5,
    149.25,
    "TP1_EMA25",
    false,
  );

  assert.equal(soldA, true);
  assert.equal(placedOrdersA.length, 2);
  assert.equal(placedOrdersA[0]!.type, "IOC");
  assert.equal(placedOrdersA[1]!.type, "MARKET");
  assert.equal((stratA as any).slots[0].qty, 17.5);

  // Case B: Protective limit throws timeout, BUT cancel-race filled on exchange -> reconcile accounts fill, NO duplicate MARKET order!
  const placedOrdersB: PlaceOrderParams[] = [];
  const exB = createMockExchange({
    getAllPositions: async () => [{ symbol: "VOO", qty: 6, qtyAvailable: 6, avgEntryPrice: 657.44, currentPrice: 665.5, marketValue: 3993 }],
    getOrderByClientId: async (_sym, cid): Promise<ExchangeOrderResult> => ({
      symbol: "VOO",
      orderId: "ord_voo_race_filled",
      clientOrderId: cid,
      transactTime: Math.floor(Date.now() / 1000),
      price: 665.51,
      origQty: 3,
      executedQty: 3,
      cummulativeQuoteQty: 665.51 * 3,
      status: "FILLED",
      timeInForce: "DAY",
      type: "LIMIT",
      side: "SELL",
    }),
    placeOrder: async (params: PlaceOrderParams) => {
      placedOrdersB.push(params);
      const err = new Error(`Alpaca protective limit ${params.symbol} @ $${params.price} unfilled within timeout (new)`) as any;
      err.orderId = "ord_voo_race_filled";
      err.orderStatus = "new";
      err.isTerminal = false;
      throw err;
    },
  });

  const stratB = new MaFanPortfolioStrategy({
    exchange: exB,
    symbolInfo: await exB.getExchangeInfo("VOO"),
    config: { ...cfgA, dbPath: path.join(tmpDir, "dbB.json"), symbol: "VOO" },
    atrSource: { getAtr: () => 0 } as any,
    log: () => {},
  });
  (stratB as any).isMarketClosed = false;
  (stratB as any).portfolio.slots = [
    {
      slotIndex: 2,
      symbol: "VOO",
      entryTime: Date.now() - 3_600_000,
      entryPrice: 657.44,
      allocatedCapital: 3944.64,
      stopLossPrice: 659.08,
      qty: 6,
      initialQty: 6,
      troughPrice: 657,
      peakPrice: 666,
      lastKnownPrice: 665.5,
      beLocked: true,
      tp1Done: false,
      realizedProceeds: 0,
      entryFeeQuote: 0,
      realizedExitFees: 0,
      activeBarsHeld: 4,
    },
  ];

  const soldB = await (stratB as any).executeExitSlot(
    (stratB as any).slots[0],
    3,
    665.5,
    "TP1_EMA25",
    false,
  );

  assert.equal(soldB, true);
  assert.equal(placedOrdersB.length, 1); // Only the initial IOC; ZERO duplicate MARKET sell!
  assert.equal((stratB as any).slots[0].qty, 3);
});
