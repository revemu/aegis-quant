import test from "node:test";
import assert from "node:assert/strict";
import { CorePortfolioManager, computeSlotEntryBudget } from "../src/core/portfolio-manager.js";
import type { PortfolioSlotRecord } from "../src/db.js";
import type {
  ExchangeAccountBalances,
  ExchangeLiveHolding,
  ExchangeOrderResult,
  ExchangeSymbolInfo,
  IExchangeClient,
} from "../src/exchange/types.js";

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
    quoteFree: 50_000,
    quoteLocked: 0,
    quoteTotal: 50_000,
    allBalances: { USD: { free: 50_000, locked: 0 } },
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
    getTopOfBook: async () => undefined,
    placeOrder: async (): Promise<ExchangeOrderResult> => {
      throw new Error("not mocked");
    },
    cancelOrder: async () => ({ success: true }),
    cancelAllOpenOrders: async () => ({ success: true }),
    updateCredentials: () => {},
    ...overrides,
  };
}

function createPm(opts: {
  exchange?: IExchangeClient;
  dryRun?: boolean;
  maxSlots?: number;
  capPct?: number;
  maxBudget?: number;
  slPct?: number;
  feeRate?: number;
  livePrices?: Record<string, number>;
} = {}) {
  const exchange = opts.exchange ?? createMockExchange();
  const prices = opts.livePrices ?? {};
  return new CorePortfolioManager({
    exchange,
    isDryRun: () => Boolean(opts.dryRun),
    getMaxSlots: () => opts.maxSlots ?? 3,
    getCapitalUsagePct: () => opts.capPct ?? 50,
    getMaxBudgetUsd: () => opts.maxBudget ?? 0,
    getDisasterSlPct: () => opts.slPct ?? 2.0,
    getDefaultCapital: () => 10_000,
    getFeeRate: () => opts.feeRate ?? 0,
    getLivePrice: (s) => prices[s],
  });
}

test("1. Capital & MM Budgeting respects initialCapital anchor (<=1.25x), 50% MM cap, and deployed subtraction", () => {
  const pm = createPm({ maxSlots: 3, capPct: 50 });
  pm.initialCapital = 100_000;
  pm.walletQuoteTotal = 90_000; // Drawn down 10%, within 1.25x (112.5k), so cap anchors to 100k
  pm.walletQuoteBalance = 40_000;
  pm.lastBalanceSyncTs = Date.now();

  assert.equal(pm.getEffectiveBudgetCap(), 100_000);
  assert.equal(pm.getMaxActiveBudget(), 50_000);
  assert.ok(Math.abs(pm.getStandardSlotUnit() - 50_000 / 3) < 0.01);
  assert.equal(pm.getAvailableBuyCash(), 40_000);

  // Deploy 1 slot of $16,666.67 -> remaining MM budget is $33,333.33
  pm.applyBuyFill({
    symbol: "XOM",
    qty: 100,
    execPrice: 166.6667,
    targetCapital: 16666.67,
  });
  assert.ok(Math.abs(pm.getAvailableBuyCash() - (40_000 - 16_666.67)) < 0.05);

  // Explicit maxBudget overrides dynamic equity
  const pmFixed = createPm({ maxSlots: 2, capPct: 50, maxBudget: 200 });
  assert.equal(pmFixed.getEffectiveBudgetCap(), 200);
  assert.equal(pmFixed.getMaxActiveBudget(), 100);
  assert.equal(pmFixed.getStandardSlotUnit(), 50);
});

test("2. computeSlotEntryBudget handles Q4 ATR scaling, survival scale, and cash clamp", () => {
  const normal = computeSlotEntryBudget({
    standardSlotUnit: 16_666.67,
    availCash: 40_000,
    sizeScale: 1.0,
    volTargetedEnabled: true,
    atrPct: 0.4,
    medianAtrPct: 0.4,
  });
  assert.equal(normal.volScale, 1.0);
  assert.equal(normal.allocQuote, 16_666.67);
  assert.equal(normal.targetCapital, 16_666.67);

  // High-ATR stock (>1.35x median) scales down up to 0.70x floor
  const highVol = computeSlotEntryBudget({
    standardSlotUnit: 16_666.67,
    availCash: 40_000,
    sizeScale: 1.0,
    volTargetedEnabled: true,
    atrPct: 1.2,
    medianAtrPct: 0.4,
  });
  assert.equal(highVol.volScale, 0.7);
  assert.ok(Math.abs(highVol.allocQuote - 16_666.67 * 0.7) < 0.01);

  // Cash clamp never exceeds availCash
  const lowCash = computeSlotEntryBudget({
    standardSlotUnit: 16_666.67,
    availCash: 4_000,
    sizeScale: 1.0,
  });
  assert.equal(lowCash.allocQuote, 4_000);
});

test("3. Buy Fill & Delta Top-Up recomputes exact VWAP entryPrice, SL, cash, and preserves targetCapital", () => {
  const pm = createPm({ dryRun: true, slPct: 2.0 });
  pm.cashBalance = 40_000;
  pm.walletQuoteBalance = 40_000;

  const r1 = pm.applyBuyFill({
    symbol: "NVDA",
    qty: 35,
    execPrice: 147.66,
    targetCapital: 16_666.67,
  });
  assert.equal(r1.isTopUp, false);
  assert.equal(r1.slot.qty, 35);
  assert.equal(r1.slot.targetCapital, 16_666.67);
  assert.ok(Math.abs(pm.cashBalance - (40_000 - 35 * 147.66)) < 0.01);

  const r2 = pm.applyBuyFill({
    symbol: "NVDA",
    qty: 77,
    execPrice: 148.0,
    targetCapital: 20_000, // Even if passed larger, never inflates original targetCapital
  });
  assert.equal(r2.isTopUp, true);
  assert.equal(r2.slot.qty, 112);
  assert.equal(r2.slot.initialQty, 112);
  assert.equal(r2.slot.targetCapital, 16_666.67);
  const expectedVwap = (35 * 147.66 + 77 * 148.0) / 112;
  assert.ok(Math.abs(r2.slot.entryPrice - expectedVwap) < 1e-6);
  assert.ok(Math.abs(r2.slot.stopLossPrice - expectedVwap * 0.98) < 1e-6);
});

test("4. Partial TP1 and Final Exit compute exact pro-rata PnL, fees, and cash reconciliation", () => {
  const pm = createPm({ dryRun: true, feeRate: 0.001 });
  pm.cashBalance = 10_000;
  pm.walletQuoteBalance = 10_000;

  const { slot } = pm.applyBuyFill({
    symbol: "BTCUSDT",
    qty: 10,
    execPrice: 100,
    targetCapital: 1_000,
  });
  assert.equal(slot.entryFeeQuote, 1.0); // 1000 * 0.001

  // TP1 sells 50% (5 qty @ $110 -> proceeds $550, exitFee $0.55, cost $500, entryFeeSlice $0.50 => net +$48.95)
  const tp1 = pm.applySellFill(slot, { sellQty: 5, execPrice: 110 });
  assert.ok(Math.abs(tp1.legPnl - 48.95) < 1e-6);
  assert.equal(tp1.remainingQty, 5);

  // Final close sells remaining 5 qty @ $120 -> proceeds $600, exitFee $0.60, cost $500, entryFeeSlice $0.50 => net +$98.90
  const tp2 = pm.applySellFill(slot, { sellQty: 5, execPrice: 120 });
  assert.ok(Math.abs(tp2.legPnl - 98.9) < 1e-6);
  assert.equal(tp2.remainingQty, 0);
  assert.ok(Math.abs(pm.realizedPnl - (48.95 + 98.9)) < 1e-6);
  // Final cash: 10000 - 1000 + (550 - 0.55) + (600 - 0.60) = 10148.85
  assert.ok(Math.abs(pm.cashBalance - 10_148.85) < 1e-6);
});

test("5. syncPositionsWithExchange retains/updates, detects shrink/clear, and adopts with targetCapital recovery", async () => {
  const positions: ExchangeLiveHolding[] = [
    { symbol: "XOM", qty: 90, qtyAvailable: 90, avgEntryPrice: 159.21, currentPrice: 160, marketValue: 14_400 },
    { symbol: "NVDA", qty: 35, qtyAvailable: 35, avgEntryPrice: 147.66, currentPrice: 149, marketValue: 5_215 },
  ];
  const exchange = createMockExchange({
    getAllPositions: async () => positions,
  });
  const pm = createPm({ exchange, maxSlots: 3, capPct: 50 });
  pm.initialCapital = 100_000;
  pm.walletQuoteTotal = 100_000;
  pm.walletQuoteBalance = 80_000;
  pm.lastBalanceSyncTs = Date.now();

  pm.slots.push(
    {
      slotIndex: 0,
      symbol: "XOM",
      entryTime: 1000,
      entryPrice: 159.21,
      allocatedCapital: 16_000,
      stopLossPrice: 156,
      qty: 100, // shrunk from 100 -> 90
      initialQty: 100,
      troughPrice: 159,
      peakPrice: 160,
      lastKnownPrice: 159,
      beLocked: false,
      tp1Done: false,
      realizedProceeds: 0,
      entryFeeQuote: 0,
      realizedExitFees: 0,
      activeBarsHeld: 2,
    },
    {
      slotIndex: 1,
      symbol: "VOO",
      entryTime: 1000,
      entryPrice: 657,
      allocatedCapital: 3_942,
      stopLossPrice: 644,
      qty: 6, // closed on exchange -> should be cleared
      initialQty: 6,
      troughPrice: 657,
      peakPrice: 658,
      lastKnownPrice: 658,
      beLocked: false,
      tp1Done: false,
      realizedProceeds: 0,
      entryFeeQuote: 0,
      realizedExitFees: 0,
      activeBarsHeld: 1,
    },
  );

  const deadline = Date.now() + 900_000;
  const syncRes = await pm.syncPositionsWithExchange({
    getRecentEntryTarget: (sym) =>
      sym === "NVDA" ? { targetCapital: 25_000, entryTime: 2000, deadlineMs: deadline } : undefined,
    adoptedReason: "Adopted Test",
  });

  assert.equal(syncRes.supported, true);
  assert.equal(syncRes.shrunkSymbols.has("XOM"), true);
  assert.equal(syncRes.clearedSlots.map((s) => s.symbol).join(","), "VOO");
  assert.equal(syncRes.adoptedSlots.map((s) => s.symbol).join(","), "NVDA");

  const xom = pm.slots.find((s) => s.symbol === "XOM")!;
  assert.equal(xom.qty, 90);
  assert.equal(xom.lastKnownPrice, 160);

  const nvda = pm.slots.find((s) => s.symbol === "NVDA")!;
  // targetCapital clamped strictly to standardSlotUnit ($50k / 3 = $16,666.67)
  assert.ok(Math.abs((nvda.targetCapital || 0) - 50_000 / 3) < 0.05);
  assert.equal(nvda.topUpDeadlineMs, deadline);
  assert.equal(nvda.slotIndex, 1); // Reused freed slotIndex 1!
});

test("6. exportState / loadState round-trip and resetState restore cashBalance", () => {
  const pm1 = createPm({ dryRun: true });
  pm1.initialCapital = 20_000;
  pm1.cashBalance = 20_000;
  pm1.applyBuyFill({ symbol: "AAPL", qty: 20, execPrice: 250, targetCapital: 5_000 });
  const exported = pm1.exportState();

  const pm2 = createPm({ dryRun: true });
  pm2.loadState(exported as { portfolioSlots: PortfolioSlotRecord[] } & typeof exported);
  assert.equal(pm2.slots.length, 1);
  assert.equal(pm2.slots[0]?.symbol, "AAPL");
  assert.equal(pm2.cashBalance, 15_000);
  assert.equal(pm2.initialCapital, 20_000);

  pm2.resetState({ resetPnl: true });
  assert.equal(pm2.slots.length, 0);
  assert.equal(pm2.realizedPnl, 0);
  assert.equal(pm2.cashBalance, 20_000);
});
