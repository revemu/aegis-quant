import test from "node:test";
import assert from "node:assert/strict";
import {
  CoreOrderManager,
  computeDeltaBuyPlan,
  type CorePendingOrderRecord,
} from "../src/core/order-manager.js";
import type {
  ExchangeAccountBalances,
  ExchangeLiveHolding,
  ExchangeOpenOrder,
  ExchangeOrderResult,
  ExchangeSymbolInfo,
  IExchangeClient,
  PlaceOrderParams,
} from "../src/exchange/types.js";

function createMockExchange(overrides: Partial<IExchangeClient> = {}): IExchangeClient {
  const defaultSymInfo: ExchangeSymbolInfo = {
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
  const defaultBal: ExchangeAccountBalances = {
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
    getExchangeInfo: async () => defaultSymInfo,
    getAccountBalances: async () => defaultBal,
    getOpenOrders: async () => [],
    getOrder: async () => {
      throw new Error("Failed to fetch order: HTTP 404");
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
      price: params.price || 100,
      origQty: params.qty,
      executedQty: params.qty,
      cummulativeQuoteQty: (params.price || 100) * params.qty,
      status: "FILLED",
      timeInForce: "DAY",
      type: "LIMIT",
      side: params.side,
    }),
    cancelOrder: async () => ({ success: true }),
    cancelAllOpenOrders: async () => ({ success: true }),
    updateCredentials: () => {},
    ...overrides,
  };
}

test("1. computeDeltaBuyPlan enforces hardSlotCap, live holding subtraction, and dip cost protection", () => {
  // Fresh buy
  const fresh = computeDeltaBuyPlan({
    targetCapital: 16666.67,
    hardSlotCap: 16666.67,
    liveHeldQty: 0,
    livePrice: 150,
    availCash: 40000,
    stepSize: 0.0001,
  });
  assert.equal(fresh.shouldBuy, true);
  assert.ok(Math.abs(fresh.deltaQuote - 16666.67) < 0.05);

  // Partial fill already held (NVDA $5,168.10 held of $16,666.67 target -> only buys delta ~$11,498.57)
  const partial = computeDeltaBuyPlan({
    targetCapital: 16666.67,
    hardSlotCap: 16666.67,
    liveHeldQty: 35,
    livePrice: 147.66,
    avgEntryPrice: 147.66,
    availCash: 24220,
    stepSize: 0.0001,
  });
  assert.equal(partial.shouldBuy, true);
  assert.ok(partial.deltaQuote > 11400 && partial.deltaQuote < 11550);
  assert.ok(partial.liveHeldNotional + partial.deltaQuote <= 16666.7);

  // Already >= 90% of target -> refuses extra buy
  const satisfied = computeDeltaBuyPlan({
    targetCapital: 16666.67,
    hardSlotCap: 16666.67,
    liveHeldQty: 102,
    livePrice: 150,
    avgEntryPrice: 150,
    availCash: 40000,
    stepSize: 0.0001,
  });
  assert.equal(satisfied.shouldBuy, false);
  assert.equal(satisfied.reason, "ALREADY_AT_TARGET");

  // Dip after buy: 100 @ avgEntry 166.67, price drops to $140 (mkt = $14,000, cost = $16,667) -> must NOT buy more
  const dipped = computeDeltaBuyPlan({
    targetCapital: 16666.67,
    hardSlotCap: 16666.67,
    liveHeldQty: 100,
    livePrice: 140,
    avgEntryPrice: 166.67,
    availCash: 40000,
    stepSize: 0.0001,
  });
  assert.equal(dipped.shouldBuy, false);
  assert.equal(dipped.reason, "ALREADY_AT_TARGET");

  // targetCapital > hardSlotCap -> strictly clamped to hardSlotCap
  const clamped = computeDeltaBuyPlan({
    targetCapital: 50000,
    hardSlotCap: 16666.67,
    liveHeldQty: 0,
    livePrice: 100,
    availCash: 50000,
    stepSize: 0.0001,
  });
  assert.equal(clamped.shouldBuy, true);
  assert.ok(clamped.deltaQuote <= 16666.68);
});

test("2. Lock 1: Open BUY order on exchange blocks duplicate submission", async () => {
  let placeCalls = 0;
  let persisted: CorePendingOrderRecord[] = [];
  const exchange = createMockExchange({
    getOpenOrders: async () => [
      {
        symbol: "NVDA",
        orderId: "open_buy_1",
        clientOrderId: "cid_open_1",
        price: 148,
        origQty: 100,
        executedQty: 0,
        cummulativeQuoteQty: 0,
        status: "NEW",
        timeInForce: "DAY",
        type: "LIMIT",
        side: "BUY",
        time: Math.floor(Date.now() / 1000),
        updateTime: Math.floor(Date.now() / 1000),
        isWorking: true,
      },
    ],
    placeOrder: async (p) => {
      placeCalls++;
      return {
        symbol: p.symbol,
        orderId: "x",
        clientOrderId: p.clientOrderId || "",
        transactTime: 0,
        price: 148,
        origQty: p.qty,
        executedQty: p.qty,
        cummulativeQuoteQty: p.qty * 148,
        status: "FILLED",
        timeInForce: "DAY",
        type: "LIMIT",
        side: "BUY",
      };
    },
  });

  const oms = new CoreOrderManager({
    exchange,
    isDryRun: () => false,
    persist: (p) => {
      persisted = p;
    },
  });

  const res = await oms.submitTargetBuy({
    symbol: "NVDA",
    intent: "ENTRY",
    targetCapital: 16667,
    hardSlotCap: 16667,
    availCash: 30000,
    refPrice: 148,
    stepSize: 0.0001,
  });

  assert.equal(res.outcome, "BLOCKED");
  assert.equal(res.reason, "EXCHANGE_OPEN_BUY_EXISTS");
  assert.equal(placeCalls, 0);
  assert.equal(persisted.length, 1);
});

test("3. Lock 2: Fail-Closed on getOpenOrders or getLiveHolding API errors", async () => {
  let placeCalls = 0;
  const errOpenExchange = createMockExchange({
    getOpenOrders: async () => {
      throw new Error("503 Service Unavailable");
    },
    placeOrder: async () => {
      placeCalls++;
      throw new Error("should not be called");
    },
  });

  const oms1 = new CoreOrderManager({
    exchange: errOpenExchange,
    isDryRun: () => false,
    persist: () => {},
  });
  const res1 = await oms1.submitTargetBuy({
    symbol: "AAPL",
    intent: "ENTRY",
    targetCapital: 16667,
    hardSlotCap: 16667,
    availCash: 30000,
    refPrice: 250,
    stepSize: 0.0001,
  });
  assert.equal(res1.outcome, "ABORTED_FAIL_CLOSED");
  assert.equal(res1.reason, "OPEN_ORDERS_CHECK_FAILED");
  assert.equal(placeCalls, 0);

  const errPosExchange = createMockExchange({
    getLiveHolding: async () => {
      throw new Error("ECONNRESET");
    },
    placeOrder: async () => {
      placeCalls++;
      throw new Error("should not be called");
    },
  });
  const oms2 = new CoreOrderManager({
    exchange: errPosExchange,
    isDryRun: () => false,
    persist: () => {},
  });
  const res2 = await oms2.submitTargetBuy({
    symbol: "AAPL",
    intent: "ENTRY",
    targetCapital: 16667,
    hardSlotCap: 16667,
    availCash: 30000,
    refPrice: 250,
    stepSize: 0.0001,
  });
  assert.equal(res2.outcome, "ABORTED_FAIL_CLOSED");
  assert.equal(res2.reason, "HOLDING_CHECK_FAILED");
  assert.equal(placeCalls, 0);
});

test("4. Lock 3: Existing live position on exchange prevents VOO-style 2x over-buy", async () => {
  let placeCalls = 0;
  const exchange = createMockExchange({
    getLiveHolding: async () => ({
      symbol: "VOO",
      qty: 25,
      qtyAvailable: 25,
      avgEntryPrice: 657.44,
      marketValue: 16436,
    }),
    placeOrder: async () => {
      placeCalls++;
      throw new Error("should not be called");
    },
  });

  const oms = new CoreOrderManager({
    exchange,
    isDryRun: () => false,
    persist: () => {},
  });

  const res = await oms.submitTargetBuy({
    symbol: "VOO",
    intent: "ENTRY",
    targetCapital: 16667,
    hardSlotCap: 16667,
    availCash: 30000,
    refPrice: 657.44,
    stepSize: 0.0001,
  });

  assert.equal(res.outcome, "ALREADY_SATISFIED");
  assert.equal(placeCalls, 0);
});

test("5. Write-Ahead persistence, restart recovery, fail-closed reconcile, delta anti-double-count, 404 disappeared, and stale cancel", async () => {
  let now = 1_700_000_000_000;
  let persisted: CorePendingOrderRecord[] = [];
  let savedBeforePlace = 0;

  // Scenario A: placeOrder throws timeout AFTER write-ahead persist; reconcile fails closed on 503; then partial fills 10 -> 100 without double-counting!
  let lookupState: "ERROR_503" | "PARTIAL_10" | "FILLED_100" = "ERROR_503";
  const exchangeA = createMockExchange({
    placeOrder: async () => {
      savedBeforePlace = persisted.length;
      throw new Error("Request timeout");
    },
    getOrderByClientId: async (_sym, cid) => {
      if (lookupState === "ERROR_503") throw new Error("HTTP 503");
      if (lookupState === "PARTIAL_10") {
        return {
          symbol: "NVDA",
          orderId: "alp_ord_99",
          clientOrderId: cid,
          price: 147.66,
          origQty: 100,
          executedQty: 10,
          cummulativeQuoteQty: 1476.6,
          status: "PARTIALLY_FILLED",
          timeInForce: "DAY",
          type: "LIMIT",
          side: "BUY",
          time: Math.floor(now / 1000),
          updateTime: Math.floor(now / 1000),
          isWorking: true,
        };
      }
      return {
        symbol: "NVDA",
        orderId: "alp_ord_99",
        clientOrderId: cid,
        price: 147.66,
        origQty: 100,
        executedQty: 100,
        cummulativeQuoteQty: 14766,
        status: "FILLED",
        timeInForce: "DAY",
        type: "LIMIT",
        side: "BUY",
        time: Math.floor(now / 1000),
        updateTime: Math.floor(now / 1000),
        isWorking: false,
      };
    },
    getOrder: async (_sym, oid) => {
      if (lookupState === "ERROR_503") throw new Error("HTTP 503");
      if (lookupState === "PARTIAL_10") {
        return {
          symbol: "NVDA",
          orderId: String(oid),
          clientOrderId: persisted[0]?.clientOrderId || "",
          price: 147.66,
          origQty: 100,
          executedQty: 10,
          cummulativeQuoteQty: 1476.6,
          status: "PARTIALLY_FILLED",
          timeInForce: "DAY",
          type: "LIMIT",
          side: "BUY",
          time: Math.floor(now / 1000),
          updateTime: Math.floor(now / 1000),
          isWorking: true,
        };
      }
      return {
        symbol: "NVDA",
        orderId: String(oid),
        clientOrderId: persisted[0]?.clientOrderId || "",
        price: 147.66,
        origQty: 100,
        executedQty: 100,
        cummulativeQuoteQty: 14766,
        status: "FILLED",
        timeInForce: "DAY",
        type: "LIMIT",
        side: "BUY",
        time: Math.floor(now / 1000),
        updateTime: Math.floor(now / 1000),
        isWorking: false,
      };
    },
  });

  const omsA = new CoreOrderManager({
    exchange: exchangeA,
    isDryRun: () => false,
    nowFn: () => now,
    staleBuyCancelMs: 30_000,
    notFoundGraceMs: 15_000,
    persist: (p) => {
      persisted = p;
    },
  });

  const subRes = await omsA.submitTargetBuy({
    symbol: "NVDA",
    intent: "ENTRY",
    targetCapital: 14766,
    hardSlotCap: 16667,
    availCash: 30000,
    refPrice: 147.66,
    stepSize: 0.0001,
  });
  assert.equal(savedBeforePlace, 1); // Persisted BEFORE placeOrder ran
  assert.equal(subRes.outcome, "ERROR_PENDING_RECONCILE");
  assert.equal(persisted.length, 1);
  assert.equal(omsA.hasBlockingPendingOrder("NVDA", "BUY"), true);

  // Restart recovery into a brand new CoreOrderManager instance
  const omsRestarted = new CoreOrderManager({
    exchange: exchangeA,
    isDryRun: () => false,
    nowFn: () => now,
    staleBuyCancelMs: 30_000,
    notFoundGraceMs: 15_000,
    persist: (p) => {
      persisted = p;
    },
  });
  omsRestarted.loadPendingOrders(persisted);
  assert.equal(omsRestarted.hasBlockingPendingOrder("NVDA", "BUY"), true);

  // Reconcile under 503 error -> FAIL-CLOSED (must keep pending record intact!)
  const recErr = await omsRestarted.reconcilePendingOrders();
  assert.equal(recErr.apiErrors, 1);
  assert.equal(persisted.length, 1);
  assert.equal(omsRestarted.hasBlockingPendingOrder("NVDA", "BUY"), true);

  // Reconcile partial 10 then final 100 -> total accounted must be strictly 10 + 90 = 100 (never 110!)
  const deltas: number[] = [];
  lookupState = "PARTIAL_10";
  await omsRestarted.reconcilePendingOrders({
    onFillDelta: (_rec, d) => {
      deltas.push(d);
    },
  });
  assert.deepEqual(deltas, [10]);
  assert.equal(persisted.length, 1); // still open, remains blocking

  lookupState = "FILLED_100";
  let terminalCalled = false;
  await omsRestarted.reconcilePendingOrders({
    onFillDelta: (_rec, d) => {
      deltas.push(d);
    },
    onTerminal: () => {
      terminalCalled = true;
    },
  });
  assert.deepEqual(deltas, [10, 90]);
  assert.equal(terminalCalled, true);
  assert.equal(persisted.length, 0);

  // Scenario B: Order Disappeared (404 past grace window) & Stale Working Cancel
  let canceledIds: string[] = [];
  let disappearedCalled = false;
  const exchangeB = createMockExchange({
    getOrder: async (_sym, oid) => {
      if (oid === "stale_ord") {
        return {
          symbol: "XOM",
          orderId: "stale_ord",
          clientOrderId: "cid_stale",
          price: 159,
          origQty: 50,
          executedQty: 0,
          cummulativeQuoteQty: 0,
          status: "NEW",
          timeInForce: "DAY",
          type: "LIMIT",
          side: "BUY",
          time: Math.floor(now / 1000),
          updateTime: Math.floor(now / 1000),
          isWorking: true,
        };
      }
      throw new Error("HTTP 404");
    },
    getOrderByClientId: async () => null,
    getOpenOrders: async () => [],
    cancelOrder: async (_sym, oid) => {
      canceledIds.push(String(oid));
      return { success: true };
    },
  });
  let persistedB: CorePendingOrderRecord[] = [
    {
      clientOrderId: "cid_lost",
      symbol: "AAPL",
      side: "BUY",
      intent: "ENTRY",
      targetCapital: 10000,
      requestedQty: 40,
      filledQtyAccounted: 0,
      refPrice: 250,
      createdAt: now,
      updatedAt: now,
      status: "OPEN",
    },
    {
      clientOrderId: "cid_stale",
      orderId: "stale_ord",
      symbol: "XOM",
      side: "BUY",
      intent: "ENTRY",
      targetCapital: 8000,
      requestedQty: 50,
      filledQtyAccounted: 0,
      refPrice: 159,
      createdAt: now,
      updatedAt: now,
      status: "OPEN",
    },
  ];
  const omsB = new CoreOrderManager({
    exchange: exchangeB,
    isDryRun: () => false,
    nowFn: () => now,
    staleBuyCancelMs: 30_000,
    notFoundGraceMs: 15_000,
    persist: (p) => {
      persistedB = p;
    },
  });
  omsB.loadPendingOrders(persistedB);

  // Within grace window: 404 keeps AAPL pending
  await omsB.reconcilePendingOrders({
    onDisappeared: () => {
      disappearedCalled = true;
    },
  });
  assert.equal(disappearedCalled, false);
  assert.equal(persistedB.length, 2);

  // Advance 35s: AAPL confirmed disappeared (check #2 past 15s grace), XOM triggers cancelOrder and stays CANCELING!
  now += 35_000;
  await omsB.reconcilePendingOrders({
    onDisappeared: () => {
      disappearedCalled = true;
    },
  });
  assert.equal(disappearedCalled, true);
  assert.deepEqual(canceledIds, ["stale_ord"]);
  assert.equal(persistedB.length, 1);
  assert.equal(persistedB[0]?.symbol, "XOM");
  assert.equal(persistedB[0]?.status, "CANCELING");
});

test("6. Partial Fill -> Delta Top-Up respects 15s cooldown then strictly buys remaining shortfall <= hardSlotCap", async () => {
  let heldQty = 0;
  let callNum = 0;
  let now = 1_700_000_000_000;
  const submittedQtys: number[] = [];

  const exchange = createMockExchange({
    getLiveHolding: async (sym) => ({
      symbol: sym,
      qty: heldQty,
      qtyAvailable: heldQty,
      avgEntryPrice: heldQty > 0 ? 147.66 : 0,
      marketValue: heldQty * 147.66,
    }),
    placeOrder: async (params) => {
      callNum++;
      submittedQtys.push(params.qty);
      const fillQty = callNum === 1 ? 35 : params.qty; // Call 1 partial-fills 35/112; Call 2 fills full delta
      heldQty += fillQty;
      return {
        symbol: params.symbol,
        orderId: `ord_${callNum}`,
        clientOrderId: params.clientOrderId || "",
        transactTime: Math.floor(now / 1000),
        price: 147.66,
        origQty: params.qty,
        executedQty: fillQty,
        cummulativeQuoteQty: fillQty * 147.66,
        status: fillQty < params.qty ? "CANCELED" : "FILLED",
        timeInForce: "DAY",
        type: "LIMIT",
        side: "BUY",
      };
    },
  });

  const oms = new CoreOrderManager({
    exchange,
    isDryRun: () => false,
    nowFn: () => now,
    persist: () => {},
  });

  const targetCap = 16666.67;
  const entry = await oms.submitTargetBuy({
    symbol: "NVDA",
    intent: "ENTRY",
    targetCapital: targetCap,
    hardSlotCap: targetCap,
    availCash: 40000,
    refPrice: 147.66,
    stepSize: 0.0001,
  });
  assert.equal(entry.outcome, "PARTIAL_TERMINAL");
  assert.equal(entry.executedQty, 35);

  // Within 15s retry cooldown -> must block rapid repeat buys
  const tooEarly = await oms.submitTargetBuy({
    symbol: "NVDA",
    intent: "TOP_UP",
    targetCapital: targetCap,
    hardSlotCap: targetCap,
    availCash: 40000 - 35 * 147.66,
    refPrice: 147.66,
    stepSize: 0.0001,
    attempt: 1,
    knownHeldQty: 35,
    knownAvgEntryPrice: 147.66,
  });
  assert.equal(tooEarly.outcome, "BLOCKED");
  assert.equal(tooEarly.reason, "SYMBOL_BUY_COOLDOWN");

  // After 15s cooldown -> buys ONLY remaining delta
  now += 16_000;
  const topUp = await oms.submitTargetBuy({
    symbol: "NVDA",
    intent: "TOP_UP",
    targetCapital: targetCap,
    hardSlotCap: targetCap,
    availCash: 40000 - 35 * 147.66,
    refPrice: 147.66,
    stepSize: 0.0001,
    attempt: 1,
    knownHeldQty: 35,
    knownAvgEntryPrice: 147.66,
  });
  assert.equal(topUp.outcome, "FILLED");
  const totalValue = (entry.executedQty + topUp.executedQty) * 147.66;
  assert.ok(totalValue > 16660 && totalValue <= targetCap + 0.05);
  assert.ok(submittedQtys[1]! < submittedQtys[0]!); // 2nd buy ordered ONLY delta (~77.87), NOT full ~112.87!
});
