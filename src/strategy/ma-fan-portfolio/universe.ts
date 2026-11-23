import type { Candle } from "../../market-structure.js";
import type { Config } from "../../config.js";
import { calcEMA } from "../ma-fan/ribbon.js";
import { calcMACD, calcRSI } from "../ma-fan/exhaustion.js";
import { fetchAlpacaBars, fetchAlpacaMultiBars, fetchAlpacaMultiSnapshots, intervalToSeconds, isUsRegularTradingHoursBar, normalizeStockSymbol } from "../../exchange/alpaca/feed.js";
import { getActiveBinanceTradingSymbols } from "../../exchange/binance/client.js";
import { STABLE_TOKENS } from "../../backtest-service.js";
import { isSyntheticOrStock } from "../../pair-rotator.js";
import { filterChangedCandles, mergeCandles, type PgStore } from "../../pg-store.js";
import type { SymbolIndicatorSnapshot } from "./types.js";

export const US_STOCK_TOP49_UNIVERSE = [
  // Benchmarks & Core ETFs
  "SPY", "QQQ", "DIA", "IWM", "SCHD", "VYM", "VTI", "VOO",
  // Mega-Cap Tech & Semis
  "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "AMD", "QCOM", "ORCL", "IBM", "CSCO",
  // Dividend Aristocrats & Defensive Consumer
  "KO", "PEP", "MCD", "WMT", "COST", "PG", "JNJ", "ABBV", "MRK", "PFE", "UNH",
  // Financial & Payment Giants
  "JPM", "BAC", "WFC", "V", "MA",
  // Industrial, Energy & Real Estate
  "CAT", "HON", "GE", "RTX", "CVX", "XOM", "O", "HD", "LOW", "T", "VZ", "NEE",
];

export const US_STOCK_TOP25_UNIVERSE = [
  "SPY", "QQQ", "SCHD", "VTI", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
  "AVGO", "AMD", "QCOM", "IBM", "CSCO", "KO", "PEP", "WMT", "PFE", "UNH",
  "JPM", "WFC", "V", "XOM", "T",
];

export function resolvePortfolioUniverseSymbols(
  exchangeName: string,
  cfg: Pick<Config, "portfolioUniversePreset" | "portfolioMaxUniversePairs" | "portfolioCustomWhitelist" | "portfolioBlacklist" | "symbol">,
): string[] {
  const ex = (exchangeName || "binance").toLowerCase();
  const blacklist = new Set((cfg.portfolioBlacklist || []).map((s) => s.toUpperCase().trim()));
  const custom = (cfg.portfolioCustomWhitelist || []).map((s) => s.toUpperCase().trim()).filter(Boolean);
  const maxPairs = Math.max(3, Math.min(60, cfg.portfolioMaxUniversePairs || 49));

  if (ex === "alpaca") {
    let base: string[];
    if (cfg.portfolioUniversePreset === "custom" && custom.length > 0) {
      base = custom.map(normalizeStockSymbol);
    } else if (cfg.portfolioUniversePreset === "us_top25") {
      base = US_STOCK_TOP25_UNIVERSE;
    } else {
      base = US_STOCK_TOP49_UNIVERSE;
    }
    return Array.from(new Set(base.filter((s) => s && !blacklist.has(s)))).slice(0, maxPairs);
  }

  if (custom.length > 0 && cfg.portfolioUniversePreset === "custom") {
    return Array.from(new Set(custom.filter((s) => !blacklist.has(s)))).slice(0, maxPairs);
  }
  // Crypto pairs discovered dynamically from Binance 24hr ticker (returned asynchronously in loadUniverseData)
  return [];
}

async function fetchBinanceKlinesSimple(
  symbol: string,
  interval: string,
  limit: number,
  baseUrl: string,
  startTimeMs?: number,
): Promise<Candle[]> {
  let url = `${baseUrl.replace(/\/+$/, "")}/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${Math.min(1000, limit)}`;
  if (startTimeMs && startTimeMs > 0) {
    url += `&startTime=${Math.floor(startTimeMs)}`;
  }
  const res = await fetch(url);
  if (!res.ok) return [];
  const raw = (await res.json()) as any[];
  if (!Array.isArray(raw)) return [];
  const nowMs = Date.now();
  return raw
    .map((k) => ({
      time: Math.floor(Number(k[0]) / 1000),
      open: Number(k[1]),
      high: Number(k[2]),
      low: Number(k[3]),
      close: Number(k[4]),
      volume: Number(k[5] ?? 0),
      isClosed: Number(k[6]) < nowMs,
    }))
    .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close) && c.close > 0);
}

export async function discoverBinancePortfolioSymbols(
  quoteAsset: "FDUSD" | "USDT",
  maxPairs: number,
  blacklist: string[],
  baseUrl: string,
): Promise<string[]> {
  const bl = new Set(blacklist.map((s) => s.toUpperCase().trim()));
  const minVol = quoteAsset === "FDUSD" ? 500_000 : 3_000_000;
  try {
    const [res, activeTradingSymbols] = await Promise.all([
      fetch(`${baseUrl.replace(/\/+$/, "")}/api/v3/ticker/24hr`),
      getActiveBinanceTradingSymbols().catch(() => new Set<string>()),
    ]);
    if (!res.ok) return [];
    const tickers = (await res.json()) as Array<{ symbol: string; quoteVolume: string }>;
    const filtered = tickers.filter((t) => {
      const sym = String(t.symbol || "").toUpperCase();
      if (activeTradingSymbols.size > 0 && !activeTradingSymbols.has(sym)) return false;
      if (!sym.endsWith(quoteAsset)) return false;
      const base = sym.slice(0, -quoteAsset.length);
      if (STABLE_TOKENS.has(base)) return false;
      if (isSyntheticOrStock(sym)) return false;
      if (bl.has(sym)) return false;
      return Number(t.quoteVolume) >= minVol;
    });
    filtered.sort((a, b) => Number(b.quoteVolume) - Number(a.quoteVolume));
    return filtered.slice(0, maxPairs).map((t) => t.symbol);
  } catch {
    return [];
  }
}

export function buildSymbolIndicators(symbol: string, c15: Candle[], c1h: Candle[], livePrice?: number): SymbolIndicatorSnapshot | null {
  if (!c15 || c15.length < 210) return null;
  const closed15 = c15.filter((c) => c.isClosed !== false);
  if (closed15.length < 205) return null;

  let lastClosedIdx = c15.length - 1;
  while (lastClosedIdx >= 0 && c15[lastClosedIdx]!.isClosed === false) {
    lastClosedIdx--;
  }
  if (lastClosedIdx < 204) return null;

  const closes15 = c15.map((c) => c.close);
  const closes1h = c1h.map((c) => c.close);

  return {
    symbol,
    c15,
    c1h,
    m4: calcEMA(closes15, 4),
    m8: calcEMA(closes15, 8),
    m16: calcEMA(closes15, 16),
    m25: calcEMA(closes15, 25),
    m75: calcEMA(closes15, 75),
    m200: calcEMA(closes15, 200),
    rsi15: calcRSI(c15, 14),
    macdHist15: calcMACD(c15).hist,
    rsi1h: c1h.length >= 20 ? calcRSI(c1h, 14) : new Array(c1h.length).fill(50),
    lastClosedIdx,
    lastClosedTime: c15[lastClosedIdx]!.time,
    livePrice: livePrice && livePrice > 0 ? livePrice : c15[c15.length - 1]!.close,
  };
}

export interface UniverseFetchResult {
  symbols: string[];
  snapshots: Map<string, SymbolIndicatorSnapshot>;
  benchmarkSymbol: string;
  benchmark4h: Candle[];
  benchmark1d: Candle[];
  benchmark15m: Candle[];
  usdProxySymbol: string;
  usdProxy1d: Candle[];
}

export async function fetchPortfolioUniverseAndBenchmark(opts: {
  exchangeName: string;
  cfg: Config;
  alpacaCreds: { apiKey: string; apiSecret: string; dataUrl: string };
  existingLivePrices?: Map<string, number>;
  existingCandles?: Map<string, { c15: Candle[]; c1h: Candle[] }>;
  existingBenchmarkCandles?: { c4h?: Candle[]; c1d?: Candle[]; c15?: Candle[]; usd1d?: Candle[] };
  pgStore?: PgStore | null;
  log?: (msg: string) => void;
}): Promise<UniverseFetchResult> {
  const ex = (opts.exchangeName || "binance").toLowerCase();
  const isAlpaca = ex === "alpaca";
  const pg = opts.pgStore ?? null;
  const upsertItems: Array<{ exchange: string; symbol: string; timeframe: string; candles: Candle[] }> = [];

  const queueCandleChanges = (exchange: string, symbol: string, timeframe: string, prev: Candle[], incoming: Candle[]) => {
    if (!pg || incoming.length === 0) return;
    const changed = filterChangedCandles(prev, incoming);
    if (changed.length > 0) {
      upsertItems.push({ exchange, symbol, timeframe, candles: changed });
    }
  };

  if (isAlpaca) {
    const symbols = resolvePortfolioUniverseSymbols("alpaca", opts.cfg);
    const allSyms = Array.from(new Set(["SPY", ...symbols]));

    const fetchAlpacaBatchCached = async (
      interval: "15m" | "1h",
      targetLimit: number,
    ): Promise<Map<string, Candle[]>> => {
      const ivSec = intervalToSeconds(interval);
      const cachedMap = new Map<string, Candle[]>();
      const missingInRam: string[] = [];

      for (const sym of allSyms) {
        const mem = opts.existingCandles?.get(sym);
        const memArr = interval === "15m" ? mem?.c15 : mem?.c1h;
        if (memArr && memArr.length >= targetLimit) {
          cachedMap.set(sym, memArr);
        } else if (sym === "SPY" && interval === "15m" && opts.existingBenchmarkCandles?.c15 && opts.existingBenchmarkCandles.c15.length >= targetLimit) {
          cachedMap.set(sym, opts.existingBenchmarkCandles.c15);
        } else {
          missingInRam.push(sym);
        }
      }

      if (pg && missingInRam.length > 0) {
        try {
          const dbBatch = await pg.loadMarketCandlesBatch("alpaca", missingInRam, interval, Math.ceil(targetLimit * 1.25));
          for (const [sym, bars] of dbBatch.entries()) {
            const rthBars = bars.filter((c) => isUsRegularTradingHoursBar(c.time, interval)).slice(-targetLimit);
            if (rthBars.length > 0) cachedMap.set(sym, rthBars);
          }
        } catch (err) {
          opts.log?.(`⚠️ PG candle batch read (${interval}) failed: ${(err as Error).message}`);
        }
      }

      let maxWarmTime = 0;
      for (const bars of cachedMap.values()) {
        if (bars.length >= targetLimit) {
          const lastT = bars[bars.length - 1]?.time || 0;
          if (lastT > maxWarmTime) maxWarmTime = lastT;
        }
      }

      const warmSyms: string[] = [];
      const coldSyms: string[] = [];
      let minRecentWarmTime = Infinity;

      for (const sym of allSyms) {
        const bars = cachedMap.get(sym);
        const lastT = bars?.[bars.length - 1]?.time || 0;
        if (bars && bars.length >= targetLimit && maxWarmTime > 0 && maxWarmTime - lastT <= 5 * 86_400) {
          warmSyms.push(sym);
          if (lastT < minRecentWarmTime) minRecentWarmTime = lastT;
        } else {
          coldSyms.push(sym);
        }
      }

      const [warmFetched, coldFetched] = await Promise.all([
        warmSyms.length > 0 && Number.isFinite(minRecentWarmTime)
          ? fetchAlpacaMultiBars({
              symbols: warmSyms,
              interval,
              limit: targetLimit,
              startTimeSec: Math.max(1, minRecentWarmTime - ivSec * 4),
              ...opts.alpacaCreds,
            })
          : Promise.resolve(new Map<string, Candle[]>()),
        coldSyms.length > 0
          ? fetchAlpacaMultiBars({
              symbols: coldSyms,
              interval,
              limit: targetLimit,
              ...opts.alpacaCreds,
            })
          : Promise.resolve(new Map<string, Candle[]>()),
      ]);

      const mergedOut = new Map<string, Candle[]>();
      for (const sym of allSyms) {
        const prev = cachedMap.get(sym) || [];
        const incoming = warmFetched.get(sym) || coldFetched.get(sym) || [];
        const merged = mergeCandles(prev, incoming)
          .filter((c) => isUsRegularTradingHoursBar(c.time, interval))
          .slice(-targetLimit);
        mergedOut.set(sym, merged);
        queueCandleChanges("alpaca", sym, interval, prev, incoming);
      }
      return mergedOut;
    };

    const fetchAlpacaSingleCached = async (
      symbol: string,
      interval: "4h" | "1d",
      targetLimit: number,
      memBars?: Candle[],
    ): Promise<Candle[]> => {
      const ivSec = intervalToSeconds(interval);
      let prev = memBars && memBars.length >= targetLimit ? memBars : [];
      if (prev.length < targetLimit && pg) {
        try {
          prev = await pg.loadMarketCandles("alpaca", symbol, interval, targetLimit);
        } catch {}
      }
      const lastT = prev.length >= targetLimit ? prev[prev.length - 1]?.time || 0 : 0;
      const incoming = await fetchAlpacaBars({
        symbol,
        interval,
        limit: targetLimit,
        startTimeSec: lastT > 0 ? Math.max(1, lastT - ivSec * 3) : undefined,
        ...opts.alpacaCreds,
      }).catch(() => [] as Candle[]);
      queueCandleChanges("alpaca", symbol, interval, prev, incoming);
      return mergeCandles(prev, incoming, targetLimit);
    };

    const [bars15Map, bars1hMap, snapMap, spy4h, spy1d, uup1d] = await Promise.all([
      fetchAlpacaBatchCached("15m", 260),
      fetchAlpacaBatchCached("1h", 120),
      fetchAlpacaMultiSnapshots({
        symbols: allSyms,
        ...opts.alpacaCreds,
      }),
      fetchAlpacaSingleCached("SPY", "4h", 120, opts.existingBenchmarkCandles?.c4h),
      fetchAlpacaSingleCached("SPY", "1d", 260, opts.existingBenchmarkCandles?.c1d),
      fetchAlpacaSingleCached("UUP", "1d", 120, opts.existingBenchmarkCandles?.usd1d).catch(() => [] as Candle[]),
    ]);

    if (pg && upsertItems.length > 0) {
      await pg.upsertMarketCandlesBatch(upsertItems).catch((err) => {
        opts.log?.(`⚠️ PG candle batch upsert failed: ${(err as Error).message}`);
      });
    }

    const snapshots = new Map<string, SymbolIndicatorSnapshot>();
    for (const sym of symbols) {
      const c15 = bars15Map.get(sym) || [];
      const c1h = bars1hMap.get(sym) || [];
      const snap = snapMap.get(sym);
      const lp = snap?.mid || opts.existingLivePrices?.get(sym);
      const ind = buildSymbolIndicators(sym, c15, c1h, lp);
      if (ind) snapshots.set(sym, ind);
    }

    return {
      symbols: Array.from(snapshots.keys()),
      snapshots,
      benchmarkSymbol: "SPY",
      benchmark4h: spy4h,
      benchmark1d: spy1d,
      benchmark15m: bars15Map.get("SPY") || [],
      usdProxySymbol: "UUP",
      usdProxy1d: uup1d,
    };
  }

  // Binance CEX Portfolio
  const quoteAsset: "FDUSD" | "USDT" = String(opts.cfg.symbol || "").toUpperCase().endsWith("FDUSD") ? "FDUSD" : "USDT";
  let symbols = resolvePortfolioUniverseSymbols("binance", opts.cfg);
  if (symbols.length === 0) {
    symbols = await discoverBinancePortfolioSymbols(
      quoteAsset,
      Math.min(30, opts.cfg.portfolioMaxUniversePairs || 25),
      opts.cfg.portfolioBlacklist || [],
      opts.cfg.binanceBaseUrl,
    );
  }
  const benchSym = quoteAsset === "FDUSD" ? "BTCFDUSD" : "BTCUSDT";
  const baseUrl = opts.cfg.binanceBaseUrl || "https://api.binance.com";

  // Preload Binance candles from RAM or PostgreSQL batch in 1 query per timeframe
  const preloadBinanceBatch = async (syms: string[], tf: "15m" | "1h", limit: number): Promise<Map<string, Candle[]>> => {
    const map = new Map<string, Candle[]>();
    const needDb: string[] = [];
    for (const s of syms) {
      const mem = opts.existingCandles?.get(s);
      const arr = tf === "15m" ? mem?.c15 : mem?.c1h;
      if (arr && arr.length >= limit) map.set(s, arr);
      else needDb.push(s);
    }
    if (pg && needDb.length > 0) {
      const dbMap = await pg.loadMarketCandlesBatch("binance", needDb, tf, limit).catch(() => new Map<string, Candle[]>());
      for (const [s, bars] of dbMap.entries()) map.set(s, bars);
    }
    return map;
  };

  const fetchBinanceIncremental = async (sym: string, tf: string, targetLimit: number, prev: Candle[]): Promise<Candle[]> => {
    const ivSec = intervalToSeconds(tf);
    const nowSec = Math.floor(Date.now() / 1000);
    const lastT = prev.length >= targetLimit ? prev[prev.length - 1]?.time || 0 : 0;
    const canIncremental = lastT > 0 && nowSec - lastT < ivSec * 800;
    const startTimeMs = canIncremental ? Math.max(1, (lastT - ivSec * 3) * 1000) : undefined;
    const fetchLimit = canIncremental ? Math.min(1000, Math.max(5, Math.ceil((nowSec - lastT) / ivSec) + 5)) : targetLimit;
    const incoming = await fetchBinanceKlinesSimple(sym, tf, fetchLimit, baseUrl, startTimeMs);
    queueCandleChanges("binance", sym, tf, prev, incoming);
    return mergeCandles(prev, incoming, targetLimit);
  };

  const [db15Map, db1hMap] = await Promise.all([
    preloadBinanceBatch(symbols, "15m", 260),
    preloadBinanceBatch(symbols, "1h", 120),
  ]);

  const loadBinSeries = async (sym: string, tf: "15m" | "4h" | "1d", limit: number, mem?: Candle[]): Promise<Candle[]> => {
    let prev = mem && mem.length >= limit ? mem : [];
    if (prev.length < limit && pg) {
      prev = await pg.loadMarketCandles("binance", sym, tf, limit).catch(() => [] as Candle[]);
    }
    return fetchBinanceIncremental(sym, tf, limit, prev);
  };

  const [bench15, bench4h, bench1d, eur1d] = await Promise.all([
    loadBinSeries(benchSym, "15m", 260, opts.existingBenchmarkCandles?.c15),
    loadBinSeries(benchSym, "4h", 120, opts.existingBenchmarkCandles?.c4h),
    loadBinSeries(benchSym, "1d", 260, opts.existingBenchmarkCandles?.c1d),
    loadBinSeries("EURUSDT", "1d", 120, opts.existingBenchmarkCandles?.usd1d).catch(() => [] as Candle[]),
  ]);

  const snapshots = new Map<string, SymbolIndicatorSnapshot>();
  const chunkSize = 5;
  for (let i = 0; i < symbols.length; i += chunkSize) {
    const chunk = symbols.slice(i, i + chunkSize);
    await Promise.all(
      chunk.map(async (sym) => {
        const [c15, c1h] = await Promise.all([
          fetchBinanceIncremental(sym, "15m", 260, db15Map.get(sym) || []),
          fetchBinanceIncremental(sym, "1h", 120, db1hMap.get(sym) || []),
        ]);
        const lp = opts.existingLivePrices?.get(sym);
        const ind = buildSymbolIndicators(sym, c15, c1h, lp);
        if (ind) snapshots.set(sym, ind);
      }),
    );
  }

  if (pg && upsertItems.length > 0) {
    await pg.upsertMarketCandlesBatch(upsertItems).catch((err) => {
      opts.log?.(`⚠️ PG candle batch upsert failed: ${(err as Error).message}`);
    });
  }

  return {
    symbols: Array.from(snapshots.keys()),
    snapshots,
    benchmarkSymbol: benchSym,
    benchmark4h: bench4h,
    benchmark1d: bench1d,
    benchmark15m: bench15,
    usdProxySymbol: "EURUSDT",
    usdProxy1d: eur1d,
  };
}

export function evaluateBenchmarkSitOut(opts: {
  benchmarkSymbol: string;
  benchmark4h: Candle[];
  benchmark1d: Candle[];
  benchmark15m: Candle[];
  sitOutMode: Config["portfolioSitOutMode"];
}): {
  isSitOut: boolean;
  pass4h: boolean;
  pass1d: boolean;
  benchmarkPrice: number;
  benchmarkRet24h: number;
  ema4h75?: number;
  ema1d25?: number;
  gap4hPct?: number;
  gap1dPct?: number;
  reason: string;
} {
  const closed4h = opts.benchmark4h.filter((c) => c.isClosed !== false);
  const closed1d = opts.benchmark1d.filter((c) => c.isClosed !== false);
  const ema4h75Arr = closed4h.length >= 75 ? calcEMA(closed4h.map((c) => c.close), 75) : [];
  const ema1d25Arr = closed1d.length >= 25 ? calcEMA(closed1d.map((c) => c.close), 25) : [];

  const last4h = closed4h[closed4h.length - 1];
  const last1d = closed1d[closed1d.length - 1];
  const val4h75 = ema4h75Arr.length >= 75 ? ema4h75Arr[ema4h75Arr.length - 1] : undefined;
  const val1d25 = ema1d25Arr.length >= 25 ? ema1d25Arr[ema1d25Arr.length - 1] : undefined;
  const pass4h = val4h75 !== undefined && last4h ? last4h.close >= val4h75 : true;
  const pass1d = val1d25 !== undefined && last1d ? last1d.close >= val1d25 : true;
  const gap4hPct = val4h75 && val4h75 > 0 && last4h ? ((last4h.close - val4h75) / val4h75) * 100 : undefined;
  const gap1dPct = val1d25 && val1d25 > 0 && last1d ? ((last1d.close - val1d25) / val1d25) * 100 : undefined;

  let isSitOut = false;
  if (opts.sitOutMode === "dual") isSitOut = !pass4h || !pass1d;
  else if (opts.sitOutMode === "btc1d") isSitOut = !pass1d;
  else if (opts.sitOutMode === "btc4h") isSitOut = !pass4h;

  const c15 = opts.benchmark15m;
  const benchmarkPrice = c15[c15.length - 1]?.close ?? last4h?.close ?? last1d?.close ?? 0;
  const ref24 = c15.length > 96 ? c15[c15.length - 97]!.open : c15[0]?.open ?? benchmarkPrice;
  const benchmarkRet24h = ref24 > 0 ? ((benchmarkPrice - ref24) / ref24) * 100 : 0;

  let reason = `${opts.benchmarkSymbol} Bullish (Entries Allowed)`;
  if (opts.sitOutMode === "off") {
    reason = `Macro Gate OFF (${opts.benchmarkSymbol}: $${benchmarkPrice.toFixed(2)})`;
  } else if (isSitOut) {
    const fails: string[] = [];
    if (!pass4h && (opts.sitOutMode === "dual" || opts.sitOutMode === "btc4h")) fails.push("4H < EMA75");
    if (!pass1d && (opts.sitOutMode === "dual" || opts.sitOutMode === "btc1d")) fails.push("1D < EMA25");
    reason = `Macro Sit-Out (${opts.benchmarkSymbol} ${fails.join(" & ")})`;
  }

  return {
    isSitOut,
    pass4h,
    pass1d,
    benchmarkPrice,
    benchmarkRet24h,
    ema4h75: val4h75,
    ema1d25: val1d25,
    gap4hPct,
    gap1dPct,
    reason,
  };
}
