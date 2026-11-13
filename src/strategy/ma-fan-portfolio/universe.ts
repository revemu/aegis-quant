import type { Candle } from "../../market-structure.js";
import type { Config } from "../../config.js";
import { calcEMA } from "../ma-fan/ribbon.js";
import { calcMACD, calcRSI } from "../ma-fan/exhaustion.js";
import { fetchAlpacaBars, fetchAlpacaMultiBars, fetchAlpacaMultiSnapshots, normalizeStockSymbol } from "../../exchange/alpaca/feed.js";
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

const STABLE_TOKENS = new Set([
  "USDT", "FDUSD", "USDC", "TUSD", "USDP", "DAI", "BUSD", "EURI", "EUR", "AEUR", "USD1", "RLUSD",
]);

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

async function fetchBinanceKlinesSimple(symbol: string, interval: string, limit: number, baseUrl: string): Promise<Candle[]> {
  const url = `${baseUrl.replace(/\/+$/, "")}/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${Math.min(1000, limit)}`;
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
    const res = await fetch(`${baseUrl.replace(/\/+$/, "")}/api/v3/ticker/24hr`);
    if (!res.ok) return [];
    const tickers = (await res.json()) as Array<{ symbol: string; quoteVolume: string }>;
    const filtered = tickers.filter((t) => {
      const sym = String(t.symbol || "").toUpperCase();
      if (!sym.endsWith(quoteAsset)) return false;
      const base = sym.slice(0, -quoteAsset.length);
      if (STABLE_TOKENS.has(base)) return false;
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

  const closes15 = c15.map((c) => c.close);
  const closes1h = c1h.map((c) => c.close);
  const lastClosedIdx = closed15.length - 1;

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
    lastClosedTime: closed15[lastClosedIdx]!.time,
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
}

export async function fetchPortfolioUniverseAndBenchmark(opts: {
  exchangeName: string;
  cfg: Config;
  alpacaCreds: { apiKey: string; apiSecret: string; dataUrl: string };
  existingLivePrices?: Map<string, number>;
}): Promise<UniverseFetchResult> {
  const ex = (opts.exchangeName || "binance").toLowerCase();
  const isAlpaca = ex === "alpaca";

  if (isAlpaca) {
    const symbols = resolvePortfolioUniverseSymbols("alpaca", opts.cfg);
    const allSyms = Array.from(new Set(["SPY", ...symbols]));

    const [bars15Map, bars1hMap, snapMap, spy4h, spy1d] = await Promise.all([
      fetchAlpacaMultiBars({
        symbols: allSyms,
        interval: "15m",
        limit: 260,
        ...opts.alpacaCreds,
      }),
      fetchAlpacaMultiBars({
        symbols: allSyms,
        interval: "1h",
        limit: 120,
        ...opts.alpacaCreds,
      }),
      fetchAlpacaMultiSnapshots({
        symbols: allSyms,
        ...opts.alpacaCreds,
      }),
      fetchAlpacaBars({ symbol: "SPY", interval: "4h", limit: 120, ...opts.alpacaCreds }).catch(() => [] as Candle[]),
      fetchAlpacaBars({ symbol: "SPY", interval: "1d", limit: 60, ...opts.alpacaCreds }).catch(() => [] as Candle[]),
    ]);

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

  const [bench15, bench4h, bench1d] = await Promise.all([
    fetchBinanceKlinesSimple(benchSym, "15m", 260, baseUrl),
    fetchBinanceKlinesSimple(benchSym, "4h", 120, baseUrl),
    fetchBinanceKlinesSimple(benchSym, "1d", 60, baseUrl),
  ]);

  const snapshots = new Map<string, SymbolIndicatorSnapshot>();
  const chunkSize = 5;
  for (let i = 0; i < symbols.length; i += chunkSize) {
    const chunk = symbols.slice(i, i + chunkSize);
    await Promise.all(
      chunk.map(async (sym) => {
        const [c15, c1h] = await Promise.all([
          fetchBinanceKlinesSimple(sym, "15m", 260, baseUrl),
          fetchBinanceKlinesSimple(sym, "1h", 120, baseUrl),
        ]);
        const lp = opts.existingLivePrices?.get(sym);
        const ind = buildSymbolIndicators(sym, c15, c1h, lp);
        if (ind) snapshots.set(sym, ind);
      }),
    );
  }

  return {
    symbols: Array.from(snapshots.keys()),
    snapshots,
    benchmarkSymbol: benchSym,
    benchmark4h: bench4h,
    benchmark1d: bench1d,
    benchmark15m: bench15,
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
  reason: string;
} {
  const closed4h = opts.benchmark4h.filter((c) => c.isClosed !== false);
  const closed1d = opts.benchmark1d.filter((c) => c.isClosed !== false);
  const ema4h75 = closed4h.length >= 75 ? calcEMA(closed4h.map((c) => c.close), 75) : [];
  const ema1d25 = closed1d.length >= 25 ? calcEMA(closed1d.map((c) => c.close), 25) : [];

  const last4h = closed4h[closed4h.length - 1];
  const last1d = closed1d[closed1d.length - 1];
  const pass4h = ema4h75.length >= 75 && last4h ? last4h.close >= ema4h75[ema4h75.length - 1]! : true;
  const pass1d = ema1d25.length >= 25 && last1d ? last1d.close >= ema1d25[ema1d25.length - 1]! : true;

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

  return { isSitOut, pass4h, pass1d, benchmarkPrice, benchmarkRet24h, reason };
}
