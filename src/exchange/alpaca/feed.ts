/**
 * Alpaca US Equities Market Data Feeds
 * Provides historical & live bars (ATR + Dow Structure) and Top-of-Book polling
 * via Alpaca Data API v2 (IEX feed, 0 extra data cost on Free/Basic plans).
 */

import type { AtrSource } from "../../types.js";
import type { Candle } from "../../market-structure.js";
import type { BookTickerData } from "../binance/feed.js";

export interface AlpacaFeedConfig {
  symbol: string;
  interval: string;
  apiKey: string;
  apiSecret: string;
  dataUrl?: string;
  staleMs?: number;
  initialCandleCount?: number;
}

export interface FetchAlpacaBarsParams {
  symbol: string;
  interval: string;
  limit?: number;
  apiKey: string;
  apiSecret: string;
  dataUrl?: string;
}

/**
 * Normalize US stock ticker symbols cleanly:
 * - "AAPL" -> "AAPL"
 * - "AAPL/USD", "AAPL:USD" -> "AAPL"
 * - "AAPLUSD", "NVDAUSDT" -> "AAPL", "NVDA" (only if length > 4 so short tickers ending in USD aren't clipped)
 */
export function normalizeStockSymbol(symbol: string): string {
  const s = (symbol || "").trim().toUpperCase();
  if (!s) return "";
  if (s.includes("/") || s.includes(":")) {
    return s.split(/[:/]/)[0] || s;
  }
  if (s.length > 4 && s.endsWith("USDT")) return s.slice(0, -4);
  if (s.length > 4 && s.endsWith("USD")) return s.slice(0, -3);
  return s;
}

export function intervalToSeconds(interval: string): number {
  switch (interval.toLowerCase()) {
    case "1m":
      return 60;
    case "5m":
      return 300;
    case "15m":
      return 900;
    case "1h":
      return 3_600;
    case "4h":
      return 14_400;
    case "1d":
      return 86_400;
    default:
      return 3_600;
  }
}

export function mapIntervalToAlpacaTimeframe(interval: string): { timeframe: string; aggregateHours: number } {
  switch (interval.toLowerCase()) {
    case "1m":
      return { timeframe: "1Min", aggregateHours: 1 };
    case "5m":
      return { timeframe: "5Min", aggregateHours: 1 };
    case "15m":
      return { timeframe: "15Min", aggregateHours: 1 };
    case "1h":
      return { timeframe: "1Hour", aggregateHours: 1 };
    case "4h":
      return { timeframe: "1Hour", aggregateHours: 4 };
    case "1d":
      return { timeframe: "1Day", aggregateHours: 1 };
    default:
      return { timeframe: "1Hour", aggregateHours: 1 };
  }
}

function aggregateFourHourBars(bars: Candle[]): Candle[] {
  if (bars.length === 0) return [];
  const buckets = new Map<number, Candle>();
  for (const b of bars) {
    const bucketTime = Math.floor(b.time / 14_400) * 14_400;
    const existing = buckets.get(bucketTime);
    if (!existing) {
      buckets.set(bucketTime, { ...b, time: bucketTime });
    } else {
      existing.high = Math.max(existing.high, b.high);
      existing.low = Math.min(existing.low, b.low);
      existing.close = b.close;
      existing.volume = (existing.volume ?? 0) + (b.volume ?? 0);
      existing.isClosed = b.isClosed;
    }
  }
  return Array.from(buckets.values()).sort((a, b) => a.time - b.time);
}

function estimateDaysBack(interval: string, barCount: number): number {
  let barsPerDay = 9;
  switch (interval.toLowerCase()) {
    case "1m":
      barsPerDay = 200;
      break;
    case "5m":
      barsPerDay = 45;
      break;
    case "15m":
      barsPerDay = 9;
      break;
    case "1h":
    case "4h":
      barsPerDay = 4.5;
      break;
    case "1d":
      barsPerDay = 0.65;
      break;
  }
  return Math.max(7, Math.ceil(barCount / barsPerDay) + 10);
}

/**
 * Parse an Alpaca `/v2/stocks/{symbol}/snapshot` payload into a standardized BookTickerData.
 * Rejects one-sided stub quotes (where bp<=0 or ap<=0 or size==0) in favor of latestTrade / bar close.
 */
export function parseAlpacaSnapshotToBook(snap: {
  latestQuote?: { bp?: number; ap?: number; bs?: number; as?: number; t?: string };
  latestTrade?: { p?: number; t?: string };
  minuteBar?: { c?: number; t?: string };
  dailyBar?: { c?: number; t?: string };
}): BookTickerData | undefined {
  const q = snap?.latestQuote;
  const tradePrice = Number(snap?.latestTrade?.p || 0);
  const barPrice = Number(snap?.minuteBar?.c || snap?.dailyBar?.c || 0);
  const fallbackPrice = tradePrice > 0 ? tradePrice : barPrice;

  const qBid = Number(q?.bp || 0);
  const qAsk = Number(q?.ap || 0);
  const qBidSize = Number(q?.bs || 0);
  const qAskSize = Number(q?.as || 0);

  const hasTwoSidedQuote = qBid > 0 && qAsk > 0 && qBid <= qAsk && qBidSize > 0 && qAskSize > 0;
  let bestBid: number;
  let bestAsk: number;
  if (hasTwoSidedQuote) {
    bestBid = qBid;
    bestAsk = qAsk;
  } else if (fallbackPrice > 0) {
    bestBid = fallbackPrice;
    bestAsk = fallbackPrice;
  } else {
    bestBid = qBid > 0 ? qBid : qAsk;
    bestAsk = qAsk > 0 ? qAsk : qBid;
  }

  const mid = bestBid > 0 && bestAsk > 0 ? (bestBid + bestAsk) / 2 : bestBid || bestAsk || fallbackPrice;
  if (!mid || mid <= 0) return undefined;

  const tsStr = hasTwoSidedQuote ? q?.t : (snap?.latestTrade?.t || snap?.minuteBar?.t || q?.t);
  const parsedMs = tsStr ? new Date(tsStr).getTime() : Date.now();
  const time = Number.isFinite(parsedMs) ? Math.floor(parsedMs / 1000) : Math.floor(Date.now() / 1000);

  return {
    bestBid: bestBid > 0 ? bestBid : mid,
    bestAsk: bestAsk > 0 ? bestAsk : mid,
    mid,
    bidQty: hasTwoSidedQuote ? qBidSize : 100,
    askQty: hasTwoSidedQuote ? qAskSize : 100,
    time,
  };
}

/**
 * Fetch historical OHLCV bars for a US equity/ETF from Alpaca Data API v2.
 */
export async function fetchAlpacaBars(params: FetchAlpacaBarsParams): Promise<Candle[]> {
  const cleanSym = normalizeStockSymbol(params.symbol);
  if (!cleanSym) return [];

  const { timeframe, aggregateHours } = mapIntervalToAlpacaTimeframe(params.interval);
  const requestedLimit = Math.max(10, Math.min(params.limit ?? 300, 10_000));
  const rawLimit = Math.min(10_000, requestedLimit * aggregateHours);
  const dataUrl = (params.dataUrl || "https://data.alpaca.markets").replace(/\/+$/, "");

  const daysBack = estimateDaysBack(params.interval, rawLimit);
  const startIso = new Date(Date.now() - daysBack * 86_400_000).toISOString().split(".")[0] + "Z";

  const headers: Record<string, string> = {};
  if (params.apiKey && params.apiSecret) {
    headers["APCA-API-KEY-ID"] = params.apiKey;
    headers["APCA-API-SECRET-KEY"] = params.apiSecret;
  }

  const rawBars: Array<{ t: string; o: number; h: number; l: number; c: number; v?: number }> = [];
  let pageToken: string | undefined;
  let attempts = 0;

  do {
    let url = `${dataUrl}/v2/stocks/${encodeURIComponent(cleanSym)}/bars?timeframe=${timeframe}&start=${encodeURIComponent(startIso)}&limit=${rawLimit}&sort=desc&adjustment=all&feed=iex`;
    if (pageToken) url += `&page_token=${encodeURIComponent(pageToken)}`;

    const res = await fetch(url, { headers });
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      throw new Error(`Alpaca bars HTTP ${res.status}${errText ? `: ${errText}` : ""}`);
    }

    const data = (await res.json()) as {
      bars?: Array<{ t: string; o: number; h: number; l: number; c: number; v?: number }> | null;
      next_page_token?: string | null;
    };
    if (Array.isArray(data?.bars) && data.bars.length > 0) {
      rawBars.push(...data.bars);
    }
    pageToken = data?.next_page_token || undefined;
    attempts++;
  } while (pageToken && rawBars.length < rawLimit && attempts < 5);

  if (rawBars.length === 0) return [];

  const intervalSec = intervalToSeconds(params.interval);
  const nowSec = Math.floor(Date.now() / 1000);

  let candles: Candle[] = rawBars
    .slice()
    .reverse()
    .map((b) => {
      const time = Math.floor(new Date(b.t).getTime() / 1000);
      return {
        time,
        open: Number(b.o),
        high: Number(b.h),
        low: Number(b.l),
        close: Number(b.c),
        volume: Number(b.v ?? 0),
        isClosed: true,
      };
    })
    .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close) && c.close > 0);

  if (aggregateHours > 1) {
    candles = aggregateFourHourBars(candles);
  }

  if (candles.length > 0) {
    const last = candles[candles.length - 1]!;
    if (last.time + intervalSec > nowSec) {
      last.isClosed = false;
    }
  }

  if (candles.length > requestedLimit) {
    candles = candles.slice(-requestedLimit);
  }

  return candles;
}

export class AlpacaAtrFeed implements AtrSource {
  private trPctHistory: number[] = [];
  private prevClose?: number;
  private latestPrice?: number;
  private stopped = false;
  private connected = false;
  private lastMessageAt = 0;
  private lastEmittedClosedTime = 0;
  private lastEmittedBarKey = "";
  private pollTimer?: ReturnType<typeof setTimeout>;
  private candleListeners: ((candle: Candle, isClosed: boolean) => void)[] = [];

  constructor(
    private readonly cfg: AlpacaFeedConfig,
    private readonly lookback: number,
    private readonly log: (msg: string) => void,
  ) {}

  onCandle(fn: (candle: Candle, isClosed: boolean) => void): void {
    this.candleListeners.push(fn);
  }

  async start(): Promise<void> {
    this.stopped = false;
    const warmCount = Math.max(this.lookback + 10, this.cfg.initialCandleCount ?? 300);

    try {
      const bars = await fetchAlpacaBars({
        symbol: this.cfg.symbol,
        interval: this.cfg.interval,
        limit: warmCount,
        apiKey: this.cfg.apiKey,
        apiSecret: this.cfg.apiSecret,
        dataUrl: this.cfg.dataUrl,
      });

      this.trPctHistory = [];
      this.prevClose = undefined;

      for (const bar of bars) {
        this.latestPrice = bar.close;
        if (bar.isClosed) {
          this.onClosedBar(bar.high, bar.low, bar.close);
          this.lastEmittedClosedTime = Math.max(this.lastEmittedClosedTime, bar.time);
        }
      }

      if (bars.length > 0) {
        this.connected = true;
        this.lastMessageAt = Date.now();
        this.log(
          `alpaca ATR feed warmed up with ${this.trPctHistory.length} closed bars (${normalizeStockSymbol(this.cfg.symbol)} ${this.cfg.interval})`,
        );
      }
    } catch (err) {
      this.log(`warning: alpaca historical bars error (${(err as Error).message}), retrying in poll loop`);
    }

    this.scheduleNextPoll();
  }

  stop(): void {
    this.stopped = true;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = undefined;
    }
  }

  private scheduleNextPoll(): void {
    if (this.stopped) return;
    const intervalSec = intervalToSeconds(this.cfg.interval);
    const delayMs = Math.max(15_000, Math.min(intervalSec * 250, 60_000));
    this.pollTimer = setTimeout(() => {
      void this.pollLatestBars();
    }, delayMs);
  }

  private async pollLatestBars(): Promise<void> {
    if (this.stopped) return;
    try {
      const bars = await fetchAlpacaBars({
        symbol: this.cfg.symbol,
        interval: this.cfg.interval,
        limit: 5,
        apiKey: this.cfg.apiKey,
        apiSecret: this.cfg.apiSecret,
        dataUrl: this.cfg.dataUrl,
      });

      if (bars.length > 0) {
        this.connected = true;
        this.lastMessageAt = Date.now();

        for (const bar of bars) {
          if (bar.isClosed && bar.time > this.lastEmittedClosedTime) {
            this.lastEmittedClosedTime = bar.time;
            this.latestPrice = bar.close;
            this.onClosedBar(bar.high, bar.low, bar.close);
            for (const listener of this.candleListeners) {
              listener(bar, true);
            }
          }
        }

        const latest = bars[bars.length - 1]!;
        this.latestPrice = latest.close;
        const key = `${latest.time}:${latest.close}:${latest.high}:${latest.low}:${latest.isClosed}`;
        if (!latest.isClosed && key !== this.lastEmittedBarKey) {
          this.lastEmittedBarKey = key;
          for (const listener of this.candleListeners) {
            listener(latest, false);
          }
        }
      }
    } catch (err) {
      this.log(`alpaca ATR poll error: ${(err as Error).message}`);
    } finally {
      this.scheduleNextPoll();
    }
  }

  private onClosedBar(high: number, low: number, close: number): void {
    const tr =
      this.prevClose === undefined
        ? high - low
        : Math.max(high - low, Math.abs(high - this.prevClose), Math.abs(low - this.prevClose));
    const trPct = tr / close;
    this.trPctHistory.push(trPct);
    if (this.trPctHistory.length > this.lookback) this.trPctHistory.shift();
    this.prevClose = close;
  }

  atrPct(): number | undefined {
    if (this.trPctHistory.length < Math.min(this.lookback, 3)) return undefined;
    const sum = this.trPctHistory.reduce((a, b) => a + b, 0);
    return sum / this.trPctHistory.length;
  }

  barsReady(): number {
    return this.trPctHistory.length;
  }

  getLatestPrice(): number | undefined {
    return this.latestPrice;
  }

  isFresh(): boolean {
    const staleMs = this.cfg.staleMs ?? 150_000;
    return this.connected && Date.now() - this.lastMessageAt < staleMs;
  }
}

export interface AlpacaBookFeedConfig {
  symbol: string;
  apiKey: string;
  apiSecret: string;
  dataUrl?: string;
}

/**
 * Polls Alpaca Data API v2 `/snapshot` for real-time Top-of-Book quotes with
 * automatic fallback to latestTrade / minuteBar outside market hours.
 */
export class AlpacaBookTickerFeed {
  private stopped = false;
  private pollTimer?: ReturnType<typeof setTimeout>;
  private latestBook?: BookTickerData;
  private listeners: ((book: BookTickerData) => void)[] = [];

  constructor(
    private readonly cfg: AlpacaBookFeedConfig,
    private readonly log: (msg: string) => void,
  ) {}

  onBook(fn: (book: BookTickerData) => void): void {
    this.listeners.push(fn);
  }

  getLatest(): BookTickerData | undefined {
    return this.latestBook;
  }

  start(): void {
    this.stopped = false;
    void this.pollOnce();
  }

  stop(): void {
    this.stopped = true;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = undefined;
    }
  }

  private async pollOnce(): Promise<void> {
    if (this.stopped) return;
    let nextDelayMs = 3_000;

    try {
      const cleanSym = normalizeStockSymbol(this.cfg.symbol);
      const dataUrl = (this.cfg.dataUrl || "https://data.alpaca.markets").replace(/\/+$/, "");
      const headers: Record<string, string> = {};
      if (this.cfg.apiKey && this.cfg.apiSecret) {
        headers["APCA-API-KEY-ID"] = this.cfg.apiKey;
        headers["APCA-API-SECRET-KEY"] = this.cfg.apiSecret;
      }

      const res = await fetch(`${dataUrl}/v2/stocks/${encodeURIComponent(cleanSym)}/snapshot?feed=iex`, { headers });
      if (res.ok) {
        const snap = (await res.json()) as Parameters<typeof parseAlpacaSnapshotToBook>[0];
        const book = parseAlpacaSnapshotToBook(snap);
        if (book) {
          // Slow polling outside active quote windows to conserve rate limits
          if (Date.now() - book.time * 1000 > 5 * 60_000) {
            nextDelayMs = 15_000;
          }
          // Strategy wsBookTicker freshness expects received timestamp in milliseconds (matching BinanceBookTickerFeed)
          const wsBook: BookTickerData = { ...book, time: Date.now() };
          this.latestBook = wsBook;
          for (const fn of this.listeners) {
            fn(wsBook);
          }
        }
      }
    } catch (err) {
      this.log(`alpaca book ticker poll error: ${(err as Error).message}`);
      nextDelayMs = 10_000;
    }

    if (!this.stopped) {
      this.pollTimer = setTimeout(() => {
        void this.pollOnce();
      }, nextDelayMs);
    }
  }
}

/**
 * Fetch latest snapshots for multiple US equity tickers in a single batched Alpaca Data API v2 call.
 */
export async function fetchAlpacaMultiSnapshots(params: {
  symbols: string[];
  apiKey: string;
  apiSecret: string;
  dataUrl?: string;
}): Promise<Map<string, BookTickerData>> {
  const result = new Map<string, BookTickerData>();
  const cleanSyms = Array.from(new Set(params.symbols.map(normalizeStockSymbol).filter(Boolean)));
  if (cleanSyms.length === 0) return result;

  const dataUrl = (params.dataUrl || "https://data.alpaca.markets").replace(/\/+$/, "");
  const headers: Record<string, string> = {};
  if (params.apiKey && params.apiSecret) {
    headers["APCA-API-KEY-ID"] = params.apiKey;
    headers["APCA-API-SECRET-KEY"] = params.apiSecret;
  }

  for (let i = 0; i < cleanSyms.length; i += 50) {
    const chunk = cleanSyms.slice(i, i + 50);
    const url = `${dataUrl}/v2/stocks/snapshots?symbols=${encodeURIComponent(chunk.join(","))}&feed=iex`;
    const res = await fetch(url, { headers });
    if (!res.ok) continue;
    const payload = (await res.json()) as Record<string, Parameters<typeof parseAlpacaSnapshotToBook>[0]>;
    if (!payload || typeof payload !== "object") continue;
    for (const [sym, snap] of Object.entries(payload)) {
      const book = parseAlpacaSnapshotToBook(snap);
      if (book) {
        result.set(normalizeStockSymbol(sym), { ...book, time: Date.now() });
      }
    }
  }
  return result;
}

/**
 * Fetch recent OHLCV bars for multiple US equity tickers using batched Alpaca Data API v2 requests.
 */
export async function fetchAlpacaMultiBars(params: {
  symbols: string[];
  interval: string;
  limit?: number;
  apiKey: string;
  apiSecret: string;
  dataUrl?: string;
}): Promise<Map<string, Candle[]>> {
  const out = new Map<string, Candle[]>();
  const cleanSyms = Array.from(new Set(params.symbols.map(normalizeStockSymbol).filter(Boolean)));
  if (cleanSyms.length === 0) return out;

  const { timeframe, aggregateHours } = mapIntervalToAlpacaTimeframe(params.interval);
  const perSymLimit = Math.max(10, Math.min(params.limit ?? 280, 1000));
  const intervalSec = intervalToSeconds(params.interval);
  const nowSec = Math.floor(Date.now() / 1000);
  const daysBack = estimateDaysBack(params.interval, perSymLimit * aggregateHours);
  const startIso = new Date(Date.now() - daysBack * 86_400_000).toISOString().split(".")[0] + "Z";

  const dataUrl = (params.dataUrl || "https://data.alpaca.markets").replace(/\/+$/, "");
  const headers: Record<string, string> = {};
  if (params.apiKey && params.apiSecret) {
    headers["APCA-API-KEY-ID"] = params.apiKey;
    headers["APCA-API-SECRET-KEY"] = params.apiSecret;
  }

  // Batch 8 symbols at a time so total bar limit (8 * 300 = 2400) fits cleanly within Alpaca 10,000 multi-bar cap
  const batchSize = 8;
  for (let i = 0; i < cleanSyms.length; i += batchSize) {
    const chunk = cleanSyms.slice(i, i + batchSize);
    const totalLimit = Math.min(10_000, chunk.length * perSymLimit * aggregateHours + 200);
    const rawBySym = new Map<string, Array<{ t: string; o: number; h: number; l: number; c: number; v?: number }>>();
    let pageToken: string | undefined;
    let attempts = 0;

    do {
      let url = `${dataUrl}/v2/stocks/bars?symbols=${encodeURIComponent(chunk.join(","))}&timeframe=${timeframe}&start=${encodeURIComponent(startIso)}&limit=${totalLimit}&sort=desc&adjustment=all&feed=iex`;
      if (pageToken) url += `&page_token=${encodeURIComponent(pageToken)}`;
      const res = await fetch(url, { headers });
      if (!res.ok) break;
      const data = (await res.json()) as {
        bars?: Record<string, Array<{ t: string; o: number; h: number; l: number; c: number; v?: number }>>;
        next_page_token?: string | null;
      };
      if (data?.bars && typeof data.bars === "object") {
        for (const [sym, arr] of Object.entries(data.bars)) {
          if (!Array.isArray(arr)) continue;
          const prev = rawBySym.get(sym) || [];
          prev.push(...arr);
          rawBySym.set(sym, prev);
        }
      }
      pageToken = data?.next_page_token || undefined;
      attempts++;
    } while (pageToken && attempts < 6);

    for (const [sym, rawBars] of rawBySym.entries()) {
      let candles: Candle[] = rawBars
        .slice()
        .sort((a, b) => Date.parse(a.t) - Date.parse(b.t))
        .map((b) => {
          const tsSec = Math.floor(Date.parse(b.t) / 1000);
          return {
            time: tsSec,
            open: Number(b.o),
            high: Number(b.h),
            low: Number(b.l),
            close: Number(b.c),
            volume: Number(b.v ?? 0),
            isClosed: true,
          };
        })
        .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close) && c.close > 0);

      if (aggregateHours > 1) candles = aggregateFourHourBars(candles);
      if (candles.length > 0) {
        const last = candles[candles.length - 1]!;
        if (last.time + intervalSec > nowSec) last.isClosed = false;
      }
      if (candles.length > perSymLimit) candles = candles.slice(-perSymLimit);
      out.set(normalizeStockSymbol(sym), candles);
    }
  }
  return out;
}
