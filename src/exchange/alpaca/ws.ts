/**
 * Alpaca US Equities Real-Time Market Data WebSocket Stream (IEX v2)
 *
 * Basic/Free plan enforces two strict limits on `wss://stream.data.alpaca.markets/v2/iex`:
 *  1. Maximum 30 unique symbols subscribed concurrently across the connection.
 *  2. Maximum 1 active WebSocket connection per API key.
 *
 * This client handles authentication, heartbeat pings, incremental diff-based
 * subscribe/unsubscribe (strictly capped at 30 priority-ordered symbols),
 * two-sided quote vs trade price normalization, and clean shutdown / backoff.
 */

import WebSocket from "ws";
import { normalizeStockSymbol } from "./feed.js";

export const ALPACA_WS_MAX_SYMBOLS = 30;

export interface AlpacaPortfolioWsConfig {
  apiKey: string;
  apiSecret: string;
  dataUrl?: string;
  wsUrl?: string;
  onPriceUpdate: (symbol: string, price: number, timestampMs: number) => void;
  onStatusChange?: (connected: boolean) => void;
  log?: (msg: string) => void;
}

interface QuoteBookState {
  bid: number;
  ask: number;
  bidSize: number;
  askSize: number;
  lastTrade: number;
}

export class AlpacaPortfolioWsClient {
  private ws?: WebSocket;
  private stopped = false;
  private connected = false;
  private authenticated = false;
  private reconnectDelayMs = 2_000;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private pingTimer?: ReturnType<typeof setInterval>;

  private desiredSymbols: string[] = [];
  private subscribedSymbols = new Set<string>();
  private lastPriceBySymbol = new Map<string, number>();
  private lastTickTsBySymbol = new Map<string, number>();
  private quoteStateBySymbol = new Map<string, QuoteBookState>();
  private lastMessageAt = 0;

  private readonly log: (msg: string) => void;

  constructor(private readonly cfg: AlpacaPortfolioWsConfig) {
    this.log = cfg.log ?? ((msg) => console.log(`[alpaca-ws] ${msg}`));
  }

  private resolveWsUrl(): string {
    if (this.cfg.wsUrl) return this.cfg.wsUrl;
    const dataUrl = (this.cfg.dataUrl || "").toLowerCase();
    if (dataUrl.includes("sandbox")) {
      return "wss://stream.data.sandbox.alpaca.markets/v2/iex";
    }
    return "wss://stream.data.alpaca.markets/v2/iex";
  }

  public isConnected(): boolean {
    return this.connected && this.authenticated;
  }

  public getLastMessageAt(): number {
    return this.lastMessageAt;
  }

  public getSymbolLastTickAt(symbol: string): number {
    return this.lastTickTsBySymbol.get(normalizeStockSymbol(symbol)) || 0;
  }

  public getSubscribedSymbols(): string[] {
    return Array.from(this.subscribedSymbols);
  }

  public start(): void {
    if (!this.cfg.apiKey || !this.cfg.apiSecret) {
      this.log("⚠️ Alpaca API credentials missing — skipping WebSocket stream.");
      return;
    }
    this.stopped = false;
    this.connect();
  }

  public stop(): void {
    this.stopped = true;
    this.connected = false;
    this.authenticated = false;
    this.subscribedSymbols.clear();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = undefined;
    }
    if (this.ws) {
      try {
        if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) {
          this.ws.close(1000, "shutdown");
        }
      } catch {
        // ignore close error
      }
      this.ws = undefined;
    }
    this.cfg.onStatusChange?.(false);
  }

  /**
   * Update priority-ordered symbol list (strictly capped at 30 unique tickers).
   * Diffs against currently subscribed tickers, sending unsubscribes before subscribes.
   */
  public setSymbols(symbols: string[]): void {
    const cleanOrdered: string[] = [];
    const seen = new Set<string>();
    for (const raw of symbols) {
      const sym = normalizeStockSymbol(raw);
      if (!sym || seen.has(sym)) continue;
      seen.add(sym);
      cleanOrdered.push(sym);
      if (cleanOrdered.length >= ALPACA_WS_MAX_SYMBOLS) break;
    }
    this.desiredSymbols = cleanOrdered;
    if (this.authenticated && this.ws?.readyState === WebSocket.OPEN) {
      this.syncSubscriptions();
    }
  }

  private connect(): void {
    if (this.stopped) return;
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const url = this.resolveWsUrl();
    const ws = new WebSocket(url);
    this.ws = ws;
    this.connected = false;
    this.authenticated = false;
    this.subscribedSymbols.clear();

    ws.on("open", () => {
      if (this.stopped || this.ws !== ws) return;
      this.connected = true;
      this.startPing();
    });

    ws.on("message", (rawData) => {
      if (this.stopped || this.ws !== ws) return;
      this.lastMessageAt = Date.now();
      let messages: any[];
      try {
        const parsed = JSON.parse(rawData.toString());
        messages = Array.isArray(parsed) ? parsed : [parsed];
      } catch {
        return;
      }

      for (const msg of messages) {
        this.handleMessage(ws, msg);
      }
    });

    ws.on("error", (err) => {
      if (this.stopped || this.ws !== ws) return;
      this.log(`⚠️ WebSocket error: ${err.message}`);
    });

    ws.on("close", (code, reason) => {
      if (this.ws !== ws) return;
      this.connected = false;
      this.authenticated = false;
      this.subscribedSymbols.clear();
      this.stopPing();
      this.cfg.onStatusChange?.(false);

      if (!this.stopped) {
        const reasonStr = reason?.toString() || "";
        this.log(`🔌 Alpaca WS closed (code=${code}${reasonStr ? `, ${reasonStr}` : ""}) — reconnecting in ${(this.reconnectDelayMs / 1000).toFixed(0)}s`);
        this.scheduleReconnect();
      }
    });
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        try {
          this.ws.ping();
        } catch {
          // ignore ping error
        }
      }
    }, 25_000);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = undefined;
    }
  }

  private scheduleReconnect(overrideDelayMs?: number): void {
    if (this.stopped || this.reconnectTimer) return;
    const delay = overrideDelayMs ?? this.reconnectDelayMs;
    if (!overrideDelayMs) {
      this.reconnectDelayMs = Math.min(30_000, this.reconnectDelayMs * 2);
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delay);
  }

  private handleMessage(ws: WebSocket, msg: any): void {
    if (!msg || typeof msg !== "object") return;
    const type = msg.T;

    if (type === "success") {
      if (msg.msg === "connected") {
        ws.send(
          JSON.stringify({
            action: "auth",
            key: this.cfg.apiKey,
            secret: this.cfg.apiSecret,
          }),
        );
      } else if (msg.msg === "authenticated") {
        this.authenticated = true;
        this.reconnectDelayMs = 2_000;
        this.cfg.onStatusChange?.(true);
        this.log(`🟢 Alpaca IEX WebSocket authenticated (streaming up to ${ALPACA_WS_MAX_SYMBOLS} symbols)`);
        this.syncSubscriptions();
      }
      return;
    }

    if (type === "error") {
      const code = Number(msg.code || 0);
      const errMsg = String(msg.msg || "unknown error");
      this.log(`⚠️ Alpaca WS error (${code}): ${errMsg}`);
      if (code === 406) {
        // Connection limit exceeded (usually an orphaned session clearing within 60-90s)
        this.reconnectDelayMs = 75_000;
        try {
          ws.close();
        } catch {}
      } else if (code === 401 || code === 402 || code === 403) {
        this.reconnectDelayMs = 120_000;
      }
      return;
    }

    if (type === "subscription") {
      const active = new Set<string>();
      for (const s of [...(msg.trades || []), ...(msg.quotes || [])]) {
        const clean = normalizeStockSymbol(String(s));
        if (clean) active.add(clean);
      }
      this.subscribedSymbols = active;
      return;
    }

    if (type === "t") {
      const sym = normalizeStockSymbol(String(msg.S || ""));
      const price = Number(msg.p);
      if (!sym || !Number.isFinite(price) || price <= 0) return;

      const state = this.getOrCreateQuoteState(sym);
      state.lastTrade = price;
      const tsMs = msg.t ? Date.parse(msg.t) : Date.now();
      this.emitPrice(sym, price, Number.isFinite(tsMs) ? tsMs : Date.now());
      return;
    }

    if (type === "q") {
      const sym = normalizeStockSymbol(String(msg.S || ""));
      const bp = Number(msg.bp || 0);
      const ap = Number(msg.ap || 0);
      const bs = Number(msg.bs || 0);
      const as = Number(msg.as || 0);
      if (!sym) return;

      const state = this.getOrCreateQuoteState(sym);
      state.bid = bp;
      state.ask = ap;
      state.bidSize = bs;
      state.askSize = as;

      // Reject one-sided or abnormally wide IEX stub quotes (>5% spread)
      if (bp > 0 && ap > 0 && bp <= ap && bs > 0 && as > 0) {
        const mid = (bp + ap) / 2;
        if ((ap - bp) / mid <= 0.05) {
          const tsMs = msg.t ? Date.parse(msg.t) : Date.now();
          this.emitPrice(sym, mid, Number.isFinite(tsMs) ? tsMs : Date.now());
        }
      }
    }
  }

  private getOrCreateQuoteState(symbol: string): QuoteBookState {
    let st = this.quoteStateBySymbol.get(symbol);
    if (!st) {
      st = { bid: 0, ask: 0, bidSize: 0, askSize: 0, lastTrade: 0 };
      this.quoteStateBySymbol.set(symbol, st);
    }
    return st;
  }

  private emitPrice(symbol: string, price: number, tsMs: number): void {
    this.lastTickTsBySymbol.set(symbol, tsMs);
    const prev = this.lastPriceBySymbol.get(symbol);
    this.lastPriceBySymbol.set(symbol, price);
    if (prev !== price) {
      this.cfg.onPriceUpdate(symbol, price, tsMs);
    }
  }

  private syncSubscriptions(): void {
    if (!this.authenticated || this.ws?.readyState !== WebSocket.OPEN) return;

    const desiredSet = new Set(this.desiredSymbols);
    const toRemove = Array.from(this.subscribedSymbols).filter((s) => !desiredSet.has(s));

    if (toRemove.length > 0) {
      this.ws.send(
        JSON.stringify({
          action: "unsubscribe",
          trades: toRemove,
          quotes: toRemove,
        }),
      );
      for (const s of toRemove) {
        this.subscribedSymbols.delete(s);
      }
    }

    const remainingCapacity = Math.max(0, ALPACA_WS_MAX_SYMBOLS - this.subscribedSymbols.size);
    const toAdd = this.desiredSymbols.filter((s) => !this.subscribedSymbols.has(s)).slice(0, remainingCapacity);

    if (toAdd.length > 0) {
      this.ws.send(
        JSON.stringify({
          action: "subscribe",
          trades: toAdd,
          quotes: toAdd,
        }),
      );
      for (const s of toAdd) {
        this.subscribedSymbols.add(s);
      }
    }
  }
}
